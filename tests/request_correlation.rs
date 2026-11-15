//! Every request carries an id, and every log line it causes carries the same
//! one (#2076).
//!
//! The JSON logs (#2088) made a slow-query warning machine-readable but not
//! attributable: on a busy server it was one of many interleaved lines with
//! nothing saying which request caused it. Each HTTP request and each RESP
//! command now runs in a `request` span with an id; HTTP returns the id in
//! `X-Request-Id`, and keeps the caller's own when it sends a plausible one.
//!
//! The log assertions install the server's own JSON subscriber
//! (`json_subscriber`) over a buffer, so what is checked is the configuration
//! the binary runs. `#[tokio::test]` is single-threaded, so the server's tasks
//! run on the thread the subscriber is installed on.

use std::io::Write;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::{read_credentials, HttpServer};
use samyama::logging::{accept_request_id, json_subscriber, loggable_name, MAX_REQUEST_ID_LEN};
use samyama::{RespServer, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tower::ServiceExt;

/// Slow at 1 ms, so the query below is always logged as slow. Set once, before
/// any engine is built: `QueryEngine::new` reads it.
fn slow_at_one_ms() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| std::env::set_var("SLOW_QUERY_MS", "1"));
}

const SLOW: &str = "UNWIND range(1, 300000) AS x RETURN sum(x) AS s";

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
    type Writer = Buffer;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Buffer {
    fn slow_query_lines(&self) -> Vec<serde_json::Value> {
        let text = String::from_utf8(self.0.lock().unwrap().clone()).unwrap();
        text.lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}")))
            .filter(|l| l["fields"]["message"] == "slow query")
            .collect()
    }
}

fn app() -> axum::Router {
    slow_at_one_ms();
    HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router()
}

fn query(q: &str, request_id: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json");
    if let Some(id) = request_id {
        b = b.header("x-request-id", id);
    }
    b.body(Body::from(serde_json::json!({ "query": q }).to_string())).unwrap()
}

fn id_of(res: &axum::response::Response) -> String {
    res.headers()
        .get("x-request-id")
        .unwrap_or_else(|| panic!("no X-Request-Id on the response: {:?}", res.headers()))
        .to_str()
        .unwrap()
        .to_string()
}

fn is_made_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

// ── HTTP ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn every_response_carries_a_request_id_and_no_two_share_one() {
    let app = app();
    let a = app.clone().oneshot(query("RETURN 1", None)).await.unwrap();
    let b = app.clone().oneshot(query("RETURN 1", None)).await.unwrap();
    assert_eq!(a.status(), StatusCode::OK);
    let (a, b) = (id_of(&a), id_of(&b));
    assert!(is_made_id(&a) && is_made_id(&b), "{a} / {b}");
    assert_ne!(a, b);

    // Not only the query route: the layer is outside the router.
    let health = app
        .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert!(is_made_id(&id_of(&health)));
}

#[tokio::test]
async fn the_callers_id_is_kept_and_a_bad_one_is_replaced() {
    let app = app();
    let kept = app.clone().oneshot(query("RETURN 1", Some("trace-42:a.b_c"))).await.unwrap();
    assert_eq!(id_of(&kept), "trace-42:a.b_c");

    let long = "a".repeat(MAX_REQUEST_ID_LEN + 1);
    for bad in ["has space", "quote\"d", long.as_str(), ""] {
        let res = app.clone().oneshot(query("RETURN 1", Some(bad))).await.unwrap();
        // Replaced, not refused: a bad correlation header is no reason to fail a query.
        assert_eq!(res.status(), StatusCode::OK, "{bad:?}");
        assert!(is_made_id(&id_of(&res)), "{bad:?} was not replaced");
    }
}

#[tokio::test]
async fn a_request_refused_for_its_credential_still_has_an_id() {
    let path = std::env::temp_dir().join(format!("samyama-reqid-creds-{}", std::process::id()));
    std::fs::write(&path, format!("ops:{}\n", "0".repeat(64))).unwrap();
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0)
        .with_credentials(read_credentials(&path).unwrap())
        .router();
    let res = app.oneshot(query("RETURN 1", None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert!(is_made_id(&id_of(&res)));
}

#[tokio::test]
async fn a_slow_query_is_logged_under_the_id_the_caller_got_back() {
    let out = Buffer::default();
    let _sub = tracing::subscriber::set_default(json_subscriber(out.clone()));
    let res = app().oneshot(query(SLOW, None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let id = id_of(&res);

    let lines = out.slow_query_lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    let span = &lines[0]["span"];
    assert_eq!(span["name"], "request", "{span}");
    assert_eq!(span["request_id"], id.as_str(), "{span}");
    assert_eq!(span["protocol"], "http");
    assert_eq!(span["method"], "POST");
    assert_eq!(span["path"], "/api/query");
    assert_eq!(span["tenant"], "default", "the graph from the body is on the span");
}

#[tokio::test]
async fn a_streamed_query_keeps_its_id_across_the_blocking_thread() {
    let out = Buffer::default();
    let _sub = tracing::subscriber::set_default(json_subscriber(out.clone()));
    let mut req = query(SLOW, Some("stream-1"));
    req.headers_mut().insert("accept", samyama::http::handler::NDJSON.parse().unwrap());
    let res = app().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(id_of(&res), "stream-1");
    // Drain the stream so the query finishes and its event is written.
    let _ = res.into_body().collect().await.unwrap();

    let lines = out.slow_query_lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["span"]["request_id"], "stream-1", "{}", lines[0]);
}

// ── RESP ────────────────────────────────────────────────────────────────────

async fn resp_server() -> u16 {
    slow_at_one_ms();
    let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
    let server = RespServer::new(
        ServerConfig { port, ..ServerConfig::default() },
        Arc::new(RwLock::new(GraphStore::new())),
    );
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return port;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the RESP server did not start");
}

async fn send(s: &mut TcpStream, args: &[&str]) -> String {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{a}\r\n", a.len()));
    }
    s.write_all(out.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 65536];
    let n = tokio::time::timeout(Duration::from_secs(10), s.read(&mut buf))
        .await
        .expect("no reply in 10s")
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[tokio::test]
async fn each_resp_command_is_logged_under_its_own_id() {
    let out = Buffer::default();
    let _sub = tracing::subscriber::set_default(json_subscriber(out.clone()));
    let port = resp_server().await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    for _ in 0..2 {
        let reply = send(&mut s, &["GRAPH.QUERY", "default", SLOW]).await;
        assert!(!reply.starts_with('-'), "{reply}");
    }

    let lines = out.slow_query_lines();
    assert_eq!(lines.len(), 2, "{lines:?}");
    let ids: Vec<&str> = lines.iter().map(|l| l["span"]["request_id"].as_str().unwrap()).collect();
    assert!(ids.iter().all(|id| is_made_id(id)), "{ids:?}");
    assert_ne!(ids[0], ids[1], "two commands on one connection are two requests");
    for l in &lines {
        let span = &l["span"];
        assert_eq!(span["protocol"], "resp", "{span}");
        assert_eq!(span["command"], "GRAPH.QUERY");
        assert_eq!(span["tenant"], "default");
    }
}

// ── What may be written into a log line ─────────────────────────────────────

#[test]
fn only_plausible_ids_and_names_reach_the_log() {
    assert_eq!(accept_request_id("abc-123_x.y:z"), Some("abc-123_x.y:z"));
    assert_eq!(accept_request_id(&"a".repeat(MAX_REQUEST_ID_LEN)).map(str::len), Some(MAX_REQUEST_ID_LEN));
    for bad in ["", "two words", "line\nbreak", "tab\t", "é", "{\"x\":1}"] {
        assert_eq!(accept_request_id(bad), None, "{bad:?}");
    }
    assert_eq!(accept_request_id(&"a".repeat(MAX_REQUEST_ID_LEN + 1)), None);

    // A graph name is recorded before anything validates it; one that could
    // forge a text log line is replaced, not written.
    assert_eq!(loggable_name("default"), "default");
    assert_eq!(loggable_name("x\nINFO forged"), "(unloggable)");
}
