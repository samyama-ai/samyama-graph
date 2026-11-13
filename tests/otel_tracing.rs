//! Each request is a trace, with a span per stage, continuing the caller's
//! trace when it sends one (#2077).
//!
//! Most tests install the export layer (`samyama::otel::layer`) over an
//! in-memory exporter, so what is checked is the layer the server installs,
//! and the spans are read back as OpenTelemetry would send them. The last one
//! sends over real OTLP/HTTP to a collector served by the test.
#![cfg(feature = "otel")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry::Value;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::persistence::PersistenceManager;
use samyama::{RespServer, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

const CALLER_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const CALLER_SPAN: &str = "00f067aa0ba902b7";

#[derive(Clone, Default, Debug)]
struct Collected(Arc<Mutex<Vec<SpanData>>>);

impl SpanExporter for Collected {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

impl Collected {
    fn all(&self) -> Vec<SpanData> {
        self.0.lock().unwrap().clone()
    }
    fn named(&self, name: &str) -> Vec<SpanData> {
        self.all().into_iter().filter(|s| s.name == name).collect()
    }
    fn one(&self, name: &str) -> SpanData {
        let found = self.named(name);
        assert_eq!(found.len(), 1, "{name}: {:?}", self.all().iter().map(|s| &s.name).collect::<Vec<_>>());
        found.into_iter().next().unwrap()
    }
}

fn attr(span: &SpanData, key: &str) -> Option<Value> {
    span.attributes.iter().find(|kv| kv.key.as_str() == key).map(|kv| kv.value.clone())
}

/// The server's export layer over an in-memory exporter, installed for this
/// thread. `#[tokio::test]` is single-threaded, so the server's tasks run here.
fn traced() -> (Collected, tracing::subscriber::DefaultGuard) {
    let out = Collected::default();
    let provider = SdkTracerProvider::builder().with_simple_exporter(out.clone()).build();
    let sub = tracing_subscriber::registry().with(samyama::otel::layer(&provider));
    (out, tracing::subscriber::set_default(sub))
}

fn query(q: &str, traceparent: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json");
    if let Some(tp) = traceparent {
        b = b.header("traceparent", tp);
    }
    b.body(Body::from(serde_json::json!({ "query": q }).to_string())).unwrap()
}

fn app() -> axum::Router {
    HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router()
}

// ── HTTP ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_read_is_traced_stage_by_stage_under_the_callers_trace() {
    let (out, _sub) = traced();
    let tp = format!("00-{CALLER_TRACE}-{CALLER_SPAN}-01");
    let res = app().oneshot(query("MATCH (n) RETURN count(n)", Some(&tp))).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let request_id = res.headers()["x-request-id"].to_str().unwrap().to_string();

    let request = out.one("request");
    assert_eq!(request.span_context.trace_id(), TraceId::from_hex(CALLER_TRACE).unwrap());
    assert_eq!(request.parent_span_id, SpanId::from_hex(CALLER_SPAN).unwrap());
    assert!(request.parent_span_is_remote, "the parent is the caller's span");
    assert_eq!(attr(&request, "request_id"), Some(Value::from(request_id)), "one id in logs and traces");
    assert_eq!(attr(&request, "path"), Some(Value::from("/api/query")));

    let me = request.span_context.span_id();
    let lock = out.one("samyama.lock.acquire");
    assert_eq!(lock.parent_span_id, me);
    assert_eq!(attr(&lock, "mode"), Some(Value::from("read")));
    // Parsed once to classify the statement and once to run it; the second is
    // served from the parse cache, and the span says so.
    let parses = out.named("samyama.query.parse");
    assert!(!parses.is_empty() && parses.iter().all(|p| p.parent_span_id == me));
    assert!(parses.iter().any(|p| attr(p, "cache_hit") == Some(Value::from(true))));
    let execute = out.one("samyama.query.execute");
    assert_eq!(execute.parent_span_id, me);
    let plan = out.one("samyama.query.plan");
    assert_eq!(plan.parent_span_id, execute.span_context.span_id(), "planning is part of executing");

    for s in out.all() {
        assert_eq!(s.span_context.trace_id(), request.span_context.trace_id(), "{} left the trace", s.name);
    }
}

#[tokio::test]
async fn without_a_valid_traceparent_each_request_starts_its_own_trace() {
    let (out, _sub) = traced();
    let app = app();
    for tp in [None, Some("not-a-traceparent"), Some("00-00000000000000000000000000000000-00f067aa0ba902b7-01")] {
        let res = app.clone().oneshot(query("RETURN 1", tp)).await.unwrap();
        // A bad header is ignored, not refused.
        assert_eq!(res.status(), StatusCode::OK, "{tp:?}");
    }
    let requests = out.named("request");
    assert_eq!(requests.len(), 3);
    for r in &requests {
        assert_eq!(r.parent_span_id, SpanId::INVALID, "a root span");
    }
    assert_ne!(requests[0].span_context.trace_id(), requests[1].span_context.trace_id());
    assert_ne!(requests[1].span_context.trace_id(), requests[2].span_context.trace_id());
}

#[tokio::test]
async fn a_write_traces_the_writers_lock_and_the_disk_write() {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(PersistenceManager::new(dir.path()).unwrap());
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0)
        .with_persistence(pm)
        .router();
    let (out, _sub) = traced();
    let res = app.oneshot(query("CREATE (:Traced {n: 1})", None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let me = out.one("request").span_context.span_id();
    let lock = out.one("samyama.lock.acquire");
    assert_eq!(attr(&lock, "mode"), Some(Value::from("write")));
    assert_eq!(lock.parent_span_id, me);
    let persist = out.one("samyama.storage.persist");
    assert_eq!(persist.parent_span_id, me);
    assert!(matches!(attr(&persist, "mutations"), Some(Value::I64(n)) if n >= 1), "{persist:?}");
}

// ── RESP ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_resp_command_is_a_trace_of_its_own() {
    let (out, _sub) = traced();
    let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
    let server = RespServer::new(
        ServerConfig { port, ..ServerConfig::default() },
        Arc::new(RwLock::new(GraphStore::new())),
    );
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    let mut s = loop {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let q = "MATCH (n) RETURN count(n)";
    let cmd = format!("*3\r\n$11\r\nGRAPH.QUERY\r\n$7\r\ndefault\r\n${}\r\n{q}\r\n", q.len());
    s.write_all(cmd.as_bytes()).await.unwrap();
    let mut buf = [0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(10), s.read(&mut buf)).await.unwrap().unwrap();
    assert!(!buf[..n].starts_with(b"-"), "{}", String::from_utf8_lossy(&buf[..n]));

    let request = out.one("request");
    assert_eq!(attr(&request, "protocol"), Some(Value::from("resp")));
    assert_eq!(request.parent_span_id, SpanId::INVALID);
    let me = request.span_context.span_id();
    assert_eq!(out.one("samyama.lock.acquire").parent_span_id, me);
    assert_eq!(out.one("samyama.query.execute").parent_span_id, me);
}

// ── Over the wire ───────────────────────────────────────────────────────────

/// What `otlp_provider` sends reaches a collector as OTLP/HTTP protobuf on the
/// traces path, named as our service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spans_reach_an_otlp_collector() {
    let received: Arc<Mutex<Vec<(String, String, Vec<u8>)>>> = Arc::default();
    let sink = Arc::clone(&received);
    let collector = axum::Router::new().fallback(move |req: Request<Body>| {
        let sink = Arc::clone(&sink);
        async move {
            let path = req.uri().path().to_string();
            let ctype = req
                .headers()
                .get("content-type")
                .map(|v| v.to_str().unwrap().to_string())
                .unwrap_or_default();
            let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap().to_vec();
            sink.lock().unwrap().push((path, ctype, body));
            StatusCode::OK
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, collector).await.unwrap() });

    let endpoint = format!("http://{addr}/v1/traces");
    // Off the runtime's threads, as the server does it: the blocking client
    // must not be built or dropped on one.
    tokio::task::spawn_blocking(move || {
        let provider = samyama::otel::otlp_provider(&endpoint).unwrap();
        let sub = tracing_subscriber::registry().with(samyama::otel::layer(&provider));
        tracing::subscriber::with_default(sub, || {
            tracing::info_span!(target: "samyama::http::server", "request", request_id = "wire-1").in_scope(|| {});
        });
        provider.shutdown().unwrap();
    })
    .await
    .unwrap();

    let got = received.lock().unwrap().clone();
    assert!(!got.is_empty(), "the collector received nothing");
    let (path, ctype, body) = &got[0];
    assert_eq!(path, "/v1/traces");
    assert_eq!(ctype, "application/x-protobuf");
    let has = |needle: &[u8]| body.windows(needle.len()).any(|w| w == needle);
    assert!(has(b"samyama"), "service name not in the export");
    assert!(has(b"wire-1"), "span attribute not in the export");
}

// ── Off ─────────────────────────────────────────────────────────────────────

/// Without export the server installs only its log subscriber, which stops at
/// `info`: the stage spans are `debug`, so they are disabled at the call site
/// and never created. This is what keeps tracing off the hot path when it is
/// not asked for, and why the logs do not change.
#[test]
fn without_export_the_stage_spans_are_never_created() {
    let _sub = tracing::subscriber::set_default(samyama::logging::json_subscriber(std::io::sink));
    assert!(tracing::debug_span!(target: "samyama::query", "samyama.query.parse").is_disabled());
    assert!(!tracing::info_span!(target: "samyama::logging", "request").is_disabled(), "the request span stays");
}
