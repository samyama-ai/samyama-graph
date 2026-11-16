//! `/api/slowlog` returns the queries the slow-query log line named (#2079).
//!
//! One test in its own binary: the ring is process-wide, and a second test
//! recording or resetting beside it would move what this one reads.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use samyama::graph::GraphStore;
use samyama::http::server::{read_credentials, HttpServer};
use samyama::query::{slowlog, QueryEngine};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

fn app(credentials_file_body: Option<&str>) -> axum::Router {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let server = HttpServer::new(store, 0);
    match credentials_file_body {
        None => server.router(),
        Some(body) => {
            let path = std::env::temp_dir()
                .join(format!("samyama-slowlog-creds-{}.txt", std::process::id()));
            std::fs::write(&path, body).unwrap();
            server.with_credentials(read_credentials(&path).unwrap()).router()
        }
    }
}

fn request(method: &str, path: &str, bearer: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(path);
    if let Some(t) = bearer {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Body::empty()).unwrap()
}

async fn json(app: axum::Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}

fn digest(token: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(token.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn slow_queries_are_kept_served_and_cleared() {
    slowlog::reset();

    // Through the engine: a query past the threshold is kept, a fast one is not.
    let engine = QueryEngine::new().with_slow_query_ms(1);
    let slow = "UNWIND range(1, 300000) AS x RETURN sum(x) AS s";
    engine.execute(slow, &GraphStore::new()).unwrap();
    let off = QueryEngine::new().with_slow_query_ms(0);
    off.execute("UNWIND range(1, 300000) AS x RETURN count(x) AS c", &GraphStore::new())
        .unwrap();
    let got = slowlog::recent(10);
    assert_eq!(got.len(), 1, "threshold 0 records nothing: {got:?}");
    let first = &got[0];
    assert_eq!(first.query, slow);
    assert_eq!((first.rows, first.failed, first.threshold_ms), (1, false, 1));
    assert!(first.elapsed_ms >= 1 && first.timestamp_ms > 0, "{first:?}");

    // The ring keeps the newest `max_len`, newest first, ids never reused.
    let cap = slowlog::max_len();
    for i in 0..cap + 5 {
        slowlog::record(&format!("q{i}"), 10, 1, 0, i % 2 == 0);
    }
    assert_eq!(slowlog::len(), cap);
    let all = slowlog::recent(cap);
    assert_eq!(all[0].query, format!("q{}", cap + 4), "newest first");
    assert!(all.iter().all(|e| e.query != slow && e.query != "q0"), "oldest dropped");
    assert!(all.windows(2).all(|w| w[0].id > w[1].id));
    assert_eq!(slowlog::recent(3).len(), 3);
    let long = "x".repeat(2000);
    slowlog::record(&long, 10, 1, 0, false);
    assert_eq!(slowlog::recent(1)[0].query.chars().count(), 512, "query text is cut");

    // Over HTTP, open server.
    let (st, body) = json(app(None), request("GET", "/api/slowlog?limit=2", None)).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["len"], cap);
    assert_eq!(body["max_len"], cap);
    assert_eq!(body["entries"].as_array().unwrap().len(), 2);
    assert_eq!(body["entries"][1]["query"], format!("q{}", cap + 4));
    assert!(body["entries"][0]["failed"].is_boolean());

    let last_id = slowlog::recent(1)[0].id;
    let (st, body) = json(app(None), request("DELETE", "/api/slowlog", None)).await;
    assert_eq!((st, body["cleared"].as_u64()), (StatusCode::OK, Some(cap as u64)));
    let (_, body) = json(app(None), request("GET", "/api/slowlog", None)).await;
    assert_eq!(body["len"], 0);
    slowlog::record("after reset", 10, 1, 0, false);
    assert!(slowlog::recent(1)[0].id > last_id, "a reset does not reuse ids");

    // With credentials: query text from every tenant is an admin's view.
    let creds = format!(
        "reader:{}:roles=read\nops:{}:roles=admin\n",
        digest("read-token"),
        digest("admin-token")
    );
    for method in ["GET", "DELETE"] {
        let (st, _) = json(app(Some(&creds)), request(method, "/api/slowlog", Some("read-token"))).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{method} with a read credential");
    }
    let (st, _) = json(app(Some(&creds)), request("GET", "/api/slowlog", Some("admin-token"))).await;
    assert_eq!(st, StatusCode::OK);

    assert_eq!(slowlog::max_len_from(Some("16")), 16);
    assert_eq!(slowlog::max_len_from(Some("0")), slowlog::DEFAULT_MAX_LEN);
    assert_eq!(slowlog::max_len_from(Some("lots")), slowlog::DEFAULT_MAX_LEN);
    assert_eq!(slowlog::max_len_from(None), slowlog::DEFAULT_MAX_LEN);
}
