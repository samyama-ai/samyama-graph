//! The tenant's `max_connections` over HTTP (#1594).
//!
//! The quota existed, defaulted to 100, and nothing counted against it. Over
//! HTTP a "connection" is a request in flight: it holds a slot from the moment
//! it is admitted until its response is finished, streamed bodies included,
//! and it shares the count with RESP connections. The RESP side is tested in
//! `src/protocol/server.rs`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::persistence::{ResourceQuotas, TenantManager};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

fn limited(max: usize) -> (axum::Router, Arc<TenantManager>) {
    let tm = Arc::new(TenantManager::new());
    tm.update_quotas("default", ResourceQuotas { max_connections: Some(max), ..ResourceQuotas::unlimited() })
        .unwrap();
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store, 0).with_tenant_manager(Arc::clone(&tm)).router();
    (app, tm)
}

fn query(q: &str, accept: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json");
    if let Some(a) = accept {
        b = b.header("accept", a);
    }
    b.body(Body::from(json!({ "query": q }).to_string())).unwrap()
}

fn active(tm: &TenantManager) -> usize {
    tm.get_usage("default").unwrap().active_connections
}

#[tokio::test]
async fn a_request_past_the_limit_is_refused_with_429() {
    let (app, tm) = limited(1);
    // The one slot is taken, as an open RESP connection would take it.
    let held = tm.admit_connection("default").unwrap();

    let res = app.clone().oneshot(query("RETURN 1 AS x", None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    let err = body["error"].as_str().unwrap_or_default();
    assert!(err.contains("connections (1/1)"), "{body}");
    assert_eq!(active(&tm), 1, "a refused request took a slot");

    drop(held);
    let res = app.clone().oneshot(query("RETURN 1 AS x", None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "a slot given back was not reusable");
    let _ = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(active(&tm), 0, "a finished request kept its slot");
}

#[tokio::test]
async fn a_streamed_response_holds_its_slot_until_the_body_is_done() {
    let (app, tm) = limited(5);
    let res = app
        .clone()
        .oneshot(query("UNWIND range(1, 10) AS i RETURN i", Some("application/x-ndjson")))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(active(&tm), 1, "the stream let go of its slot before its body was read");

    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("\"done\""), "the stream was cut off");
    assert_eq!(active(&tm), 0);
}

#[tokio::test]
async fn without_a_tenant_manager_nothing_is_counted() {
    // An embedded server with no tenants has no quota to enforce.
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store, 0).router();
    for _ in 0..3 {
        let res = app.clone().oneshot(query("RETURN 1 AS x", None)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
