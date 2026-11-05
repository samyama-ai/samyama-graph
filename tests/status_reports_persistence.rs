//! `/api/status` says when persistence has failed (#2082).
//!
//! One test in its own binary: the degraded flag is process-wide, and any test
//! beside it that writes would be refused.

use axum::body::Body;
use axum::http::Request;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::persistence::health;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

async fn status() -> serde_json::Value {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let req = Request::builder().uri("/api/status").body(Body::empty()).unwrap();
    let res = HttpServer::new(store, 0).router().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn status_turns_degraded_when_a_write_misses_the_disk() {
    health::reset_for_test();
    let ok = status().await;
    assert_eq!(ok["status"], "healthy");
    assert_eq!(ok["storage"]["persistence_degraded"], false);

    health::mark_degraded("disk full at /secret/path");
    let sick = status().await;
    assert_eq!(sick["status"], "degraded");
    assert_eq!(sick["storage"]["persistence_degraded"], true);
    assert!(!sick.to_string().contains("/secret/path"), "the reason stays in the log: {sick}");
    health::reset_for_test();
}
