//! Chrome Private Network Access preflight (#342).
//!
//! A public HTTPS origin — the hosted Studio — may not call a loopback address unless the
//! local server opts in. Chrome sends `Access-Control-Request-Private-Network: true` on the
//! preflight and requires `Access-Control-Allow-Private-Network: true` in reply; without it
//! the request is blocked in the browser and never reaches the engine at all.
//!
//! `CorsLayer::permissive()` does not cover this — tower-http has no PNA support — so the
//! header has to be added alongside it, and this test is what keeps it there.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use tokio::sync::RwLock;
use tower::ServiceExt;

fn app() -> axum::Router {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    HttpServer::new(store, 0).router()
}

#[tokio::test]
async fn preflight_from_a_public_origin_opts_into_the_loopback_address_space() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/status")
                .header("Origin", "https://graph.samyama.cloud")
                .header("Access-Control-Request-Method", "GET")
                .header("Access-Control-Request-Private-Network", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.headers().get("access-control-allow-private-network"),
        Some(&axum::http::HeaderValue::from_static("true")),
        "preflight must opt in, or Chrome blocks the request before it reaches the engine"
    );
}

#[tokio::test]
async fn the_header_is_not_sent_when_it_was_not_asked_for() {
    // Only echoed for requests that actually carry the PNA request header, so ordinary
    // same-origin traffic is unaffected.
    let response = app()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/status")
                .header("Origin", "https://graph.samyama.cloud")
                .header("Access-Control-Request-Method", "GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response
        .headers()
        .get("access-control-allow-private-network")
        .is_none());
}

#[tokio::test]
async fn a_normal_request_still_works() {
    let response = app()
        .oneshot(
            Request::builder()
                .uri("/api/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
