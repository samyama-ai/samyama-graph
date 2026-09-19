//! The browser path into an unauthenticated write API is closed (#1328).
//!
//! `POST /api/query` executes arbitrary Cypher including `DELETE`, and nothing
//! reads a credential off the request. Until this change two layers made that
//! reachable from a web page: `CorsLayer::permissive()` accepted any origin,
//! and the Private Network Access middleware echoed
//! `Access-Control-Allow-Private-Network: true` to whichever origin asked.
//!
//! **That second one is the part worth stating.** Chrome sends the PNA
//! preflight so a *public* page cannot reach a private address unless the local
//! service opts in. Opting in for every requesting origin does not grant the
//! permission to the Studio; it removes the protection for everyone.
//!
//! These tests drive `HttpServer::router()` — the shipped stack, layers and all
//! — rather than assembling a miniature router. A layer tested through a router
//! that does not carry it is a test of nothing, which is how both defects
//! survived: the existing HTTP tests each build their own `Router`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use samyama::graph::GraphStore;
use samyama::http::HttpServer;
use tokio::sync::RwLock;
use tower::util::ServiceExt;

const STUDIO: &str = "https://graph.samyama.cloud";
const ATTACKER: &str = "https://evil.example";

fn server(origins: Vec<&str>) -> HttpServer {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    HttpServer::new(store, 0)
        .with_allowed_origins(origins.into_iter().map(String::from).collect())
}

/// A CORS preflight that also asks for private-network access, as Chrome sends
/// it from a public page to a loopback address.
fn preflight(origin: &str) -> Request<Body> {
    Request::builder()
        .method("OPTIONS")
        .uri("/api/query")
        .header("origin", origin)
        .header("access-control-request-method", "POST")
        .header("access-control-request-private-network", "true")
        .body(Body::empty())
        .unwrap()
}

async fn headers_for(origins: Vec<&str>, origin: &str) -> axum::http::HeaderMap {
    let res = server(origins)
        .router()
        .oneshot(preflight(origin))
        .await
        .expect("router");
    res.headers().clone()
}

#[tokio::test]
async fn the_default_allows_no_origin() {
    let h = headers_for(vec![], ATTACKER).await;
    assert!(
        h.get("access-control-allow-origin").is_none(),
        "an empty allowlist must not permit a cross-origin call: {h:?}"
    );
}

#[tokio::test]
async fn the_default_does_not_echo_private_network_access() {
    let h = headers_for(vec![], ATTACKER).await;
    assert!(
        h.get("access-control-allow-private-network").is_none(),
        "the PNA opt-in was echoed with no origin configured, which is the \
         browser protection removed rather than granted: {h:?}"
    );
}

#[tokio::test]
async fn an_unlisted_origin_gets_no_private_network_access() {
    // The Studio is configured; a different origin asks. This is the case the
    // old middleware got wrong: it echoed on the *request header* alone and
    // never looked at who was asking.
    let h = headers_for(vec![STUDIO], ATTACKER).await;
    assert!(
        h.get("access-control-allow-private-network").is_none(),
        "{ATTACKER} is not on the allowlist and was granted private-network \
         access: {h:?}"
    );
    assert_ne!(
        h.get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some(ATTACKER),
        "{ATTACKER} was allowed as an origin: {h:?}"
    );
}

#[tokio::test]
async fn a_listed_origin_still_works() {
    // The reason the echo exists at all (#342): the hosted Studio calling a
    // loopback container. Closing the hole must not close this.
    let h = headers_for(vec![STUDIO], STUDIO).await;
    assert_eq!(
        h.get("access-control-allow-private-network")
            .and_then(|v| v.to_str().ok()),
        Some("true"),
        "the configured origin was refused private-network access: {h:?}"
    );
    assert_eq!(
        h.get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some(STUDIO),
        "the configured origin was not allowed: {h:?}"
    );
}

#[tokio::test]
async fn a_request_that_does_not_ask_for_private_network_never_gets_it() {
    // The header is a response to a question. Volunteering it would widen the
    // surface for every ordinary same-origin request.
    let res = server(vec![STUDIO])
        .router()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/query")
                .header("origin", STUDIO)
                .header("access-control-request-method", "POST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router");
    assert!(res.headers().get("access-control-allow-private-network").is_none());
}

#[tokio::test]
async fn same_origin_requests_are_unaffected() {
    // Nothing here may break a local client that sends no Origin at all: the
    // CLI, curl, and every SDK.
    let res = server(vec![])
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"RETURN 1"}"#))
                .unwrap(),
        )
        .await
        .expect("router");
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "a request with no Origin header must still be served"
    );
}
