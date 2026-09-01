//! What does the shipped HTTP surface actually return? (TRUST-06, API-07)
//!
//! Both requirements are tempting to answer with a grep -- "no `plan_hash` in
//! `src/http/`" -- and a grep is the wrong instrument for a negative claim. It
//! cannot see a field added by a layer, a wrapper, or a `serde` flatten, and
//! this repo has already produced one deferred decision based on a filtered
//! grep for a type that existed. So this drives `HttpServer::router()` --
//! the shipped stack, layers and all -- and reads the bytes that come back.
//!
//! It reports what is there. It does not assert that provenance is present,
//! because it is not, and a probe that fails is a probe someone silences.

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::http::server::HttpServer;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

/// The three fields TRUST-06 names, in the order the requirement names them.
const PROVENANCE_FIELDS: [&str; 3] = ["engine_version", "snapshot_hash", "plan_hash"];

async fn post_query(server: &HttpServer, cypher: &str) -> (axum::http::HeaderMap, serde_json::Value) {
    let req = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "query": cypher, "graph": "default" }).to_string(),
        ))
        .unwrap();
    let resp = server.router().oneshot(req).await.expect("router answers");
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (headers, json)
}

#[tokio::main]
async fn main() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    {
        let mut s = store.write().await;
        // Enough rows that a streaming implementation would have something to
        // stream; a one-row answer cannot distinguish streamed from buffered.
        for i in 0..2_000i64 {
            let n = s.create_node_with_labels([Label::new("Row")]);
            s.set_node_property("default", n, "i", PropertyValue::Integer(i)).unwrap();
        }
    }
    let server = HttpServer::new(Arc::clone(&store), 0);

    let (headers, body) = post_query(&server, "MATCH (n:Row) RETURN n.i").await;

    // TRUST-06 -- provenance on the result.
    let top: Vec<String> = body.as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default();
    let serialized = body.to_string();
    let present: Vec<&str> = PROVENANCE_FIELDS
        .iter()
        .copied()
        // Anywhere in the document, not just at the top level: a field nested
        // under a `meta` object still satisfies the requirement.
        .filter(|f| serialized.contains(&format!("\"{f}\"")))
        .collect();
    println!("TRUST-06 provenance fields present: {}/3 {present:?}", present.len());
    println!("  response keys: {top:?}");

    // API-07 -- streamed or buffered. A `Content-Length` means the whole body
    // was materialized before the first byte went out; chunked would not have
    // one. This reads the actual response rather than the handler's type.
    let len = headers.get("content-length").and_then(|v| v.to_str().ok()).map(str::to_string);
    let te = headers.get("transfer-encoding").and_then(|v| v.to_str().ok()).map(str::to_string);
    let rows = body.get("records").and_then(|r| r.as_array()).map(|a| a.len()).unwrap_or(0);
    println!(
        "API-07 rows={rows} content-length={} transfer-encoding={}",
        len.clone().unwrap_or_else(|| "-".into()),
        te.clone().unwrap_or_else(|| "-".into())
    );
    println!(
        "  verdict: {}",
        if te.as_deref() == Some("chunked") { "streamed" } else { "buffered" }
    );
}
