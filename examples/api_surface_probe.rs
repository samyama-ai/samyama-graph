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

/// The nearest thing the response actually carries, where it is not the field
/// the requirement names. `snapshot_version` is the MVCC version the read saw:
/// it identifies the snapshot, which is what the requirement is *for*, but it
/// is not a content hash of it. Reported as a proxy rather than counted, so
/// the headline number cannot be improved by renaming a field.
const PROVENANCE_PROXIES: [(&str, &str); 1] = [("snapshot_hash", "snapshot_version")];

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
    let proxies: Vec<String> = PROVENANCE_PROXIES
        .iter()
        .filter(|(named, _)| !present.contains(named))
        .filter(|(_, actual)| serialized.contains(&format!("\"{actual}\"")))
        .map(|(named, actual)| format!("{named}<-{actual}"))
        .collect();
    println!("TRUST-06 proxies: {}/3 {proxies:?}", proxies.len());
    println!("  response keys: {top:?}");

    // API-07 -- streamed or buffered. Asked twice, because streaming is opt-in
    // (#1554): without `Accept: application/x-ndjson` the server answers one
    // JSON document, and that is the expected answer, not a failure.
    //
    // The first version of this check looked for `transfer-encoding: chunked`.
    // That header is added by hyper on a real connection; a router called with
    // `oneshot`, as here, never carries it, so the check could only ever print
    // "buffered" -- it kept reading buffered after #1554 shipped a stream. What
    // the router does show is the body itself: a buffered body is one frame with
    // a Content-Length, a streamed one arrives as several frames with none.
    let len = headers.get("content-length").and_then(|v| v.to_str().ok()).map(str::to_string);
    let rows = body.get("records").and_then(|r| r.as_array()).map(|a| a.len()).unwrap_or(0);
    println!(
        "API-07 default rows={rows} content-length={}",
        len.clone().unwrap_or_else(|| "-".into())
    );

    let req = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json")
        .header("accept", "application/x-ndjson")
        .body(Body::from(
            serde_json::json!({ "query": "MATCH (n:Row) RETURN n.i", "graph": "default" })
                .to_string(),
        ))
        .unwrap();
    let resp = server.router().oneshot(req).await.expect("router answers");
    let nd_len = resp.headers().get("content-length").is_some();
    let mut body = resp.into_body();
    let (mut frames, mut text) = (0usize, Vec::new());
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.expect("body frame").into_data() {
            frames += 1;
            text.extend_from_slice(&data);
        }
    }
    let lines: Vec<serde_json::Value> = String::from_utf8_lossy(&text)
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let nd_rows = lines.iter().filter(|v| v.get("row").is_some()).count();
    let done = lines.last().and_then(|v| v.get("done")).and_then(|v| v.as_bool()) == Some(true);
    println!(
        "API-07 ndjson frames={frames} content-length={} rows={nd_rows} trailer_done={done}",
        if nd_len { "present" } else { "-" }
    );
    // All four, because each alone has a cheap false positive: one frame is a
    // buffered body in a different format, a missing trailer is a truncated
    // stream, a short row count is a stream that lost rows.
    let streamed = frames > 1 && !nd_len && nd_rows == 2_000 && done;
    println!("  verdict: {}", if streamed { "streamed" } else { "buffered" });
}
