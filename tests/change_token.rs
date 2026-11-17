//! `GET /api/changes`: has the graph's data changed since a token? (#1771)
//!
//! A view keeps itself current by polling this rather than re-running its
//! query. Every case goes through `HttpServer::router()`, the chain the binary
//! serves, so the route is reached the way a client reaches it.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use tokio::sync::RwLock;
use tower::ServiceExt;

fn app() -> axum::Router {
    HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router()
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
    let res = app.clone().oneshot(req).await.unwrap();
    let (status, headers) = (res.status(), res.headers().clone());
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, headers, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}

async fn post(app: &axum::Router, uri: &str, body: serde_json::Value) {
    let req = Request::post(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let (status, _, b) = send(app, req).await;
    assert_eq!(status, StatusCode::OK, "{uri}: {b}");
}

async fn query(app: &axum::Router, q: &str) {
    post(app, "/api/query", serde_json::json!({ "query": q })).await;
}

/// `(token, changed)` for `GET /api/changes[?since=…]`.
async fn changes(app: &axum::Router, since: Option<&str>) -> (String, bool) {
    let uri = match since {
        Some(t) => format!("/api/changes?since={t}"),
        None => "/api/changes".to_string(),
    };
    let (status, headers, b) = send(app, Request::get(uri).body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK, "{b}");
    assert_eq!(headers["cache-control"], "no-store", "a cached 'unchanged' would be wrong");
    (b["token"].as_str().expect("token").to_string(), b["changed"].as_bool().expect("changed"))
}

#[tokio::test]
async fn nothing_written_means_unchanged_and_the_same_token() {
    let app = app();
    query(&app, "CREATE (:A {v: 1})").await;
    let (t, first) = changes(&app, None).await;
    assert!(first, "no `since` is a client that has seen nothing");
    for _ in 0..3 {
        assert_eq!(changes(&app, Some(&t)).await, (t.clone(), false));
    }
}

#[tokio::test]
async fn reads_do_not_move_the_token() {
    let app = app();
    query(&app, "CREATE (:A {v: 1})-[:R]->(:B)").await;
    let (t, _) = changes(&app, None).await;
    for q in [
        "MATCH (n) RETURN n",
        "MATCH (a)-[r]->(b) RETURN count(r) AS c",
        "EXPLAIN MATCH (n:A) RETURN n",
        "MATCH (n:A) RETURN n.v AS v",
    ] {
        query(&app, q).await;
        assert!(!changes(&app, Some(&t)).await.1, "{q} is a read");
    }
}

#[tokio::test]
async fn every_kind_of_write_moves_the_token() {
    let app = app();
    let (mut t, _) = changes(&app, None).await;
    for q in [
        "CREATE (:A {v: 1})",
        "MATCH (n:A) SET n.v = 2",
        "MATCH (n:A) SET n:Extra",
        "MATCH (n:A) REMOVE n.v",
        "MATCH (a:A) CREATE (a)-[:R]->(:B)",
        "MATCH ()-[r:R]->() DELETE r",
        "MATCH (n) DETACH DELETE n",
    ] {
        query(&app, q).await;
        let (next, changed) = changes(&app, Some(&t)).await;
        assert!(changed && next != t, "{q} changed the data but not the token");
        t = next;
    }
}

#[tokio::test]
async fn an_import_moves_the_token() {
    let app = app();
    let (t, _) = changes(&app, None).await;
    post(&app, "/api/import/json", serde_json::json!({ "label": "Doc", "nodes": [{ "body": "x" }] })).await;
    assert!(changes(&app, Some(&t)).await.1, "an import is a data change");
}

#[tokio::test]
async fn a_token_this_process_did_not_issue_reads_as_changed() {
    let app = app();
    let (t, _) = changes(&app, None).await;
    let (prefix, count) = t.rsplit_once('-').expect("prefix-count");
    // The same count from another process: a restart that has since made as
    // many writes. The count alone would say unchanged.
    let other = format!("{}-{count}", "0".repeat(prefix.len()));
    for since in [other.as_str(), "", "garbage", "-", "x-0"] {
        assert!(changes(&app, Some(since)).await.1, "{since:?} was read as current");
    }
}

/// The token errs one way only. A write that fails part-way may still move
/// it -- the store counts a change when a write takes a mutable reference,
/// before it knows whether the write will succeed -- so a poller may re-run
/// a query that would have returned the same rows. It never errs the other
/// way: the data did not change, and the next poll with the new token is
/// unchanged again.
#[tokio::test]
async fn a_failed_write_can_only_cost_one_extra_refresh() {
    let app = app();
    query(&app, "CREATE (:A {v: 1})").await;
    let (t, _) = changes(&app, None).await;
    let req = Request::post("/api/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({ "query": "CREATE (:A {v: 1/0})" }).to_string()))
        .unwrap();
    let (status, _, _) = send(&app, req).await;
    assert_ne!(status, StatusCode::OK);
    let (after, _) = changes(&app, Some(&t)).await;
    assert!(!changes(&app, Some(&after)).await.1, "settles at once");
    let req = Request::post("/api/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({ "query": "MATCH (n:A) RETURN count(n) AS c" }).to_string()))
        .unwrap();
    let (_, _, b) = send(&app, req).await;
    assert_eq!(b["records"][0][0], 1, "and the data really is unchanged: {b}");
}
