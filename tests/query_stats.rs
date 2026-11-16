//! Every query response says what the statement cost (#2079, COST-06).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use tokio::sync::RwLock;
use tower::ServiceExt;

async fn post(app: &axum::Router, uri: &str, body: serde_json::Value) -> serde_json::Value {
    let res = app
        .clone()
        .oneshot(
            Request::post(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "{uri}");
    serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

async fn query(app: &axum::Router, q: &str) -> serde_json::Value {
    post(app, "/api/query", serde_json::json!({ "query": q })).await
}

fn f(v: &serde_json::Value) -> f64 {
    v.as_f64().unwrap_or_else(|| panic!("not a number: {v}"))
}

// ── HTTP: stats on every response ───────────────────────────────────────────

#[tokio::test]
async fn a_read_and_a_write_each_report_what_they_cost() {
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router();

    let write = query(&app, "UNWIND range(1, 5) AS i CREATE (:S {i: i})").await;
    let s = &write["stats"];
    assert_eq!(s["is_write"], true, "{s}");
    assert_eq!(s["rows"], 0, "a write with no RETURN returns no rows");
    assert!(f(&s["elapsed_ms"]) >= 0.0 && f(&s["lock_wait_ms"]) >= 0.0, "{s}");

    let read = query(&app, "MATCH (n:S) RETURN n.i AS i").await;
    let s = &read["stats"];
    assert_eq!(s["is_write"], false);
    assert_eq!(s["rows"], 5);
    assert_eq!(s["rows"].as_u64().unwrap() as usize, read["records"].as_array().unwrap().len());
}

#[tokio::test]
async fn elapsed_grows_with_the_work_the_statement_does() {
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router();
    // Warm the parse cache and the code paths, then compare the two in the
    // same process: a ratio, not a wall-clock bound.
    let _ = query(&app, "RETURN 1 AS x").await;
    let small = f(&query(&app, "RETURN 1 AS x").await["stats"]["elapsed_ms"]);
    let large = f(&query(&app, "UNWIND range(1, 300000) AS x RETURN sum(x) AS s").await["stats"]["elapsed_ms"]);
    assert!(large > small * 10.0, "small {small} ms, large {large} ms");
}

/// Contention is reported where it happened: in `lock_wait_ms`, not mixed
/// into `elapsed_ms`. The lower bound is the time this test held the lock
/// itself, so it does not depend on the machine's speed.
#[tokio::test]
async fn waiting_for_the_lock_is_reported_apart_from_running() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(Arc::clone(&store), 0).router();
    let held = Duration::from_millis(150);

    for q in ["RETURN 1 AS x", "CREATE (:Waited)"] {
        let guard = store.write().await;
        let app2 = app.clone();
        let pending = tokio::spawn(async move { query(&app2, q).await });
        tokio::time::sleep(held).await;
        drop(guard);
        let s = pending.await.unwrap()["stats"].clone();
        let waited = f(&s["lock_wait_ms"]);
        assert!(waited >= held.as_millis() as f64 * 0.9, "{q}: waited {waited} ms of {held:?}: {s}");
        assert!(f(&s["elapsed_ms"]) < waited, "{q}: the wait is not counted as running: {s}");
    }
}

#[tokio::test]
async fn a_statement_in_a_transaction_reports_no_lock_wait() {
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router();
    let tx = post(&app, "/api/tx/begin", serde_json::json!({})).await["tx"].as_str().unwrap().to_string();
    let r = post(&app, "/api/query", serde_json::json!({ "query": "CREATE (:T) RETURN 1 AS one", "tx": tx })).await;
    let s = &r["stats"];
    assert_eq!(s["is_write"], true, "{r}");
    assert_eq!(s["rows"], 1);
    assert_eq!(f(&s["lock_wait_ms"]), 0.0, "the transaction already holds the lock");
}
