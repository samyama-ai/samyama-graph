//! `POST /api/query` streams a large result instead of building it first
//! (API-07, samyama-graph#1393, ADR-039).
//!
//! The issue is specific about the fix that must not pass: wrapping the
//! finished `RecordBatch` in a chunked body gives chunked encoding and no
//! `Content-Length` -- the probe goes green -- while peak memory and
//! time-to-first-byte stay exactly where they were. So nothing here checks the
//! encoding alone. Each test checks something only a real stream can do:
//!
//! * the first rows reach the client **while the query still holds the read
//!   lock** -- a materialise-then-chunk handler has released it by then;
//! * a consumer that stops reading **stops the query** and, after the stall
//!   budget, **releases the lock** rather than blocking writers for as long as
//!   it likes -- and the stream ends there, with an error trailer, instead of
//!   resuming against a different graph;
//! * at the engine, a sink that stops after the first chunk ends the query
//!   **before an operator has produced the whole result**, which a
//!   collect-then-hand-over implementation cannot do (it trips the row budget
//!   first).
//!
//! No wall-clock assertion anywhere: every wait below is an upper bound on a
//! condition, never a measured speed.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::query::BoundParams;
use samyama::QueryEngine;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tower::ServiceExt;

/// Stall budget for every test in this binary. The variable is process-wide,
/// so it is set to one value, once.
const STALL_MS: u64 = 1500;

fn setup() -> (Arc<RwLock<GraphStore>>, axum::Router) {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| std::env::set_var("SAMYAMA_STREAM_STALL_MS", STALL_MS.to_string()));
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(Arc::clone(&store), 0).router();
    (store, app)
}

fn streamed(query: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json")
        .header("accept", "application/x-ndjson")
        .body(Body::from(json!({ "query": query }).to_string()))
        .expect("request")
}

/// Next data frame of the body, or `None` at its end.
async fn next_chunk(body: &mut Body) -> Option<bytes::Bytes> {
    loop {
        let frame = body.frame().await?.expect("body frame");
        if let Ok(data) = frame.into_data() {
            return Some(data);
        }
    }
}

fn lines(buf: &[u8]) -> Vec<Value> {
    buf.split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| {
            serde_json::from_slice(l)
                .unwrap_or_else(|e| panic!("not a JSON line ({e}): {}", String::from_utf8_lossy(l)))
        })
        .collect()
}

fn is_row(v: &Value) -> bool {
    v.get("row").is_some()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_rows_leave_while_the_query_still_holds_its_read() {
    let (store, app) = setup();
    const TOTAL: i64 = 20_000;

    let resp = app
        .oneshot(streamed(&format!("UNWIND range(1, {TOTAL}) AS x RETURN x")))
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
        Some("application/x-ndjson"),
        "the client asked for a stream and did not get one"
    );
    assert!(
        resp.headers().get(header::CONTENT_LENGTH).is_none(),
        "a Content-Length means the body was complete before it was sent"
    );

    let mut body = resp.into_body();
    let first = next_chunk(&mut body).await.expect("a first chunk");

    // The load-bearing check. The query runs under the store's read lock and
    // releases it when the last row has been produced; a handler that built
    // the result and then chunked it would have released it before sending
    // anything.
    assert!(
        store.try_write().is_err(),
        "the first chunk arrived after the read had finished: that is a buffered \
         result in a chunked body, not a stream"
    );

    let first_lines = lines(&first);
    assert_eq!(first_lines[0]["columns"], json!(["x"]), "header line first");
    assert!(first_lines[0]["snapshot_version"].is_u64());
    let first_rows = first_lines.iter().filter(|l| is_row(l)).count();
    assert!(
        first_rows > 0 && (first_rows as i64) < TOTAL,
        "the first chunk carried {first_rows} of {TOTAL} rows"
    );

    let mut all = first_lines;
    while let Some(chunk) = next_chunk(&mut body).await {
        all.extend(lines(&chunk));
    }
    let rows: Vec<i64> = all
        .iter()
        .filter(|l| is_row(l))
        .map(|l| l["row"][0].as_i64().expect("integer cell"))
        .collect();
    assert_eq!(rows, (1..=TOTAL).collect::<Vec<_>>(), "every row, once, in order");

    let trailer = all.last().expect("trailer");
    assert_eq!(trailer["done"], json!(true), "trailer: {trailer}");
    assert_eq!(trailer["rows"], json!(TOTAL));

    // The lock is dropped before the trailer is sent.
    assert!(store.try_write().is_ok(), "the read outlived its stream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_stops_reading_stops_the_query_and_frees_the_lock() {
    let (store, app) = setup();
    const TOTAL: i64 = 200_000;

    let resp = app
        .oneshot(streamed(&format!("UNWIND range(1, {TOTAL}) AS x RETURN x")))
        .await
        .expect("response");
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
        Some("application/x-ndjson"),
        "not streamed"
    );
    let mut body = resp.into_body();
    let mut seen = lines(&next_chunk(&mut body).await.expect("first chunk"));

    // Stop reading. Backpressure: the query is paused on a full channel, not
    // racing ahead into memory, so it still holds the read.
    assert!(store.try_write().is_err(), "the read ended before the client read it");

    // A writer gets in within the stall budget (plus slack for a debug build),
    // not "when the client gets round to it".
    let writer = tokio::time::timeout(Duration::from_millis(STALL_MS * 20), store.write())
        .await
        .expect("a stalled consumer kept every writer out");
    drop(writer);

    // Resume. The stream does not pick up where it left off against a graph
    // that has moved on: it ends, and says why.
    while let Some(chunk) = next_chunk(&mut body).await {
        seen.extend(lines(&chunk));
    }
    let rows = seen.iter().filter(|l| is_row(l)).count() as i64;
    assert!(rows < TOTAL, "all {TOTAL} rows arrived: the query never stopped");
    let trailer = seen.last().expect("trailer");
    assert!(trailer.get("done").is_none(), "a cut-short stream claims completion: {trailer}");
    let err = trailer["error"].as_str().unwrap_or_else(|| panic!("no error trailer: {trailer}"));
    assert!(err.contains("did not read"), "error does not say why: {err}");
    assert_eq!(trailer["rows"], json!(rows), "trailer row count disagrees with the body");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_accept_header_the_response_is_the_buffered_envelope() {
    let (_store, app) = setup();
    let req = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": "UNWIND range(1, 3) AS x RETURN x" }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&bytes).expect("one JSON document");
    assert_eq!(v["columns"], json!(["x"]));
    assert_eq!(v["records"], json!([[1], [2], [3]]));
    assert!(v["nodes"].is_array() && v["edges"].is_array());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_reports_errors_and_refuses_what_it_cannot_stream() {
    let (_store, app) = setup();

    // Fails before any row: still a 400 with the usual envelope.
    let resp = app.clone().oneshot(streamed("MATCH (n RETURN n")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let v: Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert!(v["error"].is_string());

    // A write runs under the writer's lock, which a stream must not hand to
    // the client's read speed.
    let resp = app.clone().oneshot(streamed("CREATE (:X) RETURN 1")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_ACCEPTABLE);

    // An empty result is a header and a trailer.
    let resp = app.oneshot(streamed("MATCH (n:Nothing) RETURN n")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let all = lines(&resp.into_body().collect().await.unwrap().to_bytes());
    assert_eq!(all.len(), 2, "{all:?}");
    assert_eq!(all[0]["columns"], json!(["n"]));
    assert_eq!(all[1]["done"], json!(true));
    assert_eq!(all[1]["rows"], json!(0));
}

// ------------------------------------------------------------- engine ----

fn cross_store(side: usize) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..side {
        let a = store.create_node("A");
        store.set_node_property("default", a, "i", i as i64).unwrap();
        let b = store.create_node("B");
        store.set_node_property("default", b, "j", i as i64).unwrap();
    }
    store
}

const CROSS: &str = "MATCH (a:A), (b:B) RETURN a.i AS i, b.j AS j";

#[test]
fn a_sink_that_stops_early_ends_the_query_before_the_result_exists() {
    let store = cross_store(100); // 10,000 rows through a cartesian product
    let engine = QueryEngine::new().with_row_budget(5_000);

    // Collected, the product passes the budget: the result is built whole.
    let whole = engine.execute(CROSS, &store);
    assert!(whole.is_err(), "the row budget does not see this product; the test proves nothing");

    // Streamed, a consumer that has seen enough stops it far short of that.
    let mut calls = 0;
    let mut got = 0;
    let out = engine.execute_streaming_with_params(
        CROSS,
        &store,
        &BoundParams::new(),
        256,
        &mut |cols, rows| {
            assert_eq!(cols, ["i", "j"]);
            assert!(rows.len() <= 256, "a chunk of {} rows", rows.len());
            calls += 1;
            got += rows.len();
            if got >= 1_000 {
                Err("enough".to_string())
            } else {
                Ok(())
            }
        },
    );
    let err = out.expect_err("the sink's stop was lost").to_string();
    assert!(
        err.contains("enough"),
        "the query ran past its consumer and failed on its own: {err}"
    );
    assert!(calls >= 4, "rows were handed over in {calls} call(s)");
}

#[test]
fn a_streamed_result_is_the_collected_result() {
    let store = cross_store(40);
    let engine = QueryEngine::new();
    for q in [
        CROSS,
        "MATCH (a:A) RETURN a.i AS i ORDER BY i DESC",
        "MATCH (a:A) RETURN count(a) AS c",
        "MATCH (a:A) RETURN a.i AS v UNION MATCH (b:B) RETURN b.j AS v",
        "MATCH (a:Nothing) RETURN a",
        "EXPLAIN MATCH (a:A) RETURN a",
    ] {
        let collected = engine.execute(q, &store).unwrap_or_else(|e| panic!("{q}: {e}"));
        let mut rows = Vec::new();
        let done = engine
            .execute_streaming_with_params(q, &store, &BoundParams::new(), 7, &mut |cols, recs| {
                for r in &recs {
                    rows.push(cols.iter().map(|c| format!("{:?}", r.get(c))).collect::<Vec<_>>());
                }
                Ok(())
            })
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        let expected: Vec<Vec<String>> = collected
            .records
            .iter()
            .map(|r| collected.columns.iter().map(|c| format!("{:?}", r.get(c))).collect())
            .collect();
        assert_eq!(done.columns, collected.columns, "{q}");
        assert_eq!(done.rows, expected.len(), "{q}");
        assert_eq!(rows, expected, "{q}");
    }
}
