//! A long algorithm can be watched while it runs and stopped on request
//! (ALGO-12, #1693).
//!
//! #2041 stopped long algorithms at the query deadline. A caller still could
//! not see how far one had got, and could stop one only by waiting for its
//! deadline. `GET /api/algorithms/running` now lists every algorithm call in
//! progress with `phase`, `done` and `total`, and
//! `POST /api/algorithms/running/{id}/cancel` stops one.
//!
//! The runs here are Louvain on 20,000 nodes and 200,000 relationships, which
//! takes tens of seconds in a debug build -- long enough to be watched and
//! cancelled part-way. Nothing asserts how long anything took: each test waits
//! for a state (listed, past its first sweep, gone) and fails with what it saw
//! if that state does not come within a generous bound, so a fast or slow host
//! changes only how long the wait is.
//!
//! The two tests share the process-wide registry, so they hold a lock and run
//! one at a time: each must be able to say "the only Louvain running is mine".

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::query::running::{self, RunningAlgorithm};
use samyama::query::QueryEngine;
use tokio::sync::RwLock;
use tower::ServiceExt;

const NODES: u64 = 20_000;
const EDGES_PER_NODE: usize = 10;

/// Louvain's bound on this call: 20 passes x 10 sweeps.
const LOUVAIN_TOTAL_SWEEPS: u64 = 200;

const LOUVAIN: &str = "CALL algo.louvain() YIELD node, communityId RETURN count(*)";

/// How long a test waits for a state before failing with what it saw. Far
/// above anything a run needs; it bounds a broken test, it measures nothing.
const PATIENCE: Duration = Duration::from_secs(120);

fn one_at_a_time() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// The same graph on every run: xorshift targets, no seedable RNG needed.
fn build() -> GraphStore {
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..NODES).map(|_| s.create_node("P")).collect();
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for &from in &ids {
        for _ in 0..EDGES_PER_NODE {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            s.create_edge(from, ids[(x % NODES) as usize], "L").unwrap();
        }
    }
    s
}

fn graph() -> &'static GraphStore {
    static GRAPH: OnceLock<GraphStore> = OnceLock::new();
    GRAPH.get_or_init(build)
}

fn louvain_calls() -> Vec<RunningAlgorithm> {
    running::list().into_iter().filter(|r| r.algorithm == "algo.louvain").collect()
}

/// Poll the registry until `ready` holds for the one Louvain call, and return
/// that call as it was then. `what` names the state for the failure message.
fn wait_for(what: &str, ready: impl Fn(&RunningAlgorithm) -> bool) -> RunningAlgorithm {
    let start = Instant::now();
    let mut last = Vec::new();
    while start.elapsed() < PATIENCE {
        last = louvain_calls();
        if let [call] = last.as_slice() {
            if ready(call) {
                return call.clone();
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("never saw {what}; last listing: {last:?}");
}

fn wait_until_gone(id: u64) {
    let start = Instant::now();
    while running::list().iter().any(|r| r.id == id) {
        assert!(start.elapsed() < PATIENCE, "call {id} was still listed after it ended");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// `... stopped after <done> of <total> <unit>`, as numbers.
fn stopped_at(msg: &str) -> (u64, u64) {
    let after = msg.split(" after ").nth(1).unwrap_or_else(|| panic!("no 'after' in: {msg}"));
    let mut words = after.split_whitespace();
    let done = words.next().unwrap().parse().unwrap();
    assert_eq!(words.next(), Some("of"), "{msg}");
    (done, words.next().unwrap().parse().unwrap())
}

#[test]
fn a_running_algorithm_is_listed_with_its_progress_and_stops_when_cancelled() {
    let _serial = one_at_a_time();
    let store = graph();
    let run = std::thread::spawn(move || {
        QueryEngine::new().execute(LOUVAIN, store).map(|b| b.records.len()).map_err(|e| e.to_string())
    });

    // Listed while it runs, past the view build and at least one sweep in:
    // progress is readable from outside the query, as it moves.
    let first = wait_for("algo.louvain past its first sweep", |c| c.phase == "running" && c.done >= 1);
    assert_eq!(first.total, LOUVAIN_TOTAL_SWEEPS, "{first:?}");
    assert!(!first.cancel_requested);
    let later = wait_for("the sweep count move on", |c| c.done > first.done);
    assert_eq!(later.id, first.id, "a second Louvain appeared");
    assert!(later.elapsed_ms >= first.elapsed_ms);

    assert!(running::cancel(first.id), "a listed call could not be cancelled");
    let err = run.join().unwrap().expect_err("the cancelled run returned rows");
    assert!(err.contains("Query cancelled: algo.louvain stopped after"), "{err}");
    let (done, total) = stopped_at(&err);
    assert!(done >= later.done && done < total, "stopped at {done} of {total}, after {later:?}");

    // Gone from the list, nothing left to cancel, and the engine answers the
    // next query -- the cancel stopped one call, not the engine.
    wait_until_gone(first.id);
    assert!(!running::cancel(first.id), "an ended call was still cancellable");
    let rows = QueryEngine::new().execute("MATCH (p:P) RETURN count(p) AS n", store).unwrap();
    assert_eq!(rows.records.len(), 1);
}

#[test]
fn a_cancel_during_the_view_build_stops_it_there() {
    let _serial = one_at_a_time();
    let store = graph();
    let run = std::thread::spawn(move || {
        QueryEngine::new().execute(LOUVAIN, store).map(|b| b.records.len()).map_err(|e| e.to_string())
    });
    // Cancelled the moment it is listed, before any sweep: the stop lands in
    // the view build or, at the latest, before the first sweep.
    let listed = wait_for("algo.louvain listed", |c| c.done == 0 || c.phase != "running");
    assert!(running::cancel(listed.id));
    let err = run.join().unwrap().expect_err("the cancelled run returned rows");
    assert!(err.contains("Query cancelled: algo.louvain stopped"), "{err}");
    if err.contains("building its graph view") {
        let (done, total) = stopped_at(&err);
        assert!(done < total && total == NODES, "{err}");
    } else {
        assert_eq!(stopped_at(&err), (0, LOUVAIN_TOTAL_SWEEPS), "{err}");
    }
    wait_until_gone(listed.id);
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}

fn get(uri: &str) -> Request<Body> {
    Request::get(uri).body(Body::empty()).unwrap()
}

fn post(uri: &str, body: serde_json::Value) -> Request<Body> {
    Request::post(uri).header("content-type", "application/json").body(Body::from(body.to_string())).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_http_a_long_algorithm_is_listed_cancelled_and_the_server_answers_throughout() {
    let _serial = one_at_a_time();
    let app = HttpServer::new(Arc::new(RwLock::new(build())), 0).router();

    // Nothing of ours is running yet; the listing answers anyway.
    let (status, body) = send(&app, get("/api/algorithms/running")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["running"].is_array(), "{body}");

    let query = tokio::spawn({
        let app = app.clone();
        async move { send(&app, post("/api/query", serde_json::json!({ "query": LOUVAIN }))).await }
    });

    // Watched over HTTP until it is into its sweeps.
    let start = Instant::now();
    let call = loop {
        let (status, body) = send(&app, get("/api/algorithms/running")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let mine: Vec<&serde_json::Value> =
            body["running"].as_array().unwrap().iter().filter(|r| r["algorithm"] == "algo.louvain").collect();
        if let [call] = mine.as_slice() {
            if call["phase"] == "running" && call["done"].as_u64() >= Some(1) {
                break (*call).clone();
            }
        }
        assert!(start.elapsed() < PATIENCE, "never saw algo.louvain running over HTTP: {body}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(call["total"], LOUVAIN_TOTAL_SWEEPS, "{call}");
    for key in ["id", "started_ms", "elapsed_ms", "done", "total", "cancel_requested"] {
        assert!(!call[key].is_null(), "{key} missing: {call}");
    }

    // The server answers other requests while the algorithm runs.
    let (status, _) = send(&app, get("/api/status")).await;
    assert_eq!(status, StatusCode::OK, "/api/status did not answer during the run");

    let id = call["id"].as_u64().unwrap();
    let (status, body) = send(&app, post(&format!("/api/algorithms/running/{id}/cancel"), serde_json::json!({}))).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body, serde_json::json!({ "id": id, "cancel_requested": true }));

    let (status, body) = query.await.unwrap();
    assert_ne!(status, StatusCode::OK, "the cancelled query answered: {body}");
    assert!(body.to_string().contains("Query cancelled: algo.louvain stopped after"), "{status} {body}");

    // Ended: no longer listed, and a second cancel finds nothing to stop.
    wait_until_gone(id);
    let (status, body) = send(&app, post(&format!("/api/algorithms/running/{id}/cancel"), serde_json::json!({}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(body["error"].as_str().unwrap_or("").contains(&format!("no algorithm call {id} is running")), "{body}");

    // And the server goes on answering queries.
    let (status, body) = send(&app, post("/api/query", serde_json::json!({ "query": "MATCH (p:P) RETURN count(p) AS n" }))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
