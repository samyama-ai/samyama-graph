//! `/metrics` says how busy the server is and whether writes reach disk
//! (#2080 items 4 and 5).
//!
//! Its own binary, and its tests one at a time: the in-flight gauge and the
//! persistence counters are process-wide, so a query or a persist in another
//! test would move the numbers this one is reading.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::persistence::{health, PersistenceManager, TenantManager};
use samyama::query::{metrics, QueryEngine};
use tokio::sync::RwLock;
use tower::ServiceExt;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// The HTTP tests drive the router on a runtime of their own, so the lock
/// above is held by a plain function and never across an `.await`.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(f)
}

/// The value of an unlabelled sample in Prometheus text.
fn sample(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
}

// ── queries in flight ───────────────────────────────────────────────────────

#[test]
fn running_queries_are_counted_while_they_run_and_not_after() {
    let _s = serial();
    assert_eq!(metrics::queries_in_flight(), 0, "nothing is running yet");
    let engine = Arc::new(QueryEngine::new());
    let store = Arc::new(GraphStore::new());
    let slow = "UNWIND range(1, 3000000) AS x RETURN sum(x) AS s";

    let workers: Vec<_> = (0..3)
        .map(|_| {
            let (engine, store) = (Arc::clone(&engine), Arc::clone(&store));
            std::thread::spawn(move || engine.execute(slow, &store).map(|_| ()).map_err(|e| e.to_string()))
        })
        .collect();
    // Polled, not slept for: the three are running together for as long as
    // the slowest of them, and the gauge must see all three at some point.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = 0;
    while Instant::now() < deadline && seen < 3 {
        seen = seen.max(metrics::queries_in_flight());
        if workers.iter().all(|w| w.is_finished()) {
            break;
        }
        std::thread::yield_now();
    }
    for w in workers {
        w.join().unwrap().expect("query");
    }
    assert_eq!(seen, 3, "three concurrent queries, seen as {seen}");
    assert_eq!(metrics::queries_in_flight(), 0, "all three finished");
    assert_eq!(sample(&metrics::render(), "samyama_queries_in_flight"), Some(0.0));
}

#[test]
fn a_query_that_fails_is_not_left_counted() {
    let _s = serial();
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    assert!(engine.execute("MATCH (n RETURN n", &store).is_err(), "parse error");
    assert!(engine.execute("RETURN 1 / 0 AS x", &store).is_err(), "runtime error");
    assert!(engine.execute_mut("CREATE (:A {v: 1 / 0})", &mut store, "default").is_err(), "write error");
    assert_eq!(metrics::queries_in_flight(), 0, "an error path kept a query counted");
}

// ── persistence ─────────────────────────────────────────────────────────────

#[test]
fn writes_reaching_disk_and_failing_to_are_both_counted() {
    let _s = serial();
    health::reset_for_test();
    let dir = tempfile::tempdir().unwrap();
    let pm = PersistenceManager::new(dir.path()).expect("persistence");
    let mut store = GraphStore::new();
    store.enable_write_log();
    store.create_node("N");
    store.create_node("N");
    let two = store.take_write_log();
    assert_eq!(two.len(), 2);

    let before = health::counters();
    pm.apply_mutations("default", &store, &two).expect("persist");
    let after = health::counters();
    assert_eq!(after.mutations_persisted - before.mutations_persisted, 2);
    assert_eq!(after.errors, before.errors);

    // An empty log is a read or a write that changed nothing: not a persist.
    pm.apply_mutations("default", &store, &[]).expect("empty");
    assert_eq!(health::counters(), after);

    let text = metrics::render();
    assert_eq!(sample(&text, "samyama_persistence_degraded"), Some(0.0));

    pm.fail_next_apply_for_test();
    assert!(pm.apply_mutations("default", &store, &two).is_err());
    let failed = health::counters();
    assert_eq!(failed.errors - after.errors, 1);
    assert_eq!(failed.mutations_persisted, after.mutations_persisted, "nothing landed");

    // The server marks itself degraded on that failure; the gauge says so.
    health::mark_degraded("injected");
    let text = metrics::render();
    assert_eq!(sample(&text, "samyama_persistence_degraded"), Some(1.0));
    assert_eq!(sample(&text, "samyama_persistence_errors_total"), Some(failed.errors as f64));
    assert_eq!(
        sample(&text, "samyama_mutations_persisted_total"),
        Some(failed.mutations_persisted as f64)
    );
    health::reset_for_test();
}

/// Through the server: a write over HTTP moves the persisted counter, so the
/// count is taken on the path a client's write takes, not only on the call.
#[test]
fn a_write_over_http_is_counted_as_persisted() {
    let _s = serial();
    block_on(async {
        health::reset_for_test();
        let dir = tempfile::tempdir().unwrap();
        let pm = Arc::new(PersistenceManager::new(dir.path()).expect("persistence"));
        let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0)
            .with_persistence(pm)
            .router();
        let before = health::counters().mutations_persisted;
        let res = app
            .oneshot(
                Request::post("/api/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"query": "UNWIND range(1, 3) AS i CREATE (:P {i: i})"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(res.status().is_success());
        assert!(health::counters().mutations_persisted - before >= 3, "three nodes created");
    });
}

// ── active connections ──────────────────────────────────────────────────────

async fn scrape(app: axum::Router) -> String {
    let res = app.oneshot(Request::get("/metrics").body(Body::empty()).unwrap()).await.unwrap();
    String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap()
}

#[test]
fn connections_are_reported_as_max_connections_counts_them() {
    let _s = serial();
    block_on(async {
        let tenants = Arc::new(TenantManager::new());
        let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0)
            .with_tenant_manager(Arc::clone(&tenants))
            .router();
        // The scrape is itself a request being served.
        assert_eq!(sample(&scrape(app.clone()).await, "samyama_active_connections"), Some(1.0));
        // Two more held open, as a RESP connection holds its slot.
        let a = tenants.admit_connection("default").expect("slot");
        let b = tenants.admit_connection("default").expect("slot");
        assert_eq!(sample(&scrape(app.clone()).await, "samyama_active_connections"), Some(3.0));
        drop((a, b));
        assert_eq!(sample(&scrape(app).await, "samyama_active_connections"), Some(1.0));
    });
}

#[test]
fn without_a_tenant_manager_there_is_no_connection_count_to_report() {
    let _s = serial();
    block_on(async {
        let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router();
        let text = scrape(app).await;
        assert_eq!(sample(&text, "samyama_active_connections"), None, "absent, not a false 0");
        assert!(sample(&text, "samyama_queries_in_flight").is_some());
    });
}
