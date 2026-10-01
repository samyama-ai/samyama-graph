//! A write statement has a deadline, and one that misses it leaves nothing
//! behind (#1593).
//!
//! Reads stopped at `SAMYAMA_QUERY_TIMEOUT`; writes had no deadline at all, so
//! a runaway `MATCH … SET` held the writer's lock for as long as it ran. And
//! the tenant's `max_query_time_ms` was configured, defaulted to 60 s, and read
//! by nothing.
//!
//! Following CLAUDE.md, nothing here measures elapsed time. A limit of zero
//! has already passed when the statement starts, so the statement must stop
//! after its first batch, deterministically; what the tests assert is the
//! error and the state of the store afterwards.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use samyama::graph::{GraphStore, Label};
use samyama::http::server::HttpServer;
use samyama::persistence::tenant::ResourceQuotas;
use samyama::persistence::TenantManager;
use samyama::protocol::command::CommandHandler;
use samyama::protocol::resp::RespValue;
use samyama::query::{BoundParams, QueryEngine};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tower::ServiceExt;

/// Already past when the statement starts.
const NOW: Option<Duration> = Some(Duration::ZERO);

/// More rows than one batch (1024), so a statement that ignored the deadline
/// between batches could not pass by finishing inside the first one.
const ROWS: i64 = 3000;

fn count(store: &GraphStore, label: &str) -> usize {
    store.get_nodes_by_label(&Label::new(label)).len()
}

fn write(engine: &QueryEngine, store: &mut GraphStore, q: &str, limit: Option<Duration>) -> Result<(), String> {
    engine
        .execute_mut_with_params_within(q, store, "default", &BoundParams::new(), limit)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn seeded() -> (QueryEngine, GraphStore) {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    write(&engine, &mut store, &format!("UNWIND range(1, {ROWS}) AS i CREATE (:P {{i: i, x: 1}})"), None)
        .expect("seed");
    (engine, store)
}

fn assert_timed_out(outcome: Result<(), String>) {
    let err = outcome.expect_err("a statement past its deadline succeeded");
    assert!(err.contains("timed out"), "not the timeout error: {err}");
}

// ------------------------------------------------------------- engine ----

#[test]
fn a_create_past_its_deadline_creates_nothing() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    assert_timed_out(write(
        &engine,
        &mut store,
        &format!("UNWIND range(1, {ROWS}) AS i CREATE (:N {{i: i}})"),
        NOW,
    ));
    assert_eq!(count(&store, "N"), 0, "the first batch of a timed-out CREATE stayed");
    assert_eq!(store.node_count(), 0);
}

#[test]
fn a_set_past_its_deadline_changes_nothing() {
    let (engine, mut store) = seeded();
    assert_timed_out(write(&engine, &mut store, "MATCH (n:P) SET n.x = 2", NOW));

    let engine2 = QueryEngine::new();
    let changed = engine2
        .execute("MATCH (n:P) WHERE n.x <> 1 RETURN count(n) AS c", &store)
        .unwrap();
    assert_eq!(
        changed.records[0].get("c").and_then(|v| v.as_property()).and_then(|p| p.as_integer()),
        Some(0),
        "a timed-out SET left some of its writes"
    );
}

#[test]
fn a_delete_past_its_deadline_deletes_nothing() {
    let (engine, mut store) = seeded();
    write(&engine, &mut store, "CREATE INDEX ON :P(i)", None).expect("index");
    assert_timed_out(write(&engine, &mut store, "MATCH (n:P) DETACH DELETE n", NOW));

    assert_eq!(count(&store, "P"), ROWS as usize, "a timed-out DELETE removed nodes");
    // Restored into the index as well as the label scan.
    let found = engine
        .execute("MATCH (n:P) WHERE n.i = 2500 RETURN n.x AS x", &store)
        .unwrap();
    assert_eq!(found.records.len(), 1, "a restored node is missing from the property index");
}

#[test]
fn a_tenant_limit_shorter_than_the_servers_wins() {
    // The server's limit is the engine default, 120 s; the tenant's is zero.
    let (engine, mut store) = seeded();
    assert_timed_out(write(&engine, &mut store, "MATCH (n:P) SET n.x = 3", NOW));
    // With no tenant limit the server's applies, and the same write finishes.
    write(&engine, &mut store, "MATCH (n:P) SET n.x = 3", None).expect("the server's limit alone");

    // Reads take the tenant's limit too.
    let read = engine.execute_with_params_within("MATCH (n:P) RETURN n.i", &store, &BoundParams::new(), NOW);
    assert!(
        read.as_ref().err().is_some_and(|e| e.to_string().contains("timed out")),
        "a read ignored the tenant's limit: {:?}",
        read.map(|b| b.records.len())
    );
}

/// Atomicity is not special to the deadline: any statement that fails part
/// way is undone, which is what makes the deadline safe to have.
#[test]
fn a_statement_that_fails_part_way_is_undone() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    write(&engine, &mut store, "CREATE CONSTRAINT ON (n:U) ASSERT n.k IS UNIQUE", None).expect("constraint");
    let err = write(&engine, &mut store, "UNWIND [1, 2, 3, 2] AS k CREATE (:U {k: k})", None)
        .expect_err("the duplicate key was admitted");
    assert!(!err.contains("timed out"), "{err}");
    assert_eq!(count(&store, "U"), 0, "the rows before the duplicate stayed");

    // And the store is usable afterwards: the keys the failed statement had
    // taken are free again.
    write(&engine, &mut store, "UNWIND [1, 2, 3] AS k CREATE (:U {k: k})", None).expect("retry");
    assert_eq!(count(&store, "U"), 3);
}

/// What a failed statement journalled for persistence goes with it: none of
/// it happened, so none of it may reach disk.
#[test]
fn a_failed_statement_leaves_nothing_to_persist() {
    let (engine, mut store) = seeded();
    store.enable_write_log();
    write(&engine, &mut store, "CREATE (:Before)", None).expect("write");
    assert_timed_out(write(&engine, &mut store, "MATCH (n:P) SET n.x = 9", NOW));
    let log = store.take_write_log();
    assert_eq!(log.len(), 1, "the journal kept the failed statement's writes: {log:?}");
}

/// Inside a transaction the caller opened, the transaction is the unit: the
/// statement's error is returned and ROLLBACK undoes it with the rest.
#[test]
fn inside_a_transaction_the_transaction_decides() {
    let (engine, mut store) = seeded();
    store.begin_session_transaction().unwrap();
    write(&engine, &mut store, "CREATE (:InTxn)", None).expect("first statement");
    assert_timed_out(write(&engine, &mut store, "MATCH (n:P) SET n.x = 5", NOW));
    assert!(store.session_transaction_version().is_some(), "the caller's transaction was closed");
    assert_eq!(count(&store, "InTxn"), 1, "the earlier statement was undone by the later one");

    store.rollback_session_transaction().unwrap();
    assert_eq!(count(&store, "InTxn"), 0);
    let changed = engine
        .execute("MATCH (n:P) WHERE n.x <> 1 RETURN count(n) AS c", &store)
        .unwrap();
    assert_eq!(
        changed.records[0].get("c").and_then(|v| v.as_property()).and_then(|p| p.as_integer()),
        Some(0)
    );
}

#[test]
fn successful_writes_leave_the_store_as_before() {
    // The statement transaction must not change what a plain write does.
    let (engine, mut store) = seeded();
    write(&engine, &mut store, "MATCH (n:P) WHERE n.i <= 10 SET n.x = 7", None).unwrap();
    write(&engine, &mut store, "MATCH (n:P) WHERE n.i > 2990 DETACH DELETE n", None).unwrap();
    let r = engine
        .execute("MATCH (n:P) RETURN count(n) AS c, sum(n.x) AS s", &store)
        .unwrap();
    let get = |k: &str| r.records[0].get(k).and_then(|v| v.as_property()).and_then(|p| p.as_integer());
    assert_eq!(get("c"), Some(2990));
    assert_eq!(get("s"), Some(2990 + 10 * 6));
}

// --------------------------------------------------------------- RESP ----

fn tenants_with_no_time() -> Arc<TenantManager> {
    let tm = Arc::new(TenantManager::new());
    let quotas = ResourceQuotas { max_query_time_ms: Some(0), ..ResourceQuotas::unlimited() };
    tm.update_quotas("default", quotas).unwrap();
    tm
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(s.as_bytes().to_vec()))
}

async fn resp(h: &CommandHandler, store: &Arc<RwLock<GraphStore>>, q: &str) -> RespValue {
    let args = vec![bulk("GRAPH.QUERY"), bulk("default"), bulk(q)];
    h.handle_command(&RespValue::Array(args), store, None).await
}

#[tokio::test]
async fn resp_returns_the_timeout_error_and_keeps_the_store() {
    let (_, seeded) = seeded();
    let store = Arc::new(RwLock::new(seeded));
    let h = CommandHandler::new_with_tenants(None, tenants_with_no_time());

    match resp(&h, &store, "MATCH (n:P) SET n.x = 2").await {
        RespValue::Error(e) => assert!(e.contains("timed out"), "{e}"),
        other => panic!("a write past the tenant's limit succeeded over RESP: {other:?}"),
    }
    match resp(&h, &store, "MATCH (n:P) RETURN n.i").await {
        RespValue::Error(e) => assert!(e.contains("timed out"), "{e}"),
        other => panic!("a read past the tenant's limit succeeded over RESP: {other:?}"),
    }
    let guard = store.read().await;
    let changed = QueryEngine::new()
        .execute("MATCH (n:P) WHERE n.x <> 1 RETURN count(n) AS c", &guard)
        .unwrap();
    assert_eq!(
        changed.records[0].get("c").and_then(|v| v.as_property()).and_then(|p| p.as_integer()),
        Some(0)
    );
}

#[tokio::test]
async fn resp_without_a_tenant_limit_runs_the_same_write() {
    let (_, seeded) = seeded();
    let store = Arc::new(RwLock::new(seeded));
    let tm = Arc::new(TenantManager::new());
    tm.update_quotas("default", ResourceQuotas::unlimited()).unwrap();
    let h = CommandHandler::new_with_tenants(None, tm);
    let reply = resp(&h, &store, "MATCH (n:P) SET n.x = 2").await;
    assert!(!matches!(reply, RespValue::Error(_)), "{reply:?}");
}

// --------------------------------------------------------------- HTTP ----

async fn post(app: &axum::Router, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn http_returns_the_timeout_error_and_keeps_the_store() {
    let (_, seeded) = seeded();
    let store = Arc::new(RwLock::new(seeded));
    let app = HttpServer::new(Arc::clone(&store), 0)
        .with_tenant_manager(tenants_with_no_time())
        .router();

    for q in ["MATCH (n:P) SET n.x = 2", "MATCH (n:P) RETURN n.i"] {
        let (status, body) = post(&app, json!({ "query": q })).await;
        assert_ne!(status, StatusCode::OK, "{q:?} past the tenant's limit succeeded over HTTP");
        let err = body["error"].as_str().unwrap_or_default();
        assert!(err.contains("timed out"), "{q:?}: {body}");
    }
    let guard = store.read().await;
    let changed = QueryEngine::new()
        .execute("MATCH (n:P) WHERE n.x <> 1 RETURN count(n) AS c", &guard)
        .unwrap();
    assert_eq!(
        changed.records[0].get("c").and_then(|v| v.as_property()).and_then(|p| p.as_integer()),
        Some(0)
    );
}
