//! A tenant quota is an admission decision, taken before the row exists (#1483).
//!
//! # What was wrong
//!
//! The quota was checked inside `PersistenceManager::apply_mutations`, which runs
//! *after* the statement has written to the store. Measured on `ad0e95a` with
//! `--max-nodes 1000000` and batches of 200,000 `CREATE`:
//!
//! ```text
//! attempts 1-6  200          (1,200,000 nodes accepted against a 1,000,000 limit)
//! attempt  7    503
//!
//! CREATE (:Q {id:999})       -> writes are refused: an earlier write did not reach disk
//! CREATE (:Unrelated {x:1})  -> same refusal     <- nothing to do with the quota
//! MATCH (n:Q) RETURN count(n) -> 1,200,000
//! ```
//!
//! Three things follow from the enforcement point, and all three are fixed by
//! moving it rather than by changing what is reported:
//!
//! 1. The rows were in memory and not on disk, so the durability invariant really
//!    was broken and `mark_degraded` was the correct response to it. That flag is
//!    process-global, so one tenant's quota stopped writes for everything the
//!    process served.
//! 2. 1,200,000 were admitted against 1,000,000: the ceiling was soft by whatever
//!    batch size happened to be in use.
//! 3. The operator was told a write had not reached disk and went and looked at
//!    their storage.
//!
//! # What these tests pin
//!
//! The check now runs in the store, before the node or edge exists, against a
//! ceiling the write path resolves once per statement. A breach is an ordinary
//! failed statement: nothing is created, the disk and memory still agree, and the
//! process is not degraded.
//!
//! Every test that could touch the degraded flag is serialised, for the reason
//! `persist_failure_is_reported.rs` gives: the flag is process-global.

use samyama::graph::{GraphStore, WriteAdmission};
use samyama::persistence::tenant::ResourceQuotas;
use samyama::persistence::{health, PersistenceManager};
use samyama::query::QueryEngine;

const T: &str = "default";

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn guard() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// A persistence manager whose default tenant has the given ceilings.
fn manager(max_nodes: Option<usize>, max_edges: Option<usize>) -> (PersistenceManager, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let pm = PersistenceManager::new(dir.path()).expect("persistence");
    let quotas = ResourceQuotas { max_nodes, max_edges, ..ResourceQuotas::unlimited() };
    pm.tenants().update_quotas(T, quotas).expect("quotas");
    (pm, dir)
}

/// One statement, run the way `protocol/command.rs` and `http/server.rs` run it:
/// resolve the ceiling, execute, then persist whatever the *store* changed.
///
/// Returns the statement's own error, if it had one, and whether persistence
/// failed. The two are different outcomes and the whole point of the fix is that
/// a quota breach is now the first and never the second.
fn run(
    pm: &PersistenceManager,
    engine: &QueryEngine,
    store: &mut GraphStore,
    cypher: &str,
) -> (Option<String>, Option<String>) {
    store.enable_write_log();
    store.set_write_admission(pm.write_admission(T));
    let stmt_err = engine.execute_mut(cypher, store, T).err().map(|e| e.to_string());
    let muts = store.take_write_log();
    let persist_err = pm.apply_mutations(T, store, &muts).err().map(|e| e.to_string());
    if let Some(e) = &persist_err {
        health::mark_degraded(e.clone());
    }
    (stmt_err, persist_err)
}

#[test]
fn a_create_at_the_ceiling_is_a_failed_statement_and_not_a_failed_persist() {
    let _serial = guard();
    health::reset_for_test();
    let (pm, _dir) = manager(Some(3), None);
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();

    for i in 0..3 {
        let (stmt, persist) = run(&pm, &engine, &mut store, &format!("CREATE (:Q {{id:{i}}})"));
        assert_eq!(stmt, None, "node {i} was within the quota");
        assert_eq!(persist, None, "node {i} was within the quota");
    }

    let (stmt, persist) = run(&pm, &engine, &mut store, "CREATE (:Q {id:99})");
    let stmt = stmt.expect("the statement past the ceiling must fail");
    assert!(
        stmt.contains("nodes (3/3)"),
        "the refusal must name the quota it hit, got: {stmt}"
    );
    // The class, not just the text. A quota breach is the caller asking for
    // more than they are allowed; it arrived as
    // `DatabaseError.Statement.GraphAccessFailed`, which says the server
    // failed. A client that treats `DatabaseError` as transient retries this
    // forever, and one that reads it as "the database is broken" pages
    // somebody for a configuration limit. The row budget — the other resource
    // limit a caller can hit — is already a `ClientError`, and two resource
    // limits must not be classified two different ways.
    assert!(
        stmt.contains(samyama::query::error_code::QUOTA_EXCEEDED),
        "the refusal must carry {}, got: {stmt}",
        samyama::query::error_code::QUOTA_EXCEEDED
    );
    assert!(
        !stmt.contains("DatabaseError"),
        "a quota breach must not be classed as a database failure, got: {stmt}"
    );
    assert_eq!(
        persist, None,
        "nothing was created, so there was nothing to persist and nothing to diverge"
    );
    assert!(
        !health::is_degraded(),
        "a quota breach is a rejected write; it must not put the process in the state \
         reserved for a write that did not reach disk"
    );
}

#[test]
fn the_count_does_not_exceed_the_limit_it_refused_at() {
    let _serial = guard();
    health::reset_for_test();
    let (pm, _dir) = manager(Some(10), None);
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();

    // The reported shape: batches that step over the ceiling rather than land on
    // it. Four batches of four against a limit of ten used to leave sixteen.
    for _ in 0..4 {
        let _ = run(&pm, &engine, &mut store, "UNWIND range(1,4) AS i CREATE (:Q {id:i})");
    }

    assert_eq!(
        store.node_count(),
        10,
        "the ceiling is the ceiling: the store held 1.2x the limit when the check ran at persist time"
    );
    assert_eq!(
        pm.tenants().get_usage(T).unwrap().node_count,
        10,
        "and the counter the quota is read from agrees with it"
    );
    assert!(!health::is_degraded());
}

#[test]
fn a_breach_does_not_stop_writes_that_have_nothing_to_do_with_it() {
    let _serial = guard();
    health::reset_for_test();
    // Room for the three :Q nodes that fill the node quota plus the unrelated one.
    let (pm, _dir) = manager(Some(4), None);
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();

    for i in 0..4 {
        run(&pm, &engine, &mut store, &format!("CREATE (:Q {{id:{i}}})"));
    }
    let (stmt, _) = run(&pm, &engine, &mut store, "CREATE (:Q {id:99})");
    assert!(stmt.is_some(), "the breach itself must fail");

    // The reported case: a different label, nothing to do with the node quota,
    // refused with "an earlier write did not reach disk". It is still refused --
    // the quota is on nodes, not on labels -- but it is refused as a quota, and
    // a write that creates no node is unaffected.
    let (stmt, persist) = run(&pm, &engine, &mut store, "MATCH (n:Q {id:0}) SET n.seen = true");
    assert_eq!(stmt, None, "a SET creates no node and must not be refused: {stmt:?}");
    assert_eq!(persist, None);
    assert!(
        !health::is_degraded(),
        "one tenant reaching its quota must not take the process read-only"
    );
}

#[test]
fn an_edge_quota_is_enforced_before_the_edge_exists() {
    let _serial = guard();
    health::reset_for_test();
    let (pm, _dir) = manager(None, Some(2));
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();

    run(&pm, &engine, &mut store, "CREATE (:P {id:1}), (:P {id:2})");
    for _ in 0..2 {
        let (stmt, persist) =
            run(&pm, &engine, &mut store, "MATCH (a:P {id:1}), (b:P {id:2}) CREATE (a)-[:R]->(b)");
        assert_eq!((stmt, persist), (None, None));
    }
    let (stmt, persist) =
        run(&pm, &engine, &mut store, "MATCH (a:P {id:1}), (b:P {id:2}) CREATE (a)-[:R]->(b)");
    let stmt = stmt.expect("the edge past the ceiling must fail");
    assert!(stmt.contains("edges (2/2)"), "got: {stmt}");
    assert!(
        stmt.contains(samyama::query::error_code::QUOTA_EXCEEDED),
        "the edge refusal must carry the same client-error class as the node one, got: {stmt}"
    );
    assert_eq!(persist, None);
    assert_eq!(store.edge_count(), 2);
    assert!(!health::is_degraded());
}

/// A batch that crosses the ceiling stops at it and keeps what it wrote.
///
/// The engine has no statement rollback (LANG-07), so a statement that fails
/// partway already leaves its earlier rows in the store, and the write paths
/// persist on the outcome of the *store* rather than of the statement. Those
/// rows are therefore on disk as well as in memory, which is the property that
/// matters: the statement fails, and nothing diverges.
///
/// The alternative — refuse the whole batch — would mean undoing rows the engine
/// cannot undo. Restoring a `SET` in the same statement is not possible at all,
/// the old value is gone, so "fail whole" could only ever be conditional on the
/// statement's shape. A rule that holds for pure creates and silently does not
/// hold otherwise is worse than one that always holds.
#[test]
fn a_batch_that_crosses_the_ceiling_stops_at_it_and_keeps_what_it_wrote() {
    let _serial = guard();
    health::reset_for_test();
    let (pm, _dir) = manager(Some(6), None);
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();

    let (stmt, persist) = run(&pm, &engine, &mut store, "UNWIND range(1,100) AS i CREATE (:Q {id:i})");
    assert!(stmt.is_some(), "a batch of 100 against a ceiling of 6 must fail");
    assert_eq!(persist, None, "and must not fail at persist time");
    assert_eq!(store.node_count(), 6, "it stopped at the ceiling");
    assert_eq!(
        pm.tenants().get_usage(T).unwrap().node_count,
        6,
        "and the six it wrote reached disk, so memory and disk agree"
    );
    assert!(!health::is_degraded());
}

#[test]
fn a_store_with_no_admission_set_is_unchanged() {
    // The default. An embedded store, a benchmark and every existing test must
    // behave exactly as they did, which means paying nothing and refusing nothing.
    let mut store = GraphStore::new();
    assert_eq!(store.write_admission(), None);
    assert!(store.admit_node().is_ok());
    assert!(store.admit_edge().is_ok());
    for _ in 0..100 {
        store.create_node("N");
    }
    assert_eq!(store.node_count(), 100);
}

#[test]
fn unlimited_quotas_produce_no_admission_at_all() {
    let dir = tempfile::tempdir().unwrap();
    let pm = PersistenceManager::new(dir.path()).unwrap();
    pm.tenants().update_quotas(T, ResourceQuotas::unlimited()).unwrap();
    assert_eq!(
        pm.write_admission(T),
        None,
        "`--max-nodes unlimited` must not even install a ceiling to compare against"
    );
}

#[test]
fn the_ceiling_counts_what_is_already_there_plus_what_this_statement_made() {
    // The two halves of the check, separately: `nodes_used` is what the quota
    // counter held when the statement began, and the store adds its own births.
    // Getting either wrong gives a ceiling that is either permanently open or
    // one that refuses the first write on a graph well under its limit.
    let mut store = GraphStore::new();
    store.set_write_admission(Some(WriteAdmission {
        nodes_used: 8,
        max_nodes: Some(10),
        edges_used: 0,
        max_edges: None,
    }));
    assert!(store.admit_node().is_ok(), "8 of 10 used: there is room");
    store.create_node("N");
    assert!(store.admit_node().is_ok(), "9 of 10");
    store.create_node("N");
    let e = store.admit_node().expect_err("10 of 10");
    assert!(e.to_string().contains("nodes (10/10)"), "got: {e}");
}
