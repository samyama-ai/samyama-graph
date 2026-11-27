//! A rollback is a write, and reaches the disk like one (#2093).
//!
//! The Smriti spec (§11): checkpoints are not durable, but rolling back to one
//! changes the graph, and a restart must find the graph as the rollback left
//! it -- not the turn it undid. `PersistenceManager::rollback_to` runs the
//! rollback with the write log on and persists what it journals; if that
//! fails, the process is marked degraded, since memory is then ahead of the
//! disk.
//!
//! The failure is injected (`fail_next_apply_for_test`), as in
//! `persist_failure_is_reported.rs`, and for the same reason these tests share
//! one lock: the degraded flag is process-global.

use std::path::Path;

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::persistence::{health, PersistenceManager};
use samyama::query::QueryEngine;

const T: &str = "default";

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn guard() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Run `q` the way the server does: statement, then persist its log.
fn run(pm: &PersistenceManager, store: &mut GraphStore, q: &str) {
    QueryEngine::new().execute_mut(q, store, T).expect(q);
    let mutations = store.take_write_log();
    pm.apply_mutations(T, store, &mutations).expect(q);
}

fn restart(dir: &Path) -> GraphStore {
    let pm = PersistenceManager::new(dir).unwrap();
    let mut store = GraphStore::new();
    pm.recover_into(T, &mut store).unwrap();
    store
}

/// `(name, age)` of every `:Person`, sorted, plus the relationship count.
type People = (Vec<(String, Option<i64>)>, usize);

fn people(store: &GraphStore) -> People {
    let mut out: Vec<(String, Option<i64>)> = store
        .get_nodes_by_label(&Label::new("Person"))
        .iter()
        .map(|n| {
            let props = store.node_properties_full(n.id);
            let name = match props.get("name") {
                Some(PropertyValue::String(s)) => s.clone(),
                other => format!("{other:?}"),
            };
            let age = match props.get("age") {
                Some(PropertyValue::Integer(i)) => Some(*i),
                _ => None,
            };
            (name, age)
        })
        .collect();
    out.sort();
    (out, store.edge_count())
}

/// A graph persisted statement by statement, a checkpoint, and a turn that
/// creates, updates and deletes, all persisted too.
fn a_turn_on_disk(dir: &Path) -> (PersistenceManager, GraphStore, u64, People) {
    let pm = PersistenceManager::new(dir).unwrap();
    let mut store = GraphStore::new();
    store.enable_write_log();
    for q in [
        "CREATE (:Person {name: 'ada', age: 36})",
        "CREATE (:Person {name: 'charles', age: 79})",
        "MATCH (a:Person {name: 'ada'}), (c:Person {name: 'charles'}) CREATE (a)-[:KNEW]->(c)",
    ] {
        run(&pm, &mut store, q);
    }
    let before = people(&store);
    let v = store.checkpoint().unwrap();
    for q in [
        "MATCH (a:Person {name: 'ada'}) SET a.age = 37",
        "MATCH (c:Person {name: 'charles'}) DETACH DELETE c",
        "CREATE (:Person {name: 'mary', age: 28})",
    ] {
        run(&pm, &mut store, q);
    }
    assert_ne!(people(&store), before, "the turn changed nothing");
    (pm, store, v, before)
}

#[test]
fn a_restart_finds_the_graph_as_the_rollback_left_it() {
    let _serial = guard();
    health::reset_for_test();
    let dir = tempfile::tempdir().unwrap();
    {
        let (pm, mut store, v, before) = a_turn_on_disk(dir.path());
        let stats = pm.rollback_to(T, &mut store, v).expect("rollback");
        assert!(stats.failures.is_empty(), "{:?}", stats.failures);
        assert_eq!(people(&store), before);
        assert!(
            store.write_log_enabled(),
            "the log was on before, and stays on"
        );
    }
    let recovered = restart(dir.path());
    assert_eq!(
        people(&recovered),
        (
            vec![
                ("ada".to_string(), Some(36)),
                ("charles".to_string(), Some(79))
            ],
            1
        ),
        "a restart brought back the turn the rollback undid"
    );
    assert!(!health::is_degraded());
}

#[test]
fn a_store_without_a_write_log_is_left_without_one() {
    let _serial = guard();
    health::reset_for_test();
    let dir = tempfile::tempdir().unwrap();
    let pm = PersistenceManager::new(dir.path()).unwrap();
    let mut store = GraphStore::new();
    let n = store.create_node("Person");
    let v = store.checkpoint().unwrap();
    store
        .set_node_property(T, n, "age", PropertyValue::Integer(1))
        .unwrap();
    pm.rollback_to(T, &mut store, v).expect("rollback");
    assert!(!store.write_log_enabled());
    assert!(!store.node_properties_full(n).contains_key("age"));
}

#[test]
fn a_rollback_that_cannot_be_persisted_marks_the_process_degraded() {
    let _serial = guard();
    health::reset_for_test();
    let dir = tempfile::tempdir().unwrap();
    let (pm, mut store, v, before) = a_turn_on_disk(dir.path());

    pm.fail_next_apply_for_test();
    let err = pm
        .rollback_to(T, &mut store, v)
        .expect_err("the persist was made to fail");
    assert!(err.contains("could not be persisted"), "{err}");
    // In memory the rollback happened; it is the disk that is behind.
    assert_eq!(people(&store), before);
    assert!(
        health::is_degraded(),
        "memory is ahead of the disk and nothing says so"
    );
    health::reset_for_test();
}

#[test]
fn a_refused_rollback_persists_nothing_and_degrades_nothing() {
    let _serial = guard();
    health::reset_for_test();
    let dir = tempfile::tempdir().unwrap();
    let pm = PersistenceManager::new(dir.path()).unwrap();
    let mut store = GraphStore::new();
    let n = store.create_node("Person");
    let v = store.checkpoint().unwrap();
    let txn = store.begin_transaction(samyama::graph::IsolationLevel::ReadCommitted);
    store
        .txn_set_node_property(txn, n, "age", PropertyValue::Integer(2))
        .unwrap();
    store.commit_transaction(txn).unwrap();

    let err = pm
        .rollback_to(T, &mut store, v)
        .expect_err("another writer's commit");
    assert!(err.contains("refused"), "{err}");
    assert!(!health::is_degraded());
    assert_eq!(
        store.node_properties_full(n).get("age"),
        Some(&PropertyValue::Integer(2))
    );
    assert_eq!(store.pinned_checkpoints(), vec![v]);
}
