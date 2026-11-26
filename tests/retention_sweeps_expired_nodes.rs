//! Per-label retention, swept in the background (HW-03 / NDS-03, #2217).
//!
//! ```cypher
//! CALL db.retention.set('Reading', {maxAge: '1h', maxRecords: 100000})
//! CALL db.retention.list()
//! CALL db.retention.drop('Reading')
//! ```
//!
//! An edge gateway writes readings forever; a rule bounds how many it keeps
//! and for how long. What these tests pin:
//!
//! * a sweep removes nodes older than `maxAge` and the oldest beyond
//!   `maxRecords`, with their relationships, and nothing else;
//! * a node is swept only when every label it has carries a rule, so adding
//!   `:Anomaly` to a reading keeps it;
//! * a rule and what a sweep removed both survive a restart;
//! * the procedures take the write path and refuse arguments they cannot use.

use std::sync::Arc;

use samyama::graph::retention::{sweep_once, RetentionRule};
use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::persistence::PersistenceManager;
use samyama::query::QueryEngine;

const T: &str = "default";
const HOUR: i64 = 3_600_000;

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// A node with `labels`, created `age_ms` ago.
fn node_aged(s: &mut GraphStore, labels: &[&str], age_ms: i64) -> NodeId {
    let id = s.create_node_with_labels(labels.iter().map(|l| Label::new(*l)));
    s.get_node_mut(id).unwrap().created_at = now() - age_ms;
    id
}

fn ok(engine: &QueryEngine, s: &mut GraphStore, q: &str) -> samyama::query::RecordBatch {
    engine
        .execute_mut(q, s, T)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn rule(age: Option<u64>, records: Option<u64>) -> RetentionRule {
    RetentionRule {
        max_age_ms: age,
        max_records: records,
    }
}

#[test]
fn readings_older_than_max_age_go_and_newer_ones_stay() {
    let mut s = GraphStore::new();
    let old = node_aged(&mut s, &["Reading"], 2 * HOUR);
    let new = node_aged(&mut s, &["Reading"], HOUR / 2);
    s.set_retention("Reading", rule(Some(HOUR as u64), None));

    assert_eq!(s.sweep_retention(T, now()), 1);
    assert!(s.get_node(old).is_none());
    assert!(s.get_node(new).is_some());
}

#[test]
fn max_records_keeps_the_newest() {
    let mut s = GraphStore::new();
    // Ages 5h, 4h, ... 1h: the first two are the oldest.
    let ids: Vec<_> = (1..=5)
        .rev()
        .map(|h| node_aged(&mut s, &["Reading"], h * HOUR))
        .collect();
    s.set_retention("Reading", rule(None, Some(3)));

    assert_eq!(s.sweep_retention(T, now()), 2);
    let left: Vec<_> = ids.iter().map(|id| s.get_node(*id).is_some()).collect();
    assert_eq!(left, [false, false, true, true, true]);
    // A second sweep has nothing more to do.
    assert_eq!(s.sweep_retention(T, now()), 0);
}

#[test]
fn a_promoted_reading_is_kept_and_its_relationships_go_with_the_rest() {
    let mut s = GraphStore::new();
    let plain = node_aged(&mut s, &["Reading"], 2 * HOUR);
    let promoted = node_aged(&mut s, &["Reading", "Anomaly"], 2 * HOUR);
    let sensor = node_aged(&mut s, &["Sensor"], 2 * HOUR);
    s.create_edge(sensor, plain, "EMITTED").unwrap();
    s.create_edge(sensor, promoted, "EMITTED").unwrap();
    s.set_retention("Reading", rule(Some(HOUR as u64), Some(1)));

    assert_eq!(s.sweep_retention(T, now()), 1);
    assert!(s.get_node(plain).is_none());
    assert!(
        s.get_node(promoted).is_some(),
        ":Anomaly has no rule, so the node is not governed"
    );
    assert!(
        s.get_node(sensor).is_some(),
        "a label with no rule is never swept"
    );
    assert_eq!(
        s.edge_count(),
        1,
        "the swept reading's relationship went with it"
    );
}

#[test]
fn a_node_with_no_recorded_creation_time_has_no_age() {
    let mut s = GraphStore::new();
    let id = node_aged(&mut s, &["Reading"], 0);
    s.get_node_mut(id).unwrap().created_at = 0;
    s.set_retention("Reading", rule(Some(1), None));
    assert_eq!(s.sweep_retention(T, now()), 0);
}

#[test]
fn the_procedures_set_list_and_drop_a_rule() {
    let engine = QueryEngine::new();
    let mut s = GraphStore::new();

    let r = ok(
        &engine,
        &mut s,
        "CALL db.retention.set('Reading', {maxAge: '1h', maxRecords: 100})",
    );
    let row = &r.records[0];
    assert_eq!(
        row.get("maxAgeMs").and_then(|v| v.as_property()),
        Some(&PropertyValue::Integer(HOUR))
    );
    assert_eq!(
        row.get("replaced").and_then(|v| v.as_property()),
        Some(&PropertyValue::Boolean(false))
    );
    assert_eq!(
        s.retention_rules()["Reading"],
        rule(Some(HOUR as u64), Some(100))
    );

    let r = ok(
        &engine,
        &mut s,
        "CALL db.retention.set('Reading', {maxRecords: 5})",
    );
    assert_eq!(
        r.records[0].get("replaced").and_then(|v| v.as_property()),
        Some(&PropertyValue::Boolean(true))
    );
    assert_eq!(s.retention_rules()["Reading"], rule(None, Some(5)));

    let r = engine
        .execute(
            "CALL db.retention.list() YIELD label, maxRecords RETURN label, maxRecords",
            &s,
        )
        .unwrap();
    assert_eq!(r.records.len(), 1);
    assert_eq!(
        r.records[0].get("maxRecords").and_then(|v| v.as_property()),
        Some(&PropertyValue::Integer(5))
    );

    let r = ok(&engine, &mut s, "CALL db.retention.drop('Reading')");
    assert_eq!(
        r.records[0].get("dropped").and_then(|v| v.as_property()),
        Some(&PropertyValue::Boolean(true))
    );
    assert!(s.retention_rules().is_empty());
}

#[test]
fn set_and_drop_take_the_write_path_and_list_does_not() {
    let engine = QueryEngine::new();
    assert!(engine
        .statement_is_write("CALL db.retention.set('R', {maxRecords: 1})")
        .unwrap());
    assert!(engine
        .statement_is_write("CALL db.retention.drop('R')")
        .unwrap());
    assert!(!engine
        .statement_is_write("CALL db.retention.list()")
        .unwrap());
}

#[test]
fn arguments_it_cannot_use_are_refused() {
    let engine = QueryEngine::new();
    let mut s = GraphStore::new();
    for q in [
        "CALL db.retention.set('Reading', {})",
        "CALL db.retention.set('Reading', {maxAge: '1 fortnight'})",
        "CALL db.retention.set('Reading', {maxAge: 0})",
        "CALL db.retention.set('Reading', {maxRecords: -1})",
        "CALL db.retention.set('Reading', {maxRecord: 10})",
        "CALL db.retention.set('', {maxRecords: 10})",
        "CALL db.retention.set('Reading')",
    ] {
        assert!(
            engine.execute_mut(q, &mut s, T).is_err(),
            "{q} was accepted"
        );
    }
    assert!(s.retention_rules().is_empty());
}

#[tokio::test]
async fn a_rule_and_what_it_swept_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = QueryEngine::new();

    let pm = Arc::new(PersistenceManager::new(dir.path()).unwrap());
    let store = tokio::sync::RwLock::new(GraphStore::new());
    let (old, new) = {
        let mut s = store.write().await;
        s.enable_write_log();
        let old = node_aged(&mut s, &["Reading"], 2 * HOUR);
        let new = node_aged(&mut s, &["Reading"], 0);
        ok(
            &engine,
            &mut s,
            "CALL db.retention.set('Reading', {maxAge: '1h'})",
        );
        let journal = s.take_write_log();
        pm.apply_mutations(T, &s, &journal).unwrap();
        (old, new)
    };
    assert_eq!(sweep_once(&store, Some(&pm), T).await, 1);
    pm.flush().unwrap();
    drop(store);
    drop(pm);

    let pm = PersistenceManager::new(dir.path()).unwrap();
    let mut s = GraphStore::new();
    pm.recover_into(T, &mut s).unwrap();
    assert!(s.get_node(old).is_none(), "the swept reading came back");
    assert!(s.get_node(new).is_some());
    assert_eq!(
        s.retention_rules()["Reading"],
        rule(Some(HOUR as u64), None)
    );
}

#[tokio::test]
async fn a_sweep_without_rules_or_persistence_does_nothing_harmful() {
    let store = tokio::sync::RwLock::new(GraphStore::new());
    node_aged(&mut *store.write().await, &["Reading"], 10 * HOUR);
    assert_eq!(
        sweep_once(&store, None, T).await,
        0,
        "no rule, nothing swept"
    );
    store
        .write()
        .await
        .set_retention("Reading", rule(Some(HOUR as u64), None));
    assert_eq!(sweep_once(&store, None, T).await, 1);
    assert_eq!(store.read().await.node_count(), 0);
}
