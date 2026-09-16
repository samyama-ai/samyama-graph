//! The isolation-anomaly table (REL-01) as tests: step 1 of the MVCC design in
//! samyama-graph#1200.
//!
//! Each test states one anomaly and what snapshot isolation must do about it,
//! against today's transaction API (`begin_transaction`, `txn_write_node`,
//! `get_node_for_txn`, `commit_transaction`, `abort_transaction`).
//!
//! Today writes go straight to the live store: a transaction only records
//! which entities it touched, commit checks for a write-write conflict and bumps
//! the version, and abort only marks the transaction. So the anomalies an MVCC
//! layer exists to prevent are not prevented. The tests that fail today are
//! `#[ignore]`d with the #1200 step that makes them pass; run them with
//! `cargo test --test mvcc_isolation_anomalies -- --ignored` to see the gap.
//! The two that pass today (lost update, write skew) document behaviour the
//! design keeps.

use samyama::graph::{GraphStore, IsolationLevel, NodeId, PropertyValue};

fn x(store: &GraphStore, n: NodeId) -> Option<PropertyValue> {
    store.get_node(n).and_then(|node| node.get_property("x").cloned())
}

fn x_for(store: &GraphStore, txn: u64, n: NodeId) -> Option<PropertyValue> {
    store.get_node_for_txn(txn, n).and_then(|node| node.get_property("x").cloned())
}

fn int(i: i64) -> Option<PropertyValue> {
    Some(PropertyValue::Integer(i))
}

/// One node with `x = 1`, written outside any transaction.
fn one() -> (GraphStore, NodeId) {
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    store.set_node_property("default", n, "x", 1i64).unwrap();
    (store, n)
}

/// A write inside a transaction, the only way today's API expresses one: the
/// store write plus the write-set entry.
fn write(store: &mut GraphStore, txn: u64, n: NodeId, v: i64) {
    store.set_node_property("default", n, "x", v).unwrap();
    store.txn_write_node(txn, n);
}

/// **Dirty read.** Another transaction must not see a write that has not been
/// committed. Today the write is already in the live store.
#[test]
#[ignore = "#1200 step 4: transactions buffer their writes"]
fn no_dirty_read() {
    let (mut store, n) = one();
    let writer = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let reader = store.begin_transaction(IsolationLevel::ReadCommitted);
    write(&mut store, writer, n, 2);
    assert_eq!(x_for(&store, reader, n), int(1), "a read-committed reader saw an uncommitted write");
    assert_eq!(x(&store, n), int(1), "a plain read saw an uncommitted write");
}

/// **Rollback.** An aborted transaction leaves nothing behind. Today abort only
/// marks the transaction Aborted.
#[test]
#[ignore = "#1200 step 4: ROLLBACK drops the transaction's buffer"]
fn abort_undoes_the_write() {
    let (mut store, n) = one();
    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    write(&mut store, t, n, 2);
    store.abort_transaction(t).unwrap();
    assert_eq!(x(&store, n), int(1), "an aborted write is still visible");
}

/// **Non-repeatable read / snapshot.** A snapshot keeps seeing the value it
/// started with after another transaction commits a change. Today the change is
/// written at the snapshot's own version, so the snapshot sees it.
#[test]
#[ignore = "#1200 steps 2 and 4: the undo log keeps the old value; commit writes at a new version"]
fn a_snapshot_does_not_see_a_later_commit() {
    let (mut store, n) = one();
    let snapshot = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let writer = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    write(&mut store, writer, n, 2);
    store.commit_transaction(writer).unwrap();
    assert_eq!(x_for(&store, snapshot, n), int(1), "the snapshot saw a commit made after it started");
    assert_eq!(x(&store, n), int(2), "the committed value is the current value");
}

/// **Phantom by creation.** A snapshot does not see an entity created after it
/// started. Today creation is not versioned.
#[test]
#[ignore = "#1200 steps 2 and 4: step 2 records the birth version, but create_node inside a transaction still runs at the snapshot's own version until step 4 commits at a new one"]
fn a_snapshot_does_not_see_a_node_created_after_it() {
    let (mut store, _) = one();
    let snapshot = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let writer = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let fresh = store.create_node("N");
    store.txn_write_node(writer, fresh);
    store.commit_transaction(writer).unwrap();
    assert!(store.get_node_for_txn(snapshot, fresh).is_none(), "the snapshot saw a node created after it");
}

/// **Lost update.** Two transactions read the same value and both write it;
/// the second commit must be refused, not silently overwrite the first.
/// First-committer-wins does this today, and the design keeps it.
#[test]
fn the_second_of_two_conflicting_commits_is_refused() {
    let (mut store, n) = one();
    let a = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let b = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    write(&mut store, a, n, 2);
    write(&mut store, b, n, 3);
    assert!(store.commit_transaction(a).is_ok());
    assert!(store.commit_transaction(b).is_err(), "a lost update was committed");
}

/// **Write skew.** Two transactions each read both of two values and write a
/// different one. Snapshot isolation allows both to commit; serializable would
/// refuse one. Documented as allowed, per #1200's open question on the default
/// isolation level. If serializable becomes a requirement, this test flips.
#[test]
fn write_skew_is_allowed_under_snapshot_isolation() {
    let mut store = GraphStore::new();
    let p = store.create_node("N");
    let q = store.create_node("N");
    store.set_node_property("default", p, "x", 1i64).unwrap();
    store.set_node_property("default", q, "x", 1i64).unwrap();
    let a = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let b = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    write(&mut store, a, p, 0);
    write(&mut store, b, q, 0);
    assert!(store.commit_transaction(a).is_ok());
    assert!(store.commit_transaction(b).is_ok(), "disjoint write sets must both commit under snapshot isolation");
}
