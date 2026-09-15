//! The isolation-anomaly table (REL-01) as tests, from step 1 of the MVCC
//! design in samyama-graph#1200.
//!
//! Each test states one anomaly and what snapshot isolation must do about it.
//! Step 1 wrote them against the transaction API as it was, where writes went
//! straight to the live store, and four failed. Step 4 made a transaction
//! buffer its writes (`txn_set_node_property`, `txn_create_node`), read them
//! back over its snapshot (`get_node_for_txn`), apply them at a new version on
//! `commit_transaction`, and drop them on `abort_transaction`. All of them now
//! pass. Lost update and write skew passed from the start; they document the
//! behaviour the design keeps: first-committer-wins, and write skew allowed
//! under snapshot isolation.

use samyama::graph::{GraphStore, IsolationLevel, Label, NodeId, PropertyValue, TxnStatus};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;

/// The current value, column first -- what every reader outside a transaction sees.
fn x(store: &GraphStore, n: NodeId) -> Option<PropertyValue> {
    store.node_property(n, "x")
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

/// A write inside a transaction: buffered until commit.
fn write(store: &mut GraphStore, txn: u64, n: NodeId, v: i64) {
    store.txn_set_node_property(txn, n, "x", v).unwrap();
}

/// **Dirty read.** Another transaction must not see a write that has not been
/// committed, and neither must a plain read.
#[test]
fn no_dirty_read() {
    let (mut store, n) = one();
    let writer = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let reader = store.begin_transaction(IsolationLevel::ReadCommitted);
    write(&mut store, writer, n, 2);
    assert_eq!(x_for(&store, reader, n), int(1), "a read-committed reader saw an uncommitted write");
    assert_eq!(x(&store, n), int(1), "a plain read saw an uncommitted write");
}

/// **Rollback.** An aborted transaction leaves nothing behind.
#[test]
fn abort_undoes_the_write() {
    let (mut store, n) = one();
    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    write(&mut store, t, n, 2);
    store.abort_transaction(t).unwrap();
    assert_eq!(x(&store, n), int(1), "an aborted write is still visible");
}

/// **Non-repeatable read / snapshot.** A snapshot keeps seeing the value it
/// started with after another transaction commits a change.
#[test]
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
/// started, and nobody but its creator sees it before the commit.
#[test]
fn a_snapshot_does_not_see_a_node_created_after_it() {
    let (mut store, _) = one();
    let snapshot = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let writer = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let fresh = store.txn_create_node(writer, [Label::new("N")]).unwrap();
    assert!(store.get_node_for_txn(writer, fresh).is_some(), "the creator cannot see its own node");
    assert!(store.get_node(fresh).is_none(), "an uncommitted node is visible outside its transaction");
    store.commit_transaction(writer).unwrap();
    assert!(store.get_node_for_txn(snapshot, fresh).is_none(), "the snapshot saw a node created after it");
    assert!(store.get_node(fresh).is_some(), "the committed node is missing");
}

/// **Lost update.** Two transactions write the same value; the second commit
/// must be refused, not silently overwrite the first.
#[test]
fn the_second_of_two_conflicting_commits_is_refused() {
    let (mut store, n) = one();
    let a = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let b = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    write(&mut store, a, n, 2);
    write(&mut store, b, n, 3);
    assert!(store.commit_transaction(a).is_ok());
    assert!(store.commit_transaction(b).is_err(), "a lost update was committed");
    assert_eq!(x(&store, n), int(2), "the refused commit still wrote");
}

/// **Write skew.** Two transactions write different values. Snapshot isolation
/// allows both to commit; serializable would refuse one. Documented as allowed
/// (#1200, 2026-09-15). If serializable becomes a requirement, this test flips.
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

/// **Reads its own writes.** Inside the transaction the write is visible at
/// once; outside it is not.
#[test]
fn a_transaction_reads_its_own_writes() {
    let (mut store, n) = one();
    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    write(&mut store, t, n, 2);
    assert_eq!(x_for(&store, t, n), int(2), "the transaction cannot see its own write");
    assert_eq!(x(&store, n), int(1));
}

/// **A removal is buffered too.** `Null` removes, as outside a transaction.
#[test]
fn a_removal_is_buffered_like_a_write() {
    let (mut store, n) = one();
    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    store.txn_set_node_property(t, n, "x", PropertyValue::Null).unwrap();
    assert_eq!(x_for(&store, t, n), None, "the transaction still sees the removed key");
    assert_eq!(x(&store, n), int(1), "an uncommitted removal is visible outside");
    store.commit_transaction(t).unwrap();
    assert_eq!(x(&store, n), None);
}

/// **An aborted create leaves no node**, and gives its id back.
#[test]
fn an_aborted_create_leaves_no_node() {
    let mut store = GraphStore::new();
    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let fresh = store.txn_create_node(t, [Label::new("N")]).unwrap();
    store.txn_set_node_property(t, fresh, "x", 1i64).unwrap();
    store.abort_transaction(t).unwrap();
    assert!(store.get_node(fresh).is_none(), "an aborted create left a node");
    assert_eq!(store.node_count(), 0);
    assert_eq!(store.create_node("N"), fresh, "the reserved id was not returned");
}

/// **A commit that fails part-way changes nothing.** The buffer is applied at
/// commit, where a unique constraint can still refuse one write; the writes
/// already applied are put back and the created node is removed.
#[test]
fn a_commit_refused_by_a_constraint_changes_nothing() {
    let mut store = GraphStore::new();
    let query = parse_query("CREATE CONSTRAINT FOR (n:K) REQUIRE n.id IS UNIQUE").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string()).execute(&query).unwrap();
    let p = store.create_node("K");
    store.set_node_property("default", p, "id", 1i64).unwrap();

    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let q = store.txn_create_node(t, [Label::new("K")]).unwrap();
    store.txn_set_node_property(t, q, "name", "q").unwrap();
    store.txn_set_node_property(t, q, "id", 1i64).unwrap();
    store.txn_set_node_property(t, p, "note", "touched").unwrap();

    assert!(store.commit_transaction(t).is_err(), "the duplicate id was committed");
    assert!(store.get_node(q).is_none(), "the refused commit left its node");
    assert_eq!(store.node_property(p, "note"), None, "the refused commit left a write");
    assert_eq!(store.node_property(p, "id"), int(1));
    assert_eq!(store.active_transactions[&t].status, TxnStatus::Aborted);
}
