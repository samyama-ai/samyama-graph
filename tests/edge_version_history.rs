//! Relationship property history, through every setter, after the version
//! log stopped copying the property map on each write (samyama-graph#602).
//!
//! The log's newest entry now carries no properties while its version is the
//! current one -- nothing reads them then -- and takes its copy only when a
//! write lands at a later version. So every path that changes an edge's
//! properties has to take that copy *before* it changes them, or a read at the
//! older version would see the newer value. Only `set_edge_property` recorded
//! history before; the other setters must not lose what it recorded.
//!
//! `current_version` is set directly, as the store's own MVCC tests do: it is
//! what `commit_transaction` advances.

use samyama::graph::{EdgeId, GraphStore, Mutation, PropertyValue};

fn one_edge() -> (GraphStore, EdgeId) {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let e = store.create_edge(a, b, "R").unwrap();
    (store, e)
}

fn at(store: &GraphStore, e: EdgeId, version: u64, key: &str) -> Option<PropertyValue> {
    store
        .get_edge_at_version(e, version)
        .and_then(|edge| edge.properties.get(key).cloned())
}

fn int(i: i64) -> Option<PropertyValue> {
    Some(PropertyValue::Integer(i))
}

#[test]
fn a_value_written_before_a_commit_is_still_read_at_that_version() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.current_version = 2;
    store.set_edge_property(e, "w", 2i64).unwrap();

    assert_eq!(at(&store, e, 1, "w"), int(1));
    assert_eq!(at(&store, e, 2, "w"), int(2));
    assert_eq!(store.edge_property(e, "w"), int(2));
}

#[test]
fn three_versions_each_read_their_own_value() {
    let (mut store, e) = one_edge();
    for v in 1..=3u64 {
        store.current_version = v;
        store.set_edge_property(e, "w", v as i64 * 10).unwrap();
    }
    assert_eq!(at(&store, e, 1, "w"), int(10));
    assert_eq!(at(&store, e, 2, "w"), int(20));
    assert_eq!(at(&store, e, 3, "w"), int(30));
}

#[test]
fn every_write_inside_a_version_is_part_of_that_version() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.set_edge_property(e, "x", 5i64).unwrap();
    store.current_version = 2;
    store.set_edge_property(e, "w", 2i64).unwrap();

    assert_eq!(at(&store, e, 1, "w"), int(1));
    assert_eq!(at(&store, e, 1, "x"), int(5));
    assert_eq!(at(&store, e, 2, "x"), int(5));
}

#[test]
fn a_commit_with_no_later_write_reads_the_same_value_at_both_versions() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.current_version = 2;

    assert_eq!(at(&store, e, 1, "w"), int(1));
    assert_eq!(at(&store, e, 2, "w"), int(1));
}

#[test]
fn the_row_map_setter_after_a_commit_keeps_the_older_value() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.current_version = 2;
    store.set_edge_property_sparse(e, "w", 9i64);

    assert_eq!(at(&store, e, 1, "w"), int(1));
    assert_eq!(at(&store, e, 2, "w"), int(9));
}

#[test]
fn the_batch_setter_after_a_commit_keeps_the_older_value() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.current_version = 2;
    store.set_edge_properties_sparse(e, [("w", PropertyValue::Integer(9))]);

    assert_eq!(at(&store, e, 1, "w"), int(1));
    assert_eq!(at(&store, e, 2, "w"), int(9));
}

#[test]
fn a_removal_after_a_commit_keeps_the_older_value() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.current_version = 2;
    store.remove_edge_property(e, "w");

    assert_eq!(at(&store, e, 1, "w"), int(1));
    assert_eq!(at(&store, e, 2, "w"), None);
}

#[test]
fn a_write_through_the_mutable_map_after_a_commit_keeps_the_older_value() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.current_version = 2;
    store
        .get_edge_properties_mut(e)
        .unwrap()
        .insert("w".to_string(), PropertyValue::Integer(7));

    assert_eq!(at(&store, e, 1, "w"), int(1));
}

/// The first version keeps no log entry. An edge untouched through version 2
/// and written at 3 must still read its first value at 1 and at 2.
#[test]
fn an_edge_untouched_through_a_version_reads_its_first_value_there() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.current_version = 2;
    store.current_version = 3;
    store.set_edge_property(e, "w", 3i64).unwrap();

    assert_eq!(at(&store, e, 1, "w"), int(1));
    assert_eq!(at(&store, e, 2, "w"), int(1));
    assert_eq!(at(&store, e, 3, "w"), int(3));
}

/// An edge created after the first version is not one that has been there
/// since the first: its writes at its own version start its history.
#[test]
fn an_edge_created_after_a_commit_keeps_its_own_history() {
    let (mut store, a_edge) = one_edge();
    let (a, b) = store.get_edge_endpoints(a_edge).unwrap();
    store.current_version = 2;
    let e = store.create_edge(a, b, "R").unwrap();
    store.set_edge_property(e, "w", 2i64).unwrap();
    store.current_version = 3;
    store.set_edge_property(e, "w", 3i64).unwrap();

    assert_eq!(at(&store, e, 2, "w"), int(2));
    assert_eq!(at(&store, e, 3, "w"), int(3));
}

/// Setting a property through the transaction-free path never builds a
/// version log: after many writes at the first version, a commit and one more
/// write, history still reads right.
#[test]
fn many_first_version_writes_then_a_commit() {
    let (mut store, e) = one_edge();
    for i in 0..100i64 {
        store.set_edge_property(e, "w", i).unwrap();
    }
    store.current_version = 2;
    store.set_edge_property(e, "w", -1i64).unwrap();

    assert_eq!(at(&store, e, 1, "w"), int(99));
    assert_eq!(at(&store, e, 2, "w"), int(-1));
}

/// It wrote the journal twice: once itself and once inside
/// `set_edge_property_sparse`, which it calls.
#[test]
fn set_edge_property_journals_the_edge_once() {
    let (mut store, e) = one_edge();
    store.enable_write_log();
    let _ = store.take_write_log();
    store.set_edge_property(e, "w", 1i64).unwrap();
    let upserts = store
        .take_write_log()
        .iter()
        .filter(|m| matches!(m, Mutation::EdgeUpserted(id) if *id == e))
        .count();
    assert_eq!(upserts, 1);
}

/// Setting `Null` on a missing edge was an error and setting anything else was
/// not: the value went into the column and the row map under an id no edge
/// holds, where the next edge to take that id would read it.
#[test]
fn setting_a_property_on_a_missing_edge_is_an_error() {
    let (mut store, e) = one_edge();
    let missing = EdgeId::new(e.as_u64() + 1000);
    assert!(store.set_edge_property(missing, "w", 1i64).is_err());
    assert!(store.set_edge_property(missing, "w", PropertyValue::Null).is_err());
    assert_eq!(store.edge_property(missing, "w"), None);
}
