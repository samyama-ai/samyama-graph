//! A node can be found by an external key held under a unique constraint (#542).
//!
//! The issue: an ETL pipeline that syncs a relational table into the graph
//! every day has a primary key for each row and no way to ask the store which
//! node holds it, so it kept its own key -> `NodeId` map across runs.
//! `GraphStore::find_node_by_unique` answers that from the unique-constraint
//! index, which the store already maintains to enforce the constraint.
//!
//! The index only answers correctly if every write keeps it current, and three
//! did not: a key changed by `set_node_property` stayed registered to the node
//! under its old value, a deleted node's keys were never removed (the removal
//! read the row copy, which has been empty since #1188), and
//! `create_node_with_properties` never registered its keys at all. The first
//! two refused the next node to take the key as a duplicate of a node that no
//! longer held it -- the delete-and-reload case of the same pipeline. The
//! tests after the lookup ones pin each of those.
//!
//! That the lookup visits no other node is counted, not timed, in
//! `graph::store::tests::test_find_node_by_unique_reads_only_the_node_it_returns`,
//! where the column-read counter is visible.

use samyama::graph::{GraphError, GraphStore, Label, NodeId, PropertyMap, PropertyValue};

const T: &str = "default";

fn key(k: i64) -> PropertyValue {
    PropertyValue::Integer(k)
}

fn acct() -> Label {
    Label::new("Acct")
}

/// A store with `:Acct(krid)` unique and one node per key.
fn store_with(keys: &[i64]) -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    s.create_unique_constraint(&acct(), "krid").unwrap();
    let ids = keys
        .iter()
        .map(|&k| {
            let id = s.create_node("Acct");
            s.set_node_property(T, id, "krid", k).unwrap();
            id
        })
        .collect();
    (s, ids)
}

fn find(s: &GraphStore, k: i64) -> Option<NodeId> {
    s.find_node_by_unique(&acct(), "krid", &key(k))
        .expect("constrained lookup")
}

#[test]
fn a_key_finds_the_node_that_holds_it() {
    let (s, ids) = store_with(&[10, 20, 30]);
    assert_eq!(find(&s, 10), Some(ids[0]));
    assert_eq!(find(&s, 20), Some(ids[1]));
    assert_eq!(find(&s, 30), Some(ids[2]));
}

#[test]
fn a_missing_key_is_none() {
    let (s, _) = store_with(&[10]);
    assert_eq!(find(&s, 11), None);
    // Exact equality, as the index keys it.
    assert_eq!(
        s.find_node_by_unique(&acct(), "krid", &PropertyValue::Float(10.0))
            .unwrap(),
        None
    );
}

#[test]
fn without_a_unique_constraint_it_refuses_rather_than_scans() {
    let mut s = GraphStore::new();
    let id = s.create_node("Acct");
    s.set_node_property(T, id, "krid", 1i64).unwrap();
    // A plain index is not enough: it does not promise one node per key.
    s.create_property_index(&acct(), "krid");

    match s.find_node_by_unique(&acct(), "krid", &key(1)) {
        Err(GraphError::NoUniqueConstraint { label, property }) => {
            assert_eq!((label.as_str(), property.as_str()), ("Acct", "krid"));
        }
        other => panic!("expected NoUniqueConstraint, got {other:?}"),
    }
    // A constraint on another property of the label does not cover this one.
    s.create_unique_constraint(&acct(), "email").unwrap();
    assert!(s.find_node_by_unique(&acct(), "krid", &key(1)).is_err());
}

#[test]
fn a_changed_key_is_found_under_the_new_value_only() {
    let (mut s, ids) = store_with(&[10]);
    s.set_node_property(T, ids[0], "krid", 11i64).unwrap();
    assert_eq!(find(&s, 10), None);
    assert_eq!(find(&s, 11), Some(ids[0]));

    // The old key is free again.
    let other = s.create_node("Acct");
    s.set_node_property(T, other, "krid", 10i64)
        .expect("the old key is still registered to the node that gave it up");
    assert_eq!(find(&s, 10), Some(other));
}

#[test]
fn a_removed_key_is_gone_and_free() {
    let (mut s, ids) = store_with(&[10]);
    s.remove_node_property(ids[0], "krid");
    assert_eq!(find(&s, 10), None);

    let other = s.create_node("Acct");
    s.set_node_property(T, other, "krid", 10i64)
        .expect("the removed key is still registered");
    assert_eq!(find(&s, 10), Some(other));
}

#[test]
fn a_deleted_node_is_not_found_and_its_key_can_be_reloaded() {
    let (mut s, ids) = store_with(&[10, 20]);
    s.delete_node(T, ids[0]).unwrap();
    assert_eq!(find(&s, 10), None);
    assert_eq!(find(&s, 20), Some(ids[1]));

    // The daily reload: the row comes back as a new node with the same key.
    // Use a label-less node first so the deleted id is not simply reused by the
    // reloaded row, which would hide a stale entry.
    let _filler = s.create_node("Other");
    let reloaded = s.create_node("Acct");
    assert_ne!(reloaded, ids[0]);
    s.set_node_property(T, reloaded, "krid", 10i64)
        .expect("a deleted node's key is still registered");
    assert_eq!(find(&s, 10), Some(reloaded));
}

#[test]
fn the_same_key_under_two_labels_finds_each_labels_node() {
    let mut s = GraphStore::new();
    s.create_unique_constraint(&acct(), "krid").unwrap();
    s.create_unique_constraint(&Label::new("Branch"), "krid")
        .unwrap();
    let a = s.create_node("Acct");
    s.set_node_property(T, a, "krid", 7i64).unwrap();
    let b = s.create_node("Branch");
    s.set_node_property(T, b, "krid", 7i64).unwrap();

    assert_eq!(find(&s, 7), Some(a));
    assert_eq!(
        s.find_node_by_unique(&Label::new("Branch"), "krid", &key(7))
            .unwrap(),
        Some(b)
    );
}

#[test]
fn a_node_created_with_its_properties_is_found() {
    let mut s = GraphStore::new();
    s.create_unique_constraint(&acct(), "krid").unwrap();
    let mut props = PropertyMap::new();
    props.insert("krid".to_string(), PropertyValue::String("R-001".into()));
    let id = s.create_node_with_properties(T, vec![acct()], props);

    let found = s.find_node_by_unique(&acct(), "krid", &PropertyValue::String("R-001".into()));
    assert_eq!(found.unwrap(), Some(id));
    // And the key now conflicts with a write through the checked path.
    let other = s.create_node("Acct");
    assert!(s.set_node_property(T, other, "krid", "R-001").is_err());
}

#[test]
fn a_key_on_nodes_present_before_the_constraint_is_found() {
    let mut s = GraphStore::new();
    let id = s.create_node("Acct");
    s.set_node_property(T, id, "krid", 5i64).unwrap();
    s.create_unique_constraint(&acct(), "krid").unwrap();
    assert_eq!(find(&s, 5), Some(id));
}

#[test]
fn a_node_that_loses_the_label_is_not_found_under_it() {
    let (mut s, ids) = store_with(&[10]);
    s.remove_label_from_node(ids[0], &acct()).unwrap();
    assert_eq!(find(&s, 10), None);
}

#[test]
fn a_rolled_back_delete_is_found_again() {
    let (mut s, ids) = store_with(&[10]);
    s.begin_session_transaction().unwrap();
    s.delete_node(T, ids[0]).unwrap();
    assert_eq!(find(&s, 10), None);
    s.rollback_session_transaction().unwrap();
    assert_eq!(find(&s, 10), Some(ids[0]));
    // The plain index the constraint created is back too: `MATCH` uses it.
    let plain = s.property_index.get_index(&acct(), "krid").unwrap();
    assert_eq!(plain.read().unwrap().get(&key(10)), vec![ids[0]]);
}
