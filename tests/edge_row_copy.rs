//! A relationship property is stored once, in the column (#545, step 5b of
//! #1200).
//!
//! `set_edge_property_sparse` (the setter CREATE, MERGE and SET use),
//! `set_edge_properties_sparse` and `create_edge_with_properties` wrote every
//! value to the column and to the edge's row map. Store writes now leave the
//! row alone; only `get_edge_properties_mut` still writes it, and every read
//! falls back to it. What a caller observes must not change: `edge_property`,
//! the map `get_edge` builds, and history at an earlier version.

use samyama::graph::{EdgeId, GraphStore, PropertyMap, PropertyValue};

fn int(i: i64) -> Option<PropertyValue> {
    Some(PropertyValue::Integer(i))
}

fn one_edge() -> (GraphStore, EdgeId) {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let e = store.create_edge(a, b, "R").unwrap();
    (store, e)
}

fn row_is_empty(store: &GraphStore, e: EdgeId) -> bool {
    store.get_edge_properties(e).is_none_or(|p| p.is_empty())
}

#[test]
fn a_property_write_keeps_no_row_copy() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.set_edge_property(e, "w", 2i64).unwrap();

    assert!(row_is_empty(&store, e), "the row holds a second copy");
    assert_eq!(store.edge_property(e, "w"), int(2));
    assert_eq!(store.get_edge(e).unwrap().properties.get("w").cloned(), int(2));
}

#[test]
fn create_with_properties_keeps_no_row_copy() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let mut props = PropertyMap::new();
    props.insert("w".to_string(), PropertyValue::Integer(1));
    let e = store.create_edge_with_properties(a, b, "R", props).unwrap();

    assert!(row_is_empty(&store, e), "the row holds a second copy");
    assert_eq!(store.get_edge(e).unwrap().properties.get("w").cloned(), int(1));
}

#[test]
fn a_store_write_supersedes_a_value_written_to_the_row_directly() {
    let (mut store, e) = one_edge();
    store.get_edge_properties_mut(e).unwrap().insert("w".to_string(), PropertyValue::Integer(1));
    store.set_edge_property(e, "w", 2i64).unwrap();

    assert_eq!(store.edge_property(e, "w"), int(2));
    assert_eq!(
        store.get_edge(e).unwrap().properties.get("w").cloned(),
        int(2),
        "the edge's map kept the row's superseded value"
    );
}

#[test]
fn a_row_only_value_is_still_read() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.get_edge_properties_mut(e).unwrap().insert("y".to_string(), PropertyValue::Integer(7));

    let edge = store.get_edge(e).unwrap();
    assert_eq!(store.edge_property(e, "y"), int(7));
    assert_eq!(edge.properties.get("y").cloned(), int(7));
    assert_eq!(edge.properties.get("w").cloned(), int(1));
}

#[test]
fn history_keeps_a_value_held_only_in_the_column() {
    let (mut store, e) = one_edge();
    store.set_edge_property(e, "w", 1i64).unwrap();
    store.current_version = 2;
    store.set_edge_property(e, "w", 2i64).unwrap();

    let at = |v| store.get_edge_at_version(e, v).and_then(|edge| edge.properties.get("w").cloned());
    assert_eq!(at(1), int(1), "history lost a value the row never held");
    assert_eq!(at(2), int(2));
}
