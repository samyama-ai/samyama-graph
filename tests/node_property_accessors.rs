//! The two node-property accessors answer different questions (#1313).
//!
//! `Node::get_property` / `Node::inline_property` read the node's own inline
//! map. `GraphStore::node_property` reads the columnar store first and falls
//! back to inline. The Cypher write path stores columnar (ADR-021), so a
//! property written through Cypher is invisible to the first and visible to
//! the second.
//!
//! That asymmetry had no test, which is why it survived long enough to cost
//! the parity exporter its `or.solve` check: both properties came back `None`,
//! the export wrote `NaN`, JSON wrote `null`, the comparator died on it, and
//! the harness then read an eleven-day-old report as today's result. One
//! silent `None` at the bottom of that chain.
//!
//! These tests pin the behaviour rather than assert it is good. While a `Node`
//! is a detached value with no store handle it cannot do better, and a test
//! that says so is the difference between a documented boundary and a trap.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

#[test]
fn a_cypher_written_property_is_invisible_to_the_inline_accessor() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:Item {cost: 7.0})", &mut store, "default")
        .unwrap();

    let nodes = store.all_nodes();
    let node = nodes[0];

    assert_eq!(
        node.inline_property("cost"),
        None,
        "the Cypher write path stores columnar; the inline map is empty"
    );
    assert_eq!(
        node.get_property("cost"),
        None,
        "get_property is the same read under an older name"
    );
    assert_eq!(
        store.node_property(node.id, "cost"),
        Some(PropertyValue::Float(7.0)),
        "the store reads both sides, and this is the property the user wrote"
    );
}

#[test]
fn a_cypher_set_is_invisible_to_the_inline_accessor_too() {
    // CREATE and SET take different paths through the executor. Both land
    // columnar, and a test on only one of them would leave half the surface
    // unpinned.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:Item {n: 1})", &mut store, "default")
        .unwrap();
    engine
        .execute_mut("MATCH (i:Item) SET i.tag = 'x'", &mut store, "default")
        .unwrap();

    let nodes = store.all_nodes();
    let node = nodes[0];
    assert_eq!(node.inline_property("tag"), None);
    assert_eq!(
        store.node_property(node.id, "tag"),
        Some(PropertyValue::String("x".into()))
    );
}

#[test]
fn a_rust_api_write_is_visible_to_both() {
    // The control. Without it a reader could conclude the inline accessor is
    // simply broken, when what it does is read one of two stores.
    let mut store = GraphStore::new();
    let id = store.create_node("Item");
    store
        .get_node_mut(id)
        .unwrap()
        .set_property("cost", PropertyValue::Float(7.0));

    let node = store.get_node(id).unwrap();
    assert_eq!(
        node.inline_property("cost"),
        Some(&PropertyValue::Float(7.0))
    );
    assert_eq!(
        store.node_property(id, "cost"),
        Some(PropertyValue::Float(7.0))
    );
}

#[test]
fn the_store_accessor_is_the_one_that_answers_for_both_write_paths() {
    // The recommendation, as a test: one accessor gets it right whichever way
    // the property was written, and it is the one callers should reach for.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:Item {viaCypher: 1})", &mut store, "default")
        .unwrap();
    let rust_id = store.create_node("Item");
    store
        .get_node_mut(rust_id)
        .unwrap()
        .set_property("viaRust", PropertyValue::Integer(2));

    let cypher_id = store
        .all_nodes()
        .iter()
        .find(|n| n.id != rust_id)
        .unwrap()
        .id;

    assert_eq!(
        store.node_property(cypher_id, "viaCypher"),
        Some(PropertyValue::Integer(1))
    );
    assert_eq!(
        store.node_property(rust_id, "viaRust"),
        Some(PropertyValue::Integer(2))
    );
}
