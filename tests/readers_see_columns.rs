//! Readers that used to read only the node row copy see columnar values (#545).
//!
//! Snapshot import leaves the row empty for every scalar. Each test here builds
//! that state and asserts a reader still finds the value -- the state #545 will
//! make universal once the row copy stops being written.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;

fn restored(setup: &str) -> GraphStore {
    let engine = QueryEngine::new();
    let mut src = GraphStore::new();
    engine.execute_mut(setup, &mut src, "default").expect(setup);
    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(&src, &mut buf).expect("export");
    let mut dst = GraphStore::new();
    samyama::snapshot::import_tenant(&mut dst, &buf[..]).expect("import");
    dst
}

/// `schema_summary` is the schema text handed to the NLQ prompt. On a restored
/// graph it listed only a node's arrays, because scalars sit in the column.
#[test]
fn schema_summary_lists_scalar_properties_of_a_restored_graph() {
    let store = restored(r#"CREATE (:Doc {title: "a", year: 2024, emb: [0.1, 0.2]})"#);
    let s = store.schema_summary();
    for key in ["title", "year", "emb"] {
        assert!(s.contains(key), "schema omits `{key}` on a restored graph:\n{s}");
    }
}

/// Five keys at most, sorted, so the same graph describes itself the same way
/// on every start -- a HashMap's key order changes per process.
#[test]
fn schema_summary_samples_keys_in_a_stable_order() {
    let store = restored(r#"CREATE (:P {e: 1, d: 2, c: 3, b: 4, a: 5, f: 6})"#);
    let s = store.schema_summary();
    assert!(s.contains(":P has properties: a, b, c, d, e"), "{s}");
}

/// Vector-index discovery registers an index for each (label, property) that
/// holds an embedding. An embedding present only in the column was skipped, and
/// nothing errored: vector search just returned nothing for that label.
#[test]
fn vector_discovery_finds_an_embedding_held_only_in_the_column() {
    let mut store = GraphStore::new();
    let n = store.create_node_with_labels([Label::new("Doc")]);
    store.set_column_property(
        n,
        "emb",
        PropertyValue::Array(vec![PropertyValue::Float(0.1), PropertyValue::Float(0.2)]),
    );
    assert!(
        store.get_node(n).unwrap().properties.is_empty(),
        "precondition: the row must be empty for this test to mean anything"
    );
    assert_eq!(
        store.rebuild_vector_index_full(), 1,
        "an embedding held only in the column registered no vector index"
    );
}
