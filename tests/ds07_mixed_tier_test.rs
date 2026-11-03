//! DS-07 Phase 1: mixed-tier adjacency integration test.
//!
//! Verifies the hybrid CSR + write-buffer contract end-to-end:
//!   1. snapshot import lands edges in the frozen (CSR) tier,
//!   2. subsequent CREATE edges land in the write buffer,
//!   3. queries transparently see both tiers.

use samyama::graph::{GraphStore, Label, NodeId};
use samyama::snapshot::{export_tenant, import_tenant};
use samyama::QueryEngine;

/// Build a small graph and export it to a .sgsnap byte blob.
fn build_and_export_snapshot() -> Vec<u8> {
    let mut store = GraphStore::new();
    let alice = store.create_node("Person");
    store.get_node_mut(alice).unwrap().set_property("name", "Alice");
    let bob = store.create_node("Person");
    store.get_node_mut(bob).unwrap().set_property("name", "Bob");
    let carol = store.create_node("Person");
    store.get_node_mut(carol).unwrap().set_property("name", "Carol");

    store.create_edge(alice, bob, "KNOWS").unwrap();
    store.create_edge(bob, carol, "KNOWS").unwrap();

    let mut buf = Vec::new();
    export_tenant(&store, &mut buf).expect("export");
    buf
}

#[test]
fn snapshot_import_populates_frozen_tier_not_buffer() {
    let bytes = build_and_export_snapshot();
    let mut store = GraphStore::new();
    import_tenant(&mut store, bytes.as_slice()).expect("import");

    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_edges, 2, "imported edges must land in frozen tier");
    assert_eq!(stats.buffer_edges, 0, "write buffer must be empty after import");
    assert!(stats.frozen_segments >= 1);
}

#[test]
fn post_import_creates_go_to_write_buffer() {
    let bytes = build_and_export_snapshot();
    let mut store = GraphStore::new();
    import_tenant(&mut store, bytes.as_slice()).expect("import");

    let alice_id = find_person_by_name(&store, "Alice").expect("Alice in import");

    let dave = store.create_node("Person");
    store.get_node_mut(dave).unwrap().set_property("name", "Dave");
    store.create_edge(alice_id, dave, "KNOWS").unwrap();

    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_edges, 2, "frozen tier must not grow on CREATE");
    assert_eq!(stats.buffer_edges, 1, "CREATE must go to write buffer");
}

fn find_person_by_name(store: &GraphStore, name: &str) -> Option<NodeId> {
    // Imports store properties in the columnar column store, not on the
    // Node struct — scan that directly.
    for node in store.get_nodes_by_label(&Label::new("Person")) {
        let pv = store.node_columns.get_property(node.id.as_u64() as usize, "name");
        if pv.as_string().map_or(false, |s| s == name) {
            return Some(node.id);
        }
    }
    None
}

#[test]
fn match_query_sees_both_tiers() {
    let bytes = build_and_export_snapshot();
    let mut store = GraphStore::new();
    import_tenant(&mut store, bytes.as_slice()).expect("import");

    let carol_id = find_person_by_name(&store, "Carol").expect("Carol in import");

    let dave = store.create_node("Person");
    store.get_node_mut(dave).unwrap().set_property("name", "Dave");
    store.create_edge(carol_id, dave, "KNOWS").unwrap();

    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_edges, 2);
    assert_eq!(stats.buffer_edges, 1);

    let engine = QueryEngine::new();
    let result = engine
        .execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN d.name AS name",
            &store,
        )
        .expect("query");

    assert_eq!(result.len(), 1, "expected one 3-hop path spanning both tiers");
    let name = result.records[0]
        .get("name")
        .unwrap()
        .as_property()
        .unwrap()
        .as_string()
        .unwrap()
        .to_string();
    assert_eq!(name, "Dave");
}
