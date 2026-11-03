//! A relationship deleted after compaction must not come back from a snapshot
//! (samyama-graph#1096).
//!
//! `compact_adjacency` freezes adjacency into immutable CSR segments. A
//! relationship deleted afterwards keeps its entry there, hidden by an
//! `EDGE_TYPE_UNSET` tombstone. Export walked the frozen entries, and for a
//! dead one `get_edge_type` missed, so it wrote the relationship with
//! `edge_type: ""`. Import passed `""` to `create_edge_stub`, which interns it
//! as a type: the deleted relationship came back, live, under a type no
//! `-[:TYPE]->` pattern can match, and the snapshot header counted it.

use samyama::graph::GraphStore;
use samyama::snapshot::{export_tenant, import_tenant};

/// Three people, three relationships, compacted, then one deleted.
fn compacted_then_deleted() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let c = store.create_node("P");
    store.create_edge(a, b, "R").unwrap();
    let bc = store.create_edge(b, c, "R").unwrap();
    store.create_edge(a, c, "S").unwrap();
    store.compact_adjacency();
    store.delete_edge(bc).unwrap();
    assert_eq!(store.edge_count(), 2);
    store
}

fn round_trip(store: &GraphStore) -> GraphStore {
    let mut buf = Vec::new();
    export_tenant(store, &mut buf).expect("export");
    let mut restored = GraphStore::new();
    import_tenant(&mut restored, buf.as_slice()).expect("import");
    restored
}

fn types(store: &GraphStore) -> Vec<String> {
    let mut t: Vec<String> = store
        .all_edges()
        .iter()
        .map(|e| e.edge_type.as_str().to_string())
        .collect();
    t.sort();
    t
}

/// Found by the fixture above before any snapshot was taken: `edge_count`
/// summed frozen entries, a deleted one's included, and read 3.
#[test]
fn the_store_does_not_count_a_relationship_deleted_after_compaction() {
    let mut store = compacted_then_deleted();
    assert_eq!(store.edge_count(), 2);
    // After a merge the dead entry is gone from the frozen tier as well; the
    // count must not change either way.
    store.merge_frozen_segments();
    assert_eq!(store.edge_count(), 2);
}

#[test]
fn a_relationship_deleted_after_compaction_stays_deleted() {
    let restored = round_trip(&compacted_then_deleted());
    assert_eq!(restored.edge_count(), 2, "the deleted relationship came back: {:?}", types(&restored));
    assert_eq!(types(&restored), vec!["R".to_string(), "S".to_string()]);
}

#[test]
fn no_relationship_is_exported_without_a_type() {
    let restored = round_trip(&compacted_then_deleted());
    assert!(
        !types(&restored).iter().any(|t| t.is_empty()),
        "a relationship was restored with an empty type: {:?}",
        types(&restored)
    );
}

/// The same with the write buffer holding the live relationships: a delete
/// there removes the entry outright, which is why only the frozen tier leaked.
#[test]
fn an_uncompacted_delete_round_trips_too() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let e = store.create_edge(a, b, "R").unwrap();
    store.create_edge(b, a, "R").unwrap();
    store.delete_edge(e).unwrap();
    let restored = round_trip(&store);
    assert_eq!(restored.edge_count(), 1);
    assert_eq!(types(&restored), vec!["R".to_string()]);
}
