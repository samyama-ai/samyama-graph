//! A deleted edge's adjacency entry survives in the frozen CSR, and the only
//! thing hiding it is a tombstone that edge-id reuse overwrites.
//!
//! `delete_edge` removes the entry from the **write buffer** (`retain`), but the
//! frozen segments are immutable — nothing rewrites them. What keeps the stale
//! entry invisible is `edge_type_ids[id] = EDGE_TYPE_UNSET`, checked per edge by
//! `edge_type_matches`, which is why even a wildcard walk pays for that probe.
//!
//! The same `delete_edge` pushes the id onto `free_edge_ids`, and `create_edge`
//! pops from it. So the next edge created takes the dead edge's id and writes a
//! real type into the slot the tombstone was using — and the stale entry becomes
//! visible again, pointing at the endpoints of an edge that no longer exists.

use samyama::graph::GraphStore;

/// Baseline: while the id stays free, the tombstone does its job.
#[test]
fn a_deleted_edge_is_invisible_while_its_id_is_unused() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let e = store.create_edge(a, b, "R").expect("edge");

    store.compact_adjacency();
    store.delete_edge(e).expect("delete");

    let mut seen = Vec::new();
    store.for_each_outgoing_neighbor(a, None, |t, _| seen.push(t));
    assert!(seen.is_empty(), "a-[:R]->b was deleted, yet a still has {seen:?}");
}

/// The defect: creating any edge afterwards reuses the id and un-hides it.
#[test]
fn a_deleted_edge_does_not_reappear_when_its_id_is_reused() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let c = store.create_node("N");
    let d = store.create_node("N");
    let e = store.create_edge(a, b, "R").expect("edge");

    // Compaction is not exotic: snapshot import always compacts.
    store.compact_adjacency();
    store.delete_edge(e).expect("delete");

    // An unrelated edge elsewhere in the graph. This is what used to take the
    // freed id and, with it, overwrite the tombstone hiding the frozen entry.
    let fresh = store.create_edge(c, d, "R").expect("edge");
    assert_ne!(
        fresh, e,
        "a frozen edge's id must not be recycled: reuse overwrites the \
         `EDGE_TYPE_UNSET` tombstone that is the only thing hiding its \
         immutable adjacency entry"
    );

    let mut seen = Vec::new();
    store.for_each_outgoing_neighbor(a, None, |t, _| seen.push(t));
    assert!(
        seen.is_empty(),
        "`a` is connected to nothing — its only edge was deleted — but the stale \
         frozen entry became visible again when the id was reused: {seen:?}"
    );

    // And the other way round: the new edge must not be reachable from `a`.
    let mut typed = Vec::new();
    let r = store.edge_type_id(&samyama::graph::EdgeType::new("R")).expect("type");
    store.for_each_outgoing_neighbor(a, Some(&[r]), |t, _| typed.push(t));
    assert!(typed.is_empty(), "typed walk from `a` also sees the stale entry: {typed:?}");
}
