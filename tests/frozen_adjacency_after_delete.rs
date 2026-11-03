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

/// Merging releases the ids that #739 had to retire.
///
/// `delete_edge` cannot touch an immutable frozen segment, so #739 stopped
/// recycling a frozen edge's id — reuse overwrote the `EDGE_TYPE_UNSET`
/// tombstone that was the only thing hiding its stale entry. That is correct
/// and it leaks ids, and `edge_type_ids` / `edge_endpoints` are indexed by id.
///
/// `merge_frozen_segments` rebuilds the frozen tier from what is live, so
/// nothing references the dead entry any more and the id is safe again (#740).
#[test]
fn merging_frozen_segments_makes_a_retired_id_reusable() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let c = store.create_node("N");
    let d = store.create_node("N");
    let e = store.create_edge(a, b, "R").expect("edge");

    store.compact_adjacency();
    store.delete_edge(e).expect("delete");

    // Before the merge: retired, exactly as #739 requires.
    let fresh = store.create_edge(c, d, "R").expect("edge");
    assert_ne!(fresh, e, "a frozen id must stay retired until the segment is rebuilt");

    store.merge_frozen_segments();

    // After: the stale entry is gone, so the id is safe to hand out again.
    let recycled = store.create_edge(c, d, "R2").expect("edge");
    assert_eq!(recycled, e, "merging should release the id it had to retire");

    // And the point of all of it — `a` still has no edges.
    let mut seen = Vec::new();
    store.for_each_outgoing_neighbor(a, None, |t, _| seen.push(t));
    assert!(seen.is_empty(), "the deleted edge must not reappear after a merge: {seen:?}");
}

/// A merge must not change any answer.
#[test]
fn merging_preserves_every_live_edge() {
    let mut store = GraphStore::new();
    let ns: Vec<_> = (0..30).map(|_| store.create_node("N")).collect();
    let mut edges = Vec::new();
    for (i, &n) in ns.iter().enumerate() {
        for j in 1..4 {
            edges.push(store.create_edge(n, ns[(i + j) % ns.len()], "R").unwrap());
        }
    }
    store.compact_adjacency();
    // A second segment, so the merge has something to merge.
    for (i, &n) in ns.iter().enumerate().take(10) {
        edges.push(store.create_edge(n, ns[(i + 7) % ns.len()], "S").unwrap());
    }
    store.compact_adjacency();
    // Delete a scattering.
    for e in edges.iter().step_by(7) {
        let _ = store.delete_edge(*e);
    }

    let before: Vec<Vec<u64>> = ns
        .iter()
        .map(|&n| {
            let mut v = Vec::new();
            store.for_each_outgoing_neighbor(n, None, |t, _| v.push(t.as_u64()));
            v.sort();
            v
        })
        .collect();

    store.merge_frozen_segments();

    for (i, &n) in ns.iter().enumerate() {
        let mut after = Vec::new();
        store.for_each_outgoing_neighbor(n, None, |t, _| after.push(t.as_u64()));
        after.sort();
        assert_eq!(after, before[i], "merge changed the neighbours of {n:?}");
    }
}
