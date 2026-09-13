//! The frozen CSR tier counts its dead entries, and a merge can be asked for
//! when they or the segments pile up (samyama-graph#740).
//!
//! `compact_adjacency` only appends segments. A deleted edge's entry stays in
//! its segment behind a tombstone, and #739 retired its id so reuse could not
//! resurrect it. `merge_frozen_segments` (#751) rebuilds one segment from what
//! is live and releases those ids, but nothing called it, because nothing
//! could say how much of the frozen tier was dead. These tests pin the count
//! and the trigger built on it.

use samyama::graph::{EdgeId, GraphStore, NodeId};

/// `n` edges in a chain, compacted into one frozen segment.
fn frozen_chain(n: usize) -> (GraphStore, Vec<NodeId>, Vec<EdgeId>) {
    let mut store = GraphStore::new();
    let nodes: Vec<NodeId> = (0..=n).map(|_| store.create_node("N")).collect();
    let edges: Vec<EdgeId> = (0..n)
        .map(|i| store.create_edge(nodes[i], nodes[i + 1], "R").unwrap())
        .collect();
    store.compact_adjacency();
    (store, nodes, edges)
}

fn out_degree(store: &GraphStore, node: NodeId) -> usize {
    let mut n = 0;
    store.for_each_outgoing_neighbor(node, None, |_, _| n += 1);
    n
}

#[test]
fn a_deleted_frozen_edge_counts_once_and_a_deleted_buffer_edge_not_at_all() {
    let (mut store, nodes, edges) = frozen_chain(10);
    assert_eq!(store.adjacency_stats().frozen_dead_edges, 0);
    for &e in &edges[..3] {
        store.delete_edge(e).unwrap();
    }
    assert_eq!(store.adjacency_stats().frozen_dead_edges, 3);

    // Created after the compaction, so it lives in the write buffer, where a
    // delete removes the entry outright.
    let fresh = store.create_edge(nodes[0], nodes[5], "R").unwrap();
    store.delete_edge(fresh).unwrap();
    assert_eq!(store.adjacency_stats().frozen_dead_edges, 3);
}

#[test]
fn a_merge_drops_the_dead_entries_and_releases_their_ids() {
    let (mut store, nodes, edges) = frozen_chain(10);
    let dead: Vec<u64> = edges[..4].iter().map(|e| e.as_u64()).collect();
    for &e in &edges[..4] {
        store.delete_edge(e).unwrap();
    }
    store.merge_frozen_segments();

    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_dead_edges, 0);
    assert_eq!(stats.frozen_edges, 6, "only live edges are left in the frozen tier");
    assert_eq!(stats.frozen_segments, 1);
    // Deleted edges no longer appear in a walk; live ones still do.
    assert_eq!(out_degree(&store, nodes[0]), 0);
    assert_eq!(out_degree(&store, nodes[9]), 1);
    // And their ids can be handed out again.
    let reused = store.create_edge(nodes[0], nodes[1], "R").unwrap();
    assert!(dead.contains(&reused.as_u64()), "expected a released id, got {}", reused.as_u64());
}

#[test]
fn merge_if_needed_waits_for_the_dead_fraction() {
    let (mut store, _nodes, edges) = frozen_chain(100);
    for &e in &edges[..10] {
        store.delete_edge(e).unwrap();
    }
    assert!(!store.merge_frozen_segments_if_needed(8, 0.25), "10% dead is under a 25% threshold");
    assert_eq!(store.adjacency_stats().frozen_dead_edges, 10);

    for &e in &edges[10..30] {
        store.delete_edge(e).unwrap();
    }
    assert!(store.merge_frozen_segments_if_needed(8, 0.25), "30% dead is over it");
    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_dead_edges, 0);
    assert_eq!(stats.frozen_edges, 70);
}

#[test]
fn merge_if_needed_merges_too_many_segments() {
    let (mut store, nodes, _edges) = frozen_chain(10);
    for round in 0..3 {
        store.create_edge(nodes[round], nodes[round + 2], "R").unwrap();
        store.compact_adjacency();
    }
    assert_eq!(store.adjacency_stats().frozen_segments, 4);
    assert!(!store.merge_frozen_segments_if_needed(4, 1.0));
    assert!(store.merge_frozen_segments_if_needed(3, 1.0));
    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_segments, 1);
    assert_eq!(stats.frozen_edges, 13);
    assert_eq!(out_degree(&store, nodes[0]), 2);
}

#[test]
fn an_empty_frozen_tier_never_merges() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    store.create_edge(a, b, "R").unwrap();
    assert!(!store.merge_frozen_segments_if_needed(0, 0.0));
    assert_eq!(store.adjacency_stats().frozen_segments, 0);
}

#[test]
fn clear_forgets_the_count() {
    let (mut store, _nodes, edges) = frozen_chain(5);
    store.delete_edge(edges[0]).unwrap();
    store.clear();
    assert_eq!(store.adjacency_stats().frozen_dead_edges, 0);
}
