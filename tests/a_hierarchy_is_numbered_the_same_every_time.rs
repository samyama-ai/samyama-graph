//! The same hierarchy gets the same node numbering in every process.
//!
//! # What was wrong
//!
//! `Poset::from_edges` interns a node the first time it is seen, so the dense
//! index every structure in the poset is built on was a function of the order
//! the edges arrived in. `Poset::from_store` takes those from
//! `get_edges_by_type`, which walks a `HashSet<EdgeId>`, and `std`'s hasher is
//! keyed per `HashSet` instance.
//!
//! Measured over twelve stores built from identical data:
//!
//! ```text
//! before:  12 distinct edge orders,  12 distinct topological orders
//! after:   12 distinct edge orders,   1 topological order
//! ```
//!
//! The edge scan is still hash-ordered — it feeds the expand operators, which
//! are hot, and fixing it there is a separate question. The poset no longer
//! depends on it.
//!
//! # Was anything wrong with the answers?
//!
//! No, and that is worth stating rather than overclaiming. A topological order
//! is not unique, and reachability was correct under any of the twelve. What
//! was unreproducible is `idx()`, `node_at()` and everything derived from the
//! numbering — which matters now that index definitions persist (#1477), since
//! a rebuilt index has to agree with the one it replaced, and it matters for
//! any measurement that compares two builds of one hierarchy.
//!
//! Same class as #1448: a `HashSet` whose iteration order reaches something
//! that assigns positions.

use samyama::graph::{EdgeType, GraphStore, NodeId};
use samyama::index::hierarchy::poset::Poset;

const ET: &str = "SUBCLASS_OF";

/// A chain of `n` classes plus a second branch, built identically every call.
fn store(n: usize) -> GraphStore {
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..n).map(|_| s.create_node("Class")).collect();
    for w in ids.windows(2) {
        s.create_edge(w[0], w[1], ET).expect("edge");
    }
    // A branch, so the topological order has real freedom rather than one
    // legal answer that any construction would land on.
    for i in (0..n.saturating_sub(2)).step_by(3) {
        s.create_edge(ids[i], ids[n - 1], ET).expect("branch");
    }
    s
}

fn fingerprint(s: &GraphStore) -> String {
    let p = Poset::from_store(s, &[EdgeType::new(ET)], false).expect("poset");
    format!("{:?}|{:?}", p.node_ids(), p.topo_up())
}

#[test]
fn twelve_identical_stores_give_one_numbering() {
    let seen: std::collections::HashSet<String> = (0..12).map(|_| fingerprint(&store(30))).collect();
    assert_eq!(
        seen.len(),
        1,
        "the same hierarchy was numbered {} different ways across 12 stores",
        seen.len()
    );
}

#[test]
fn the_numbering_does_not_depend_on_the_order_edges_are_supplied_in() {
    // `from_edges` directly, with the same edges in three different orders.
    let edges: Vec<(NodeId, NodeId)> = (0..20u64)
        .map(|i| (NodeId::new(i), NodeId::new(i + 1)))
        .collect();

    let forward = Poset::from_edges(edges.clone(), std::iter::empty()).expect("forward");
    let mut reversed = edges.clone();
    reversed.reverse();
    let backward = Poset::from_edges(reversed, std::iter::empty()).expect("backward");
    let mut shuffled = edges.clone();
    shuffled.swap(0, 11);
    shuffled.swap(3, 17);
    let shuffled = Poset::from_edges(shuffled, std::iter::empty()).expect("shuffled");

    assert_eq!(forward.node_ids(), backward.node_ids());
    assert_eq!(forward.node_ids(), shuffled.node_ids());
    assert_eq!(forward.topo_up(), backward.topo_up());
    assert_eq!(forward.topo_up(), shuffled.topo_up());
}

#[test]
fn extra_nodes_do_not_move_the_numbering_either() {
    let edges: Vec<(NodeId, NodeId)> = vec![(NodeId::new(5), NodeId::new(6))];
    let a = Poset::from_edges(edges.clone(), vec![NodeId::new(9), NodeId::new(1)]).expect("a");
    let b = Poset::from_edges(edges, vec![NodeId::new(1), NodeId::new(9)]).expect("b");
    assert_eq!(a.node_ids(), b.node_ids());
}

#[test]
fn the_numbering_is_ascending_by_node_id() {
    // Deterministic is the requirement; ascending is what makes it readable and
    // makes two hierarchies comparable by eye.
    let p = Poset::from_store(&store(25), &[EdgeType::new(ET)], false).expect("poset");
    let ids: Vec<u64> = p.node_ids().iter().map(|n| n.as_u64()).collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "not ascending: {ids:?}");
}

#[test]
fn the_poset_still_describes_the_same_graph() {
    // Ordering must not have changed the structure.
    let s = store(30);
    let p = Poset::from_store(&s, &[EdgeType::new(ET)], false).expect("poset");
    assert_eq!(p.n(), 30, "every class is a node");
    assert!(p.m() >= 29, "at least the chain's edges are present, got {}", p.m());
    assert_eq!(p.topo_up().len(), p.n(), "the topological order covers every node");
    let mut seen = vec![false; p.n()];
    for &i in p.topo_up() {
        assert!(!seen[i as usize], "node {i} appears twice in the topological order");
        seen[i as usize] = true;
    }
}

#[test]
fn a_reversed_poset_is_also_deterministic() {
    let s = store(20);
    let a = Poset::from_store(&s, &[EdgeType::new(ET)], true).expect("a");
    let b = Poset::from_store(&s, &[EdgeType::new(ET)], true).expect("b");
    assert_eq!(a.node_ids(), b.node_ids());
    assert_eq!(a.topo_up(), b.topo_up());
}
