//! `get_edges_by_type` returns edges in ascending edge id, the same on every
//! store built from the same data (#1509).
//!
//! # What was wrong
//!
//! `edge_type_index` is a `HashMap<EdgeType, HashSet<EdgeId>>` and `std`'s
//! hasher is keyed per `HashSet` instance, so the set's iteration order differs
//! between stores and between processes. `get_edges_by_type` collected straight
//! from that set, so the same graph gave a different edge order every time.
//!
//! This is the third member of the class #1448 opened: `get_nodes_by_label`,
//! `node_ids_by_label(.., None)`, and this. The two node-side ones were fixed by
//! reading the label bitset, which is ascending by construction. There is no
//! equivalent bitset for edge types, so this one sorts.
//!
//! # What it did and did not break
//!
//! No wrong answer was found. `type_adjacency_from_type_index` already sorts
//! its rows, and `Poset::from_edges` interns ascending since #1511, so the two
//! order-sensitive consumers had each defended themselves locally. What was
//! left was the function's own contract: `src/http/handler.rs` and
//! `src/query/executor/operator.rs` iterate the returned vector directly, so an
//! unordered result is a result whose row order no caller can rely on and no
//! test can pin.
//!
//! # What it costs
//!
//! Nothing measurable. 1M KNOWS edges, release, three runs each, same host and
//! same binary but for this function:
//!
//! | | run 1 | run 2 | run 3 |
//! |---|---|---|---|
//! | hash order | 117.4 ms | 88.8 ms | 89.4 ms |
//! | sorted | 100.5 ms | 89.4 ms | 93.5 ms |
//!
//! Both first runs are cold and both settle to ~89-93 ms. The sort is a
//! 1M-element `sort_unstable_by_key` on `u64`; the `get_edge` lookups it sits
//! beside dominate it, so the ordering is inside the noise of the call it is
//! part of. The label case chose a bitset over a sort because there the sort
//! was 86% of the scan (#1448); here there is nothing to avoid.

use samyama::graph::{EdgeType, GraphStore};

/// A store with `n` KNOWS edges over `n + 1` nodes, built the same way every
/// time.
fn store(n: u64) -> GraphStore {
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..=n).map(|_| s.create_node("P")).collect();
    for i in 0..n as usize {
        s.create_edge(ids[i], ids[i + 1], "KNOWS").expect("create_edge");
    }
    s
}

fn edge_ids(s: &GraphStore, t: &str) -> Vec<u64> {
    s.get_edges_by_type(&EdgeType::new(t)).iter().map(|e| e.id.as_u64()).collect()
}

#[test]
fn the_scan_is_ascending_by_edge_id() {
    let ids = edge_ids(&store(200), "KNOWS");
    assert_eq!(ids.len(), 200);
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "not ascending: {:?}", &ids[..8.min(ids.len())]);
}

#[test]
fn the_order_is_the_same_on_every_store() {
    // The defect's signature, and the measurement in #1509: twelve stores built
    // from identical data gave twelve different edge orders.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..12 {
        seen.insert(edge_ids(&store(64), "KNOWS"));
    }
    assert_eq!(seen.len(), 1, "12 identical stores gave {} distinct edge orders", seen.len());
}

#[test]
fn deleting_edges_leaves_the_survivors_ascending() {
    let mut s = store(40);
    let victims: Vec<_> = s
        .get_edges_by_type(&EdgeType::new("KNOWS"))
        .iter()
        .map(|e| e.id)
        .step_by(3)
        .collect();
    for id in &victims {
        s.delete_edge(*id).expect("delete_edge");
    }
    let left = edge_ids(&s, "KNOWS");
    assert!(left.windows(2).all(|w| w[0] < w[1]), "survivors are not ascending");
    for id in &victims {
        assert!(!left.contains(&id.as_u64()), "deleted edge {id:?} is still in the scan");
    }
}

#[test]
fn two_types_do_not_bleed_into_each_other() {
    let mut s = GraphStore::new();
    let a = s.create_node("P");
    let b = s.create_node("P");
    let k = s.create_edge(a, b, "KNOWS").expect("KNOWS");
    let f = s.create_edge(b, a, "FOLLOWS").expect("FOLLOWS");
    assert_eq!(edge_ids(&s, "KNOWS"), vec![k.as_u64()]);
    assert_eq!(edge_ids(&s, "FOLLOWS"), vec![f.as_u64()]);
    assert!(edge_ids(&s, "NoSuch").is_empty());
}

/// What the ordering costs. Run with
/// `cargo test --release --test an_edge_type_scan_is_ascending -- --ignored --nocapture`.
#[test]
#[ignore = "timing; release only"]
fn what_the_sort_costs() {
    use std::time::Instant;
    let n = 1_000_000u64;
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..=n).map(|_| s.create_node("P")).collect();
    for i in 0..n as usize {
        s.create_edge(ids[i], ids[i + 1], "KNOWS").expect("create_edge");
    }
    let t = EdgeType::new("KNOWS");
    for _ in 0..3 {
        let start = Instant::now();
        let got = s.get_edges_by_type(&t);
        println!("get_edges_by_type over {} edges: {:?}", got.len(), start.elapsed());
    }
}
