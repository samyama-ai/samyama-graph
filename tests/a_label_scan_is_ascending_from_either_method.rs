//! Both label-scan spellings return ascending node ids, from the same structure
//! (#1448, #1507).
//!
//! # What was wrong
//!
//! `label_index` is a `HashMap<Label, HashSet<NodeId>>` and `std`'s hasher is
//! keyed per `HashSet` instance, so its iteration order differs on every store.
//! Two methods read it, three lines apart, and each handled that differently:
//!
//! | method | before |
//! |---|---|
//! | `node_ids_by_label(label, Some(n))` | bitset — ascending, since #1364 |
//! | `node_ids_by_label(label, None)` | **hash order**, while the doc above it said "ascending, with or without a limit" |
//! | `get_nodes_by_label(label)` | **hash order**, with no claim either way |
//!
//! Nothing had broken through the query engine, because `NodeScanOperator`
//! sorts the unlimited result afterwards and says so. The guarantee was the
//! caller's rather than the function's, and callers that were not the query
//! engine did not have it: `samyama-sdk`'s PCA builds its feature matrix by
//! position in `get_nodes_by_label`'s vector, so the same data gave a different
//! row order per process, and the graph-sample handler strides across the same
//! vector, so the same graph sampled twice returned different nodes.
//!
//! # Why the bitset rather than a sort
//!
//! Sorting buys the ordering and pays for it: 11.4 ms against a 13.3 ms scan on
//! a 1M-node label, 86%. The bitset is ascending by construction, is already
//! built for the expand's membership test, and is **2.8x faster** than the hash
//! path — 13.3 ms to 4.8 ms at 1M. The guarantee comes from the structure, so a
//! caller nobody has audited gets it too.

use samyama::graph::{GraphStore, Label};

/// A store with `n` nodes carrying `P`, built the same way every time.
fn store(n: u64) -> GraphStore {
    let mut s = GraphStore::new();
    for _ in 0..n {
        s.create_node("P");
    }
    s
}

fn ids_via_nodes(s: &GraphStore) -> Vec<u64> {
    s.get_nodes_by_label(&Label::new("P")).iter().map(|x| x.id.as_u64()).collect()
}

fn ids_via_ids(s: &GraphStore, limit: Option<usize>) -> Vec<u64> {
    s.node_ids_by_label(&Label::new("P"), limit).iter().map(|x| x.as_u64()).collect()
}

fn ascending(v: &[u64]) -> bool {
    v.windows(2).all(|w| w[0] < w[1])
}

#[test]
fn both_methods_are_ascending() {
    let s = store(200);
    assert!(ascending(&ids_via_nodes(&s)), "get_nodes_by_label");
    assert!(ascending(&ids_via_ids(&s, None)), "node_ids_by_label(.., None)");
    assert!(ascending(&ids_via_ids(&s, Some(50))), "node_ids_by_label(.., Some(50))");
}

#[test]
fn the_order_is_the_same_on_every_store() {
    // The defect's signature. Twelve stores built from identical data gave
    // twelve different orders before this; `all_nodes()`, a flattened `Vec`,
    // gave one throughout.
    let mut by_nodes = std::collections::HashSet::new();
    let mut by_ids = std::collections::HashSet::new();
    for _ in 0..12 {
        let s = store(64);
        by_nodes.insert(ids_via_nodes(&s));
        by_ids.insert(ids_via_ids(&s, None));
    }
    assert_eq!(by_nodes.len(), 1, "get_nodes_by_label gave {} distinct orders over 12 identical stores", by_nodes.len());
    assert_eq!(by_ids.len(), 1, "node_ids_by_label gave {} distinct orders over 12 identical stores", by_ids.len());
}

#[test]
fn a_limited_scan_is_a_prefix_of_the_unlimited_one() {
    // What #1364 is about: `LIMIT k` must be the first k of the same order the
    // unlimited scan produces, or SKIP/LIMIT paging without ORDER BY can skip a
    // row or return one twice.
    let s = store(300);
    let all = ids_via_ids(&s, None);
    for k in [1usize, 7, 64, 65, 299, 300, 400] {
        let got = ids_via_ids(&s, Some(k));
        assert_eq!(got.len(), k.min(all.len()), "limit {k} returned {} ids", got.len());
        assert_eq!(got[..], all[..got.len()], "limit {k} is not a prefix of the full scan");
    }
}

#[test]
fn the_two_methods_agree_with_each_other() {
    let s = store(150);
    assert_eq!(ids_via_nodes(&s), ids_via_ids(&s, None));
}

#[test]
fn a_limit_of_zero_and_a_missing_label_are_both_empty() {
    let s = store(10);
    assert!(ids_via_ids(&s, Some(0)).is_empty());
    assert!(s.node_ids_by_label(&Label::new("NoSuch"), None).is_empty());
    assert!(s.get_nodes_by_label(&Label::new("NoSuch")).is_empty());
}

#[test]
fn deleting_nodes_leaves_the_survivors_ascending_and_consistent() {
    // The bitset is cached; a delete has to invalidate it or the scan returns
    // an id that is no longer there.
    let mut s = store(40);
    let victims: Vec<_> = s.get_nodes_by_label(&Label::new("P")).iter().map(|n| n.id).step_by(3).collect();
    for id in &victims {
        s.delete_node("default", *id).expect("delete");
    }
    let left = ids_via_nodes(&s);
    assert!(ascending(&left), "survivors are not ascending");
    assert_eq!(left, ids_via_ids(&s, None), "the two methods disagree after deletes");
    for id in &victims {
        assert!(!left.contains(&id.as_u64()), "deleted node {id:?} still in the scan");
    }
}
