//! Link prediction: how likely is an edge that is not there? (ALGO-01)
//!
//! Three scores over a *pair* of nodes rather than a single node, which is
//! what makes them a different shape from centrality — the answer is a table
//! of candidate pairs, not a column beside the node list.
//!
//! All three read the same signal, shared neighbours, and differ only in how
//! much a shared neighbour is worth:
//!
//! | score | a shared neighbour is worth | so it favours |
//! |---|---|---|
//! | common neighbours | 1 | raw overlap |
//! | Jaccard | 1, divided by the union | overlap *relative to* how connected the pair is |
//! | Adamic–Adar | `1 / ln(degree)` | a shared neighbour that is **rare** |
//!
//! Adamic–Adar is the interesting one. Two people who both know a hub of ten
//! thousand share almost nothing; two who both know someone with three
//! contacts share a great deal. Weighting by the inverse log of the shared
//! neighbour's degree is what encodes that, and it is why Adamic–Adar
//! routinely beats raw overlap on real graphs.
//!
//! All three are defined on the **undirected** neighbourhood — a
//! recommendation does not care which way an edge was written — so direction
//! is collapsed before scoring.

use std::collections::HashSet;

use crate::common::{GraphView, NodeId};

/// One scored candidate pair.
#[derive(Debug, Clone, PartialEq)]
pub struct PairScore {
    pub a: NodeId,
    pub b: NodeId,
    pub score: f64,
}

/// Which score to compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkScore {
    CommonNeighbours,
    Jaccard,
    AdamicAdar,
}

/// The undirected neighbour set of every node, computed once.
///
/// A `HashSet` per node rather than the CSR slices, because every score below
/// needs intersection and union rather than iteration, and doing it on sorted
/// slices for each of `n^2` pairs is the difference between a demo and
/// something usable.
fn neighbour_sets(view: &GraphView) -> Vec<HashSet<usize>> {
    (0..view.node_count)
        .map(|i| {
            let mut s: HashSet<usize> = view.successors(i).iter().copied().collect();
            s.extend(view.predecessors(i).iter().copied());
            // A self-loop is not a shared neighbour, and leaving it in makes a
            // node similar to itself for no reason a caller would want.
            s.remove(&i);
            s
        })
        .collect()
}

fn score_pair(sets: &[HashSet<usize>], u: usize, v: usize, which: LinkScore) -> f64 {
    let (a, b) = (&sets[u], &sets[v]);
    match which {
        LinkScore::CommonNeighbours => a.intersection(b).count() as f64,
        LinkScore::Jaccard => {
            let union = a.union(b).count();
            if union == 0 {
                // Two isolated nodes have no evidence either way. Zero says
                // "no reason to connect them"; 1.0 -- which "identical empty
                // sets" would suggest -- would rank every pair of isolated
                // nodes above every real candidate.
                0.0
            } else {
                a.intersection(b).count() as f64 / union as f64
            }
        }
        LinkScore::AdamicAdar => {
            // Summed in **sorted** order, not `HashSet` order.
            //
            // Floating-point addition is not associative, and Rust's `HashSet`
            // seeds its hasher per process, so the same graph in two processes
            // summed these terms in different orders and produced answers
            // differing in the last bit. Measured across three runs of the
            // parity export: 3 of 6,840 pairs on the 120-node graph, worst
            // 4.4e-16 relative.
            //
            // Numerically that is nothing. It is still a determinism bug --
            // LANG-14 and ALGO-11 ask for *identical* output, and the parity
            // check's 1e-9 tolerance is exactly wide enough to hide it, so
            // nothing was ever going to catch this by comparing answers.
            let mut shared: Vec<usize> = a.intersection(b).copied().collect();
            shared.sort_unstable();
            shared
            .into_iter()
            .map(|w| {
                let d = sets[w].len() as f64;
                // A degree-1 shared neighbour gives ln(1) = 0 and would
                // divide by zero. NetworkX's convention is to skip it, and
                // skipping is right: a neighbour with one edge cannot be
                // *shared*, so it never actually reaches here on a consistent
                // graph -- but a self-loop or a stale index could.
                if d > 1.0 { 1.0 / d.ln() } else { 0.0 }
            })
            .sum()
        }
    }
}

/// Score every pair that is **not** already connected.
///
/// Excluding existing edges is the point: link prediction predicts links, and
/// a list whose top entries are pairs already joined is answering a question
/// nobody asked. Self-pairs are excluded for the same reason.
///
/// Returns the top `limit` by score, highest first, ties broken by node id so
/// two runs agree.
pub fn predict_links(
    view: &GraphView,
    which: LinkScore,
    limit: usize,
) -> Vec<PairScore> {
    let sets = neighbour_sets(view);
    let n = view.node_count;
    let mut out: Vec<PairScore> = Vec::new();
    for u in 0..n {
        for v in (u + 1)..n {
            if sets[u].contains(&v) {
                continue; // already connected
            }
            let score = score_pair(&sets, u, v, which);
            if score > 0.0 {
                out.push(PairScore {
                    a: view.index_to_node[u],
                    b: view.index_to_node[v],
                    score,
                });
            }
        }
    }
    out.sort_by(|x, y| {
        y.score
            .partial_cmp(&x.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(x.a.cmp(&y.a))
            .then(x.b.cmp(&y.b))
    });
    out.truncate(limit);
    out
}

/// Score one named pair, whether or not they are connected.
///
/// The pairwise question rather than the ranking one: *how similar are these
/// two?* A caller asking about a specific pair usually knows they are not
/// joined, and may legitimately ask about one that is.
pub fn score_one(
    view: &GraphView,
    which: LinkScore,
    u: usize,
    v: usize,
) -> Option<f64> {
    if u >= view.node_count || v >= view.node_count {
        return None;
    }
    Some(score_pair(&neighbour_sets(view), u, v, which))
}
