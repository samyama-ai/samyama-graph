//! Whole-graph shape: eccentricity, diameter, radius, and degree mixing
//! (ALGO-01).
//!
//! The first three are one computation read three ways. A node's
//! **eccentricity** is its distance to the furthest node it can reach; the
//! **diameter** is the largest of those and the **radius** the smallest. They
//! are separate entry points because they answer separate questions — *how
//! far is this node from everything*, *how wide is the graph*, *how tight is
//! its best centre* — and because a caller wants a column, a scalar, and a
//! scalar respectively.
//!
//! The last two describe how degrees *mix*. Assortativity asks whether
//! well-connected nodes attach to other well-connected nodes; average
//! neighbour degree asks it per node. A social network is usually assortative
//! and a technological one usually is not, which is a real structural
//! distinction and not a statistic for its own sake.
//!
//! # Disconnected graphs
//!
//! Eccentricity is undefined when a node cannot reach everything, and
//! NetworkX raises rather than answering. This returns `None` for those
//! nodes and lets diameter and radius refuse the graph, because a diameter
//! computed over only the reachable part is a smaller number that looks like
//! a real one.

use std::collections::VecDeque;

use crate::common::{GraphView, NodeId};

/// Hop distance from `source` to every node, `-1` where unreachable.
fn bfs(view: &GraphView, source: usize, bidirectional: bool) -> Vec<i64> {
    let n = view.node_count;
    let mut dist = vec![-1i64; n];
    dist[source] = 0;
    let mut q = VecDeque::new();
    q.push_back(source);
    while let Some(v) = q.pop_front() {
        let mut walk = |w: usize, dist: &mut Vec<i64>, q: &mut VecDeque<usize>| {
            if dist[w] < 0 {
                dist[w] = dist[v] + 1;
                q.push_back(w);
            }
        };
        for &w in view.successors(v) {
            walk(w, &mut dist, &mut q);
        }
        if bidirectional {
            for &w in view.predecessors(v) {
                walk(w, &mut dist, &mut q);
            }
        }
    }
    dist
}

/// Each node's distance to the furthest node it can reach, or `None` when it
/// cannot reach every node.
///
/// `None` rather than "the furthest it *can* reach", which would be a smaller
/// number indistinguishable from a real eccentricity on a connected graph.
pub fn eccentricity(view: &GraphView, bidirectional: bool) -> Vec<Option<i64>> {
    let n = view.node_count;
    (0..n)
        .map(|v| {
            let d = bfs(view, v, bidirectional);
            if d.iter().any(|&x| x < 0) {
                None
            } else {
                Some(d.into_iter().max().unwrap_or(0))
            }
        })
        .collect()
}

/// The largest eccentricity, or `None` if the graph is not connected.
pub fn diameter(view: &GraphView, bidirectional: bool) -> Option<i64> {
    let e = eccentricity(view, bidirectional);
    if e.is_empty() || e.iter().any(|x| x.is_none()) {
        return None;
    }
    e.into_iter().flatten().max()
}

/// The smallest eccentricity, or `None` if the graph is not connected.
pub fn radius(view: &GraphView, bidirectional: bool) -> Option<i64> {
    let e = eccentricity(view, bidirectional);
    if e.is_empty() || e.iter().any(|x| x.is_none()) {
        return None;
    }
    e.into_iter().flatten().min()
}

/// The average degree of each node's neighbours.
///
/// NetworkX's `average_neighbor_degree`. A node with no neighbours scores 0
/// rather than being omitted: a caller joining this back onto the node list
/// wants a row for every node, and a missing one is far easier to overlook
/// than a zero.
pub fn average_neighbour_degree(view: &GraphView, bidirectional: bool) -> Vec<f64> {
    let n = view.node_count;
    let nb = |i: usize| undirected_neighbour_set(view, i, bidirectional);
    let deg: Vec<usize> = (0..n).map(|i| nb(i).len()).collect();
    (0..n)
        .map(|i| {
            let ns = nb(i);
            if ns.is_empty() {
                return 0.0;
            }
            ns.iter().map(|&w| deg[w] as f64).sum::<f64>() / ns.len() as f64
        })
        .collect()
}

/// The neighbours of `i` under the reading these metrics use.
///
/// **Deduplicated when `bidirectional`.** A shape metric asks about the
/// undirected graph, and in the undirected reading a reciprocal pair
/// `a -> b`, `b -> a` is *one* edge. Counting it twice inflated the degree on
/// a directed graph and made both metrics below disagree with NetworkX, which
/// computes them on `nx.Graph(D)` — the collapse.
///
/// This is deliberately **not** the same convention as `degree_centrality`,
/// which is in-degree plus out-degree and counts the pair twice. The two match
/// different NetworkX functions because they answer different questions:
/// centrality asks how many edges touch a node, a shape metric asks how many
/// distinct nodes it sits beside. Both were checked against NetworkX and the
/// conventions are its, not ours.
fn undirected_neighbour_set(view: &GraphView, i: usize, bidirectional: bool) -> Vec<usize> {
    let mut v: Vec<usize> = view.successors(i).to_vec();
    if bidirectional {
        v.extend_from_slice(view.predecessors(i));
        v.sort_unstable();
        v.dedup();
    }
    v.retain(|&u| u != i);
    v
}

/// Degree assortativity: the Pearson correlation of degree across edges.
///
/// Positive means well-connected nodes attach to other well-connected nodes;
/// negative means hubs attach to leaves. Around zero means degree says nothing
/// about who connects to whom.
///
/// Computed by Newman's formulation over the edge list rather than by building
/// the full mixing matrix — the same number, without materialising a
/// `max_degree`-squared array on a graph with one hub.
///
/// `None` when every edge joins nodes of identical degree: the correlation is
/// then 0/0, and NetworkX returns NaN there. A caller cannot act on NaN, and
/// silently reporting 0.0 would claim "no assortativity" about a graph that
/// is perfectly regular.
pub fn degree_assortativity(view: &GraphView, bidirectional: bool) -> Option<f64> {
    let n = view.node_count;
    let sets: Vec<Vec<usize>> = (0..n)
        .map(|i| undirected_neighbour_set(view, i, bidirectional))
        .collect();
    let degree = |i: usize| -> f64 { sets[i].len() as f64 };

    // Every edge, counted once in each direction, as Newman's undirected
    // formulation requires: the correlation is over (degree at one end,
    // degree at the other) and both orderings are observations.
    //
    // Iterating the deduplicated sets rather than `successors` keeps a
    // reciprocal pair from contributing twice, which would weight it double
    // against every other edge.
    let (mut s1, mut s2, mut s3, mut m) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    for u in 0..n {
        for &v in sets[u].iter().filter(|&&v| v > u || !bidirectional) {
            for (a, b) in [(u, v), (v, u)] {
                let (da, db) = (degree(a), degree(b));
                s1 += da * db;
                s2 += da + db;
                s3 += da * da + db * db;
                m += 1.0;
            }
        }
    }
    if m == 0.0 {
        return None;
    }
    // r = (S1/M - (S2/2M)^2) / (S3/2M - (S2/2M)^2)
    let mean = s2 / (2.0 * m);
    let num = s1 / m - mean * mean;
    let den = s3 / (2.0 * m) - mean * mean;
    if den.abs() < 1e-12 {
        return None;
    }
    Some(num / den)
}

/// Pair scores with node ids, largest first, ties by id.
pub fn ranked_opt(view: &GraphView, scores: &[Option<i64>]) -> Vec<(NodeId, Option<i64>)> {
    let mut out: Vec<(NodeId, Option<i64>)> = scores
        .iter()
        .enumerate()
        .map(|(i, &s)| (view.index_to_node[i], s))
        .collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    out
}
