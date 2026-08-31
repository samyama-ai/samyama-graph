//! Degree, closeness and betweenness centrality (ALGO-01).
//!
//! Three answers to "which nodes matter", and they disagree on purpose:
//!
//! * **degree** — how many neighbours. Local, O(1) per node, and often
//!   enough.
//! * **closeness** — how near everything else is. Finds the well-placed node.
//! * **betweenness** — how much traffic must pass through. Finds the
//!   *bottleneck*, which is frequently a low-degree node the other two miss:
//!   the single bridge between two clusters has degree 2.
//!
//! All three are computed against NetworkX's own definitions and checked
//! against its recorded answers, because "betweenness" without a normalisation
//! convention is not a number anyone can compare.
//!
//! # Shared machinery
//!
//! Closeness and betweenness both need single-source shortest paths from every
//! node; [`bfs_sssp`] is that one traversal, and betweenness additionally uses
//! the shortest-path *counts* and predecessor lists it already computes.
//! Writing a second BFS for the second algorithm is how two algorithms come to
//! disagree about whether an unreachable node is at distance infinity or zero.
//!
//! # The `bidirectional` flag, which is easy to get wrong
//!
//! A `GraphView` can represent an undirected graph two ways, and the caller
//! knows which one it built:
//!
//! * **each edge stored once** -- pass `true`, and the traversal walks
//!   successors *and* predecessors to recover both directions.
//! * **each edge stored twice**, once per direction -- pass `false`, because
//!   the successors already are the full neighbourhood.
//!
//! Passing `true` to a view that already stores both directions visits every
//! neighbour twice. Distances survive that; **shortest-path counts do not**,
//! so betweenness comes back wrong while degree and closeness look fine. The
//! flag is named for the *traversal* rather than for the graph for exactly
//! that reason.

use std::collections::VecDeque;

use crate::common::{GraphView, NodeId};

/// A centrality score per node, in the view's dense index order.
pub type Scores = Vec<f64>;

/// Single-source shortest paths on an unweighted graph.
///
/// Returns `(dist, sigma, preds)`: hop distance (`-1` when unreachable), the
/// number of shortest paths from the source, and each node's predecessors on
/// those paths. `order` is the BFS discovery order, which betweenness walks
/// backwards.
struct Sssp {
    dist: Vec<i64>,
    sigma: Vec<f64>,
    preds: Vec<Vec<usize>>,
    order: Vec<usize>,
}

fn bfs_sssp(view: &GraphView, source: usize, bidirectional: bool) -> Sssp {
    let n = view.node_count;
    let mut s = Sssp {
        dist: vec![-1; n],
        sigma: vec![0.0; n],
        preds: vec![Vec::new(); n],
        order: Vec::with_capacity(n),
    };
    s.dist[source] = 0;
    s.sigma[source] = 1.0;
    let mut q = VecDeque::new();
    q.push_back(source);

    while let Some(v) = q.pop_front() {
        s.order.push(v);
        // An undirected reading walks both directions. The CSR stores each
        // undirected edge once, in whichever direction it was created, so
        // ignoring `in_sources` would silently make the graph directed and
        // every score would be of a different graph.
        let succ = view.successors(v).iter().copied();
        let pred = view.predecessors(v).iter().copied();
        let neighbours: Vec<usize> = if bidirectional {
            succ.chain(pred).collect()
        } else {
            succ.collect()
        };
        for w in neighbours {
            if s.dist[w] < 0 {
                s.dist[w] = s.dist[v] + 1;
                q.push_back(w);
            }
            // A second route of the same length is another shortest path, not
            // a duplicate to skip.
            if s.dist[w] == s.dist[v] + 1 {
                s.sigma[w] += s.sigma[v];
                s.preds[w].push(v);
            }
        }
    }
    s
}

/// Degree centrality: neighbours divided by the most a node could have.
///
/// NetworkX normalises by `n - 1` for both directed and undirected, so a
/// complete graph scores 1.0 everywhere.
pub fn degree_centrality(view: &GraphView, bidirectional: bool) -> Scores {
    let n = view.node_count;
    if n <= 1 {
        return vec![0.0; n];
    }
    let denom = (n - 1) as f64;
    // In *plus* out, whichever way the flag reads, because that is the set of
    // neighbours the traversal would visit -- and it is also NetworkX's
    // `degree_centrality`, which is total degree on a directed graph too
    // (`in_degree_centrality` and `out_degree_centrality` are separate
    // functions). Using out-degree alone produced a plausible ranking of a
    // different quantity, and only the parity check said so.
    //
    // A view that stores each undirected edge *twice* would double this, and
    // there is deliberately no attempt to detect that: the only signal
    // available is that every node has equal in- and out-degree, which is also
    // true of a perfectly balanced directed graph. Sniffing it would silently
    // halve that graph's scores. The storage convention is the caller's to
    // state -- see the note on `bidirectional` at the top of this module --
    // and a view built by `build_view` stores each edge once.
    let _ = bidirectional;
    (0..n)
        .map(|i| (view.out_degree(i) + view.in_degree(i)) as f64 / denom)
        .collect()
}

/// Closeness centrality, with NetworkX's disconnected-graph convention.
///
/// The convention is the whole subtlety. For a node that cannot reach
/// everything, NetworkX scales by the fraction of the graph it *can* reach:
///
/// ```text
/// C(v) = (reachable - 1) / total_distance  *  (reachable - 1) / (n - 1)
/// ```
///
/// Without the second factor a node in a tiny isolated component scores
/// **1.0** -- perfectly central, in a component of two -- and outranks a
/// genuinely central node in the main component. That is not a rounding
/// difference; it inverts the answer.
///
/// Note NetworkX measures *incoming* distance on a directed graph: how easily
/// the node is reached, not how easily it reaches. Getting that backwards
/// produces a plausible ranking of the wrong quantity.
pub fn closeness_centrality(view: &GraphView, bidirectional: bool) -> Scores {
    let n = view.node_count;
    let mut out = vec![0.0; n];
    if n <= 1 {
        return out;
    }
    for v in 0..n {
        // Reverse the traversal for the directed case: distances *into* v.
        let s = bfs_sssp_reversed(view, v, bidirectional);
        let (mut total, mut reachable) = (0i64, 0usize);
        for (u, &d) in s.dist.iter().enumerate() {
            if u != v && d >= 0 {
                total += d;
                reachable += 1;
            }
        }
        if total > 0 && reachable > 0 {
            let closeness = reachable as f64 / total as f64;
            out[v] = closeness * (reachable as f64 / (n - 1) as f64);
        }
    }
    out
}

/// `bfs_sssp` over the reversed graph, for closeness on a directed view.
fn bfs_sssp_reversed(view: &GraphView, source: usize, bidirectional: bool) -> Sssp {
    if bidirectional {
        return bfs_sssp(view, source, true);
    }
    let n = view.node_count;
    let mut s = Sssp {
        dist: vec![-1; n],
        sigma: vec![0.0; n],
        preds: vec![Vec::new(); n],
        order: Vec::with_capacity(n),
    };
    s.dist[source] = 0;
    let mut q = VecDeque::new();
    q.push_back(source);
    while let Some(v) = q.pop_front() {
        s.order.push(v);
        for &w in view.predecessors(v) {
            if s.dist[w] < 0 {
                s.dist[w] = s.dist[v] + 1;
                q.push_back(w);
            }
        }
    }
    s
}

/// Betweenness centrality by Brandes' algorithm, normalised as NetworkX does.
///
/// The fraction of shortest paths between all other pairs that run through
/// each node. Brandes computes it in O(VE) rather than the O(V^3) of counting
/// pairs directly, by accumulating a dependency backwards along each BFS:
///
/// ```text
/// delta[v] = sum over w with v as predecessor of
///              (sigma[v] / sigma[w]) * (1 + delta[w])
/// ```
///
/// Normalisation follows NetworkX exactly: divide by `(n-1)(n-2)`, **and do
/// not halve for an undirected graph.**
///
/// That last point is worth stating because it is not the natural reading. An
/// undirected BFS from every source visits each pair from both ends, so
/// halving looks obviously right, and I wrote it that way first. NetworkX's
/// normalised undirected score is therefore twice the fraction-of-pairs
/// quantity -- their convention, and the one a user comparing against them
/// expects. Only the parity check found it: the two differ by exactly 2, which
/// is a factor no eyeball on a plausible-looking ranking would catch.
///
/// An unnormalised score differs by a factor of tens of thousands on a graph
/// of any size, so "betweenness" without a stated convention is not comparable
/// to anything.
pub fn betweenness_centrality(view: &GraphView, bidirectional: bool) -> Scores {
    let n = view.node_count;
    let mut bc = vec![0.0; n];
    if n <= 2 {
        return bc;
    }

    for s in 0..n {
        let sssp = bfs_sssp(view, s, bidirectional);
        let mut delta = vec![0.0; n];
        // Backwards through the BFS order: a node's dependency is complete
        // only once every node further from the source has been settled.
        for &w in sssp.order.iter().rev() {
            let coeff = (1.0 + delta[w]) / sssp.sigma[w];
            for &v in &sssp.preds[w] {
                delta[v] += sssp.sigma[v] * coeff;
            }
            if w != s {
                bc[w] += delta[w];
            }
        }
    }

    let scale = 1.0 / ((n - 1) as f64 * (n - 2) as f64);
    for b in bc.iter_mut() {
        *b *= scale;
    }
    bc
}

/// Pair a score vector with node ids, largest first.
///
/// Ties break by node id so two runs agree; an unstable order here shows up as
/// a different "most central node" on each run.
pub fn ranked(view: &GraphView, scores: &Scores) -> Vec<(NodeId, f64)> {
    let mut out: Vec<(NodeId, f64)> = scores
        .iter()
        .enumerate()
        .map(|(i, &s)| (view.index_to_node[i], s))
        .collect();
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
    out
}
