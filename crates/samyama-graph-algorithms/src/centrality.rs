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

/// Harmonic centrality: the sum of reciprocal distances to every other node.
///
/// The repair for closeness on a disconnected graph. Closeness divides by a
/// *total* distance, so an unreachable node makes the sum infinite and the
/// whole score collapses -- which is why [`closeness_centrality`] needs
/// NetworkX's reachable-fraction convention to say anything at all. Harmonic
/// sums `1/d` instead, and an unreachable node contributes `1/inf = 0`. No
/// convention needed, and no special case.
///
/// Unnormalised, as NetworkX leaves it: the raw sum, not divided by `n - 1`.
///
/// Direction follows closeness: distances *into* the node on a directed graph.
pub fn harmonic_centrality(view: &GraphView, bidirectional: bool) -> Scores {
    let n = view.node_count;
    let mut out = vec![0.0; n];
    for v in 0..n {
        let s = bfs_sssp_reversed(view, v, bidirectional);
        out[v] = s
            .dist
            .iter()
            .enumerate()
            .filter(|(u, &d)| *u != v && d > 0)
            .map(|(_, &d)| 1.0 / d as f64)
            .sum();
    }
    out
}

/// Core number: the largest `k` for which a node survives in the k-core.
///
/// Computed by peeling. Repeatedly remove the lowest-degree node; its core
/// number is the highest degree seen so far, which is what makes this O(E)
/// rather than a search over `k`. The running maximum is the subtle part: a
/// node removed later cannot have a *lower* core number than one removed
/// earlier, so the value is the max of its degree-at-removal and everything
/// peeled before it.
///
/// Undirected by definition: a k-core is defined on degree, and NetworkX takes
/// that as in- **plus** out-degree even on a directed graph. So both
/// directions are always walked, and `bidirectional` is not consulted at all.
///
/// Following the flag instead -- successors only, on a directed view -- gave a
/// core number three too low on the reference graph. The doc comment said
/// "undirected by definition" while the code branched on direction anyway,
/// which is the sort of disagreement a parity check exists to find.
///
/// Like `degree_centrality`, this counts `out + in` without deduplicating, so
/// the caller's storage convention matters: a view holding each undirected
/// edge twice would double every degree. `build_view` stores each edge once.
pub fn core_number(view: &GraphView, _bidirectional: bool) -> Vec<usize> {
    let n = view.node_count;
    let neighbours = |i: usize| -> Vec<usize> {
        let mut v: Vec<usize> = view.successors(i).to_vec();
        v.extend_from_slice(view.predecessors(i));
        // **Not** deduplicated. NetworkX's degree on a directed graph is
        // in-degree *plus* out-degree, so a reciprocal pair `a -> b` and
        // `b -> a` counts as two, not one. Collapsing them to a single
        // neighbour gave a core number one too low on the directed reference
        // graph while agreeing on both undirected ones -- exactly the shape of
        // a bug that a single-graph check would have missed.
        //
        // The peel is consistent with that: a multi-edge neighbour appears
        // twice in this list and is decremented twice when the far end is
        // removed, which is what it means for two edges to disappear.
        //
        // A self-loop is not a neighbour under any reading, and NetworkX
        // refuses a graph containing one outright.
        v.retain(|&u| u != i);
        v
    };

    let mut deg: Vec<usize> = (0..n).map(|i| neighbours(i).len()).collect();
    let mut core = vec![0usize; n];
    let mut removed = vec![false; n];

    // Batagelj-Zaversnik, exactly. Repeatedly remove a node of minimum current
    // degree; its core number is the running maximum of the degrees at
    // removal. Because the minimum is always taken, that sequence is
    // non-decreasing, and the running maximum is what makes a node peeled
    // later inherit the level rather than reporting its own reduced degree.
    //
    // An earlier version also pushed `core[v]` onto each surviving neighbour.
    // That is not part of the algorithm, and it inflated the answer on a
    // directed graph while agreeing on every undirected one -- so the
    // undirected reference agreed and only the directed graph disagreed.
    let mut k = 0usize;
    for _ in 0..n {
        let Some(v) = (0..n).filter(|&i| !removed[i]).min_by_key(|&i| deg[i]) else {
            break;
        };
        k = k.max(deg[v]);
        core[v] = k;
        removed[v] = true;
        for u in neighbours(v) {
            if !removed[u] {
                deg[u] = deg[u].saturating_sub(1);
            }
        }
    }
    core
}

/// Eigenvector centrality by power iteration.
///
/// A node is important in proportion to the importance of what points at it,
/// which is PageRank without the damping factor or the teleport. That makes it
/// sharper on a well-connected graph and undefined on some others -- a graph
/// with no edges has no principal eigenvector, and power iteration on it
/// simply does not converge.
///
/// Returns `None` rather than a plausible-looking vector when it does not
/// converge in `max_iter`. A non-converged iterate is still a normalised
/// vector of the right shape, and handing it back is how a number that means
/// nothing gets published.
///
/// Normalised to unit L2 norm, as NetworkX does.
pub fn eigenvector_centrality(
    view: &GraphView,
    bidirectional: bool,
    max_iter: usize,
    tol: f64,
) -> Option<Scores> {
    let n = view.node_count;
    if n == 0 {
        return Some(Vec::new());
    }
    let mut x = vec![1.0 / (n as f64).sqrt(); n];
    for _ in 0..max_iter {
        let mut next = vec![0.0; n];
        // x[v] gets the mass of everything pointing *at* v, which is the
        // convention NetworkX uses for a directed graph.
        for u in 0..n {
            for &v in view.successors(u) {
                next[v] += x[u];
            }
            if bidirectional {
                for &v in view.predecessors(u) {
                    next[v] += x[u];
                }
            }
        }
        let norm = next.iter().map(|v| v * v).sum::<f64>().sqrt();
        if norm == 0.0 {
            return None;
        }
        for v in next.iter_mut() {
            *v /= norm;
        }
        let delta: f64 = next.iter().zip(&x).map(|(a, b)| (a - b).abs()).sum();
        x = next;
        // NetworkX's test is `sum |x_i - x_last_i| < n * tol`.
        if delta < n as f64 * tol {
            return Some(x);
        }
    }
    None
}
