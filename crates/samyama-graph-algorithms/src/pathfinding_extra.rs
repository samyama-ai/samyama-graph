//! Path enumeration and a PageRank variant (ALGO-01).
//!
//! | | |
//! |---|---|
//! | [`all_shortest_paths`] | *every* shortest route, not one of them |
//! | [`a_star`] | Dijkstra guided by an admissible estimate |
//! | [`yens_k_shortest`] | the k best loopless routes, in order |
//! | [`random_walk`] | a sampled walk, seeded so it repeats |
//! | [`article_rank`] | PageRank that discounts votes from prolific linkers |

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet, VecDeque};

use crate::common::{GraphView, NodeId};

/// Edge weight of the `slot`-th out-edge, or 1.0 when the view is unweighted.
fn weight_at(view: &GraphView, slot: usize) -> f64 {
    view.weights.as_ref().map_or(1.0, |w| w[slot])
}

/// Every shortest path from `source` to `target`, unweighted.
///
/// *Every* one, not one of them. "Which route" and "how many equally good
/// routes" are different questions, and a single path cannot answer the
/// second — a pair joined by one route and a pair joined by forty look
/// identical through `shortestPath`, and in a resilience review they are not.
///
/// `limit` caps the enumeration: the count of shortest paths is exponential in
/// the worst case, and a graph that hits that would otherwise hang rather than
/// answer.
pub fn all_shortest_paths(
    view: &GraphView,
    source: usize,
    target: usize,
    limit: usize,
) -> Vec<Vec<NodeId>> {
    let n = view.node_count;
    if source >= n || target >= n {
        return Vec::new();
    }
    // BFS layering, keeping every predecessor at the shortest distance.
    let mut dist = vec![usize::MAX; n];
    let mut preds: Vec<Vec<usize>> = vec![Vec::new(); n];
    dist[source] = 0;
    let mut q = VecDeque::from([source]);
    while let Some(v) = q.pop_front() {
        for &w in view.successors(v) {
            if dist[w] == usize::MAX {
                dist[w] = dist[v] + 1;
                q.push_back(w);
            }
            if dist[w] == dist[v] + 1 {
                preds[w].push(v);
            }
        }
    }
    if dist[target] == usize::MAX {
        return Vec::new();
    }

    // Walk the predecessor DAG backwards. Bounded by `limit`, checked as
    // paths are completed rather than after building them all.
    let mut out = Vec::new();
    let mut stack: Vec<(usize, Vec<usize>)> = vec![(target, vec![target])];
    while let Some((v, path)) = stack.pop() {
        if out.len() >= limit {
            break;
        }
        if v == source {
            let mut p: Vec<NodeId> = path.iter().rev().map(|&i| view.index_to_node[i]).collect();
            p.dedup();
            out.push(p);
            continue;
        }
        for &u in &preds[v] {
            let mut next = path.clone();
            next.push(u);
            stack.push((u, next));
        }
    }
    // Deterministic: the DAG walk's order depends on push order, and two runs
    // must agree on which paths come first.
    out.sort();
    out
}

/// A* : Dijkstra guided by a per-node estimate of the remaining distance.
///
/// `heuristic[v]` is an estimate of the cost from `v` to the target. It must
/// be **admissible** — never an overestimate — or the result is not the
/// shortest path, merely a path. Passing all zeros makes this exactly
/// Dijkstra, which is the honest default when a caller has no estimate:
/// A* without a heuristic *is* Dijkstra, and pretending otherwise would be
/// selling a name rather than an algorithm.
pub fn a_star(
    view: &GraphView,
    source: usize,
    target: usize,
    heuristic: &[f64],
) -> Option<(Vec<NodeId>, f64)> {
    let n = view.node_count;
    if source >= n || target >= n || heuristic.len() != n {
        return None;
    }
    let mut g = vec![f64::INFINITY; n];
    let mut parent = vec![usize::MAX; n];
    g[source] = 0.0;
    // f = g + h, ordered by f. Reverse for a min-heap; the bits trick keeps
    // f64 orderable without a wrapper type.
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
    let key = |f: f64| -> u64 { (f * 1e6).max(0.0) as u64 };
    heap.push(Reverse((key(heuristic[source]), source)));

    while let Some(Reverse((_, v))) = heap.pop() {
        if v == target {
            let mut path = vec![target];
            let mut cur = target;
            while parent[cur] != usize::MAX {
                cur = parent[cur];
                path.push(cur);
            }
            path.reverse();
            return Some((path.into_iter().map(|i| view.index_to_node[i]).collect(), g[target]));
        }
        let (lo, hi) = (view.out_offsets[v], view.out_offsets[v + 1]);
        for slot in lo..hi {
            let w = view.out_targets[slot];
            let cand = g[v] + weight_at(view, slot);
            if cand < g[w] {
                g[w] = cand;
                parent[w] = v;
                heap.push(Reverse((key(cand + heuristic[w]), w)));
            }
        }
    }
    None
}

/// Yen's algorithm: the `k` shortest loopless paths, in increasing cost.
///
/// The second-best route matters when the best one is the thing that just
/// failed. `shortestPath` cannot answer that, and re-running it after deleting
/// an edge answers a different question — this holds the graph fixed.
pub fn yens_k_shortest(
    view: &GraphView,
    source: usize,
    target: usize,
    k: usize,
) -> Vec<(Vec<NodeId>, f64)> {
    let n = view.node_count;
    if source >= n || target >= n || k == 0 {
        return Vec::new();
    }
    let zero = vec![0.0; n];
    let Some((first, cost)) = a_star(view, source, target, &zero) else {
        return Vec::new();
    };
    let mut accepted: Vec<(Vec<NodeId>, f64)> = vec![(first, cost)];
    let mut candidates: Vec<(Vec<NodeId>, f64)> = Vec::new();

    while accepted.len() < k {
        let prev = accepted.last().unwrap().0.clone();
        for i in 0..prev.len().saturating_sub(1) {
            let spur_node = prev[i];
            let root: Vec<NodeId> = prev[..=i].to_vec();
            // Ban the edges that would re-create an already accepted path
            // sharing this root, and the root's own nodes, which is what
            // makes the next path *different* rather than the same one again.
            let mut banned_edges: HashSet<(NodeId, NodeId)> = HashSet::new();
            for (p, _) in &accepted {
                if p.len() > i && p[..=i] == root[..] {
                    banned_edges.insert((p[i], p[i + 1]));
                }
            }
            let banned_nodes: HashSet<NodeId> = root[..i].iter().copied().collect();
            // `spur_node` is a NodeId; the search wants a dense index.
            let Some(&spur_idx) = view.node_to_index.get(&spur_node) else { continue };
            if let Some((spur, _)) =
                shortest_avoiding(view, spur_idx, prev[prev.len() - 1], &banned_edges, &banned_nodes)
            {
                let mut full = root[..i].to_vec();
                full.extend(spur);
                let c = path_cost(view, &full);
                if !accepted.iter().any(|(p, _)| *p == full)
                    && !candidates.iter().any(|(p, _)| *p == full)
                {
                    candidates.push((full, c));
                }
            }
        }
        if candidates.is_empty() {
            break;
        }
        // Cheapest first; ties by the path itself so two runs agree.
        candidates.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0))
        });
        accepted.push(candidates.remove(0));
    }
    accepted
}

/// Dijkstra avoiding a set of edges and nodes, for Yen's spur search.
fn shortest_avoiding(
    view: &GraphView,
    source: usize,
    target: NodeId,
    banned_edges: &HashSet<(NodeId, NodeId)>,
    banned_nodes: &HashSet<NodeId>,
) -> Option<(Vec<NodeId>, f64)> {
    let n = view.node_count;
    let tgt = *view.node_to_index.get(&target)?;
    let mut dist = vec![f64::INFINITY; n];
    let mut parent = vec![usize::MAX; n];
    dist[source] = 0.0;
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
    heap.push(Reverse((0, source)));
    while let Some(Reverse((_, v))) = heap.pop() {
        if v == tgt {
            let mut path = vec![tgt];
            let mut cur = tgt;
            while parent[cur] != usize::MAX {
                cur = parent[cur];
                path.push(cur);
            }
            path.reverse();
            return Some((
                path.into_iter().map(|i| view.index_to_node[i]).collect(),
                dist[tgt],
            ));
        }
        let (lo, hi) = (view.out_offsets[v], view.out_offsets[v + 1]);
        for slot in lo..hi {
            let w = view.out_targets[slot];
            let (a, b) = (view.index_to_node[v], view.index_to_node[w]);
            if banned_edges.contains(&(a, b)) || banned_nodes.contains(&b) {
                continue;
            }
            let cand = dist[v] + weight_at(view, slot);
            if cand < dist[w] {
                dist[w] = cand;
                parent[w] = v;
                heap.push(Reverse(((cand * 1e6) as u64, w)));
            }
        }
    }
    None
}

fn path_cost(view: &GraphView, path: &[NodeId]) -> f64 {
    let mut total = 0.0;
    for pair in path.windows(2) {
        let (Some(&u), Some(&v)) = (view.node_to_index.get(&pair[0]), view.node_to_index.get(&pair[1]))
        else {
            continue;
        };
        let (lo, hi) = (view.out_offsets[u], view.out_offsets[u + 1]);
        let mut best = f64::INFINITY;
        for slot in lo..hi {
            if view.out_targets[slot] == v {
                best = best.min(weight_at(view, slot));
            }
        }
        if best.is_finite() {
            total += best;
        }
    }
    total
}

/// A random walk of `steps` from `source`, seeded.
///
/// **Seeded, and the seed is a parameter rather than the clock.** A sampled
/// result that cannot be reproduced is not a result: a caller who sees
/// something surprising in a walk has to be able to get the same walk back.
///
/// Uses a small xorshift rather than a dependency — the quality needed here is
/// "spreads out", not cryptographic.
pub fn random_walk(view: &GraphView, source: usize, steps: usize, seed: u64) -> Vec<NodeId> {
    let n = view.node_count;
    if source >= n {
        return Vec::new();
    }
    let mut state = seed | 1; // xorshift is stuck at zero
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut walk = vec![view.index_to_node[source]];
    let mut cur = source;
    for _ in 0..steps {
        let succ = view.successors(cur);
        if succ.is_empty() {
            break; // a walk that cannot continue stops rather than teleporting
        }
        cur = succ[(next() % succ.len() as u64) as usize];
        walk.push(view.index_to_node[cur]);
    }
    walk
}

/// ArticleRank: PageRank that discounts votes from prolific linkers.
///
/// PageRank divides a node's score equally among its out-links, so a node
/// linking to a thousand others still passes on its whole score. ArticleRank
/// adds the graph's *average* out-degree to each denominator, which damps the
/// influence of a node that links indiscriminately. On a citation graph that
/// is the difference between a survey paper and a focused one.
pub fn article_rank(view: &GraphView, damping: f64, iterations: usize) -> Vec<f64> {
    let n = view.node_count;
    if n == 0 {
        return Vec::new();
    }
    let avg_out: f64 =
        (0..n).map(|i| view.out_degree(i) as f64).sum::<f64>() / n as f64;
    let mut rank = vec![1.0 / n as f64; n];
    for _ in 0..iterations {
        let mut next = vec![(1.0 - damping) / n as f64; n];
        for u in 0..n {
            let d = view.out_degree(u) as f64;
            if d == 0.0 {
                continue;
            }
            // The `+ avg_out` is the whole difference from PageRank.
            let share = damping * rank[u] / (d + avg_out);
            for &v in view.successors(u) {
                next[v] += share;
            }
        }
        rank = next;
    }
    rank
}
