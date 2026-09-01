//! Path and reachability algorithms for the H2 coverage target (ALGO-01).
//!
//! All have NetworkX equivalents, so ALGO-02's parity requirement can reach
//! them. Where NetworkX and a textbook disagree on a convention, NetworkX
//! wins and the disagreement is written down — a parity check against a
//! reference we have quietly diverged from tests nothing.

use std::collections::{HashMap, VecDeque};

use crate::common::GraphView;

/// Shortest paths from one source with **negative weights allowed**.
///
/// Dijkstra cannot do this: it finalises a node the moment it is popped, and a
/// later negative edge can still improve it. So a graph with a single negative
/// edge gets a wrong answer rather than an error, which is the failure mode
/// that makes this worth shipping separately rather than as a flag.
///
/// Returns `None` when a negative cycle is reachable from the source. That is
/// not a failure to compute: with a negative cycle there *is* no shortest
/// path, because going round again is always cheaper, and any finite number
/// returned would be a lie.
pub fn bellman_ford(view: &GraphView, source: usize) -> Option<Vec<Option<f64>>> {
    let n = view.node_count;
    if source >= n {
        return Some(vec![None; n]);
    }
    let mut dist: Vec<Option<f64>> = vec![None; n];
    dist[source] = Some(0.0);
    // n-1 relaxation rounds suffice for any shortest path, which has at most
    // n-1 edges. The nth round is the negative-cycle test: anything that still
    // improves is reachable from a cycle that can be traversed for profit.
    for round in 0..n {
        let mut changed = false;
        for u in 0..n {
            let Some(du) = dist[u] else { continue };
            let ws = view.weights(u);
            for (k, &v) in view.successors(u).iter().enumerate() {
                let w = ws.map_or(1.0, |w| w[k]);
                let cand = du + w;
                if dist[v].is_none_or(|dv| cand < dv - 1e-12) {
                    if round == n - 1 {
                        return None; // still improving after n-1 rounds
                    }
                    dist[v] = Some(cand);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    Some(dist)
}

/// Unweighted distance between every ordered pair, as `(from, to) -> hops`.
///
/// BFS from every node rather than Floyd–Warshall: these graphs are sparse, so
/// `n` BFS runs is `O(n*m)` against Floyd–Warshall's `O(n^3)`, and the answer
/// is the same. Unreachable pairs are **absent** rather than stored as
/// infinity — a sentinel that a caller has to remember to filter is how an
/// unreachable pair ends up in an average.
pub fn all_pairs_hops(view: &GraphView) -> HashMap<(usize, usize), usize> {
    let n = view.node_count;
    let mut out = HashMap::new();
    let mut dist = vec![usize::MAX; n];
    for s in 0..n {
        dist.iter_mut().for_each(|d| *d = usize::MAX);
        dist[s] = 0;
        let mut q = VecDeque::from([s]);
        while let Some(u) = q.pop_front() {
            for &v in view.successors(u) {
                if dist[v] == usize::MAX {
                    dist[v] = dist[u] + 1;
                    q.push_back(v);
                }
            }
        }
        for (t, &d) in dist.iter().enumerate() {
            if t != s && d != usize::MAX {
                out.insert((s, t), d);
            }
        }
    }
    out
}

/// The Wiener index: the sum of shortest-path distances over all pairs.
///
/// A single number for how "spread out" a graph is, and the one graph-level
/// statistic that a diameter cannot give — diameter reports the worst pair
/// and says nothing about the rest.
///
/// `None` when some pair is unreachable, matching NetworkX, which returns
/// infinity there. A graph in pieces has no finite Wiener index and reporting
/// the sum over only the reachable pairs would silently answer a different
/// question.
pub fn wiener_index(view: &GraphView) -> Option<f64> {
    let n = view.node_count;
    if n < 2 {
        return Some(0.0);
    }
    let d = all_pairs_hops(view);
    if d.len() != n * (n - 1) {
        return None;
    }
    Some(d.values().map(|&x| x as f64).sum())
}

/// The longest path in a DAG, by edge count.
///
/// Only defined on a DAG: with a cycle you can go round forever, so there is
/// no longest path and `None` is the answer rather than a very large number.
/// Longest-path on a general graph is NP-hard, and a procedure that silently
/// accepted a cyclic graph would be answering a question it cannot answer.
pub fn dag_longest_path(view: &GraphView) -> Option<Vec<usize>> {
    let n = view.node_count;
    let order = topological_order(view)?;
    let mut best = vec![0usize; n];
    let mut from = vec![usize::MAX; n];
    for &u in &order {
        for &v in view.successors(u) {
            if best[u] + 1 > best[v] {
                best[v] = best[u] + 1;
                from[v] = u;
            }
        }
    }
    let end = (0..n).max_by_key(|&i| (best[i], std::cmp::Reverse(i)))?;
    let mut path = vec![end];
    let mut cur = end;
    while from[cur] != usize::MAX {
        cur = from[cur];
        path.push(cur);
    }
    path.reverse();
    Some(path)
}

/// Kahn's order, or `None` if the graph has a cycle.
fn topological_order(view: &GraphView) -> Option<Vec<usize>> {
    let n = view.node_count;
    let mut indeg = vec![0usize; n];
    for u in 0..n {
        for &v in view.successors(u) {
            indeg[v] += 1;
        }
    }
    // Smallest index first, so the order -- and therefore the path returned
    // when several are equally long -- is the same on every run.
    let mut q: Vec<usize> = (0..n).filter(|&i| indeg[i] == 0).collect();
    q.sort_unstable();
    let mut q: VecDeque<usize> = q.into();
    let mut order = Vec::with_capacity(n);
    while let Some(u) = q.pop_front() {
        order.push(u);
        let mut freed: Vec<usize> = Vec::new();
        for &v in view.successors(u) {
            indeg[v] -= 1;
            if indeg[v] == 0 {
                freed.push(v);
            }
        }
        freed.sort_unstable();
        for v in freed {
            q.push_back(v);
        }
    }
    (order.len() == n).then_some(order)
}

/// Every pair `(u, v)` such that `v` is reachable from `u`.
///
/// The reachability relation itself, which `shortestPath` cannot express: it
/// answers one pair at a time, and "can anything in this set reach anything in
/// that set" is the question an impact analysis actually asks.
///
/// Excludes `(u, u)` unless a real cycle returns to `u`. NetworkX's
/// `transitive_closure` does include self-loops for nodes on a cycle, and the
/// distinction matters: a node that can reach itself is on a cycle, which is a
/// fact worth keeping.
pub fn transitive_closure(view: &GraphView) -> Vec<(usize, usize)> {
    let n = view.node_count;
    let mut out = Vec::new();
    let mut seen = vec![false; n];
    for s in 0..n {
        seen.iter_mut().for_each(|x| *x = false);
        let mut stack: Vec<usize> = view.successors(s).to_vec();
        while let Some(u) = stack.pop() {
            if seen[u] {
                continue;
            }
            seen[u] = true;
            stack.extend_from_slice(view.successors(u));
        }
        for (t, &r) in seen.iter().enumerate() {
            if r {
                out.push((s, t));
            }
        }
    }
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NodeId;

    fn view(n: usize, edges: &[(usize, usize, f64)]) -> GraphView {
        let index_to_node: Vec<NodeId> = (0..n).map(|i| i as NodeId).collect();
        let node_to_index: HashMap<NodeId, usize> = (0..n).map(|i| (i as NodeId, i)).collect();
        let (mut out, mut inc, mut w) = (vec![Vec::new(); n], vec![Vec::new(); n], vec![Vec::new(); n]);
        for &(a, b, x) in edges {
            out[a].push(b);
            w[a].push(x);
            inc[b].push(a);
        }
        GraphView::from_adjacency_list(n, index_to_node, node_to_index, out, inc, Some(w))
    }

    #[test]
    fn bellman_ford_beats_dijkstra_on_a_negative_edge() {
        // 0->1 costs 5; 0->2 costs 2 and 2->1 costs -4, so the real distance
        // to 1 is -2. Dijkstra finalises 1 at 5 before ever looking at 2.
        let g = view(3, &[(0, 1, 5.0), (0, 2, 2.0), (2, 1, -4.0)]);
        let d = bellman_ford(&g, 0).expect("no negative cycle");
        assert_eq!(d[1], Some(-2.0), "{d:?}");
    }

    #[test]
    fn bellman_ford_refuses_a_negative_cycle() {
        // Going round 0->1->2->0 costs -1 each lap, so no shortest path
        // exists. Any number here would be a lie.
        let g = view(3, &[(0, 1, 1.0), (1, 2, 1.0), (2, 0, -3.0)]);
        assert!(bellman_ford(&g, 0).is_none());
    }

    #[test]
    fn unreachable_pairs_are_absent_not_infinite() {
        let g = view(3, &[(0, 1, 1.0)]);
        let d = all_pairs_hops(&g);
        assert_eq!(d.get(&(0, 1)), Some(&1));
        assert!(!d.contains_key(&(0, 2)), "unreachable pair must not be stored");
    }

    #[test]
    fn wiener_index_refuses_a_disconnected_graph() {
        // 0->1 only: 2 is unreachable, so there is no finite sum over all
        // pairs. Summing the reachable ones would answer a different question.
        let g = view(3, &[(0, 1, 1.0)]);
        assert!(wiener_index(&g).is_none());
        // A complete directed triangle has all 6 ordered pairs at distance 1.
        let t = view(3, &[(0, 1, 1.0), (1, 2, 1.0), (2, 0, 1.0),
                          (1, 0, 1.0), (2, 1, 1.0), (0, 2, 1.0)]);
        assert_eq!(wiener_index(&t), Some(6.0));
    }

    #[test]
    fn dag_longest_path_finds_the_long_way_round() {
        // 0->3 direct, and 0->1->2->3 the long way. Longest is by edge count,
        // so the three-hop route wins even though both reach the same node.
        let g = view(4, &[(0, 3, 1.0), (0, 1, 1.0), (1, 2, 1.0), (2, 3, 1.0)]);
        assert_eq!(dag_longest_path(&g), Some(vec![0, 1, 2, 3]));
    }

    #[test]
    fn dag_longest_path_refuses_a_cycle() {
        // With a cycle there is no longest path; a large number would be
        // arbitrary and a small one wrong.
        let g = view(3, &[(0, 1, 1.0), (1, 2, 1.0), (2, 0, 1.0)]);
        assert!(dag_longest_path(&g).is_none());
    }

    #[test]
    fn transitive_closure_keeps_the_self_loop_of_a_cycle() {
        // On 0->1->2->0 every node reaches every node *including itself*, and
        // that self-pair is the fact that it sits on a cycle. Dropping it as
        // "trivial" would throw the information away.
        let g = view(3, &[(0, 1, 1.0), (1, 2, 1.0), (2, 0, 1.0)]);
        let tc = transitive_closure(&g);
        assert_eq!(tc.len(), 9, "{tc:?}");
        assert!(tc.contains(&(0, 0)), "{tc:?}");
        // A path has no self-pairs.
        let p = view(3, &[(0, 1, 1.0), (1, 2, 1.0)]);
        let tp = transitive_closure(&p);
        assert!(!tp.iter().any(|(a, b)| a == b), "{tp:?}");
    }
}
