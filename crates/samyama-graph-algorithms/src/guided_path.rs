//! Shortest paths guided by a lower bound on the distance still to go: A*
//! (#2037).
//!
//! The caller supplies two things:
//!
//! * `step(u, v)`: what one relationship costs, or `None` when it cannot be
//!   walked. Costs must be non-negative.
//! * `remaining(v)`: a lower bound on the cost from `v` to the target. It must
//!   never overestimate (it must be *admissible*), or the path returned is not
//!   guaranteed shortest.
//!
//! With a good bound -- the straight-line distance to the target, on a graph
//! whose nodes have positions -- the search expands only the nodes that could
//! lie on a shortest path, instead of everything closer to the source than
//! the target is. [`guided_shortest_path`] reports how many nodes it expanded,
//! and running it with `remaining = 0` (which is Dijkstra) gives the number to
//! compare against.
//!
//! Correct for any admissible bound, consistent or not: a node whose cost
//! improves after it was expanded is expanded again. Ties in priority are
//! broken by the larger cost so far (closer to the target), then by node
//! index, so the path is the same on every run.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::common::GraphView;

/// A shortest path found by [`guided_shortest_path`].
#[derive(Debug, Clone, PartialEq)]
pub struct GuidedPath {
    /// Dense indexes from source to target, both included.
    pub path: Vec<usize>,
    /// The sum of `step` along `path`.
    pub cost: f64,
    /// Nodes expanded, re-expansions included: the work done.
    pub expanded: usize,
}

#[derive(Clone, Copy)]
struct Entry {
    priority: f64,
    cost: f64,
    node: usize,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    /// Max-heap order: lowest priority first, then highest cost, then
    /// lowest index.
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .total_cmp(&self.priority)
            .then(self.cost.total_cmp(&other.cost))
            .then(other.node.cmp(&self.node))
    }
}

/// The cheapest path from `source` to `target`, or `None` when no walkable
/// path joins them. `undirected` walks relationships both ways.
///
/// # Panics
/// When `source` or `target` is not a node of `view`.
pub fn guided_shortest_path(
    view: &GraphView,
    source: usize,
    target: usize,
    undirected: bool,
    step: impl Fn(usize, usize) -> Option<f64>,
    remaining: impl Fn(usize) -> f64,
) -> Option<GuidedPath> {
    let n = view.node_count;
    assert!(
        source < n && target < n,
        "source or target is not in the view"
    );
    let mut best = vec![f64::INFINITY; n];
    let mut parent = vec![usize::MAX; n];
    let mut heap = BinaryHeap::new();
    let mut expanded = 0;
    best[source] = 0.0;
    heap.push(Entry {
        priority: remaining(source),
        cost: 0.0,
        node: source,
    });
    while let Some(Entry { cost, node, .. }) = heap.pop() {
        if cost > best[node] {
            continue; // superseded by a cheaper route
        }
        expanded += 1;
        if node == target {
            let mut path = vec![target];
            let mut at = target;
            while at != source {
                at = parent[at];
                path.push(at);
            }
            path.reverse();
            return Some(GuidedPath {
                path,
                cost,
                expanded,
            });
        }
        let preds: &[usize] = if undirected {
            view.predecessors(node)
        } else {
            &[]
        };
        for &next in view.successors(node).iter().chain(preds) {
            let Some(c) = step(node, next) else { continue };
            debug_assert!(c >= 0.0, "a negative step cost breaks the search");
            let through = cost + c;
            if through < best[next] {
                best[next] = through;
                parent[next] = node;
                heap.push(Entry {
                    priority: through + remaining(next),
                    cost: through,
                    node: next,
                });
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn view(n: usize, edges: &[(usize, usize)]) -> GraphView {
        let (mut out, mut inc) = (vec![Vec::new(); n], vec![Vec::new(); n]);
        for &(a, b) in edges {
            out[a].push(b);
            inc[b].push(a);
        }
        GraphView::from_adjacency_list(
            n,
            (0..n as u64).collect(),
            (0..n).map(|i| (i as u64, i)).collect::<HashMap<_, _>>(),
            out,
            inc,
            None,
        )
    }

    /// Points on a jittered grid, each joined to its right and lower
    /// neighbour and to a few random others: a road-network-like graph.
    fn geometric(side: usize, extra: usize, seed: u64) -> (GraphView, Vec<[f64; 2]>) {
        let mut x = seed | 1;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % 1000) as f64 / 1000.0
        };
        let n = side * side;
        let pos: Vec<[f64; 2]> = (0..n)
            .map(|i| {
                [
                    (i % side) as f64 + 0.4 * next(),
                    (i / side) as f64 + 0.4 * next(),
                ]
            })
            .collect();
        let mut edges = Vec::new();
        for i in 0..n {
            if i % side + 1 < side {
                edges.push((i, i + 1));
            }
            if i + side < n {
                edges.push((i, i + side));
            }
        }
        for _ in 0..extra {
            let a = (next() * n as f64) as usize % n;
            let b = (next() * n as f64) as usize % n;
            edges.push((a, b));
        }
        (view(n, &edges), pos)
    }

    fn dist(a: [f64; 2], b: [f64; 2]) -> f64 {
        ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2)).sqrt()
    }

    /// The longest relationship, for the hop-count bound.
    fn longest(v: &GraphView, pos: &[[f64; 2]]) -> f64 {
        (0..v.node_count)
            .flat_map(|u| v.successors(u).iter().map(move |&w| (u, w)))
            .map(|(u, w)| dist(pos[u], pos[w]))
            .fold(0.0, f64::max)
    }

    #[test]
    fn guided_and_unguided_agree_on_length_and_the_guide_saves_work() {
        let (v, pos) = geometric(30, 40, 7);
        let target = v.node_count - 1;
        let step = |u: usize, w: usize| Some(dist(pos[u], pos[w]));
        let guided =
            guided_shortest_path(&v, 0, target, true, step, |w| dist(pos[w], pos[target])).unwrap();
        let blind = guided_shortest_path(&v, 0, target, true, step, |_| 0.0).unwrap();
        assert!(
            (guided.cost - blind.cost).abs() < 1e-9,
            "{} vs {}",
            guided.cost,
            blind.cost
        );
        assert!(
            guided.expanded * 2 < blind.expanded,
            "the guide saved too little: {} vs {}",
            guided.expanded,
            blind.expanded
        );
        // The path is real and its cost is its length.
        let along: f64 = guided
            .path
            .windows(2)
            .map(|p| dist(pos[p[0]], pos[p[1]]))
            .sum();
        assert!((along - guided.cost).abs() < 1e-9);
        assert_eq!((guided.path[0], *guided.path.last().unwrap()), (0, target));
    }

    #[test]
    fn hop_counting_with_distance_over_the_longest_edge_finds_the_fewest_hops() {
        for seed in [3, 11, 19] {
            let (v, pos) = geometric(20, 30, seed);
            let w = longest(&v, &pos);
            let target = v.node_count - 1;
            let hops = |_: usize, _: usize| Some(1.0);
            let guided =
                guided_shortest_path(&v, 0, target, true, hops, |x| dist(pos[x], pos[target]) / w)
                    .unwrap();
            let bfs = guided_shortest_path(&v, 0, target, true, hops, |_| 0.0).unwrap();
            assert_eq!(guided.cost, bfs.cost, "seed {seed}");
            assert_eq!(guided.path.len() - 1, guided.cost as usize);
            assert!(guided.expanded <= bfs.expanded, "seed {seed}");
        }
    }

    #[test]
    fn unwalkable_steps_are_not_walked_and_unreachable_is_none() {
        // 0 - 1 - 2 - 3, with 1 - 2 refused.
        let v = view(4, &[(0, 1), (1, 2), (2, 3)]);
        let step = |u: usize, w: usize| {
            if (u.min(w), u.max(w)) == (1, 2) {
                None
            } else {
                Some(1.0)
            }
        };
        assert_eq!(guided_shortest_path(&v, 0, 3, true, step, |_| 0.0), None);
        assert!(guided_shortest_path(&v, 0, 1, true, step, |_| 0.0).is_some());
    }

    #[test]
    fn direction_is_followed_unless_undirected() {
        let v = view(3, &[(1, 0), (1, 2)]);
        let one = |_: usize, _: usize| Some(1.0);
        assert_eq!(guided_shortest_path(&v, 0, 2, false, one, |_| 0.0), None);
        let p = guided_shortest_path(&v, 0, 2, true, one, |_| 0.0).unwrap();
        assert_eq!((p.path, p.cost), (vec![0, 1, 2], 2.0));
    }

    #[test]
    fn the_source_is_its_own_path() {
        let v = view(2, &[(0, 1)]);
        let p = guided_shortest_path(&v, 1, 1, true, |_, _| Some(1.0), |_| 5.0).unwrap();
        assert_eq!((p.path, p.cost, p.expanded), (vec![1], 0.0, 1));
    }

    #[test]
    fn an_admissible_but_inconsistent_bound_still_finds_the_shortest() {
        // 0 -> 1 (1), 0 -> 2 (4), 1 -> 2 (1), 2 -> 3 (1). Shortest 0-1-2-3 = 3.
        // The bound at 1 is admissible (2 <= 2) but inconsistent with 0's (0):
        // 2 is first reached through 0 at cost 4 and must be improved later.
        let v = view(4, &[(0, 1), (0, 2), (1, 2), (2, 3)]);
        let cost = |u: usize, w: usize| Some(if (u, w) == (0, 2) { 4.0 } else { 1.0 });
        let bound = |x: usize| [0.0, 2.0, 0.0, 0.0][x];
        let p = guided_shortest_path(&v, 0, 3, false, cost, bound).unwrap();
        assert_eq!((p.path, p.cost), (vec![0, 1, 2, 3], 3.0));
    }

    #[test]
    fn ties_are_broken_the_same_way_every_time() {
        // A square: two equal routes from 0 to 3.
        let v = view(4, &[(0, 1), (0, 2), (1, 3), (2, 3)]);
        let one = |_: usize, _: usize| Some(1.0);
        let first = guided_shortest_path(&v, 0, 3, false, one, |_| 0.0).unwrap();
        for _ in 0..5 {
            assert_eq!(
                guided_shortest_path(&v, 0, 3, false, one, |_| 0.0).unwrap(),
                first
            );
        }
    }

    #[test]
    #[should_panic(expected = "not in the view")]
    fn a_source_outside_the_view_is_a_caller_bug() {
        let v = view(2, &[(0, 1)]);
        guided_shortest_path(&v, 5, 1, true, |_, _| Some(1.0), |_| 0.0);
    }
}
