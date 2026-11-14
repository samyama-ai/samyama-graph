//! Shortest path with a side constraint (OPT-06, #1770).
//!
//! The cheapest path from `source` to `target` whose total *resource* stays
//! within a limit: the cheapest route whose travel time is at most 2 hours,
//! the shortest supply chain whose risk stays under a threshold. Every edge
//! carries a cost and a resource, and both add up along a path.
//!
//! Solved exactly, by label setting. A label is one partial path, as the pair
//! (cost so far, resource so far). Labels are expanded cheapest first, and a
//! node keeps only labels that no other label at that node beats on *both*
//! cost and resource, because a beaten label can never lead to a better path.
//! With non-negative costs and resources, the first label that reaches
//! `target` is an optimal path within the limit.
//!
//! The problem is NP-hard in general: the number of labels a node keeps can
//! grow with the graph. So the caller passes `interrupted`, which is asked as
//! the search goes and ends it when it answers `true`.

use super::common::{GraphView, NodeId};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// The best path found: its nodes from `source` to `target`, and its totals.
#[derive(Debug, Clone, PartialEq)]
pub struct ConstrainedPath {
    pub path: Vec<NodeId>,
    pub cost: f64,
    pub resource: f64,
}

/// Why the search could not answer. "No path within the limit" is not one of
/// these: that is `Ok(None)`.
#[derive(Debug, Clone, PartialEq)]
pub enum ConstrainedPathError {
    /// `source` or `target` is not in the view.
    NodeNotFound(NodeId),
    /// An edge cost below zero (or NaN). Expanding cheapest first is only
    /// exact when no edge makes a path cheaper.
    NegativeCost(f64),
    /// An edge resource below zero (or NaN). Pruning a path that is over the
    /// limit is only safe when no edge brings it back under.
    NegativeResource(f64),
    /// `max_resource` below zero, or NaN.
    BadLimit(f64),
    /// `resources` does not have one value per edge of the view.
    Misaligned { edges: usize, resources: usize },
    /// `interrupted` answered `true`.
    Interrupted,
}

impl std::fmt::Display for ConstrainedPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeNotFound(id) => write!(f, "node {id} is not in the graph"),
            Self::NegativeCost(w) => write!(f, "an edge cost is {w}; costs must be zero or more"),
            Self::NegativeResource(w) => {
                write!(f, "an edge resource is {w}; resources must be zero or more")
            }
            Self::BadLimit(m) => write!(f, "the resource limit is {m}; it must be zero or more"),
            Self::Misaligned { edges, resources } => {
                write!(f, "{resources} resource values for {edges} edges")
            }
            Self::Interrupted => write!(f, "interrupted"),
        }
    }
}

impl std::error::Error for ConstrainedPathError {}

/// A cost, resource or limit the search cannot use: below zero, or NaN.
fn invalid(x: f64) -> bool {
    x.is_nan() || x < 0.0
}

/// How many labels are expanded between two calls to `interrupted`.
const CHECK_EVERY: usize = 1024;

/// One partial path: where it ends, its totals, and the label it extends.
struct Label {
    node: usize,
    cost: f64,
    resource: f64,
    parent: Option<usize>,
}

/// A label waiting in the queue. Ordered so the heap pops the lowest cost
/// first, then the lowest resource, then the label made first, which makes
/// the answer the same on every run.
struct Queued {
    cost: f64,
    resource: f64,
    label: usize,
}

impl Ord for Queued {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed: `BinaryHeap` is a max-heap.
        other
            .cost
            .total_cmp(&self.cost)
            .then_with(|| other.resource.total_cmp(&self.resource))
            .then_with(|| other.label.cmp(&self.label))
    }
}

impl PartialOrd for Queued {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Queued {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Queued {}

/// The cheapest path from `source` to `target` with total resource at most
/// `max_resource`, or `Ok(None)` when no path is within the limit.
///
/// Costs are `view.weights`, `1.0` per edge when the view has none (a hop
/// count). `resources` has one value per edge, in `view.out_targets` order.
/// Parallel edges are separate choices, each with its own cost and resource.
/// `source == target` is the empty path, cost and resource 0.
pub fn constrained_shortest_path(
    view: &GraphView,
    resources: &[f64],
    source: NodeId,
    target: NodeId,
    max_resource: f64,
    interrupted: &dyn Fn() -> bool,
) -> Result<Option<ConstrainedPath>, ConstrainedPathError> {
    if invalid(max_resource) {
        return Err(ConstrainedPathError::BadLimit(max_resource));
    }
    if resources.len() != view.out_targets.len() {
        return Err(ConstrainedPathError::Misaligned {
            edges: view.out_targets.len(),
            resources: resources.len(),
        });
    }
    if let Some(&w) = view.weights.iter().flatten().find(|w| invalid(**w)) {
        return Err(ConstrainedPathError::NegativeCost(w));
    }
    if let Some(&w) = resources.iter().find(|w| invalid(**w)) {
        return Err(ConstrainedPathError::NegativeResource(w));
    }
    let index = |id: NodeId| {
        view.node_to_index
            .get(&id)
            .copied()
            .ok_or(ConstrainedPathError::NodeNotFound(id))
    };
    let (from, to) = (index(source)?, index(target)?);

    let mut labels = vec![Label { node: from, cost: 0.0, resource: 0.0, parent: None }];
    // Labels still worth keeping at each node: none beats another on both
    // cost and resource. A label beaten later is marked dead, not removed
    // from the heap; it is skipped when it comes out.
    let mut kept: Vec<Vec<usize>> = vec![Vec::new(); view.node_count];
    let mut alive = vec![true];
    kept[from].push(0);
    let mut queue = BinaryHeap::from([Queued { cost: 0.0, resource: 0.0, label: 0 }]);

    let mut until_check = CHECK_EVERY;
    while let Some(Queued { label: id, .. }) = queue.pop() {
        if !alive[id] {
            continue;
        }
        let (u, cost, resource) = (labels[id].node, labels[id].cost, labels[id].resource);
        if u == to {
            let mut path = Vec::new();
            let mut at = Some(id);
            while let Some(l) = at {
                path.push(view.index_to_node[labels[l].node]);
                at = labels[l].parent;
            }
            path.reverse();
            return Ok(Some(ConstrainedPath { path, cost, resource }));
        }
        until_check -= 1;
        if until_check == 0 {
            until_check = CHECK_EVERY;
            if interrupted() {
                return Err(ConstrainedPathError::Interrupted);
            }
        }
        for edge in view.out_offsets[u]..view.out_offsets[u + 1] {
            let v = view.out_targets[edge];
            let c = cost + view.weights.as_ref().map_or(1.0, |w| w[edge]);
            let r = resource + resources[edge];
            if r > max_resource {
                continue;
            }
            // Beaten (or equalled) by a label already kept at `v`: this one
            // can never do better. Equal counts as beaten, so a cycle that
            // costs nothing is not walked twice.
            if kept[v]
                .iter()
                .any(|&k| labels[k].cost <= c && labels[k].resource <= r)
            {
                continue;
            }
            // And the labels this one beats are no longer worth keeping.
            kept[v].retain(|&k| {
                let beaten = labels[k].cost >= c && labels[k].resource >= r;
                if beaten {
                    alive[k] = false;
                }
                !beaten
            });
            let new = labels.len();
            labels.push(Label { node: v, cost: c, resource: r, parent: Some(id) });
            alive.push(true);
            kept[v].push(new);
            queue.push(Queued { cost: c, resource: r, label: new });
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A view over `n` nodes (ids `10 + index`) from `(from, to, cost,
    /// resource)` edges, and the resources in `out_targets` order.
    fn graph(n: usize, edges: &[(usize, usize, f64, f64)]) -> (GraphView, Vec<f64>) {
        let index_to_node: Vec<NodeId> = (0..n as u64).map(|i| 10 + i).collect();
        let node_to_index: HashMap<NodeId, usize> =
            index_to_node.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        let mut outgoing = vec![Vec::new(); n];
        let mut incoming = vec![Vec::new(); n];
        let mut costs = vec![Vec::new(); n];
        let mut res = vec![Vec::new(); n];
        for &(a, b, c, r) in edges {
            outgoing[a].push(b);
            incoming[b].push(a);
            costs[a].push(c);
            res[a].push(r);
        }
        let view = GraphView::from_adjacency_list(
            n, index_to_node, node_to_index, outgoing, incoming, Some(costs),
        );
        (view, res.into_iter().flatten().collect())
    }

    fn never() -> bool {
        false
    }

    fn solve(g: &(GraphView, Vec<f64>), s: usize, t: usize, max: f64) -> Option<ConstrainedPath> {
        constrained_shortest_path(&g.0, &g.1, 10 + s as u64, 10 + t as u64, max, &never).unwrap()
    }

    /// s=0, a=1, b=2, t=3. Three routes: cheap and slow (s-a-t: cost 2,
    /// time 20), dearer and quick (s-b-t: cost 10, time 2), dearest and
    /// quickest (s-t: cost 20, time 1).
    fn three_routes() -> (GraphView, Vec<f64>) {
        graph(4, &[
            (0, 1, 1.0, 10.0), (1, 3, 1.0, 10.0),
            (0, 2, 5.0, 1.0), (2, 3, 5.0, 1.0),
            (0, 3, 20.0, 1.0),
        ])
    }

    #[test]
    fn the_limit_decides_which_route_is_cheapest() {
        let g = three_routes();
        let at = |max| solve(&g, 0, 3, max).map(|p| (p.path, p.cost, p.resource));
        assert_eq!(at(100.0), Some((vec![10, 11, 13], 2.0, 20.0)), "no real limit: the cheapest");
        assert_eq!(at(20.0), Some((vec![10, 11, 13], 2.0, 20.0)), "a limit met exactly is within it");
        assert_eq!(at(19.0), Some((vec![10, 12, 13], 10.0, 2.0)), "the cheap route is too slow");
        assert_eq!(at(1.0), Some((vec![10, 13], 20.0, 1.0)), "only the direct edge fits");
        assert_eq!(at(0.5), None, "nothing fits");
    }

    #[test]
    fn parallel_edges_are_separate_choices() {
        // Two s->t edges: cheap and slow, dear and quick.
        let g = graph(2, &[(0, 1, 1.0, 10.0), (0, 1, 5.0, 1.0)]);
        assert_eq!(solve(&g, 0, 1, 100.0).map(|p| p.cost), Some(1.0));
        assert_eq!(solve(&g, 0, 1, 5.0).map(|p| (p.cost, p.resource)), Some((5.0, 1.0)));
    }

    #[test]
    fn a_cycle_that_costs_nothing_ends() {
        let g = graph(3, &[(0, 1, 0.0, 0.0), (1, 0, 0.0, 0.0), (1, 2, 1.0, 1.0)]);
        assert_eq!(solve(&g, 0, 2, 10.0).map(|p| p.path), Some(vec![10, 11, 12]));
    }

    #[test]
    fn source_equal_to_target_is_the_empty_path() {
        let g = three_routes();
        assert_eq!(
            solve(&g, 2, 2, 0.0),
            Some(ConstrainedPath { path: vec![12], cost: 0.0, resource: 0.0 })
        );
    }

    #[test]
    fn unreachable_is_none() {
        let g = three_routes();
        assert_eq!(solve(&g, 3, 0, 1e9), None, "edges are directed");
    }

    #[test]
    fn bad_input_is_refused_by_name() {
        let g = three_routes();
        let run = |g: &(GraphView, Vec<f64>), s: u64, t: u64, max: f64| {
            constrained_shortest_path(&g.0, &g.1, s, t, max, &never).unwrap_err()
        };
        assert_eq!(run(&g, 99, 13, 1.0), ConstrainedPathError::NodeNotFound(99));
        assert_eq!(run(&g, 10, 99, 1.0), ConstrainedPathError::NodeNotFound(99));
        assert_eq!(run(&g, 10, 13, -1.0), ConstrainedPathError::BadLimit(-1.0));
        assert!(matches!(run(&g, 10, 13, f64::NAN), ConstrainedPathError::BadLimit(_)));
        let neg_cost = graph(2, &[(0, 1, -1.0, 1.0)]);
        assert_eq!(run(&neg_cost, 10, 11, 5.0), ConstrainedPathError::NegativeCost(-1.0));
        let neg_res = graph(2, &[(0, 1, 1.0, -2.0)]);
        assert_eq!(run(&neg_res, 10, 11, 5.0), ConstrainedPathError::NegativeResource(-2.0));
        let short = constrained_shortest_path(&g.0, &g.1[..2], 10, 13, 1.0, &never).unwrap_err();
        assert_eq!(short, ConstrainedPathError::Misaligned { edges: 5, resources: 2 });
    }

    #[test]
    fn interrupted_ends_the_search() {
        // A 40 x 40 grid, right and down, with costs and resources that
        // trade off, so many labels survive and the search runs long enough
        // to be asked.
        let side = 40;
        let mut edges = Vec::new();
        for r in 0..side {
            for c in 0..side {
                let i = r * side + c;
                if c + 1 < side { edges.push((i, i + 1, 1.0 + (i % 7) as f64, 1.0 + (i % 5) as f64)); }
                if r + 1 < side { edges.push((i, i + side, 1.0 + (i % 5) as f64, 1.0 + (i % 7) as f64)); }
            }
        }
        let g = graph(side * side, &edges);
        let t = 10 + (side * side - 1) as u64;
        let err = constrained_shortest_path(&g.0, &g.1, 10, t, 1e9, &|| true).unwrap_err();
        assert_eq!(err, ConstrainedPathError::Interrupted);
        // And without the interruption it answers.
        assert!(constrained_shortest_path(&g.0, &g.1, 10, t, 1e9, &never).unwrap().is_some());
    }

    /// Every simple path from `s` to `t`, by depth-first search, as (cost,
    /// resource). The reference the label search is checked against.
    fn all_simple_paths(edges: &[(usize, usize, f64, f64)], n: usize, s: usize, t: usize) -> Vec<(f64, f64)> {
        fn walk(
            u: usize, t: usize, edges: &[(usize, usize, f64, f64)], seen: &mut Vec<bool>,
            cost: f64, res: f64, out: &mut Vec<(f64, f64)>,
        ) {
            if u == t {
                out.push((cost, res));
                return;
            }
            for &(a, b, c, r) in edges {
                if a == u && !seen[b] {
                    seen[b] = true;
                    walk(b, t, edges, seen, cost + c, res + r, out);
                    seen[b] = false;
                }
            }
        }
        let mut seen = vec![false; n];
        seen[s] = true;
        let mut out = Vec::new();
        walk(s, t, edges, &mut seen, 0.0, 0.0, &mut out);
        out
    }

    /// On random small graphs the answer is the true optimum: the lowest cost
    /// of any simple path within the limit (a path with a cycle is never
    /// better, since costs and resources are not negative). The path returned
    /// is a real path whose totals are the ones reported.
    #[test]
    fn matches_brute_force_on_random_graphs() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        // How many cases had a path within the limit, and in how many the
        // limit changed the answer: a check that passed because every case
        // was empty, or unconstrained, would say nothing.
        let (mut found, mut binding) = (0, 0);
        for case in 0..2000 {
            let n = 2 + (next() % 7) as usize;
            let m = (next() % (3 * n as u64)) as usize;
            let mut edges = Vec::new();
            for _ in 0..m {
                let (a, b) = ((next() % n as u64) as usize, (next() % n as u64) as usize);
                // Integer weights, so sums compare exactly.
                let (c, r) = ((next() % 10) as f64, (next() % 10) as f64);
                if a != b && !edges.iter().any(|&(x, y, _, _)| (x, y) == (a, b)) {
                    edges.push((a, b, c, r));
                }
            }
            let max = (next() % 25) as f64;
            let (s, t) = (0, n - 1);
            let g = graph(n, &edges);
            let got = solve(&g, s, t, max);
            let paths = all_simple_paths(&edges, n, s, t);
            let cheapest = |within: f64| {
                paths
                    .iter()
                    .filter(|&&(_, r)| r <= within)
                    .map(|&(c, _)| c)
                    .fold(None, |acc: Option<f64>, c| Some(acc.map_or(c, |a| a.min(c))))
            };
            let best = cheapest(max);
            if best.is_some() {
                found += 1;
            }
            if best != cheapest(f64::INFINITY) {
                binding += 1;
            }
            assert_eq!(got.as_ref().map(|p| p.cost), best, "case {case}: {edges:?}, max {max}");
            if let Some(p) = got {
                let (mut c, mut r) = (0.0, 0.0);
                for hop in p.path.windows(2) {
                    let (a, b) = ((hop[0] - 10) as usize, (hop[1] - 10) as usize);
                    let &(_, _, ec, er) = edges.iter().find(|e| (e.0, e.1) == (a, b))
                        .unwrap_or_else(|| panic!("case {case}: {a}->{b} is not an edge"));
                    c += ec;
                    r += er;
                }
                assert_eq!((c, r), (p.cost, p.resource), "case {case}: totals of the path returned");
                assert!(r <= max, "case {case}: over the limit");
                assert_eq!((p.path[0], *p.path.last().unwrap()), (10 + s as u64, 10 + t as u64));
            }
        }
        assert!(found >= 400 && binding >= 200, "{found} cases with a path, {binding} where the limit mattered");
        eprintln!("brute force: {found} of 2000 cases had a path within the limit; the limit changed the answer in {binding}");
    }
}
