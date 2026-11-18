//! The shared frontier, traversal and aggregation layer (ALGO-13, #1695).
//!
//! Before this module, eleven algorithms each carried their own breadth-first
//! walk: a `VecDeque`, a distance vector, a neighbour loop, and -- in four
//! files -- their own idea of what an undirected neighbour is. They agreed
//! because each was checked against NetworkX, not because they shared
//! anything, so a fix to one walk reached none of the others.
//!
//! What a traversal algorithm needs, and what is here:
//!
//! | Need | Here |
//! |---|---|
//! | Which edges a step follows | [`Direction`] |
//! | Who a node's neighbours are, under a stated convention | [`neighbours`] (every edge) and [`distinct_neighbours`] (each neighbour once) |
//! | A frontier expanded level by level, from one source or several | [`Bfs`]: [`walk`](Bfs::walk), [`extend`](Bfs::extend), [`distances`](Bfs::distances) |
//! | To see every edge a walk examines, and stop it | the `visit` callback, given an [`Edge`]; return [`Visit::Stop`] |
//! | To prune where the walk goes | the `expand` callback, given the node and its depth |
//! | A value per node, folded over its neighbours | [`aggregate_neighbours`] |
//!
//! [`BUILT_ON_FRONTIER`] lists the algorithms built on it; a test checks that
//! each listed function really calls into this module and that the list holds
//! at least [`FRONTIER_TARGET`] of them.
//!
//! # Adding a traversal algorithm
//!
//! Hop distance from a set of seeds to every node, reading the graph as
//! undirected, is the whole of this:
//!
//! ```
//! use samyama_graph_algorithms::frontier::{neighbours, Bfs, Direction, Visit};
//! # use samyama_graph_algorithms::GraphView;
//! # use std::collections::HashMap;
//! # let view = GraphView::from_adjacency_list(3, vec![0, 1, 2],
//! #     HashMap::from([(0, 0), (1, 1), (2, 2)]), vec![vec![1], vec![2], vec![]],
//! #     vec![vec![], vec![0], vec![1]], None);
//! let mut bfs = Bfs::new(view.node_count);
//! bfs.walk(&[2], |u, _depth| neighbours(&view, u, Direction::Both), |_edge| Visit::Continue);
//! assert_eq!(bfs.depth(0), Some(2));
//! ```
//!
//! # What the walk guarantees
//!
//! * **Order.** Nodes are expanded in discovery order (first in, first out),
//!   and each node's neighbours in the order `expand` yields them. That is the
//!   order every algorithm here used before, so their answers did not change:
//!   `frontier_parity_tests` holds each one to its pre-port output, bit for bit.
//! * **Every edge is seen.** `visit` is called for every edge `expand` yields,
//!   not only those that discover a node -- Brandes' betweenness counts the
//!   second shortest route into a node, and a bipartite check needs the edge
//!   that closes an odd cycle.
//! * **Reuse is cheap.** A [`Bfs`] is built once per graph and walked from
//!   many sources; a new walk clears only the nodes the last one reached.

use crate::common::GraphView;

/// Which edges a step follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Source to target.
    Out,
    /// Target to source: the reversed graph.
    In,
    /// Both ways: the undirected reading of a view that stores each edge once.
    Both,
}

impl Direction {
    /// The convention most algorithms here take as a flag: `true` walks both
    /// ways, `false` follows edges as stored.
    pub fn undirected_if(bidirectional: bool) -> Self {
        if bidirectional {
            Self::Both
        } else {
            Self::Out
        }
    }

    /// For distances *into* a node (closeness, harmonic): both ways when
    /// undirected, against the edges when not.
    pub fn into_node_unless(bidirectional: bool) -> Self {
        if bidirectional {
            Self::Both
        } else {
            Self::In
        }
    }
}

/// Every neighbour of `u` along `dir`, one per edge: a parallel edge appears
/// twice and a self-loop names `u`. Successors come before predecessors.
///
/// The multigraph reading, for walks that count routes: two parallel edges
/// are two shortest paths to Brandes, and dropping one halves a score.
pub fn neighbours(view: &GraphView, u: usize, dir: Direction) -> impl Iterator<Item = usize> + '_ {
    let succ: &[usize] = if dir == Direction::In { &[] } else { view.successors(u) };
    let pred: &[usize] = if dir == Direction::Out { &[] } else { view.predecessors(u) };
    succ.iter().chain(pred).copied()
}

/// Each neighbour of `u` along `dir` once, ascending, without `u` itself.
///
/// The simple-graph reading, for questions about which nodes sit beside
/// which: a reciprocal pair or a parallel edge is one adjacency, and a
/// self-loop is not an adjacency at all. Ascending, so anything built on it
/// is the same on every run.
pub fn distinct_neighbours(view: &GraphView, u: usize, dir: Direction) -> Vec<usize> {
    let mut v: Vec<usize> = neighbours(view, u, dir).collect();
    v.sort_unstable();
    v.dedup();
    v.retain(|&x| x != u);
    v
}

/// For every node, `fold` over its neighbours, starting from `init(node)`.
///
/// `neighbours_of` decides the reading -- [`neighbours`],
/// [`distinct_neighbours`], or an algorithm's own -- and is called once per
/// node. The fold runs in the order it yields, so a float sum is the same sum
/// on every run.
pub fn aggregate_neighbours<T, I>(
    node_count: usize,
    mut neighbours_of: impl FnMut(usize) -> I,
    mut init: impl FnMut(usize) -> T,
    mut fold: impl FnMut(T, usize) -> T,
) -> Vec<T>
where
    I: IntoIterator<Item = usize>,
{
    (0..node_count)
        .map(|u| neighbours_of(u).into_iter().fold(init(u), &mut fold))
        .collect()
}

/// One edge a walk examined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Edge {
    pub from: usize,
    pub to: usize,
    /// Hops from the walk's sources to `from`.
    pub from_depth: usize,
    /// Hops to `to`, set by this edge if it discovered `to`.
    pub to_depth: usize,
    /// This edge reached `to` first. On a later edge into an already reached
    /// node, `to_depth == from_depth + 1` means another shortest route.
    pub discovered: bool,
}

impl Edge {
    /// Does this edge lie on a shortest route from the sources to `to`?
    pub fn is_shortest(&self) -> bool {
        self.to_depth == self.from_depth + 1
    }
}

/// What a walk should do after an edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visit {
    Continue,
    /// End the walk now; the nodes already reached keep their depths.
    Stop,
}

/// Depth of a node no walk has reached.
const UNREACHED: usize = usize::MAX;

/// A breadth-first frontier over a graph of a fixed size, reused across walks.
#[derive(Debug, Clone)]
pub struct Bfs {
    depth: Vec<usize>,
    /// Every reached node in discovery order; also the queue, from `head` on.
    order: Vec<usize>,
    head: usize,
}

impl Bfs {
    /// A frontier for a graph of `node_count` nodes, with nothing reached.
    pub fn new(node_count: usize) -> Self {
        Self { depth: vec![UNREACHED; node_count], order: Vec::new(), head: 0 }
    }

    /// Forget the last walk. Costs the number of nodes it reached, not the
    /// size of the graph.
    pub fn clear(&mut self) {
        for &v in &self.order {
            self.depth[v] = UNREACHED;
        }
        self.order.clear();
        self.head = 0;
    }

    /// Walk from `sources`, each at depth 0, forgetting any earlier walk.
    ///
    /// `expand(node, depth)` yields the nodes one step from `node`; yielding
    /// nothing prunes the walk there. `visit` sees every edge yielded, after
    /// the walk has recorded what that edge discovered. Returns `true` when
    /// `visit` stopped the walk.
    ///
    /// # Panics
    /// When a source or a yielded node is not below `node_count`.
    pub fn walk<I>(
        &mut self,
        sources: &[usize],
        expand: impl FnMut(usize, usize) -> I,
        visit: impl FnMut(&Edge) -> Visit,
    ) -> bool
    where
        I: IntoIterator<Item = usize>,
    {
        self.clear();
        self.extend(sources, expand, visit)
    }

    /// [`walk`](Self::walk) without forgetting: nodes reached before stay
    /// reached and are not expanded again. A source already reached is
    /// skipped. Walking each unreached node in turn this way visits a graph
    /// component by component, each tree's depths counted from its own root.
    pub fn extend<I>(
        &mut self,
        sources: &[usize],
        mut expand: impl FnMut(usize, usize) -> I,
        mut visit: impl FnMut(&Edge) -> Visit,
    ) -> bool
    where
        I: IntoIterator<Item = usize>,
    {
        for &s in sources {
            if self.depth[s] == UNREACHED {
                self.depth[s] = 0;
                self.order.push(s);
            }
        }
        while self.head < self.order.len() {
            let from = self.order[self.head];
            self.head += 1;
            let from_depth = self.depth[from];
            for to in expand(from, from_depth) {
                let discovered = self.depth[to] == UNREACHED;
                if discovered {
                    self.depth[to] = from_depth + 1;
                    self.order.push(to);
                }
                let edge = Edge { from, to, from_depth, to_depth: self.depth[to], discovered };
                if visit(&edge) == Visit::Stop {
                    return true;
                }
            }
        }
        false
    }

    /// Hop distance from `source` to every node along `dir`; read the answer
    /// with [`depth`](Self::depth) and [`reached`](Self::reached).
    pub fn distances(&mut self, view: &GraphView, source: usize, dir: Direction) -> &Self {
        self.walk(&[source], |u, _| neighbours(view, u, dir), |_| Visit::Continue);
        self
    }

    /// Hops from the sources to `v`, or `None` if the walk did not reach it.
    pub fn depth(&self, v: usize) -> Option<usize> {
        let d = self.depth[v];
        (d != UNREACHED).then_some(d)
    }

    /// Every node reached, in the order the walk discovered it.
    pub fn reached(&self) -> &[usize] {
        &self.order
    }
}

/// How many algorithms [`BUILT_ON_FRONTIER`] must hold (ALGO-13's target).
pub const FRONTIER_TARGET: usize = 10;

/// The algorithms whose traversal or aggregation is this module's: the
/// public function, the file defining it, and the `CALL` names that reach it.
///
/// Checked, not just listed: a test reads each function's source and fails if
/// neither it nor a helper it calls uses this module, so an entry cannot stay
/// after its function stops using the framework. Algorithms that reach the
/// framework only through another listed one -- `diameter` and `radius`
/// through `eccentricity`, `wiener_index` through `all_pairs_hops` -- are not
/// counted again.
pub const BUILT_ON_FRONTIER: &[(&str, &str, &[&str])] = &[
    ("closeness_centrality", "centrality.rs", &["algo.closeness"]),
    ("harmonic_centrality", "centrality.rs", &["algo.harmonic"]),
    ("betweenness_centrality", "centrality.rs", &["algo.betweenness"]),
    ("eccentricity", "metrics.rs", &["algo.eccentricity", "algo.diameter", "algo.radius"]),
    ("average_neighbour_degree", "metrics.rs", &["algo.averageNeighborDegree"]),
    ("all_pairs_hops", "paths_extra.rs", &["algo.allPairsShortestPath", "algo.wienerIndex"]),
    ("transitive_closure", "paths_extra.rs", &["algo.transitiveClosure"]),
    ("global_efficiency", "structure_extra.rs", &["algo.globalEfficiency"]),
    ("bipartite_sets", "structure_extra.rs", &["algo.bipartite"]),
    ("bfs", "pathfinding.rs", &["algo.shortestPath"]),
    ("bfs_all_shortest_paths", "pathfinding.rs", &[]),
    ("all_shortest_paths", "pathfinding_extra.rs", &["algo.allShortestPaths"]),
    ("negative_samples", "negative_sampling.rs", &["algo.negativeSampling"]),
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// `0 -> 1 -> 2`, `0 -> 1` again, `3 -> 3`, and `4` alone.
    fn view() -> GraphView {
        let edges = [(0, 1), (1, 2), (0, 1), (3, 3)];
        let n = 5;
        let (mut out, mut inc) = (vec![Vec::new(); n], vec![Vec::new(); n]);
        for (a, b) in edges {
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

    #[test]
    fn neighbours_follow_the_direction_and_keep_every_edge() {
        let v = view();
        assert_eq!(neighbours(&v, 1, Direction::Out).collect::<Vec<_>>(), vec![2]);
        assert_eq!(neighbours(&v, 1, Direction::In).collect::<Vec<_>>(), vec![0, 0]);
        assert_eq!(neighbours(&v, 1, Direction::Both).collect::<Vec<_>>(), vec![2, 0, 0]);
        assert_eq!(neighbours(&v, 3, Direction::Both).collect::<Vec<_>>(), vec![3, 3]);
        assert_eq!(neighbours(&v, 4, Direction::Both).count(), 0);
    }

    #[test]
    fn distinct_neighbours_drop_repeats_and_self() {
        let v = view();
        assert_eq!(distinct_neighbours(&v, 1, Direction::Both), vec![0, 2]);
        assert_eq!(distinct_neighbours(&v, 0, Direction::Out), vec![1]);
        assert!(distinct_neighbours(&v, 3, Direction::Both).is_empty());
    }

    #[test]
    fn direction_from_a_flag() {
        assert_eq!(Direction::undirected_if(true), Direction::Both);
        assert_eq!(Direction::undirected_if(false), Direction::Out);
        assert_eq!(Direction::into_node_unless(true), Direction::Both);
        assert_eq!(Direction::into_node_unless(false), Direction::In);
    }

    #[test]
    fn a_walk_reports_depths_order_and_every_edge() {
        let v = view();
        let mut bfs = Bfs::new(v.node_count);
        let mut edges = Vec::new();
        let stopped = bfs.walk(&[0], |u, _| neighbours(&v, u, Direction::Out), |e| {
            edges.push(*e);
            Visit::Continue
        });
        assert!(!stopped);
        assert_eq!(bfs.reached(), &[0, 1, 2]);
        assert_eq!((bfs.depth(0), bfs.depth(2), bfs.depth(4)), (Some(0), Some(2), None));
        // The parallel edge is seen too, second, and is a shortest route.
        assert_eq!(edges.len(), 3);
        assert!(edges[0].discovered && !edges[1].discovered && edges[1].is_shortest());
        assert_eq!((edges[1].from, edges[1].to, edges[1].to_depth), (0, 1, 1));
    }

    #[test]
    fn a_new_walk_forgets_the_last_and_extend_does_not() {
        let v = view();
        let mut bfs = Bfs::new(v.node_count);
        bfs.distances(&v, 0, Direction::Out);
        bfs.distances(&v, 3, Direction::Out);
        assert_eq!(bfs.reached(), &[3]);
        assert_eq!(bfs.depth(0), None, "the earlier walk's depths survived");
        bfs.extend(&[0, 3], |u, _| neighbours(&v, u, Direction::Out), |_| Visit::Continue);
        assert_eq!(bfs.reached(), &[3, 0, 1, 2], "extend re-walked or lost a node");
        assert_eq!(bfs.depth(3), Some(0));
    }

    #[test]
    fn several_sources_start_at_depth_zero() {
        let v = view();
        let mut bfs = Bfs::new(v.node_count);
        bfs.walk(&[2, 4, 2], |u, _| neighbours(&v, u, Direction::Both), |_| Visit::Continue);
        assert_eq!(bfs.reached(), &[2, 4, 1, 0]);
        assert_eq!((bfs.depth(4), bfs.depth(1), bfs.depth(0)), (Some(0), Some(1), Some(2)));
    }

    #[test]
    fn visit_can_stop_the_walk_and_expand_can_prune_it() {
        let v = view();
        let mut bfs = Bfs::new(v.node_count);
        let stopped = bfs.walk(&[0], |u, _| neighbours(&v, u, Direction::Out), |e| {
            if e.to == 1 {
                Visit::Stop
            } else {
                Visit::Continue
            }
        });
        assert!(stopped);
        assert_eq!(bfs.depth(2), None, "the walk went on after Stop");
        // Pruned at depth 1: 2 is never reached.
        bfs.walk(&[0], |u, d| neighbours(&v, u, Direction::Out).filter(move |_| d < 1), |_| Visit::Continue);
        assert_eq!(bfs.reached(), &[0, 1]);
    }

    #[test]
    fn aggregation_folds_in_neighbour_order() {
        let v = view();
        let degree = aggregate_neighbours(v.node_count, |u| neighbours(&v, u, Direction::Both), |_| 0, |n, _| n + 1);
        assert_eq!(degree, vec![2, 3, 1, 2, 0]);
        let order = aggregate_neighbours(
            v.node_count,
            |u| distinct_neighbours(&v, u, Direction::Both),
            |_| Vec::new(),
            |mut seen, w| {
                seen.push(w);
                seen
            },
        );
        assert_eq!(order[1], vec![0, 2]);
    }

    #[test]
    fn an_empty_graph_walks_nowhere() {
        let mut bfs = Bfs::new(0);
        assert!(!bfs.walk(&[], |_, _| std::iter::empty(), |_| Visit::Continue));
        assert!(bfs.reached().is_empty());
    }

    /// Every listed algorithm calls into this module, directly or through a
    /// helper in its own file, and no longer keeps a queue of its own.
    #[test]
    fn every_listed_algorithm_is_built_on_the_frontier() {
        let source = |file: &str| -> &'static str {
            match file {
                "centrality.rs" => include_str!("centrality.rs"),
                "metrics.rs" => include_str!("metrics.rs"),
                "paths_extra.rs" => include_str!("paths_extra.rs"),
                "structure_extra.rs" => include_str!("structure_extra.rs"),
                "pathfinding.rs" => include_str!("pathfinding.rs"),
                "pathfinding_extra.rs" => include_str!("pathfinding_extra.rs"),
                "negative_sampling.rs" => include_str!("negative_sampling.rs"),
                other => panic!("{other}: add it to this match"),
            }
        };
        let mut wrong = Vec::new();
        for &(name, file, _) in BUILT_ON_FRONTIER {
            let text = source(file);
            let Some(body) = function_body(text, name) else {
                wrong.push(format!("{file}: no `fn {name}`"));
                continue;
            };
            if !uses_frontier(text, name) {
                wrong.push(format!("{file}: `{name}` does not use the frontier module"));
            }
            if body.contains("VecDeque") {
                wrong.push(format!("{file}: `{name}` still keeps its own queue"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
        assert!(
            BUILT_ON_FRONTIER.len() >= FRONTIER_TARGET,
            "{} algorithms on the frontier, target {FRONTIER_TARGET}",
            BUILT_ON_FRONTIER.len()
        );
    }

    /// Does `fn name`, or anything it calls in the same file, use this
    /// module? Followed through helpers: `negative_samples` reaches the walk
    /// through `khop_pairs` and then `within_hops`.
    fn uses_frontier(text: &str, name: &str) -> bool {
        let fns = helpers(text);
        let mut seen = vec![name];
        let mut todo = vec![name];
        while let Some(f) = todo.pop() {
            let Some(body) = function_body(text, f) else { continue };
            if body.contains("frontier::") {
                return true;
            }
            for (h, _) in &fns {
                if !seen.contains(h) && body.contains(&format!("{h}(")) {
                    seen.push(h);
                    todo.push(h);
                }
            }
        }
        false
    }

    /// The text of `fn name`'s body, braces matched.
    fn function_body<'a>(text: &'a str, name: &str) -> Option<&'a str> {
        let start = [format!("pub fn {name}("), format!("fn {name}(")].iter().find_map(|s| text.find(s.as_str()))?;
        let open = start + text[start..].find('{')?;
        let mut depth = 0;
        for (i, c) in text[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(&text[open..=open + i]);
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Every non-test `fn` in a file and its body.
    fn helpers(text: &str) -> Vec<(&str, &str)> {
        let code = text.split("#[cfg(test)]").next().unwrap_or(text);
        code.match_indices("fn ")
            .filter_map(|(i, _)| {
                let name = code[i + 3..].split(|c: char| !(c.is_alphanumeric() || c == '_')).next()?;
                Some((name, function_body(code, name)?))
            })
            .collect()
    }

    #[test]
    fn the_source_reader_finds_bodies_and_misses_absent_ones() {
        let text = "fn a(x: u8) { if x { b(); } }\nfn b() { frontier::x(); }\n#[cfg(test)]\nfn c() {}";
        assert_eq!(function_body(text, "a"), Some("{ if x { b(); } }"));
        assert_eq!(function_body(text, "nope"), None);
        let names: Vec<&str> = helpers(text).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["a", "b"]);
        assert!(uses_frontier(text, "a"), "a reaches the frontier through b");
        assert!(!uses_frontier("fn a() { b(); }\nfn b() { a(); }", "a"), "a cycle of calls ended the search wrongly");
    }
}
