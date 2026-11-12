//! Negative sampling for link prediction (ML-04, #1725).
//!
//! A link-prediction model learns from pairs that are connected (the edges,
//! its positives) and pairs that are not (its negatives). The graph supplies
//! the positives; the negatives have to be drawn, and how they are drawn
//! decides what the model learns:
//!
//! | strategy | a negative is | what it guards against |
//! |---|---|---|
//! | [`NegativeStrategy::Random`] | any unconnected pair, uniformly | nothing; the baseline |
//! | [`NegativeStrategy::DegreeMatched`] | an unconnected pair whose ends are drawn in proportion to their degree | a model that only learns "well-connected nodes connect": random negatives are mostly low-degree pairs, positives are not, so degree alone separates them |
//! | [`NegativeStrategy::KHop`] | an unconnected pair 2 to `k` hops apart | easy negatives: nearby pairs are the ones a model must tell apart from real edges |
//!
//! Drawing each end in proportion to its degree is what "degree-matched"
//! means here: an end of a uniformly chosen edge has exactly that
//! distribution, so the negatives' ends are distributed as the positives'
//! ends are.
//!
//! That holds while the no-edge and no-repeat rules below rarely bind. On a
//! graph of 400 nodes, sampling 300 negatives, degree-matched ends average
//! degree 16.3 against 16.7 for edge ends (random ones: 7.6, the node mean).
//! It drifts when a large share of a small graph is sampled and a few hubs
//! hold most edges: their unconnected pairs are few, are used up, and further
//! draws of them are rejected, so the result leans to lower degrees. On 60
//! nodes with 3 hubs, sampling 400, the ends averaged 5.4 against 10.2.
//!
//! Every strategy shares the rules a negative must keep: the pair is not an
//! edge in **either** direction (link prediction here is undirected, as the
//! scores in [`crate::link_prediction`] are), it is not a node with itself,
//! and no pair is returned twice. Pairs are returned with the smaller node id
//! first.
//!
//! A strategy that cannot find `count` negatives returns the ones it found
//! rather than searching forever: a dense graph has few unconnected pairs, and
//! a graph may have no pairs 2 to `k` hops apart at all. Random sampling
//! switches to listing every unconnected pair when they are scarce, so it
//! returns all of them exactly; the other two give up after a bounded number
//! of draws per sample.

use std::collections::{HashSet, VecDeque};

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use crate::common::{GraphView, NodeId};
use crate::link_prediction::neighbour_sets;

/// How negatives are drawn; see the module comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegativeStrategy {
    Random,
    DegreeMatched,
    /// Pairs at undirected distance `2..=k`; `k` is at least 2.
    KHop(usize),
}

/// Draws allowed per wanted sample before a strategy returns what it has.
const DRAWS_PER_SAMPLE: usize = 100;
/// Draws allowed on top of those, so a small `count` still gets a fair try.
const BASE_DRAWS: usize = 1_000;
/// Random sampling lists every unconnected pair once they number fewer than
/// this many times `count`, since rejection would mostly hit edges by then.
const SCARCE_FACTOR: u128 = 4;

/// Up to `count` negatives from `view` by `strategy`, reproducible from `seed`.
pub fn negative_samples(
    view: &GraphView,
    strategy: NegativeStrategy,
    count: usize,
    seed: u64,
) -> Vec<(NodeId, NodeId)> {
    let n = view.node_count;
    if n < 2 || count == 0 {
        return Vec::new();
    }
    let neighbours = neighbour_sets(view);
    let mut rng = StdRng::seed_from_u64(seed);
    let pairs = match strategy {
        NegativeStrategy::Random => random_pairs(&neighbours, count, &mut rng),
        NegativeStrategy::DegreeMatched => degree_matched_pairs(&neighbours, count, &mut rng),
        NegativeStrategy::KHop(k) => khop_pairs(&neighbours, k.max(2), count, &mut rng),
    };
    pairs
        .into_iter()
        .map(|(a, b)| {
            let (x, y) = (view.index_to_node[a], view.index_to_node[b]);
            (x.min(y), x.max(y))
        })
        .collect()
}

/// A draw budget for `count` samples.
fn draw_budget(count: usize) -> usize {
    count
        .saturating_mul(DRAWS_PER_SAMPLE)
        .saturating_add(BASE_DRAWS)
}

/// Keeps `(a, b)` if it is a new negative, as an ordered pair of indices.
struct Collector {
    seen: HashSet<(usize, usize)>,
    pairs: Vec<(usize, usize)>,
}

impl Collector {
    fn new() -> Self {
        Self {
            seen: HashSet::new(),
            pairs: Vec::new(),
        }
    }

    fn offer(&mut self, neighbours: &[HashSet<usize>], a: usize, b: usize) {
        if a == b || neighbours[a].contains(&b) {
            return;
        }
        let key = (a.min(b), a.max(b));
        if self.seen.insert(key) {
            self.pairs.push(key);
        }
    }
}

fn random_pairs(
    neighbours: &[HashSet<usize>],
    count: usize,
    rng: &mut StdRng,
) -> Vec<(usize, usize)> {
    let n = neighbours.len();
    let all_pairs = n as u128 * (n as u128 - 1) / 2;
    let edges = neighbours.iter().map(|s| s.len() as u128).sum::<u128>() / 2;
    let available = all_pairs - edges.min(all_pairs);
    if available <= SCARCE_FACTOR * count as u128 {
        // Few enough to list: every unconnected pair, shuffled, the first `count`.
        let mut every: Vec<(usize, usize)> = (0..n)
            .flat_map(|a| ((a + 1)..n).map(move |b| (a, b)))
            .filter(|&(a, b)| !neighbours[a].contains(&b))
            .collect();
        every.shuffle(rng);
        every.truncate(count);
        return every;
    }
    let mut out = Collector::new();
    for _ in 0..draw_budget(count) {
        if out.pairs.len() == count {
            break;
        }
        out.offer(neighbours, rng.gen_range(0..n), rng.gen_range(0..n));
    }
    out.pairs
}

fn degree_matched_pairs(
    neighbours: &[HashSet<usize>],
    count: usize,
    rng: &mut StdRng,
) -> Vec<(usize, usize)> {
    // Every edge end once: a uniform draw from it is a node drawn in
    // proportion to its degree.
    let ends: Vec<usize> = neighbours
        .iter()
        .enumerate()
        .flat_map(|(i, s)| std::iter::repeat_n(i, s.len()))
        .collect();
    if ends.is_empty() {
        return Vec::new();
    }
    let mut out = Collector::new();
    for _ in 0..draw_budget(count) {
        if out.pairs.len() == count {
            break;
        }
        let a = ends[rng.gen_range(0..ends.len())];
        let b = ends[rng.gen_range(0..ends.len())];
        out.offer(neighbours, a, b);
    }
    out.pairs
}

fn khop_pairs(
    neighbours: &[HashSet<usize>],
    k: usize,
    count: usize,
    rng: &mut StdRng,
) -> Vec<(usize, usize)> {
    let starts: Vec<usize> = (0..neighbours.len())
        .filter(|&i| !neighbours[i].is_empty())
        .collect();
    if starts.is_empty() {
        return Vec::new();
    }
    let mut out = Collector::new();
    for _ in 0..draw_budget(count) {
        if out.pairs.len() == count {
            break;
        }
        let source = starts[rng.gen_range(0..starts.len())];
        let candidates = within_hops(neighbours, source, k);
        if let Some(&target) = candidates.choose(rng) {
            out.offer(neighbours, source, target);
        }
    }
    out.pairs
}

/// The nodes at undirected distance `2..=k` from `source`, in BFS order.
fn within_hops(neighbours: &[HashSet<usize>], source: usize, k: usize) -> Vec<usize> {
    let mut distance = vec![usize::MAX; neighbours.len()];
    distance[source] = 0;
    let mut queue = VecDeque::from([source]);
    let mut found = Vec::new();
    while let Some(node) = queue.pop_front() {
        let d = distance[node];
        if d == k {
            continue;
        }
        // Sorted, so the BFS order, and with it the draw, does not depend on
        // the order a `HashSet` happens to iterate in.
        let mut next: Vec<usize> = neighbours[node].iter().copied().collect();
        next.sort_unstable();
        for m in next {
            if distance[m] == usize::MAX {
                distance[m] = d + 1;
                if d + 1 >= 2 {
                    found.push(m);
                }
                queue.push_back(m);
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const SEED: u64 = 11;

    /// A graph from directed edges over nodes `0..n`, with node id `i * 10`
    /// so ids and indices differ.
    fn graph(n: usize, edges: &[(usize, usize)]) -> GraphView {
        let mut out = vec![Vec::new(); n];
        let mut inc = vec![Vec::new(); n];
        for &(a, b) in edges {
            out[a].push(b);
            inc[b].push(a);
        }
        let ids: Vec<NodeId> = (0..n as u64).map(|i| i * 10).collect();
        let map: HashMap<NodeId, usize> = ids.iter().enumerate().map(|(i, &x)| (x, i)).collect();
        GraphView::from_adjacency_list(n, ids, map, out, inc, None)
    }

    /// A seeded random graph with some hubs, so degrees vary.
    fn mixed_graph() -> GraphView {
        let mut rng = StdRng::seed_from_u64(3);
        let n = 60;
        let mut edges = Vec::new();
        for hub in 0..3 {
            for v in 3..n {
                if rng.gen_bool(0.5) {
                    edges.push((hub, v));
                }
            }
        }
        for _ in 0..40 {
            let (a, b) = (rng.gen_range(3..n), rng.gen_range(3..n));
            if a != b {
                edges.push((a, b));
            }
        }
        graph(n, &edges)
    }

    fn undirected_edges(view: &GraphView) -> HashSet<(NodeId, NodeId)> {
        (0..view.node_count)
            .flat_map(|a| view.successors(a).iter().map(move |&b| (a, b)))
            .map(|(a, b)| {
                let (x, y) = (view.index_to_node[a], view.index_to_node[b]);
                (x.min(y), x.max(y))
            })
            .collect()
    }

    fn all_strategies() -> [NegativeStrategy; 3] {
        [
            NegativeStrategy::Random,
            NegativeStrategy::DegreeMatched,
            NegativeStrategy::KHop(3),
        ]
    }

    #[test]
    fn no_negative_is_an_edge_a_self_pair_or_a_repeat() {
        let view = mixed_graph();
        let edges = undirected_edges(&view);
        for strategy in all_strategies() {
            let pairs = negative_samples(&view, strategy, 200, SEED);
            assert_eq!(pairs.len(), 200, "{strategy:?}");
            let unique: HashSet<_> = pairs.iter().collect();
            assert_eq!(unique.len(), pairs.len(), "{strategy:?}: a repeat");
            for &(a, b) in &pairs {
                assert!(
                    a < b,
                    "{strategy:?}: ({a}, {b}) is not smaller-first, or is a self pair"
                );
                assert!(
                    !edges.contains(&(a, b)),
                    "{strategy:?}: ({a}, {b}) is an edge"
                );
            }
        }
    }

    #[test]
    fn direction_does_not_make_an_edge_a_negative() {
        // 0 -> 1 only: the pair (0, 1) is connected whichever way it is read.
        let view = graph(3, &[(0, 1)]);
        let pairs = negative_samples(&view, NegativeStrategy::Random, 10, SEED);
        assert!(!pairs.contains(&(0, 10)), "{pairs:?}");
        let mut pairs = pairs;
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![(0, 20), (10, 20)],
            "the only two unconnected pairs"
        );
    }

    #[test]
    fn a_complete_or_tiny_graph_has_no_negatives() {
        let complete = graph(4, &[(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
        for strategy in all_strategies() {
            assert!(
                negative_samples(&complete, strategy, 5, SEED).is_empty(),
                "{strategy:?}"
            );
            assert!(
                negative_samples(&graph(1, &[]), strategy, 5, SEED).is_empty(),
                "{strategy:?}"
            );
            assert!(
                negative_samples(&mixed_graph(), strategy, 0, SEED).is_empty(),
                "{strategy:?}"
            );
        }
    }

    #[test]
    fn asking_for_more_random_negatives_than_exist_returns_every_one() {
        // A 5-node path has 10 pairs and 4 edges: 6 negatives exist.
        let view = graph(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
        let pairs = negative_samples(&view, NegativeStrategy::Random, 50, SEED);
        assert_eq!(pairs.len(), 6);
    }

    /// A larger graph where degree varies a lot but no handful of hubs holds
    /// the edges: 40 popular nodes with about 30 neighbours each, 360 with
    /// about 2. Sampling a small fraction of its unconnected pairs, the
    /// no-edge and no-repeat rules rarely bind, so the draw's degree
    /// weighting is what the result shows.
    fn heterogeneous_graph() -> GraphView {
        let mut rng = StdRng::seed_from_u64(5);
        let (n, popular) = (400, 40);
        let mut edges = Vec::new();
        for p in 0..popular {
            for _ in 0..30 {
                let v = rng.gen_range(0..n);
                if v != p {
                    edges.push((p, v));
                }
            }
        }
        for v in popular..n {
            let u = rng.gen_range(popular..n);
            if u != v {
                edges.push((v, u));
            }
        }
        graph(n, &edges)
    }

    #[test]
    fn degree_matched_ends_have_the_degree_profile_of_edge_ends() {
        let view = heterogeneous_graph();
        let degree = |id: NodeId| {
            let i = view.node_to_index[&id];
            neighbour_sets(&view)[i].len() as f64
        };
        let mean_end_degree = |pairs: &[(NodeId, NodeId)]| {
            pairs
                .iter()
                .map(|&(a, b)| degree(a) + degree(b))
                .sum::<f64>()
                / (2 * pairs.len()) as f64
        };
        // The mean degree of an edge end is sum(d^2) / sum(d).
        let sets = neighbour_sets(&view);
        let (sum_d, sum_d2) = sets
            .iter()
            .map(|s| s.len() as f64)
            .fold((0.0, 0.0), |(a, b), d| (a + d, b + d * d));
        let edge_end = sum_d2 / sum_d;
        let node_mean = sum_d / sets.len() as f64;
        let matched = mean_end_degree(&negative_samples(
            &view,
            NegativeStrategy::DegreeMatched,
            300,
            SEED,
        ));
        let random = mean_end_degree(&negative_samples(
            &view,
            NegativeStrategy::Random,
            300,
            SEED,
        ));
        assert!(
            (matched - edge_end).abs() < 0.15 * edge_end,
            "degree-matched mean end degree {matched}, edge ends {edge_end}"
        );
        assert!(
            (random - node_mean).abs() < 0.15 * node_mean,
            "random mean end degree {random}, nodes {node_mean}"
        );
        assert!(
            matched > 1.5 * random,
            "matched {matched} vs random {random}"
        );
    }

    #[test]
    fn khop_negatives_are_two_to_k_hops_apart() {
        let view = mixed_graph();
        let sets = neighbour_sets(&view);
        // The distance is computed here, not with `within_hops`, so a broken
        // `within_hops` cannot also break the check.
        let distance = |from: usize, to: usize| {
            let mut seen = vec![usize::MAX; sets.len()];
            seen[from] = 0;
            let mut queue = VecDeque::from([from]);
            while let Some(u) = queue.pop_front() {
                for &v in &sets[u] {
                    if seen[v] == usize::MAX {
                        seen[v] = seen[u] + 1;
                        queue.push_back(v);
                    }
                }
            }
            seen[to]
        };
        for k in [2, 3] {
            for (a, b) in negative_samples(&view, NegativeStrategy::KHop(k), 150, SEED) {
                let d = distance(view.node_to_index[&a], view.node_to_index[&b]);
                assert!((2..=k).contains(&d), "k={k}: ({a}, {b}) is {d} hops apart");
            }
        }
    }

    #[test]
    fn khop_on_a_path_finds_exactly_the_two_hop_pairs() {
        // 0 - 1 - 2 - 3: at distance 2 are (0, 2) and (1, 3) only.
        let view = graph(4, &[(0, 1), (1, 2), (2, 3)]);
        let mut pairs = negative_samples(&view, NegativeStrategy::KHop(2), 10, SEED);
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(0, 20), (10, 30)]);
    }

    #[test]
    fn a_k_below_two_is_raised_to_two() {
        let view = graph(4, &[(0, 1), (1, 2), (2, 3)]);
        let mut pairs = negative_samples(&view, NegativeStrategy::KHop(1), 10, SEED);
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(0, 20), (10, 30)], "k = 1 would mean edges");
    }

    #[test]
    fn the_same_seed_gives_the_same_samples_and_another_seed_does_not() {
        let view = mixed_graph();
        for strategy in all_strategies() {
            let a = negative_samples(&view, strategy, 50, SEED);
            assert_eq!(
                a,
                negative_samples(&view, strategy, 50, SEED),
                "{strategy:?}"
            );
            assert_ne!(
                a,
                negative_samples(&view, strategy, 50, SEED + 1),
                "{strategy:?}"
            );
        }
    }
}
