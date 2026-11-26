//! Subgraph sampling: a smaller piece of a graph to train or test on (ML-03, #1720).
//!
//! Five methods, each answering "which nodes are in the sample". The
//! relationships of the sample are the ones between those nodes, which a
//! caller reads with an ordinary `MATCH`.
//!
//! | method | how it picks | seeded |
//! |---|---|---|
//! | [`forest_fire`] | burns a geometric number of a node's neighbours, then theirs (Leskovec & Faloutsos, KDD 2006) | yes |
//! | [`snowball`] | wave by wave from the seeds, up to `per_node` neighbours of each node | yes, when `per_node` caps |
//! | [`ego_network`] | the seeds and every node within `hops` | nothing random |
//! | [`random_walk_sample`] | the nodes a walk with restarts visits | yes |
//! | [`stratified`] | the same share of each label group | yes |
//!
//! The rules every method keeps, and which the tests check:
//!
//! * **No node twice.** A sample is a set; each node is reported once, in the
//!   order it joined the sample.
//! * **Only along relationships**, for the methods that walk: every node
//!   forest fire, snowball, ego network or a random walk adds after its
//!   starting points is a neighbour, in the requested [`Direction`], of a
//!   node already in the sample. Forest fire's restarts (when a fire dies
//!   out) and a walk's jumps back to a seed are the only other way in.
//! * **Seeded** with `neighbour_sampling`'s SplitMix64, written out in this
//!   crate, so a dependency bump cannot change a published sample.
//! * **Neighbours in id order** before any random choice, so the answer does
//!   not depend on the order relationships were stored in.

use std::collections::{HashSet, VecDeque};

use crate::common::{GraphView, NodeId};
use crate::neighbour_sampling::{candidates, Direction, Fanout, SplitMix64};

/// Up to `k` of `pool`, uniformly without replacement, in index order.
fn choose(pool: Vec<usize>, k: usize, rng: &mut SplitMix64) -> Vec<usize> {
    let mut pool = pool;
    if k < pool.len() {
        for i in 0..k {
            let j = i + rng.below(pool.len() - i);
            pool.swap(i, j);
        }
        pool.truncate(k);
    }
    pool.sort_unstable();
    pool
}

/// `u`'s neighbours that are not yet in the sample, by index.
fn fresh_neighbours(view: &GraphView, u: usize, direction: Direction, seen: &HashSet<usize>) -> Vec<usize> {
    candidates(view, u, direction)
        .into_iter()
        .map(|(v, _)| v)
        .filter(|v| !seen.contains(v))
        .collect()
}

/// Forest fire: `size` nodes, in the order they caught fire.
///
/// From each burning node, a geometric number of its not-yet-burnt
/// neighbours catch fire, with mean `p / (1 - p)` for forward probability
/// `p` (Leskovec and Faloutsos use 0.7), chosen uniformly. When the fire
/// dies out before `size` nodes have burnt, it restarts at the next unburnt
/// seed, or, with none left, at a node chosen uniformly from the unburnt
/// ones. So a fire always reaches `size`, which must not exceed the graph.
pub fn forest_fire(
    view: &GraphView,
    seeds: &[usize],
    size: usize,
    forward_probability: f64,
    direction: Direction,
    seed: u64,
) -> Vec<NodeId> {
    let size = size.min(view.node_count);
    let mut rng = SplitMix64(seed);
    let mut seen = HashSet::new();
    let mut order: Vec<usize> = Vec::with_capacity(size);
    let mut queue = VecDeque::new();
    let mut next_seed = seeds.iter();
    while order.len() < size {
        let Some(u) = queue.pop_front() else {
            // Ignite: the next unburnt seed, else a uniformly chosen unburnt
            // node. Rejection is cheap until the sample nears the whole
            // graph, and stays correct then.
            let start = next_seed.by_ref().copied().find(|s| !seen.contains(s)).unwrap_or_else(|| loop {
                let v = rng.below(view.node_count);
                if !seen.contains(&v) {
                    break v;
                }
            });
            seen.insert(start);
            order.push(start);
            queue.push_back(start);
            continue;
        };
        // Geometric: successes before the first failure, with
        // P(success) = forward_probability. Never more than there are
        // neighbours to burn, which also ends the loop at probability 1.
        let pool = fresh_neighbours(view, u, direction, &seen);
        let mut burn = 0usize;
        while burn < pool.len() && rng.unit_open_below() <= forward_probability {
            burn += 1;
        }
        for v in choose(pool, burn, &mut rng) {
            if order.len() == size {
                break;
            }
            seen.insert(v);
            order.push(v);
            queue.push_back(v);
        }
    }
    order.into_iter().map(|i| view.index_to_node[i]).collect()
}

/// Snowball: `(node, wave)`, the seeds at wave 0, then up to `per_node`
/// not-yet-sampled neighbours of each node of a wave, for `hops` waves, and
/// at most `max_nodes` nodes in all when given.
pub fn snowball(
    view: &GraphView,
    seeds: &[usize],
    hops: usize,
    per_node: Fanout,
    max_nodes: Option<usize>,
    direction: Direction,
    seed: u64,
) -> Vec<(NodeId, usize)> {
    let cap = max_nodes.unwrap_or(usize::MAX);
    let mut rng = SplitMix64(seed);
    let mut seen = HashSet::new();
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut wave: Vec<usize> = Vec::new();
    for &s in seeds {
        if out.len() < cap && seen.insert(s) {
            out.push((s, 0));
            wave.push(s);
        }
    }
    for h in 1..=hops {
        let mut next = Vec::new();
        for &u in &wave {
            let pool = fresh_neighbours(view, u, direction, &seen);
            let k = match per_node {
                Fanout::All => pool.len(),
                Fanout::AtMost(k) => k,
            };
            for v in choose(pool, k, &mut rng) {
                if out.len() == cap {
                    break;
                }
                seen.insert(v);
                out.push((v, h));
                next.push(v);
            }
        }
        wave = next;
    }
    out.into_iter().map(|(i, h)| (view.index_to_node[i], h)).collect()
}

/// Ego network: `(node, distance)` for the seeds (distance 0) and every node
/// within `hops` of one, at its smallest distance, ordered by distance then
/// node id. Nothing is random: this is the exact neighbourhood.
pub fn ego_network(view: &GraphView, seeds: &[usize], hops: usize, direction: Direction) -> Vec<(NodeId, usize)> {
    let mut dist = vec![usize::MAX; view.node_count];
    let mut frontier = Vec::new();
    for &s in seeds {
        if dist[s] == usize::MAX {
            dist[s] = 0;
            frontier.push(s);
        }
    }
    for h in 1..=hops {
        let mut next = Vec::new();
        for &u in &frontier {
            for (v, _) in candidates(view, u, direction) {
                if dist[v] == usize::MAX {
                    dist[v] = h;
                    next.push(v);
                }
            }
        }
        frontier = next;
    }
    let mut out: Vec<(usize, usize)> =
        dist.iter().enumerate().filter(|(_, &d)| d != usize::MAX).map(|(i, &d)| (i, d)).collect();
    out.sort_by_key(|&(i, d)| (d, view.index_to_node[i]));
    out.into_iter().map(|(i, d)| (view.index_to_node[i], d)).collect()
}

/// Random walk with restart: the first `size` distinct nodes a walk visits,
/// in the order it first reached them.
///
/// The walk starts at the first seed. At each step it jumps back to a seed
/// chosen uniformly with probability `restart_probability`, or when the
/// current node has no neighbours; otherwise it moves to a uniformly chosen
/// neighbour. It stops after `max_steps` steps: a walk whose seeds reach
/// fewer than `size` nodes returns the ones it reached.
pub fn random_walk_sample(
    view: &GraphView,
    seeds: &[usize],
    size: usize,
    restart_probability: f64,
    max_steps: usize,
    direction: Direction,
    seed: u64,
) -> Vec<NodeId> {
    let mut rng = SplitMix64(seed);
    let mut seen = HashSet::new();
    let mut order = Vec::new();
    let Some(&first) = seeds.first() else { return Vec::new() };
    let mut cur = first;
    seen.insert(cur);
    order.push(cur);
    let mut steps = 0;
    while order.len() < size && steps < max_steps {
        steps += 1;
        let nbrs = candidates(view, cur, direction);
        cur = if nbrs.is_empty() || rng.unit_open_below() <= restart_probability {
            seeds[rng.below(seeds.len())]
        } else {
            nbrs[rng.below(nbrs.len())].0
        };
        if seen.insert(cur) {
            order.push(cur);
        }
    }
    order.into_iter().map(|i| view.index_to_node[i]).collect()
}

/// How many of each group a stratified sample takes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Allocation {
    /// The same share of every group, rounded to the nearest whole node.
    Fraction(f64),
    /// This many of every group, or the whole group when it is smaller.
    PerStratum(usize),
}

/// Stratified sample: `(node, stratum)`, by stratum name, then node id.
///
/// `nodes` gives each node's stratum, for example its labels joined in
/// sorted order. Each stratum is sampled on its own, uniformly without
/// replacement, so the sample keeps the population's mix of strata up to
/// rounding. A stratum whose rounded share is 0 is left out, which only
/// happens to a group smaller than `1 / (2 * fraction)` nodes.
pub fn stratified(nodes: &[(NodeId, String)], allocation: Allocation, seed: u64) -> Vec<(NodeId, String)> {
    let mut groups: std::collections::BTreeMap<&str, Vec<NodeId>> = std::collections::BTreeMap::new();
    for (id, stratum) in nodes {
        groups.entry(stratum.as_str()).or_default().push(*id);
    }
    let mut rng = SplitMix64(seed);
    let mut out = Vec::new();
    for (stratum, mut members) in groups {
        members.sort_unstable();
        members.dedup();
        let k = match allocation {
            Allocation::Fraction(f) => (f * members.len() as f64).round() as usize,
            Allocation::PerStratum(k) => k,
        }
        .min(members.len());
        let picked = choose((0..members.len()).collect(), k, &mut rng);
        out.extend(picked.into_iter().map(|i| (members[i], stratum.to_string())));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A view from `(from, to)` pairs over nodes `0..n`; ids = index.
    fn view(n: usize, edges: &[(usize, usize)]) -> GraphView {
        let mut out: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut inc: Vec<Vec<usize>> = vec![Vec::new(); n];
        for &(a, b) in edges {
            out[a].push(b);
            inc[b].push(a);
        }
        let (mut out_offsets, mut out_targets) = (vec![0], Vec::new());
        for l in &out {
            out_targets.extend(l);
            out_offsets.push(out_targets.len());
        }
        let (mut in_offsets, mut in_sources) = (vec![0], Vec::new());
        for l in &inc {
            in_sources.extend(l);
            in_offsets.push(in_sources.len());
        }
        GraphView {
            node_count: n,
            index_to_node: (0..n as u64).collect(),
            node_to_index: (0..n).map(|i| (i as u64, i)).collect(),
            out_offsets,
            out_targets,
            in_offsets,
            in_sources,
            weights: None,
        }
    }

    /// A 10 x 10 grid, both directions: every node reachable from every other.
    fn grid() -> GraphView {
        let mut e = Vec::new();
        for r in 0..10 {
            for c in 0..10 {
                let i = r * 10 + c;
                if c < 9 {
                    e.push((i, i + 1));
                    e.push((i + 1, i));
                }
                if r < 9 {
                    e.push((i, i + 10));
                    e.push((i + 10, i));
                }
            }
        }
        view(100, &e)
    }

    fn adjacent(g: &GraphView, a: u64, b: u64) -> bool {
        g.successors(a as usize).contains(&(b as usize)) || g.successors(b as usize).contains(&(a as usize))
    }

    fn distinct(ids: &[u64]) -> bool {
        ids.iter().collect::<HashSet<_>>().len() == ids.len()
    }

    #[test]
    fn forest_fire_reaches_the_size_burns_along_edges_and_is_seeded() {
        let g = grid();
        let s = forest_fire(&g, &[0], 30, 0.7, Direction::Out, 7);
        assert_eq!(s.len(), 30);
        assert!(distinct(&s));
        assert_eq!(s[0], 0, "the fire starts at the seed");
        // Every node after the first touches one burnt before it, unless the
        // fire restarted; on a connected grid with p = 0.7 it restarts
        // rarely, so most must.
        let along = (1..s.len()).filter(|&i| s[..i].iter().any(|&b| adjacent(&g, b, s[i]))).count();
        assert!(along >= 25, "{along} of 29 caught fire from a neighbour");
        assert_eq!(s, forest_fire(&g, &[0], 30, 0.7, Direction::Out, 7));
        assert_ne!(s, forest_fire(&g, &[0], 30, 0.7, Direction::Out, 8));
    }

    #[test]
    fn a_fire_that_dies_restarts_until_it_reaches_the_size() {
        // No relationships at all: every node is its own restart.
        let g = view(20, &[]);
        let s = forest_fire(&g, &[3], 12, 0.7, Direction::Out, 1);
        assert_eq!(s.len(), 12);
        assert!(distinct(&s));
        assert_eq!(s[0], 3);
        // Probability 0 burns nothing, so it is uniform node sampling.
        assert_eq!(forest_fire(&grid(), &[], 10, 0.0, Direction::Out, 2).len(), 10);
    }

    #[test]
    fn a_higher_forward_probability_means_fewer_restarts() {
        // A chain 0 -> 1 -> ... -> 99: each burning node can only pass the
        // fire to the next, which it does with probability p. A position
        // whose node does not follow the one before it is a restart.
        let edges: Vec<_> = (0..99).map(|v| (v, v + 1)).collect();
        let g = view(100, &edges);
        let restarts = |p: f64, seed: u64| {
            let s = forest_fire(&g, &[0], 100, p, Direction::Out, seed);
            assert_eq!(s.len(), 100);
            assert!(distinct(&s));
            s.windows(2).filter(|w| w[1] != w[0] + 1).count()
        };
        // p = 0 never spreads: every node after the first is a restart. The
        // count can read a little under 99, because a restart that happens
        // to land on the next node of the chain looks like a burn.
        for seed in 0..20 {
            assert!(restarts(0.0, seed) >= 90, "seed {seed}: {}", restarts(0.0, seed));
        }
        // p = 1 always spreads, so one fire burns the whole chain from 0.
        assert_eq!(restarts(1.0, 1), 0);
        let mean = |p: f64| (0..200).map(|seed| restarts(p, seed)).sum::<usize>() as f64 / 200.0;
        let (low, high) = (mean(0.3), mean(0.9));
        assert!(low > 2.0 * high, "p 0.3: {low} restarts, p 0.9: {high}");
    }

    #[test]
    fn snowball_takes_waves_along_edges_and_caps_per_node() {
        // A binary tree: 0 -> 1, 2; 1 -> 3, 4; 2 -> 5, 6; ...
        let edges: Vec<_> = (1..31).map(|v| ((v - 1) / 2, v)).collect();
        let g = view(31, &edges);
        let all = snowball(&g, &[0], 2, Fanout::All, None, Direction::Out, 1);
        assert_eq!(all, vec![(0, 0), (1, 1), (2, 1), (3, 2), (4, 2), (5, 2), (6, 2)]);
        let one = snowball(&g, &[0], 3, Fanout::AtMost(1), None, Direction::Out, 5);
        assert_eq!(one.len(), 4, "one child per node, three waves: {one:?}");
        for w in one.windows(2) {
            assert!(adjacent(&g, w[0].0, w[1].0), "{one:?} is a chain");
            assert_eq!(w[1].1, w[0].1 + 1);
        }
        assert_eq!(snowball(&g, &[0], 3, Fanout::All, Some(5), Direction::Out, 1).len(), 5, "max_nodes caps it");
        assert_ne!(
            snowball(&g, &[0], 4, Fanout::AtMost(1), None, Direction::Out, 1),
            snowball(&g, &[0], 4, Fanout::AtMost(1), None, Direction::Out, 2)
        );
    }

    #[test]
    fn snowball_never_adds_a_node_twice() {
        let g = grid();
        let s = snowball(&g, &[0, 55], 4, Fanout::AtMost(3), None, Direction::Both, 3);
        let ids: Vec<u64> = s.iter().map(|x| x.0).collect();
        assert!(distinct(&ids));
    }

    #[test]
    fn ego_network_is_exactly_the_nodes_within_k_hops() {
        let g = grid();
        // From the corner of a grid, Manhattan distance: k+1 nodes at distance k.
        let e = ego_network(&g, &[0], 3, Direction::Out);
        assert_eq!(e.len(), 1 + 2 + 3 + 4);
        for &(id, d) in &e {
            let (r, c) = (id / 10, id % 10);
            assert_eq!((r + c) as usize, d, "node {id}");
        }
        assert!(e.windows(2).all(|w| (w[0].1, w[0].0) < (w[1].1, w[1].0)), "ordered by distance, then id");
        assert_eq!(ego_network(&g, &[0], 0, Direction::Out), vec![(0, 0)]);
        // Direction: on 1 -> 0 -> 2, out reaches 2, in reaches 1.
        let p = view(3, &[(1, 0), (0, 2)]);
        assert_eq!(ego_network(&p, &[0], 1, Direction::Out), vec![(0, 0), (2, 1)]);
        assert_eq!(ego_network(&p, &[0], 1, Direction::In), vec![(0, 0), (1, 1)]);
        assert_eq!(ego_network(&p, &[0], 1, Direction::Both), vec![(0, 0), (1, 1), (2, 1)]);
    }

    #[test]
    fn a_random_walk_sample_follows_edges_and_is_seeded() {
        let g = grid();
        let s = random_walk_sample(&g, &[44], 25, 0.0, 100_000, Direction::Out, 9);
        assert_eq!(s.len(), 25);
        assert!(distinct(&s));
        assert_eq!(s[0], 44);
        // With no restarts each new node is next to one visited before it.
        for i in 1..s.len() {
            assert!(s[..i].iter().any(|&b| adjacent(&g, b, s[i])), "{} at {i}", s[i]);
        }
        assert_eq!(s, random_walk_sample(&g, &[44], 25, 0.0, 100_000, Direction::Out, 9));
        assert_ne!(s, random_walk_sample(&g, &[44], 25, 0.0, 100_000, Direction::Out, 10));
    }

    #[test]
    fn a_walk_returns_what_it_can_reach_within_its_steps() {
        // Two components: the walk from 0 can only reach 0..=2.
        let g = view(6, &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5)]);
        let s = random_walk_sample(&g, &[0], 5, 0.15, 10_000, Direction::Out, 1);
        assert_eq!(s.iter().copied().collect::<HashSet<_>>(), HashSet::from([0, 1, 2]));
        // A dead end restarts rather than stopping.
        let line = view(3, &[(0, 1), (1, 2)]);
        assert_eq!(random_walk_sample(&line, &[0], 3, 0.0, 100, Direction::Out, 1).len(), 3);
        assert!(random_walk_sample(&line, &[], 3, 0.0, 100, Direction::Out, 1).is_empty());
    }

    #[test]
    fn restarts_keep_a_walk_near_its_seed() {
        let g = grid();
        let far = |p: f64| {
            let mut total = 0u64;
            for seed in 0..200 {
                let s = random_walk_sample(&g, &[0], 15, p, 100_000, Direction::Out, seed);
                total += s.iter().map(|id| id / 10 + id % 10).sum::<u64>();
            }
            total
        };
        assert!(far(0.6) < far(0.0), "restarting often keeps the sample closer to node 0");
    }

    fn population() -> Vec<(NodeId, String)> {
        let mut v = Vec::new();
        for i in 0..60 {
            v.push((i, "Paper".to_string()));
        }
        for i in 60..90 {
            v.push((i, "Author".to_string()));
        }
        for i in 90..100 {
            v.push((i, "Venue".to_string()));
        }
        v
    }

    #[test]
    fn a_stratified_sample_keeps_each_groups_share() {
        let s = stratified(&population(), Allocation::Fraction(0.1), 4);
        let count = |g: &str| s.iter().filter(|(_, st)| st == g).count();
        assert_eq!((count("Paper"), count("Author"), count("Venue")), (6, 3, 1));
        for (id, st) in &s {
            let want = if *id < 60 { "Paper" } else if *id < 90 { "Author" } else { "Venue" };
            assert_eq!(st, want, "node {id} is reported in its own group");
        }
        assert!(s.windows(2).all(|w| (&w[0].1, w[0].0) < (&w[1].1, w[1].0)), "by stratum, then id");
        assert_eq!(s, stratified(&population(), Allocation::Fraction(0.1), 4));
        assert_ne!(s, stratified(&population(), Allocation::Fraction(0.1), 5));
    }

    #[test]
    fn per_stratum_takes_the_same_number_from_each_or_all_of_a_small_one() {
        let s = stratified(&population(), Allocation::PerStratum(20), 1);
        let count = |g: &str| s.iter().filter(|(_, st)| st == g).count();
        assert_eq!((count("Paper"), count("Author"), count("Venue")), (20, 20, 10));
    }

    #[test]
    fn a_stratified_draw_is_uniform_within_its_group() {
        // Take 1 of the 10 Venues, 3000 times: each about 300.
        let mut hits = [0usize; 10];
        for seed in 0..3000 {
            for (id, st) in stratified(&population(), Allocation::Fraction(0.1), seed) {
                if st == "Venue" {
                    hits[(id - 90) as usize] += 1;
                }
            }
        }
        // sd ~16.4: 230..370 is over 4 sd either side.
        assert!(hits.iter().all(|&h| (230..=370).contains(&h)), "{hits:?}");
    }
}
