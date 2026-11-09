//! Neighbour sampling for GNN minibatches (ML-02, #1717).
//!
//! A graph neural network learns about a node from its neighbours, then from
//! theirs. On a real graph one node can have millions of neighbours, so a
//! trainer does not take them all: it *samples* a few at each hop, a number
//! called the hop's **fanout**. Fanouts `[10, 5]` mean: for each seed node,
//! up to 10 neighbours; for each of those, up to 5 of theirs.
//!
//! The rules every sample keeps, and which the tests check:
//!
//! * **Every sampled pair is a relationship** of the projected graph, in the
//!   requested [`Direction`].
//! * **No neighbour twice for one node.** A node's neighbours are drawn
//!   without replacement, so a node with fewer neighbours than the fanout
//!   gives all of them, once each. Parallel relationships between the same
//!   two nodes count as one neighbour; with weights, their weights add up.
//! * **No node expanded twice in a batch.** A neighbour already reached in
//!   this batch, at any hop or as a seed, is reported but not expanded again,
//!   as GraphSAGE-style samplers do. Each batch is sampled on its own, so a
//!   node can appear in several batches.
//! * **Seeded.** The same seed gives the same rows in the same order. The
//!   generator is written out here rather than taken from `rand`, so a version
//!   bump of a dependency cannot change a published training set.
//! * **Weighted** sampling draws a neighbour with probability proportional to
//!   its weight (Efraimidis-Spirakis without replacement). A weight of 0 is
//!   never drawn. A negative, infinite or NaN weight is refused: there is no
//!   probability it could mean.

use crate::common::{GraphView, NodeId};

/// Which relationships make a node's neighbours.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Along each relationship: `(node)-->(neighbour)`.
    Out,
    /// Against it: `(neighbour)-->(node)`. The GNN convention of sampling the
    /// nodes a message comes *from*.
    In,
    /// Either way: the relationship's direction is ignored.
    Both,
}

/// One hop's fanout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fanout {
    /// At most this many neighbours per node (at least 1).
    AtMost(usize),
    /// Every neighbour.
    All,
}

/// What to sample, besides the seed nodes.
#[derive(Clone, Debug)]
pub struct SampleSpec {
    /// One entry per hop, outward from the seeds.
    pub fanouts: Vec<Fanout>,
    pub direction: Direction,
    /// Draw in proportion to the view's `weights` (which must then be set).
    pub weighted: bool,
    /// Seeds per minibatch (at least 1).
    pub batch_size: usize,
    pub seed: u64,
}

/// One sampled relationship.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SampledEdge {
    /// Which minibatch, from 0: seeds `batch * batch_size ..` of the input.
    pub batch: usize,
    /// 1 for a seed's neighbours, 2 for theirs, and so on.
    pub hop: usize,
    /// The node whose neighbours were sampled.
    pub node: NodeId,
    /// The neighbour drawn for it.
    pub neighbour: NodeId,
}

/// Why a sample could not be drawn.
#[derive(Clone, Debug, PartialEq)]
pub enum SampleError {
    /// `weighted` was asked for and the view carries no weights.
    NoWeights,
    /// A relationship's weight is not a probability weight.
    BadWeight { from: NodeId, to: NodeId, weight: f64 },
    /// A seed index outside the view.
    SeedOutOfRange(usize),
}

/// SplitMix64: small, fast, and fixed here so its output never changes.
/// Shared with `subgraph_sampling`, so every sampler draws from one generator.
pub(crate) struct SplitMix64(pub(crate) u64);

impl SplitMix64 {
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`, for `n >= 1`, without modulo bias.
    pub(crate) fn below(&mut self, n: usize) -> usize {
        let n = n as u64;
        let zone = u64::MAX - u64::MAX % n;
        loop {
            let x = self.next_u64();
            if x < zone {
                return (x % n) as usize;
            }
        }
    }

    /// Uniform in `(0, 1]`: never 0, so its logarithm is finite.
    pub(crate) fn unit_open_below(&mut self) -> f64 {
        ((self.next_u64() >> 11) + 1) as f64 / (1u64 << 53) as f64
    }
}

/// Node `u`'s neighbours as `(index, weight)`, by index ascending, parallel
/// relationships merged. The weight is 1 per relationship when unweighted.
pub(crate) fn candidates(view: &GraphView, u: usize, direction: Direction) -> Vec<(usize, f64)> {
    let mut out: Vec<(usize, f64)> = Vec::new();
    let weight_at = |pos: usize| view.weights.as_ref().map_or(1.0, |w| w[pos]);
    if matches!(direction, Direction::Out | Direction::Both) {
        for pos in view.out_offsets[u]..view.out_offsets[u + 1] {
            out.push((view.out_targets[pos], weight_at(pos)));
        }
    }
    if matches!(direction, Direction::In | Direction::Both) {
        // The weights are kept with the outgoing lists, so an incoming
        // relationship's weight is found from its source's side.
        let mut sources: Vec<usize> = view.in_sources[view.in_offsets[u]..view.in_offsets[u + 1]].to_vec();
        sources.sort_unstable();
        sources.dedup();
        for s in sources {
            for pos in view.out_offsets[s]..view.out_offsets[s + 1] {
                if view.out_targets[pos] == u {
                    out.push((s, weight_at(pos)));
                }
            }
        }
    }
    out.sort_by_key(|&(v, _)| v);
    let mut merged: Vec<(usize, f64)> = Vec::with_capacity(out.len());
    for (v, w) in out {
        match merged.last_mut() {
            Some((last, total)) if *last == v => *total += w,
            _ => merged.push((v, w)),
        }
    }
    merged
}

/// Up to `fanout` of `cands`, without replacement, returned by index ascending.
fn draw(cands: &[(usize, f64)], fanout: Fanout, weighted: bool, rng: &mut SplitMix64) -> Vec<usize> {
    let pool: Vec<(usize, f64)> = if weighted {
        cands.iter().copied().filter(|&(_, w)| w > 0.0).collect()
    } else {
        cands.to_vec()
    };
    let k = match fanout {
        Fanout::All => pool.len(),
        Fanout::AtMost(k) => k.min(pool.len()),
    };
    let mut chosen: Vec<usize> = if k == pool.len() {
        pool.iter().map(|&(v, _)| v).collect()
    } else if weighted {
        // Efraimidis-Spirakis: key ln(u)/w, keep the k largest. Drawing k
        // without replacement this way is equivalent to drawing one at a
        // time in proportion to weight, removing each one drawn.
        let mut keyed: Vec<(f64, usize)> = pool
            .iter()
            .map(|&(v, w)| (rng.unit_open_below().ln() / w, v))
            .collect();
        // Largest key first; the index breaks an exact tie so the order is total.
        keyed.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        keyed.into_iter().take(k).map(|(_, v)| v).collect()
    } else {
        // A partial Fisher-Yates shuffle: the first k places are a uniform
        // k-subset.
        let mut idx: Vec<usize> = pool.iter().map(|&(v, _)| v).collect();
        for i in 0..k {
            let j = i + rng.below(idx.len() - i);
            idx.swap(i, j);
        }
        idx.truncate(k);
        idx
    };
    chosen.sort_unstable();
    chosen
}

/// Sample `spec.fanouts.len()` hops of neighbours around each seed, one
/// minibatch of `spec.batch_size` seeds at a time. `seeds` are view indices.
///
/// Rows come grouped by batch, then hop, then expanded node in the order it
/// was reached, then neighbour by node id.
pub fn sample_neighbours(
    view: &GraphView,
    seeds: &[usize],
    spec: &SampleSpec,
) -> Result<Vec<SampledEdge>, SampleError> {
    if let Some(&bad) = seeds.iter().find(|&&s| s >= view.node_count) {
        return Err(SampleError::SeedOutOfRange(bad));
    }
    if spec.weighted {
        let Some(weights) = view.weights.as_ref() else {
            return Err(SampleError::NoWeights);
        };
        // Checked up front, over the whole view, so whether a call is refused
        // does not depend on which neighbours its seed happened to draw.
        for u in 0..view.node_count {
            let span = view.out_offsets[u]..view.out_offsets[u + 1];
            for (&w, &to) in weights[span.clone()].iter().zip(&view.out_targets[span]) {
                if w.is_nan() || w.is_infinite() || w < 0.0 {
                    return Err(SampleError::BadWeight {
                        from: view.index_to_node[u],
                        to: view.index_to_node[to],
                        weight: w,
                    });
                }
            }
        }
    }

    let mut rng = SplitMix64(spec.seed);
    let mut rows = Vec::new();
    let batch_size = spec.batch_size.max(1);
    for (batch, chunk) in seeds.chunks(batch_size).enumerate() {
        let mut seen = std::collections::HashSet::new();
        let mut frontier: Vec<usize> = chunk.iter().copied().filter(|&s| seen.insert(s)).collect();
        for (h, &fanout) in spec.fanouts.iter().enumerate() {
            let mut next = Vec::new();
            for &u in &frontier {
                let cands = candidates(view, u, spec.direction);
                for v in draw(&cands, fanout, spec.weighted, &mut rng) {
                    rows.push(SampledEdge {
                        batch,
                        hop: h + 1,
                        node: view.index_to_node[u],
                        neighbour: view.index_to_node[v],
                    });
                    if seen.insert(v) {
                        next.push(v);
                    }
                }
            }
            frontier = next;
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// A view from `(from, to, weight)` triples over nodes `0..n`, ids = index.
    fn view(n: usize, edges: &[(usize, usize, f64)]) -> GraphView {
        let mut out: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
        let mut inc: Vec<Vec<usize>> = vec![Vec::new(); n];
        for &(a, b, w) in edges {
            out[a].push((b, w));
            inc[b].push(a);
        }
        let (mut out_offsets, mut out_targets, mut weights) = (vec![0], Vec::new(), Vec::new());
        for list in &out {
            for &(b, w) in list {
                out_targets.push(b);
                weights.push(w);
            }
            out_offsets.push(out_targets.len());
        }
        let (mut in_offsets, mut in_sources) = (vec![0], Vec::new());
        for list in &inc {
            in_sources.extend(list);
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
            weights: Some(weights),
        }
    }

    fn spec(fanouts: &[Fanout], direction: Direction, weighted: bool, seed: u64) -> SampleSpec {
        SampleSpec { fanouts: fanouts.to_vec(), direction, weighted, batch_size: usize::MAX, seed }
    }

    /// A star: 0 -> 1..=n, each leaf -> its own 3 grandchildren.
    fn tree(n: usize) -> GraphView {
        let mut edges = Vec::new();
        for leaf in 1..=n {
            edges.push((0, leaf, 1.0));
            for g in 0..3 {
                edges.push((leaf, n + 1 + (leaf - 1) * 3 + g, 1.0));
            }
        }
        view(1 + 4 * n, &edges)
    }

    #[test]
    fn fanout_caps_each_hop_and_every_row_is_an_edge() {
        let g = tree(20);
        let rows = sample_neighbours(&g, &[0], &spec(&[Fanout::AtMost(5), Fanout::AtMost(2)], Direction::Out, false, 1)).unwrap();
        let hop1: Vec<_> = rows.iter().filter(|r| r.hop == 1).collect();
        let hop2: Vec<_> = rows.iter().filter(|r| r.hop == 2).collect();
        assert_eq!(hop1.len(), 5);
        assert_eq!(hop2.len(), 5 * 2, "each of the 5 leaves gives 2 of its 3");
        let edges: HashSet<(u64, u64)> = (0..g.node_count)
            .flat_map(|u| g.successors(u).iter().map(move |&v| (u as u64, v as u64)))
            .collect();
        for r in &rows {
            assert!(edges.contains(&(r.node, r.neighbour)), "{r:?} is not an edge");
        }
        // Hop 2 expands exactly the nodes hop 1 drew.
        let drawn: HashSet<u64> = hop1.iter().map(|r| r.neighbour).collect();
        assert_eq!(hop2.iter().map(|r| r.node).collect::<HashSet<_>>(), drawn);
    }

    #[test]
    fn fewer_neighbours_than_the_fanout_gives_all_of_them_once() {
        let g = tree(3);
        let rows = sample_neighbours(&g, &[0], &spec(&[Fanout::AtMost(10)], Direction::Out, false, 9)).unwrap();
        let got: Vec<u64> = rows.iter().map(|r| r.neighbour).collect();
        assert_eq!(got, vec![1, 2, 3]);
        let all = sample_neighbours(&g, &[0], &spec(&[Fanout::All, Fanout::All], Direction::Out, false, 9)).unwrap();
        assert_eq!(all.len(), 3 + 9);
    }

    #[test]
    fn the_same_seed_gives_the_same_rows_and_another_seed_does_not() {
        let g = tree(30);
        let s = |seed| sample_neighbours(&g, &[0, 5, 7], &spec(&[Fanout::AtMost(4), Fanout::AtMost(2)], Direction::Out, false, seed)).unwrap();
        assert_eq!(s(42), s(42));
        assert_ne!(s(42), s(43));
    }

    #[test]
    fn no_node_is_expanded_twice_in_a_batch() {
        // A complete digraph on 6 nodes: every hop reaches nodes already seen.
        let mut edges = Vec::new();
        for a in 0..6 {
            for b in 0..6 {
                if a != b {
                    edges.push((a, b, 1.0));
                }
            }
        }
        let g = view(6, &edges);
        let rows = sample_neighbours(&g, &[0, 1], &spec(&[Fanout::All, Fanout::All, Fanout::All], Direction::Out, false, 3)).unwrap();
        // The hops at which each node had its neighbours sampled.
        let mut expanded_at: HashMap<u64, HashSet<usize>> = HashMap::new();
        for r in &rows {
            expanded_at.entry(r.node).or_default().insert(r.hop);
        }
        assert!(expanded_at.values().all(|hops| hops.len() == 1), "{expanded_at:?}");
        // Seeds 0 and 1 at hop 1; 2..=5 at hop 2; nobody left for hop 3.
        assert_eq!(expanded_at[&0], HashSet::from([1]));
        assert_eq!(expanded_at[&1], HashSet::from([1]));
        for n in 2..6 {
            assert_eq!(expanded_at[&n], HashSet::from([2]), "node {n}");
        }
        assert!(rows.iter().all(|r| r.hop < 3), "every node was already seen by hop 3");
        // Every relationship out of an expanded node is reported: 6 nodes x 5.
        assert_eq!(rows.len(), 30);
    }

    #[test]
    fn duplicate_seeds_in_a_batch_are_expanded_once() {
        let g = tree(4);
        let rows = sample_neighbours(&g, &[0, 0, 0], &spec(&[Fanout::All], Direction::Out, false, 1)).unwrap();
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn batches_are_sampled_independently() {
        let g = tree(4);
        let mut sp = spec(&[Fanout::All], Direction::Out, false, 1);
        sp.batch_size = 1;
        let rows = sample_neighbours(&g, &[0, 0], &sp).unwrap();
        assert_eq!(rows.iter().filter(|r| r.batch == 0).count(), 4);
        assert_eq!(rows.iter().filter(|r| r.batch == 1).count(), 4, "the second batch expands 0 again");
    }

    #[test]
    fn direction_picks_out_in_or_both() {
        // 1 -> 0 -> 2
        let g = view(3, &[(1, 0, 1.0), (0, 2, 1.0)]);
        let nbrs = |d| {
            sample_neighbours(&g, &[0], &spec(&[Fanout::All], d, false, 1))
                .unwrap()
                .into_iter()
                .map(|r| r.neighbour)
                .collect::<Vec<_>>()
        };
        assert_eq!(nbrs(Direction::Out), vec![2]);
        assert_eq!(nbrs(Direction::In), vec![1]);
        assert_eq!(nbrs(Direction::Both), vec![1, 2]);
    }

    #[test]
    fn parallel_relationships_are_one_neighbour_and_their_weights_add() {
        // Two relationships 0 -> 1 (weights 1 + 1) and one 0 -> 2 (weight 2):
        // equal odds once merged.
        let g = view(3, &[(0, 1, 1.0), (0, 1, 1.0), (0, 2, 2.0)]);
        assert_eq!(candidates(&g, 0, Direction::Out), vec![(1, 2.0), (2, 2.0)]);
        let mut ones = 0;
        for seed in 0..4000 {
            let rows = sample_neighbours(&g, &[0], &spec(&[Fanout::AtMost(1)], Direction::Out, true, seed)).unwrap();
            assert_eq!(rows.len(), 1);
            ones += usize::from(rows[0].neighbour == 1);
        }
        assert!((1800..=2200).contains(&ones), "{ones} of 4000");
    }

    #[test]
    fn uniform_sampling_gives_each_neighbour_its_share() {
        // 0 -> 1..=10, draw 3: each neighbour is in a sample with p = 0.3.
        let edges: Vec<_> = (1..=10).map(|v| (0, v, 1.0)).collect();
        let g = view(11, &edges);
        let trials = 5000;
        let mut counts = [0usize; 11];
        for seed in 0..trials {
            for r in sample_neighbours(&g, &[0], &spec(&[Fanout::AtMost(3)], Direction::Out, false, seed)).unwrap() {
                counts[r.neighbour as usize] += 1;
            }
        }
        // Mean 1500, sd ~32: 1350..1650 is over 4.5 sd either side.
        for (v, &c) in counts.iter().enumerate().skip(1) {
            assert!((1350..=1650).contains(&c), "neighbour {v} drawn {c} times of {trials}");
        }
    }

    #[test]
    fn weighted_sampling_follows_the_weights() {
        // 0 -> 1 (weight 8), 0 -> 2 (weight 1), 0 -> 3 (weight 1), 0 -> 4 (weight 0).
        let g = view(5, &[(0, 1, 8.0), (0, 2, 1.0), (0, 3, 1.0), (0, 4, 0.0)]);
        let trials = 5000;
        let mut counts = [0usize; 5];
        for seed in 0..trials {
            for r in sample_neighbours(&g, &[0], &spec(&[Fanout::AtMost(1)], Direction::Out, true, seed)).unwrap() {
                counts[r.neighbour as usize] += 1;
            }
        }
        // p = 0.8, 0.1, 0.1, 0: means 4000, 500, 500, 0.
        assert!((3850..=4150).contains(&counts[1]), "{counts:?}");
        assert!((420..=580).contains(&counts[2]), "{counts:?}");
        assert!((420..=580).contains(&counts[3]), "{counts:?}");
        assert_eq!(counts[4], 0, "a weight of 0 is never drawn");
        // Unweighted, the same graph treats all four alike.
        let mut flat = [0usize; 5];
        for seed in 0..trials {
            for r in sample_neighbours(&g, &[0], &spec(&[Fanout::AtMost(1)], Direction::Out, false, seed)).unwrap() {
                flat[r.neighbour as usize] += 1;
            }
        }
        assert!(flat[1..].iter().all(|&c| (1130..=1370).contains(&c)), "{flat:?}");
    }

    #[test]
    fn a_weight_that_is_not_a_probability_weight_is_refused() {
        for bad in [-1.0, f64::NAN, f64::INFINITY] {
            let g = view(3, &[(0, 1, 1.0), (1, 2, bad)]);
            let e = sample_neighbours(&g, &[0], &spec(&[Fanout::All], Direction::Out, true, 1)).unwrap_err();
            assert!(matches!(e, SampleError::BadWeight { from: 1, to: 2, .. }), "{e:?}");
            // Unweighted sampling does not read the weights.
            assert!(sample_neighbours(&g, &[0], &spec(&[Fanout::All], Direction::Out, false, 1)).is_ok());
        }
        let mut g = view(2, &[(0, 1, 1.0)]);
        g.weights = None;
        assert_eq!(
            sample_neighbours(&g, &[0], &spec(&[Fanout::All], Direction::Out, true, 1)),
            Err(SampleError::NoWeights)
        );
    }

    #[test]
    fn a_seed_outside_the_view_is_refused() {
        let g = tree(2);
        assert_eq!(
            sample_neighbours(&g, &[0, 99], &spec(&[Fanout::All], Direction::Out, false, 1)),
            Err(SampleError::SeedOutOfRange(99))
        );
    }

    #[test]
    fn a_node_with_no_neighbours_gives_no_rows_and_no_error() {
        let g = tree(2);
        let leaf_grandchild = g.node_count - 1;
        assert!(sample_neighbours(&g, &[leaf_grandchild], &spec(&[Fanout::All], Direction::Out, false, 1)).unwrap().is_empty());
    }

    #[test]
    fn the_generator_is_fixed() {
        // If this changes, every seeded sample anyone published changes too.
        let mut r = SplitMix64(42);
        assert_eq!([r.next_u64(), r.next_u64()], [0xBDD7_3226_2FEB_6E95, 0x28EF_E333_B266_F103]);
    }
}
