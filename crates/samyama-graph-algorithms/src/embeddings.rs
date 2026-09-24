//! Graph embeddings: FastRP and node2vec (ML-06).
//!
//! | | |
//! |---|---|
//! | [`fastrp`] | Fast Random Projection: sparse random init, propagated over the graph |
//! | [`node2vec`] | biased 2nd-order random walks, projected -- see the caveat below |
//!
//! Both are **seeded and deterministic**: the same seed reproduces the same
//! embedding bit-for-bit, and a different seed gives a different one. Neither
//! algorithm calls a clock or a process-global RNG -- every random draw is a
//! pure function of `(seed, node index, dimension index)` or `(seed, walk
//! number, step)`, computed with SplitMix64 rather than threaded RNG state,
//! so the result does not depend on iteration order.
//!
//! **What `node2vec` here is not.** The real node2vec (Grover & Leskovec,
//! 2016) has two parts: biased 2nd-order random walks controlled by `p`
//! (return) and `q` (in-out), and a skip-gram-with-negative-sampling (SGNS)
//! embedding trained on those walks the way word2vec trains on sentences.
//! This implements the first part faithfully -- [`biased_walk`] is the real
//! biased sampler, not a plain random walk -- and replaces SGNS with a
//! cheaper, deterministic stand-in: a sparse random projection of the walks'
//! co-occurrence counts (the same projection [`fastrp`] uses). That captures
//! the same *signal* SGNS would train on -- which nodes co-occur inside a
//! walk window -- without an iterative gradient fit. It is not skip-gram
//! training, so a loading that needs SGNS's actual embedding space (rather
//! than a graph embedding with the node2vec walk's proximity structure)
//! should not treat this as a drop-in replacement.

use crate::common::GraphView;

/// SplitMix64, used as a pure hash rather than a stream: every call is
/// independent, so two draws from different `(a, b)` never depend on call
/// order the way an advancing RNG state would.
fn hash01(seed: u64, a: u64, b: u64) -> f64 {
    let mut x = seed
        .wrapping_add(a.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(b.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    // Top 53 bits -> [0, 1), the usual float-from-u64 recipe.
    (x >> 11) as f64 / (1u64 << 53) as f64
}

/// One entry of a very-sparse random projection matrix (Achlioptas /
/// Li-Hastie-Church): `+-sqrt(3)` each with probability 1/6, `0` the rest of
/// the time. Deterministic in `(seed, row, col)`, so row `r` never needs to
/// be generated, stored, or replayed in order -- any entry can be asked for
/// on its own and always answers the same value for the same seed.
fn sparse_projection_entry(seed: u64, row: u64, col: u64) -> f64 {
    let u = hash01(seed, row, col);
    const ROOT3: f64 = 1.732_050_807_568_877_2;
    if u < 1.0 / 6.0 {
        ROOT3
    } else if u < 2.0 / 6.0 {
        -ROOT3
    } else {
        0.0
    }
}

/// The graph, stripped of direction, as an adjacency list per node index.
/// Both algorithms below treat community structure as undirected: whether an
/// edge is A->B or B->A says nothing about which side of a community split
/// A and B sit on.
fn undirected_adjacency(view: &GraphView) -> Vec<Vec<usize>> {
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); view.node_count];
    for u in 0..view.node_count {
        adj[u].extend_from_slice(view.successors(u));
        adj[u].extend_from_slice(view.predecessors(u));
    }
    adj
}

fn l2_normalize_rows(rows: &mut [Vec<f64>]) {
    for row in rows {
        let norm = row.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 0.0 {
            for x in row.iter_mut() {
                *x /= norm;
            }
        }
    }
}

/// FastRP configuration.
#[derive(Debug, Clone)]
pub struct FastRpConfig {
    /// Output vector length.
    pub dimension: usize,
    /// One weight per propagation depth; `iteration_weights.len()` is the
    /// number of hops mixed into the final embedding. `[1.0, 1.0, 1.0]` mixes
    /// depths 1-3 equally, which is FastRP's usual default shape.
    pub iteration_weights: Vec<f64>,
    /// Seed for both the initial random projection and (nothing else --
    /// propagation itself has no randomness).
    pub seed: u64,
    /// L2-normalize each row after mixing (default true). Off is mostly for
    /// inspecting raw magnitudes; comparisons should use the normalized form
    /// so that a high-degree node's larger raw magnitude does not read as
    /// "more similar to everything".
    pub normalize: bool,
}

impl Default for FastRpConfig {
    fn default() -> Self {
        Self {
            dimension: 128,
            iteration_weights: vec![1.0, 1.0, 1.0],
            seed: 0x5A_4D_59_4D_41, // "SAMYA", same constant PCA defaults to
            normalize: true,
        }
    }
}

/// FastRP (Fast Random Projection; Chen et al. 2019): assign every node a
/// sparse random vector, then repeatedly replace each node's vector with the
/// mean of its neighbours' vectors. After `k` rounds a node's vector is a
/// mix of everything within `k` hops, weighted toward what is structurally
/// close to it -- two nodes in the same dense cluster pull each other's
/// vectors toward a shared average, and nodes with no path between them
/// share no propagation term at all.
///
/// Returns one row per node, aligned with `view.index_to_node` (row `i` is
/// the embedding of `view.index_to_node[i]`).
pub fn fastrp(view: &GraphView, config: &FastRpConfig) -> Vec<Vec<f64>> {
    let n = view.node_count;
    let d = config.dimension.max(1);
    if n == 0 {
        return Vec::new();
    }

    let adjacency = undirected_adjacency(view);

    // N^0: row i is node i's own sparse random vector -- not yet touched by
    // any propagation.
    let mut current: Vec<Vec<f64>> = (0..n)
        .map(|i| {
            (0..d)
                .map(|j| sparse_projection_entry(config.seed, i as u64, j as u64))
                .collect()
        })
        .collect();

    let mut accum = vec![vec![0.0f64; d]; n];
    for &weight in &config.iteration_weights {
        let mut next = vec![vec![0.0f64; d]; n];
        for u in 0..n {
            let neigh = &adjacency[u];
            if neigh.is_empty() {
                // No edges means no information has reached this node at
                // this depth -- leaving it at the previous row would let an
                // isolated node quietly "propagate" by standing still, which
                // is not what happened.
                continue;
            }
            let inv = 1.0 / neigh.len() as f64;
            for &v in neigh {
                for j in 0..d {
                    next[u][j] += current[v][j] * inv;
                }
            }
        }
        for u in 0..n {
            for j in 0..d {
                accum[u][j] += weight * next[u][j];
            }
        }
        current = next;
    }

    if config.normalize {
        l2_normalize_rows(&mut accum);
    }
    accum
}

/// node2vec configuration. See the module doc for what the embedding step
/// is and is not.
#[derive(Debug, Clone)]
pub struct Node2VecConfig {
    /// Output vector length.
    pub dimension: usize,
    /// Steps per walk.
    pub walk_length: usize,
    /// Walks started from every node.
    pub walks_per_node: usize,
    /// `p`: the cost of immediately returning to the previous node. Low `p`
    /// biases the walk to backtrack; high `p` discourages it.
    pub return_factor: f64,
    /// `q`: the cost of moving away from the previous node's neighbourhood.
    /// Low `q` biases the walk outward (DFS-like); high `q` keeps it local
    /// (BFS-like).
    pub in_out_factor: f64,
    /// Half-width of the co-occurrence window around each walk position.
    pub window_size: usize,
    /// Seed for walk sampling and the projection.
    pub seed: u64,
}

impl Default for Node2VecConfig {
    fn default() -> Self {
        Self {
            dimension: 128,
            walk_length: 40,
            walks_per_node: 10,
            return_factor: 1.0,
            in_out_factor: 1.0,
            window_size: 5,
            // "NODE2", ungrouped: a trailing `_32` group reads to clippy as
            // a mistyped `i32`/`u32` suffix (`mistyped_literal_suffixes`,
            // deny-level in this repo), whatever digits come before it.
            seed: 0x4E4F444532,
        }
    }
}

/// One biased 2nd-order walk from `source`, as dense node indices.
///
/// This is node2vec's actual transition rule: from `(prev, cur)`, a
/// candidate neighbour `x` of `cur` is weighted `1/p` if `x == prev` (go
/// back), `1` if `x` is also a neighbour of `prev` (stay local), else `1/q`
/// (move outward). The first step has no `prev`, so it is uniform.
///
/// `walk_no` salts the seed so that the `walks_per_node` walks from the same
/// source do not all sample identically.
fn biased_walk(
    adjacency: &[Vec<usize>],
    source: usize,
    walk_length: usize,
    p: f64,
    q: f64,
    seed: u64,
    walk_no: u64,
) -> Vec<usize> {
    let salted = seed ^ walk_no.wrapping_mul(0x2545_F491_4F6C_DD1D);
    let mut walk = vec![source];
    if adjacency[source].is_empty() || walk_length < 2 {
        return walk;
    }

    let pick_uniform = |cur: usize, step: u64| -> usize {
        let cands = &adjacency[cur];
        let u = hash01(salted, cur as u64, step);
        let idx = ((u * cands.len() as f64) as usize).min(cands.len() - 1);
        cands[idx]
    };

    let mut prev = source;
    let mut cur = pick_uniform(source, 0);
    walk.push(cur);
    let mut step = 1u64;

    while walk.len() < walk_length {
        let cands = &adjacency[cur];
        if cands.is_empty() {
            break; // a walk that cannot continue stops, matching `random_walk`
        }
        let prev_neighbours: std::collections::HashSet<usize> =
            adjacency[prev].iter().copied().collect();

        let mut weights = Vec::with_capacity(cands.len());
        let mut total = 0.0;
        for &x in cands {
            let w = if x == prev {
                1.0 / p
            } else if prev_neighbours.contains(&x) {
                1.0
            } else {
                1.0 / q
            };
            total += w;
            weights.push(w);
        }

        let target = hash01(salted, cur as u64, step) * total;
        let mut acc = 0.0;
        let mut next = *cands.last().unwrap();
        for (i, &w) in weights.iter().enumerate() {
            acc += w;
            if target <= acc {
                next = cands[i];
                break;
            }
        }

        walk.push(next);
        prev = cur;
        cur = next;
        step += 1;
    }
    walk
}

/// node2vec: biased random walks, then a random-projection embedding of
/// their co-occurrence counts (see the module doc for what this is not).
///
/// Returns one row per node, aligned with `view.index_to_node`.
pub fn node2vec(view: &GraphView, config: &Node2VecConfig) -> Vec<Vec<f64>> {
    let n = view.node_count;
    let d = config.dimension.max(1);
    if n == 0 {
        return Vec::new();
    }

    let adjacency = undirected_adjacency(view);
    let p = if config.return_factor > 0.0 {
        config.return_factor
    } else {
        1.0
    };
    let q = if config.in_out_factor > 0.0 {
        config.in_out_factor
    } else {
        1.0
    };

    // Co-occurrence counts within `window_size` of each walk position -- the
    // statistic SGNS would otherwise fit by gradient descent.
    //
    // A `BTreeMap`, not a `HashMap`: the projection below sums these counts
    // in iteration order, and `HashMap`'s iteration order is randomized per
    // process (a different SipHash seed every run). Summing the same floats
    // in a different order gives a different rounding, so the *identical*
    // seed and graph produced a different embedding from one run to the
    // next -- the determinism this module promises depended on which way a
    // hash table happened to iterate. `BTreeMap` iterates in sorted key
    // order, which is fixed by the data alone.
    let mut cooc: std::collections::BTreeMap<(usize, usize), f64> =
        std::collections::BTreeMap::new();
    let mut walk_no = 0u64;
    for start in 0..n {
        for _ in 0..config.walks_per_node {
            let walk = biased_walk(
                &adjacency,
                start,
                config.walk_length,
                p,
                q,
                config.seed,
                walk_no,
            );
            walk_no += 1;
            for i in 0..walk.len() {
                let lo = i.saturating_sub(config.window_size);
                let hi = (i + config.window_size + 1).min(walk.len());
                for j in lo..hi {
                    if i != j {
                        *cooc.entry((walk[i], walk[j])).or_insert(0.0) += 1.0;
                    }
                }
            }
        }
    }

    // Project: embedding[u] = sum_v C[u, v] * R[v], the same very-sparse
    // random projection `fastrp` uses, salted differently so the two
    // algorithms do not draw identical matrices from the same seed value.
    let projection_seed = config.seed ^ 0xC0DE_2000_0000_0002;
    let mut emb = vec![vec![0.0f64; d]; n];
    for (&(u, v), &count) in cooc.iter() {
        for j in 0..d {
            emb[u][j] += count * sparse_projection_entry(projection_seed, v as u64, j as u64);
        }
    }

    l2_normalize_rows(&mut emb);
    emb
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::GraphView;
    use std::collections::HashMap;

    /// Two disjoint cliques (no edges between them) of `size` nodes each,
    /// indices `0..size` and `size..2*size`.
    fn two_cliques(size: usize) -> GraphView {
        let node_count = size * 2;
        let index_to_node: Vec<u64> = (0..node_count as u64).collect();
        let node_to_index: HashMap<u64, usize> = index_to_node
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
            .collect();

        let mut outgoing = vec![Vec::new(); node_count];
        for block in [0, size] {
            for u in block..block + size {
                for v in block..block + size {
                    if u != v {
                        outgoing[u].push(v);
                    }
                }
            }
        }
        let incoming = outgoing.clone(); // symmetric: every edge is mutual
        GraphView::from_adjacency_list(
            node_count,
            index_to_node,
            node_to_index,
            outgoing,
            incoming,
            None,
        )
    }

    fn dist(a: &[f64], b: &[f64]) -> f64 {
        a.iter()
            .zip(b)
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f64>()
            .sqrt()
    }

    fn avg_pairwise(rows: &[Vec<f64>], idx: &[usize]) -> f64 {
        let mut total = 0.0;
        let mut count = 0;
        for &i in idx {
            for &j in idx {
                if i != j {
                    total += dist(&rows[i], &rows[j]);
                    count += 1;
                }
            }
        }
        total / count as f64
    }

    fn cross_pairwise(rows: &[Vec<f64>], a: &[usize], b: &[usize]) -> f64 {
        let mut total = 0.0;
        let mut count = 0;
        for &i in a {
            for &j in b {
                total += dist(&rows[i], &rows[j]);
                count += 1;
            }
        }
        total / count as f64
    }

    // --- FastRP ---

    #[test]
    fn fastrp_dimension_matches_what_was_asked_for() {
        let view = two_cliques(4);
        let config = FastRpConfig {
            dimension: 11,
            ..Default::default()
        };
        let emb = fastrp(&view, &config);
        assert_eq!(emb.len(), view.node_count);
        for row in &emb {
            assert_eq!(row.len(), 11);
        }
    }

    #[test]
    fn fastrp_same_seed_reproduces() {
        let view = two_cliques(5);
        let config = FastRpConfig {
            seed: 7,
            ..Default::default()
        };
        let a = fastrp(&view, &config);
        let b = fastrp(&view, &config);
        assert_eq!(
            a, b,
            "identical seed and graph must give a bit-identical embedding"
        );
    }

    #[test]
    fn fastrp_different_seed_diverges() {
        let view = two_cliques(5);
        let a = fastrp(
            &view,
            &FastRpConfig {
                seed: 1,
                ..Default::default()
            },
        );
        let b = fastrp(
            &view,
            &FastRpConfig {
                seed: 2,
                ..Default::default()
            },
        );
        assert_ne!(
            a, b,
            "a different seed must not reproduce the same embedding"
        );
    }

    #[test]
    fn fastrp_same_clique_is_closer_than_across_cliques() {
        let size = 6;
        let view = two_cliques(size);
        let config = FastRpConfig {
            dimension: 16,
            seed: 42,
            ..Default::default()
        };
        let emb = fastrp(&view, &config);

        let clique_a: Vec<usize> = (0..size).collect();
        let clique_b: Vec<usize> = (size..size * 2).collect();

        let within = (avg_pairwise(&emb, &clique_a) + avg_pairwise(&emb, &clique_b)) / 2.0;
        let across = cross_pairwise(&emb, &clique_a, &clique_b);

        assert!(
            within < across,
            "within-clique distance {within} should be smaller than across-clique distance {across}"
        );
    }

    // --- node2vec ---

    #[test]
    fn node2vec_dimension_matches_what_was_asked_for() {
        let view = two_cliques(4);
        let config = Node2VecConfig {
            dimension: 9,
            walks_per_node: 3,
            walk_length: 6,
            ..Default::default()
        };
        let emb = node2vec(&view, &config);
        assert_eq!(emb.len(), view.node_count);
        for row in &emb {
            assert_eq!(row.len(), 9);
        }
    }

    #[test]
    fn node2vec_same_seed_reproduces() {
        let view = two_cliques(5);
        let config = Node2VecConfig {
            seed: 99,
            walks_per_node: 4,
            walk_length: 8,
            ..Default::default()
        };
        let a = node2vec(&view, &config);
        let b = node2vec(&view, &config);
        assert_eq!(
            a, b,
            "identical seed and graph must give a bit-identical embedding"
        );
    }

    #[test]
    fn node2vec_different_seed_diverges() {
        let view = two_cliques(5);
        let a = node2vec(
            &view,
            &Node2VecConfig {
                seed: 1,
                walks_per_node: 4,
                walk_length: 8,
                ..Default::default()
            },
        );
        let b = node2vec(
            &view,
            &Node2VecConfig {
                seed: 2,
                walks_per_node: 4,
                walk_length: 8,
                ..Default::default()
            },
        );
        assert_ne!(
            a, b,
            "a different seed must not reproduce the same embedding"
        );
    }

    #[test]
    fn node2vec_same_clique_is_closer_than_across_cliques() {
        let size = 6;
        let view = two_cliques(size);
        let config = Node2VecConfig {
            dimension: 16,
            seed: 42,
            walks_per_node: 8,
            walk_length: 10,
            window_size: 4,
            ..Default::default()
        };
        let emb = node2vec(&view, &config);

        let clique_a: Vec<usize> = (0..size).collect();
        let clique_b: Vec<usize> = (size..size * 2).collect();

        let within = (avg_pairwise(&emb, &clique_a) + avg_pairwise(&emb, &clique_b)) / 2.0;
        let across = cross_pairwise(&emb, &clique_a, &clique_b);

        assert!(
            within < across,
            "within-clique distance {within} should be smaller than across-clique distance {across}"
        );
    }

    #[test]
    fn biased_walk_with_low_q_favours_moving_outward() {
        // A path of 5 nodes: 0-1-2-3-4, walk started at the middle (2).
        // The second hop is node2vec's 2nd-order rule: from `cur` having
        // just left `prev`, a candidate that is `prev` itself gets weight
        // `1/p` (backtrack), and anything else gets `1/q` (moving further
        // out, since on a path the only other candidate is never a neighbour
        // of `prev` too). A very small q should push almost every walk to
        // keep moving outward rather than back toward the source.
        let adjacency = vec![vec![1], vec![0, 2], vec![1, 3], vec![2, 4], vec![3]];
        let mut outward = 0;
        let mut backtrack = 0;
        for walk_no in 0..200u64 {
            let walk = biased_walk(&adjacency, 2, 3, 1.0, 0.01, 123, walk_no);
            if walk.len() < 3 {
                continue;
            }
            if walk[2] == 2 {
                backtrack += 1; // stepped back to the source
            } else {
                outward += 1; // kept moving away (to 0 or to 4)
            }
        }
        assert!(
            outward + backtrack > 0,
            "the walk never reached a third step"
        );
        assert!(
            outward > backtrack,
            "low q should bias away from the node just left: outward={outward} backtrack={backtrack}"
        );
    }
}
