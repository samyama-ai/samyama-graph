//! Louvain with a spatial null model: communities that are more connected
//! than distance alone explains (#2037, part 3).
//!
//! Ordinary modularity asks whether a group has more edges inside it than
//! chance, with chance being the Newman-Girvan model `P_ij = k_i k_j / 2m`:
//! every pair equally likely to link, in proportion to degree. On a graph laid
//! out in space -- a protein, a road or power network, towers -- that is the
//! wrong chance. Nearby nodes link far more often than distant ones whatever
//! group they belong to, so ordinary Louvain mostly rediscovers *where nodes
//! are*: tidy spatial blobs, which the coordinates already said.
//!
//! This uses the gravity null model of Expert, Evans, Blondel and Lambiotte
//! (PNAS 108(19), 2011), `P_ij = k_i k_j f(d_ij)`, with the deterrence `f`
//! measured from the graph itself rather than assumed: pairs are put into
//! `bins` equal-width distance bands, and in each band
//!
//! ```text
//! f(band) = (edges between pairs in the band) / (sum of k_i k_j over pairs in the band)
//! ```
//!
//! so the model expects, at every distance, exactly as many edges as there
//! are. Summed over all pairs, `P` equals the number of edge ends, `2m`,
//! exactly: the same normalisation Newman-Girvan has, which is what makes the
//! result a modularity. A community found against it is a group whose nodes
//! link to each other *more than their distances predict*.
//!
//! With `bins = 1` the deterrence is one number and this is Newman-Girvan
//! modularity without self-pairs.
//!
//! **When it helps**, measured on Expert et al.'s benchmark (two groups mixed
//! at random in a unit square, links likelier near and within a group;
//! three graphs each, NMI against the planted groups, 1.0 is exact):
//!
//! | Links per node | Spatial | Ordinary Louvain |
//! |---|---|---|
//! | about 5, short range | 0.30 | 0.31 -- too sparse for either |
//! | about 23 | **1.00** | 0.69 -- distance drives links but is not all of it |
//! | 34 and up, long range | 1.00 | 1.00 -- distance barely matters |
//!
//! So it is for graphs where nearness explains much of who links to whom but
//! not all of it: the middle row. It cannot find structure a sparse graph
//! does not hold.
//!
//! **Dense:** the null model gives every pair a value, so the work is
//! `O(n^2)` per sweep and memory `n^2` floats. [`MAX_NODES`] bounds it.
//!
//! **Read as an undirected simple graph:** a relationship either way joins a
//! pair once; weights, parallel relationships and self-loops are ignored,
//! as the deterrence counts pairs, not relationships.

use crate::common::GraphView;

/// The most nodes one call will take: 2,000 is 4 million pair values, 32 MB.
pub const MAX_NODES: usize = 2_000;

/// A sweep that moves nothing ends a level; this bounds a level that keeps
/// moving nodes by amounts too small to matter.
const MAX_SWEEPS_PER_LEVEL: usize = 100;

/// A move must gain more than this, or float noise moves nodes back and
/// forth.
const MIN_GAIN: f64 = 1e-12;

/// The result of [`spatial_louvain`].
#[derive(Debug, Clone, PartialEq)]
pub struct SpatialCommunities {
    /// Each node's community, numbered from 0 in order of first appearance.
    pub community: Vec<usize>,
    /// The partition's spatial modularity; `None` with no edges, where it is
    /// 0/0.
    pub modularity: Option<f64>,
    /// The measured deterrence, one `(band's upper distance, f)` per band.
    pub deterrence: Vec<(f64, f64)>,
}

/// Why [`spatial_louvain`] did not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialError {
    /// More than [`MAX_NODES`] nodes.
    TooLarge(usize),
    /// `bins` was 0.
    NoBins,
}

impl std::fmt::Display for SpatialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge(n) => write!(
                f,
                "{n} nodes is more than the {MAX_NODES} the spatial null model is computed for: it \
                 gives every pair a value, so the work grows with the square of the node count"
            ),
            Self::NoBins => write!(f, "`bins` must be at least 1"),
        }
    }
}

impl std::error::Error for SpatialError {}

/// The null model and the graph, as one dense matrix `W = A - P`.
struct Model {
    n: usize,
    /// `W[i * n + j]`, symmetric, zero on the diagonal.
    w: Vec<f64>,
    /// Which pairs are joined, for listing a node's neighbours.
    neighbours: Vec<Vec<usize>>,
    two_m: f64,
    deterrence: Vec<(f64, f64)>,
}

fn model(view: &GraphView, distance: &dyn Fn(usize, usize) -> f64, bins: usize) -> Model {
    let n = view.node_count;
    let mut neighbours: Vec<Vec<usize>> = vec![Vec::new(); n];
    for u in 0..n {
        for &v in view.successors(u) {
            if u != v {
                neighbours[u].push(v);
                neighbours[v].push(u);
            }
        }
    }
    for list in &mut neighbours {
        list.sort_unstable();
        list.dedup();
    }
    let k: Vec<f64> = neighbours.iter().map(|l| l.len() as f64).collect();
    let two_m: f64 = k.iter().sum();

    let mut d = vec![0.0; n * n];
    let mut d_max: f64 = 0.0;
    for i in 0..n {
        for j in i + 1..n {
            let x = distance(i, j);
            d[i * n + j] = x;
            d[j * n + i] = x;
            d_max = d_max.max(x);
        }
    }
    let band = |x: f64| -> usize {
        if d_max <= 0.0 {
            0
        } else {
            ((x / d_max * bins as f64) as usize).min(bins - 1)
        }
    };
    let (mut edges, mut expected) = (vec![0.0; bins], vec![0.0; bins]);
    for i in 0..n {
        for j in i + 1..n {
            let b = band(d[i * n + j]);
            expected[b] += k[i] * k[j];
        }
        for &j in neighbours[i].iter().filter(|&&j| j > i) {
            edges[band(d[i * n + j])] += 1.0;
        }
    }
    let f: Vec<f64> = (0..bins).map(|b| if expected[b] > 0.0 { edges[b] / expected[b] } else { 0.0 }).collect();
    let mut w = vec![0.0; n * n];
    for i in 0..n {
        for j in 0..n {
            if i != j {
                w[i * n + j] = -k[i] * k[j] * f[band(d[i * n + j])];
            }
        }
        for &j in &neighbours[i] {
            w[i * n + j] += 1.0;
        }
    }
    let deterrence = (0..bins).map(|b| (d_max * (b + 1) as f64 / bins as f64, f[b])).collect();
    Model { n, w, neighbours, two_m, deterrence }
}

/// One level of local moving over a dense `n x n` matrix `w`, with the
/// adjacency `neighbours` deciding which communities a node may join (an
/// unjoined community can only lose: `P` is never negative). Returns each
/// node's community, numbered from 0 in order of first appearance, and
/// whether anything moved.
fn local_moving(n: usize, w: &[f64], neighbours: &[Vec<usize>]) -> (Vec<usize>, bool) {
    let mut comm: Vec<usize> = (0..n).collect();
    let mut size = vec![1usize; n];
    let mut sum = vec![0.0; n];
    let mut moved_any = false;
    for _ in 0..MAX_SWEEPS_PER_LEVEL {
        let mut moved = false;
        for i in 0..n {
            // What node i adds to each community, itself excluded.
            sum.iter_mut().for_each(|s| *s = 0.0);
            for j in (0..n).filter(|&j| j != i) {
                sum[comm[j]] += w[i * n + j];
            }
            let own = comm[i];
            // Alone is worth 0; staying is worth `sum[own]`.
            let mut best = own;
            let mut best_value = sum[own];
            let mut consider = |c: usize, value: f64| {
                if value > best_value + MIN_GAIN || (value >= best_value - MIN_GAIN && c < best && value > best_value) {
                    best = c;
                    best_value = value;
                }
            };
            let mut candidates: Vec<usize> = neighbours[i].iter().map(|&j| comm[j]).collect();
            candidates.sort_unstable();
            candidates.dedup();
            for c in candidates.into_iter().filter(|&c| c != own) {
                consider(c, sum[c]);
            }
            if size[own] > 1 {
                if let Some(empty) = size.iter().position(|&s| s == 0) {
                    consider(empty, 0.0);
                }
            }
            if best != own {
                size[own] -= 1;
                size[best] += 1;
                comm[i] = best;
                moved = true;
                moved_any = true;
            }
        }
        if !moved {
            break;
        }
    }
    (renumber(&comm), moved_any)
}

/// Community ids numbered from 0 in order of first appearance.
fn renumber(comm: &[usize]) -> Vec<usize> {
    let mut map = std::collections::HashMap::new();
    comm.iter()
        .map(|&c| {
            let next = map.len();
            *map.entry(c).or_insert(next)
        })
        .collect()
}

/// Communities of `view` against the spatial null model, `distance(i, j)`
/// being the straight-line distance between dense indexes `i` and `j`.
pub fn spatial_louvain(
    view: &GraphView,
    distance: impl Fn(usize, usize) -> f64,
    bins: usize,
) -> Result<SpatialCommunities, SpatialError> {
    let n = view.node_count;
    if bins == 0 {
        return Err(SpatialError::NoBins);
    }
    if n > MAX_NODES {
        return Err(SpatialError::TooLarge(n));
    }
    let model = model(view, &distance, bins);
    if model.two_m == 0.0 {
        return Ok(SpatialCommunities {
            community: (0..n).collect(),
            modularity: None,
            deterrence: model.deterrence,
        });
    }
    // Louvain's levels: move nodes, then merge each community into one node
    // and move those, until a level moves nothing.
    let mut membership: Vec<usize> = (0..n).collect();
    let (mut size, mut w, mut neighbours) = (n, model.w.clone(), model.neighbours.clone());
    loop {
        let (comm, moved) = local_moving(size, &w, &neighbours);
        if !moved {
            break;
        }
        let groups = comm.iter().max().map_or(0, |&m| m + 1);
        let mut merged = vec![0.0; groups * groups];
        for i in 0..size {
            for j in 0..size {
                merged[comm[i] * groups + comm[j]] += w[i * size + j];
            }
        }
        let mut joined: Vec<Vec<usize>> = vec![Vec::new(); groups];
        for (i, list) in neighbours.iter().enumerate() {
            for &j in list {
                if comm[i] != comm[j] {
                    joined[comm[i]].push(comm[j]);
                }
            }
        }
        for list in &mut joined {
            list.sort_unstable();
            list.dedup();
        }
        for m in &mut membership {
            *m = comm[*m];
        }
        // A merged node's own pairs move with it: zero the diagonal so a
        // level's sums count only pairs across nodes.
        for g in 0..groups {
            merged[g * groups + g] = 0.0;
        }
        (size, w, neighbours) = (groups, merged, joined);
        if groups == 1 {
            break;
        }
    }
    let community = renumber(&membership);
    let modularity = Some(spatial_modularity_of(&model, &community));
    Ok(SpatialCommunities { community, modularity, deterrence: model.deterrence })
}

fn spatial_modularity_of(model: &Model, community: &[usize]) -> f64 {
    let n = model.n;
    let mut q = 0.0;
    for i in 0..n {
        for j in 0..n {
            if community[i] == community[j] {
                q += model.w[i * n + j];
            }
        }
    }
    q / model.two_m
}

/// The spatial modularity of any partition, under the same null model;
/// `None` when the graph has no edges or the partition does not cover it.
pub fn spatial_modularity(
    view: &GraphView,
    distance: impl Fn(usize, usize) -> f64,
    bins: usize,
    community: &[usize],
) -> Option<f64> {
    if bins == 0 || community.len() != view.node_count || view.node_count > MAX_NODES {
        return None;
    }
    let model = model(view, &distance, bins);
    (model.two_m > 0.0).then(|| spatial_modularity_of(&model, community))
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

    struct Rng(u64);
    impl Rng {
        fn unit(&mut self) -> f64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % 1_000_000) as f64 / 1_000_000.0
        }
    }

    /// Normalised mutual information between two partitions: 1 when they
    /// agree up to renaming, 0 when one says nothing about the other.
    fn nmi(a: &[usize], b: &[usize]) -> f64 {
        let n = a.len() as f64;
        let mut joint: HashMap<(usize, usize), f64> = HashMap::new();
        let (mut pa, mut pb): (HashMap<usize, f64>, HashMap<usize, f64>) = (HashMap::new(), HashMap::new());
        for (&x, &y) in a.iter().zip(b) {
            *joint.entry((x, y)).or_default() += 1.0;
            *pa.entry(x).or_default() += 1.0;
            *pb.entry(y).or_default() += 1.0;
        }
        let h = |p: &HashMap<usize, f64>| -p.values().map(|c| c / n * (c / n).ln()).sum::<f64>();
        let i: f64 = joint.iter().map(|(&(x, y), &c)| c / n * ((c / n) / (pa[&x] / n * pb[&y] / n)).ln()).sum();
        let (ha, hb) = (h(&pa), h(&pb));
        if ha + hb == 0.0 {
            1.0
        } else {
            2.0 * i / (ha + hb)
        }
    }

    /// Expert et al.'s benchmark: nodes scattered in a unit square, two
    /// planted groups assigned at random so they are mixed in space, and
    /// links far likelier between near nodes, and likelier still within a
    /// group.
    fn interleaved(n: usize, seed: u64) -> (GraphView, Vec<[f64; 2]>, Vec<usize>) {
        let mut rng = Rng(seed | 1);
        let pos: Vec<[f64; 2]> = (0..n).map(|_| [rng.unit(), rng.unit()]).collect();
        let group: Vec<usize> = (0..n).map(|_| (rng.unit() < 0.5) as usize).collect();
        let mut edges = Vec::new();
        for i in 0..n {
            for j in i + 1..n {
                let d = ((pos[i][0] - pos[j][0]).powi(2) + (pos[i][1] - pos[j][1]).powi(2)).sqrt();
                let affinity = if group[i] == group[j] { 1.0 } else { 0.1 };
                // Gravity-like: likelier near, but never impossible far, so
                // a group's members far apart are still tied to each other.
                if rng.unit() < affinity / (1.0 + (d / 0.15).powi(2)) {
                    edges.push((i, j));
                }
            }
        }
        (view(n, &edges), pos, group)
    }

    fn euclid(pos: &[[f64; 2]]) -> impl Fn(usize, usize) -> f64 + '_ {
        move |i, j| ((pos[i][0] - pos[j][0]).powi(2) + (pos[i][1] - pos[j][1]).powi(2)).sqrt()
    }

    /// Measured with these settings (mean degree about 24) on three graphs:
    /// spatial NMI 1.000, ordinary Louvain 0.690 against the planted groups.
    /// Denser or longer-range graphs (degree 34 and up) are solved by both,
    /// and a sparse short-range one (degree 5) by neither -- see the module
    /// documentation.
    #[test]
    fn the_spatial_model_finds_groups_that_ordinary_louvain_mistakes_for_places() {
        let mut spatial_total = 0.0;
        let mut plain_total = 0.0;
        for seed in [3, 7, 11] {
            let (v, pos, group) = interleaved(300, seed);
            let spatial = spatial_louvain(&v, euclid(&pos), 20).unwrap();
            let plain = crate::louvain(&v, 20);
            spatial_total += nmi(&spatial.community, &group) / 3.0;
            plain_total += nmi(&plain, &group) / 3.0;
        }
        assert!(spatial_total > 0.95, "the spatial model did not recover the planted groups: NMI {spatial_total:.3}");
        assert!(
            spatial_total > plain_total + 0.2,
            "no clearly better than ordinary Louvain: {spatial_total:.3} vs {plain_total:.3}"
        );
    }

    #[test]
    fn the_null_model_expects_exactly_as_many_edges_as_there_are() {
        let (v, pos, _) = interleaved(120, 5);
        for bins in [1, 7, 30] {
            let m = model(&v, &euclid(&pos), bins);
            let expected: f64 = -(0..m.n * m.n).map(|x| m.w[x]).sum::<f64>() + m.two_m;
            assert!((expected - m.two_m).abs() < 1e-6 * m.two_m, "bins {bins}: P sums to {expected}, 2m is {}", m.two_m);
            // A partition of one community has modularity exactly 0.
            let one = vec![0; v.node_count];
            assert!(spatial_modularity_of(&m, &one).abs() < 1e-9);
        }
    }

    #[test]
    fn the_reported_modularity_is_the_partitions_and_at_least_the_trivial_ones() {
        let (v, pos, group) = interleaved(150, 9);
        let found = spatial_louvain(&v, euclid(&pos), 12).unwrap();
        let q = found.modularity.unwrap();
        let recomputed = spatial_modularity(&v, euclid(&pos), 12, &found.community).unwrap();
        assert!((q - recomputed).abs() < 1e-12);
        let singletons: Vec<usize> = (0..v.node_count).collect();
        assert!(q >= spatial_modularity(&v, euclid(&pos), 12, &singletons).unwrap());
        assert!(q >= 0.0);
        assert!(q >= spatial_modularity(&v, euclid(&pos), 12, &group).unwrap() - 0.05, "far below the planted partition");
    }

    #[test]
    fn with_one_band_two_joined_cliques_are_two_communities() {
        // Two 5-cliques joined by one edge; positions all equal, so distance
        // says nothing and this is plain modularity.
        let mut edges = Vec::new();
        for base in [0, 5] {
            for i in 0..5 {
                for j in i + 1..5 {
                    edges.push((base + i, base + j));
                }
            }
        }
        edges.push((4, 5));
        let v = view(10, &edges);
        let found = spatial_louvain(&v, |_, _| 0.0, 1).unwrap();
        assert_eq!(found.community, vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1]);
    }

    #[test]
    fn the_same_graph_gives_the_same_communities() {
        let (v, pos, _) = interleaved(100, 13);
        let a = spatial_louvain(&v, euclid(&pos), 10).unwrap();
        let b = spatial_louvain(&v, euclid(&pos), 10).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn no_edges_leaves_every_node_alone_with_no_modularity() {
        let v = view(4, &[]);
        let found = spatial_louvain(&v, |_, _| 1.0, 3).unwrap();
        assert_eq!((found.community, found.modularity), (vec![0, 1, 2, 3], None));
        let empty = spatial_louvain(&view(0, &[]), |_, _| 1.0, 3).unwrap();
        assert!(empty.community.is_empty());
    }

    #[test]
    fn self_loops_and_parallel_edges_count_once_or_not_at_all() {
        let plain = view(3, &[(0, 1), (1, 2)]);
        let noisy = view(3, &[(0, 1), (1, 0), (0, 1), (1, 2), (2, 2)]);
        let d = |i: usize, j: usize| (i as f64 - j as f64).abs();
        assert_eq!(spatial_louvain(&plain, d, 2).unwrap(), spatial_louvain(&noisy, d, 2).unwrap());
    }

    #[test]
    fn the_deterrence_falls_with_distance_on_a_spatial_graph() {
        let (v, pos, _) = interleaved(200, 17);
        let found = spatial_louvain(&v, euclid(&pos), 10).unwrap();
        let f: Vec<f64> = found.deterrence.iter().map(|&(_, f)| f).collect();
        assert!(f[0] > 10.0 * f[5], "{f:?}");
        assert!(found.deterrence.windows(2).all(|w| w[0].0 < w[1].0), "bands out of order");
    }

    #[test]
    fn too_many_nodes_or_no_bins_are_refused() {
        let big = view(MAX_NODES + 1, &[]);
        assert_eq!(spatial_louvain(&big, |_, _| 1.0, 3).unwrap_err(), SpatialError::TooLarge(MAX_NODES + 1));
        assert_eq!(spatial_louvain(&view(2, &[(0, 1)]), |_, _| 1.0, 0).unwrap_err(), SpatialError::NoBins);
        assert!(SpatialError::TooLarge(5).to_string().contains("square of the node count"));
    }
}
