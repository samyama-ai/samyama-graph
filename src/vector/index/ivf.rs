//! An inverted-file (IVF) vector index (#1975).
//!
//! The vectors are partitioned by k-means into `nlist` lists, each vector in
//! the list of its nearest centroid. A search ranks the centroids against
//! the query, scores every vector in the nearest `nprobe` lists exactly, and
//! returns the best `k`. Probing every list is an exact scan, which is what
//! makes the recall knob honest: `nprobe = nlist` answers exactly what the
//! brute-force path answers, ties included.
//!
//! What an IVF index does not have is a graph. Its lists hold positions into
//! the vector list every index keeps, so it holds one copy of the vectors
//! where HNSW holds two, plus `nlist` centroids. Insert places a vector in
//! its nearest list; delete and update work as they do for HNSW (#1968): the
//! old position stops answering for its node and stays in its list until the
//! index is compacted.
//!
//! ## Training
//!
//! An index trains once it holds enough vectors to be worth partitioning
//! (below that every search is an exact scan, which on so few is cheap), and
//! again each time it has doubled since, so the lists keep pace with the
//! data at an amortised cost. A bulk load -- a rebuild, a reload -- trains
//! once at the end. k-means is seeded and deterministic: the same vectors in
//! the same order give the same centroids, on any machine with the same
//! floating-point rounding. Training uses at most [`SAMPLE_PER_LIST`] vectors
//! a list; every vector is then assigned.
//!
//! ## Metrics
//!
//! - **L2**: k-means in Euclidean space; vectors and queries go to the list
//!   whose centroid is nearest.
//! - **Cosine**: spherical k-means. Vectors are clustered by direction,
//!   centroids are unit vectors, and lists are ranked by cosine distance.
//! - **Inner product**: vectors are partitioned by Euclidean k-means, as for
//!   L2, and a query probes the lists whose centroid has the largest inner
//!   product with it, as Faiss's IVF does for maximum inner product search.
//!
//! ## Persistence
//!
//! A checkpoint writes the centroids beside the vector list
//! ([`PARTITION_SUFFIX`]); reloading reads them back and assigns each vector
//! to its nearest centroid without running k-means, so a reloaded index
//! answers as the one that was checkpointed. Without that file -- a plain
//! dump, a file that failed its checksum -- the index is retrained from its
//! vector list, which partitions the same vectors, possibly differently.

use super::{
    Backend, DistanceMetric, HnswParams, IndexKind, IvfParams, MetricDistance, Quantization,
    VectorError, VectorIndex, VectorResult,
};
use crate::graph::NodeId;
use crate::vector::distance;
use half::f16;
use hnsw_rs::prelude::Distance;
use rayon::prelude::*;
use std::borrow::Cow;
use std::path::{Path, PathBuf};

/// An index trains once it holds this many vectors, and not before. Below
/// it a search scans every vector, as HNSW does below 128.
pub const TRAIN_MIN: usize = 256;
/// An explicit `nlist` also waits for this many vectors per list, so no
/// list is trained from a handful of points.
pub const MIN_PER_LIST: usize = 8;
/// k-means reads at most this many vectors per list.
pub const SAMPLE_PER_LIST: usize = 256;
/// k-means stops after this many iterations, or once no vector moves.
pub const MAX_ITERATIONS: usize = 20;
/// The k-means seed. Fixed, so training is a function of its input.
const SEED: u64 = 0x1975_1975_1975_1975;

/// Suffix of the centroid file a checkpoint writes beside an IVF index's
/// vector list. Cannot collide with a vector-list file, which ends `.hnsw`.
pub(crate) const PARTITION_SUFFIX: &str = ".partition.ivf";
/// Bumped whenever the centroid file changes.
const FORMAT: u32 = 1;

/// `nlist` when the DDL gives none: `sqrt(n)`, the usual balance between
/// ranking centroids and scanning lists, which is `sqrt(n)` work each.
pub fn auto_nlist(n: usize) -> usize {
    ((n as f64).sqrt().round() as usize).clamp(1, *IvfParams::RANGE.end())
}

/// `nprobe` when neither the DDL nor the query gives one: a sixteenth of
/// the lists, and at least 8.
pub fn auto_nprobe(nlist: usize) -> usize {
    (nlist / 16).max(8).min(nlist.max(1))
}

/// The lists of an IVF index. Empty until it is trained.
#[derive(Clone, Debug, Default)]
pub(crate) struct Ivf {
    /// One per list, at the index's dimension; unit length for cosine.
    centroids: Vec<Vec<f32>>,
    /// Positions into the index's stored vectors, by list. A superseded
    /// position stays in its list until the index is compacted.
    lists: Vec<Vec<usize>>,
    /// Live vectors when the index was last trained; it retrains at twice
    /// this many.
    trained_on: usize,
}

impl Ivf {
    fn trained(&self) -> bool {
        !self.centroids.is_empty()
    }

    /// The same centroids with empty lists, to re-place a compacted index's
    /// entries into.
    pub(super) fn emptied(&self) -> Self {
        Self {
            centroids: self.centroids.clone(),
            lists: vec![Vec::new(); self.centroids.len()],
            trained_on: self.trained_on,
        }
    }

    /// Put the vector at `pos` in its nearest list. Nothing to do before
    /// training: an untrained index scans everything.
    pub(super) fn place(&mut self, pos: usize, vector: &[f32], metric: DistanceMetric) {
        if self.trained() {
            let list = nearest(&self.centroids, vector, metric);
            self.lists[list].push(pos);
        }
    }

    /// Centroids and list entries, in bytes.
    pub(super) fn bytes(&self) -> usize {
        use std::mem::size_of;
        let dims = self.centroids.first().map_or(0, Vec::len);
        self.centroids.len() * (size_of::<Vec<f32>>() + dims * size_of::<f32>())
            + self
                .lists
                .iter()
                .map(|l| size_of::<Vec<usize>>() + l.capacity() * size_of::<usize>())
                .sum::<usize>()
    }

    /// `kind=ivf, nlist=…, nprobe=…, lists=…`: the declared parameters,
    /// then how many lists the index was trained into, 0 while untrained.
    pub(super) fn describe(&self, kind: IndexKind) -> String {
        let p = match kind {
            IndexKind::Ivf(p) => p,
            IndexKind::Hnsw => IvfParams::default(),
        };
        let show = |v: Option<usize>| v.map_or_else(|| "auto".to_string(), |v| v.to_string());
        format!(
            "kind=ivf, nlist={}, nprobe={}, lists={}",
            show(p.nlist),
            show(p.nprobe),
            self.centroids.len()
        )
    }

    /// Lists in the order a query should probe them, nearest first; ties
    /// by list number, so the order is deterministic.
    fn probe_order(&self, query: &[f32], metric: DistanceMetric) -> Vec<usize> {
        let mut ranked: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, probe_rank(c, query, metric)))
            .collect();
        ranked.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        ranked.into_iter().map(|(i, _)| i).collect()
    }
}

/// Where a vector is placed: the nearest centroid in the geometry the index
/// was trained in. Ties go to the lower list.
fn nearest(centroids: &[Vec<f32>], v: &[f32], metric: DistanceMetric) -> usize {
    let mut best = (0usize, f32::INFINITY);
    for (i, c) in centroids.iter().enumerate() {
        let d = assign_rank(c, v, metric);
        if d < best.1 {
            best = (i, d);
        }
    }
    best.0
}

/// The distance a vector is assigned by: cosine for a cosine index, whose
/// centroids are unit vectors, and squared L2 otherwise.
fn assign_rank(centroid: &[f32], v: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => {
            let (dot, na, nb) = distance::cosine_parts(centroid, v);
            super::cosine_rank(dot, na, nb)
        }
        DistanceMetric::L2 | DistanceMetric::InnerProduct => distance::l2_sq(centroid, v),
    }
}

/// The distance a query ranks lists by: as assigned, except that an inner
/// product index probes by inner product, largest first.
fn probe_rank(centroid: &[f32], query: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::InnerProduct => -distance::dot(centroid, query),
        _ => assign_rank(centroid, query, metric),
    }
}

/// SplitMix64: a small, fixed generator, so the clustering does not depend
/// on the version of an external crate.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`, `n > 0`.
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// Uniform in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn normalized(v: &[f32]) -> Vec<f32> {
    let norm = distance::dot(v, v).sqrt();
    if norm > 0.0 {
        v.iter().map(|x| x / norm).collect()
    } else {
        v.to_vec()
    }
}

/// `nlist` centroids for `data` by Lloyd's k-means, seeded by k-means++.
/// Deterministic for a given input. For cosine the input is expected unit
/// length and the centroids are renormalised after every update.
pub(crate) fn kmeans(data: &[Vec<f32>], nlist: usize, metric: DistanceMetric) -> Vec<Vec<f32>> {
    let n = data.len();
    let k = nlist.min(n);
    if k == 0 {
        return Vec::new();
    }
    let spherical = metric == DistanceMetric::Cosine;
    let mut rng = SplitMix(SEED ^ n as u64 ^ ((k as u64) << 32));

    // k-means++: each next centroid is drawn in proportion to the squared
    // distance to the nearest one already chosen.
    let mut centroids: Vec<Vec<f32>> = vec![data[rng.below(n)].clone()];
    let mut closest: Vec<f32> = data
        .par_iter()
        .map(|v| assign_rank(&centroids[0], v, metric))
        .collect();
    while centroids.len() < k {
        let total: f64 = closest.iter().map(|d| *d as f64).sum();
        let pick = if total > 0.0 {
            let target = rng.unit() * total;
            let mut acc = 0.0f64;
            let mut chosen = n - 1;
            for (i, d) in closest.iter().enumerate() {
                acc += *d as f64;
                if acc > target {
                    chosen = i;
                    break;
                }
            }
            chosen
        } else {
            // Every point sits on a centroid: duplicates. Any will do.
            rng.below(n)
        };
        let c = data[pick].clone();
        closest
            .par_iter_mut()
            .zip(data.par_iter())
            .for_each(|(d, v)| *d = d.min(assign_rank(&c, v, metric)));
        centroids.push(c);
    }

    let dims = data[0].len();
    let mut assignment: Vec<usize> = vec![usize::MAX; n];
    for _ in 0..MAX_ITERATIONS {
        let next: Vec<usize> = data
            .par_iter()
            .map(|v| nearest(&centroids, v, metric))
            .collect();
        let moved = next != assignment;
        assignment = next;
        if !moved {
            break;
        }
        // Summed in f64, in input order, so the result does not depend on
        // how rayon split the work above.
        let mut sums = vec![vec![0.0f64; dims]; k];
        let mut counts = vec![0usize; k];
        for (v, &a) in data.iter().zip(&assignment) {
            counts[a] += 1;
            for (s, x) in sums[a].iter_mut().zip(v) {
                *s += *x as f64;
            }
        }
        // An empty list takes the point farthest from its own centroid, so
        // no list is wasted. Each such point is taken once.
        let mut taken = vec![false; n];
        for c in 0..k {
            if counts[c] > 0 {
                let mean: Vec<f32> = sums[c]
                    .iter()
                    .map(|s| (*s / counts[c] as f64) as f32)
                    .collect();
                centroids[c] = if spherical { normalized(&mean) } else { mean };
            } else {
                let far = (0..n)
                    .filter(|&i| !taken[i])
                    .map(|i| (i, assign_rank(&centroids[assignment[i]], &data[i], metric)))
                    .max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)));
                if let Some((i, _)) = far {
                    taken[i] = true;
                    centroids[c] = data[i].clone();
                }
            }
        }
    }
    centroids
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Sidecar {
    format: u32,
    dimensions: usize,
    metric: DistanceMetric,
    params: IvfParams,
    trained_on: usize,
    centroids: Vec<Vec<f32>>,
}

fn sidecar_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}{PARTITION_SUFFIX}"))
}

fn bad(message: impl Into<String>) -> VectorError {
    VectorError::IndexError(message.into())
}

impl VectorIndex {
    /// The vector at `pos` at f32, whichever precision is stored.
    fn vector_f32(&self, pos: usize) -> Cow<'_, [f32]> {
        match self.quantization {
            Quantization::None => Cow::Borrowed(&self.ivf_f32[pos]),
            Quantization::Fp16 => {
                Cow::Owned(self.ivf_f16[pos].iter().map(|v| v.to_f32()).collect())
            }
        }
    }

    /// Train an IVF index if it is due: untrained and holding enough
    /// vectors, or trained and grown to twice what it was trained on. A
    /// no-op for HNSW.
    pub fn train_if_due(&mut self) {
        let (IndexKind::Ivf(p), Backend::Ivf(lists)) = (self.kind, &self.backend) else {
            return;
        };
        let live = self.len();
        let due = if lists.trained() {
            live >= lists.trained_on.saturating_mul(2)
        } else {
            live >= TRAIN_MIN.max(p.nlist.unwrap_or(0).saturating_mul(MIN_PER_LIST))
        };
        if due {
            self.train();
        }
    }

    /// Run k-means over the current entries and re-place every one of them.
    /// Superseded entries are dropped from the lists, as a compaction would.
    pub fn train(&mut self) {
        let IndexKind::Ivf(p) = self.kind else {
            return;
        };
        let live: Vec<usize> = (0..self.entries())
            .filter(|&i| self.is_current(i))
            .collect();
        let nlist = p
            .nlist
            .unwrap_or_else(|| auto_nlist(live.len()))
            .min(live.len());
        if nlist == 0 {
            return;
        }
        // A deterministic sample, at most SAMPLE_PER_LIST a list: a partial
        // Fisher-Yates over the live positions, then back in storage order.
        let cap = nlist.saturating_mul(SAMPLE_PER_LIST);
        let mut sample = live.clone();
        if sample.len() > cap {
            let mut rng = SplitMix(SEED ^ live.len() as u64);
            for i in 0..cap {
                let j = i + rng.below(sample.len() - i);
                sample.swap(i, j);
            }
            sample.truncate(cap);
            sample.sort_unstable();
        }
        let spherical = self.metric == DistanceMetric::Cosine;
        let data: Vec<Vec<f32>> = sample
            .iter()
            .map(|&i| {
                let v = self.vector_f32(i);
                if spherical {
                    normalized(&v)
                } else {
                    v.into_owned()
                }
            })
            .collect();
        let centroids = kmeans(&data, nlist, self.metric);
        self.install(centroids, live.len());
    }

    /// Use `centroids` and assign every current entry to its nearest one.
    fn install(&mut self, centroids: Vec<Vec<f32>>, trained_on: usize) {
        let metric = self.metric;
        let live: Vec<usize> = (0..self.entries())
            .filter(|&i| self.is_current(i))
            .collect();
        let assigned: Vec<usize> = live
            .par_iter()
            .map(|&i| nearest(&centroids, &self.vector_f32(i), metric))
            .collect();
        let mut lists = vec![Vec::new(); centroids.len()];
        for (&pos, &list) in live.iter().zip(&assigned) {
            lists[list].push(pos);
        }
        self.backend = Backend::Ivf(Ivf {
            centroids,
            lists,
            trained_on,
        });
    }

    /// The `k` nearest that `keep` passes, as ranks, by scoring the nearest
    /// `width` lists -- the query's `nprobe`, else the index's, else
    /// [`auto_nprobe`]. When those lists hold fewer than `k` that pass, the
    /// probe doubles until they do or every list has been scored, which is
    /// an exact scan; so a selective filter widens the search, as it does
    /// for HNSW (#1969), rather than coming back short.
    pub(super) fn ivf_search(
        &self,
        query: &[f32],
        k: usize,
        width: Option<usize>,
        keep: &dyn Fn(NodeId) -> bool,
    ) -> Vec<(NodeId, f32)> {
        let want = k.min(self.len());
        if want == 0 {
            return Vec::new();
        }
        let lists = match &self.backend {
            Backend::Ivf(lists) if lists.trained() => lists,
            _ => return self.brute_force_where(query, want, keep),
        };
        let nlist = lists.lists.len();
        let declared = match self.kind {
            IndexKind::Ivf(p) => p.nprobe,
            IndexKind::Hnsw => None,
        };
        let mut nprobe = width
            .or(declared)
            .unwrap_or_else(|| auto_nprobe(nlist))
            .clamp(1, nlist);
        let order = lists.probe_order(query, self.metric);
        let distance = MetricDistance(self.metric);
        let q16: Vec<f16> = match self.quantization {
            Quantization::Fp16 => query.iter().map(|v| f16::from_f32(*v)).collect(),
            Quantization::None => Vec::new(),
        };
        // (position, node, rank) of every probed entry that passed.
        let mut scored: Vec<(usize, NodeId, f32)> = Vec::new();
        let mut probed = 0;
        loop {
            for &list in &order[probed..nprobe] {
                for &pos in &lists.lists[list] {
                    if !self.is_current(pos) {
                        continue;
                    }
                    let node = NodeId::new(self.node_at(pos));
                    if !keep(node) {
                        continue;
                    }
                    let d = match self.quantization {
                        Quantization::None => distance.eval(query, &self.ivf_f32[pos]),
                        Quantization::Fp16 => distance.eval(&q16, &self.ivf_f16[pos]),
                    };
                    if d.is_finite() {
                        scored.push((pos, node, d));
                    }
                }
            }
            probed = nprobe;
            if scored.len() >= want || probed == nlist {
                break;
            }
            nprobe = (nprobe * 2).min(nlist);
        }
        // Storage order, then a stable sort by distance: the order the exact
        // scan breaks ties in, so probing every list is that scan.
        scored.sort_unstable_by_key(|(pos, _, _)| *pos);
        scored.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
        let mut out: Vec<(NodeId, f32)> = scored.into_iter().map(|(_, n, d)| (n, d)).collect();
        // A query that is a stored vector finds it even when its list was not
        // probed -- possible for inner product, which places by L2 and probes
        // by dot product (#1498). Not needed, and not done, once every list
        // was scored.
        if probed < nlist {
            let present: Vec<(NodeId, f32)> = self
                .exact_matches(query)
                .into_iter()
                .filter(|(id, _)| keep(*id))
                .collect();
            if !present.is_empty() {
                out.retain(|(id, _)| !present.iter().any(|(p, _)| p == id));
                let mut merged = present;
                merged.extend(out);
                merged.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                out = merged;
            }
        }
        out.truncate(want);
        out
    }

    /// The `k` nearest that `keep` passes, with the distances a search
    /// reports, probing `nprobe` lists and widening as
    /// [`ivf_search`](Self::ivf_search) does (#1975). An HNSW index has no
    /// lists, and answers by exact scan.
    pub fn search_ivf_where(
        &self,
        query: &[f32],
        k: usize,
        nprobe: Option<usize>,
        keep: &dyn Fn(NodeId) -> bool,
    ) -> VectorResult<Vec<(NodeId, f32)>> {
        if !self.kind.is_ivf() {
            return self.search_exact_where(query, k, keep);
        }
        if query.len() != self.dimensions {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }
        Ok(self.report(self.ivf_search(query, k, nprobe, keep)))
    }

    /// Lists the index is partitioned into, 0 while untrained.
    pub fn ivf_lists(&self) -> usize {
        match &self.backend {
            Backend::Ivf(lists) => lists.lists.len(),
            _ => 0,
        }
    }

    /// Write the centroids beside the vector list `name` in `dir`. An
    /// untrained index writes nothing, and reloads by training if due.
    pub(crate) fn dump_partition(&self, dir: &Path, name: &str) -> VectorResult<()> {
        let (IndexKind::Ivf(params), Backend::Ivf(lists)) = (self.kind, &self.backend) else {
            return Ok(());
        };
        if !lists.trained() {
            return Ok(());
        }
        let sidecar = Sidecar {
            format: FORMAT,
            dimensions: self.dimensions,
            metric: self.metric,
            params,
            trained_on: lists.trained_on,
            centroids: lists.centroids.clone(),
        };
        let bytes = bincode::serialize(&sidecar)
            .map_err(|e| bad(format!("IVF centroid serialization error: {e}")))?;
        std::fs::write(sidecar_path(dir, name), bytes)?;
        Ok(())
    }

    /// Reload an IVF index from its vector list and the centroids
    /// [`dump_partition`](Self::dump_partition) wrote, assigning each vector
    /// to its nearest centroid without running k-means. Refuses -- so the
    /// caller retrains instead -- centroids written for another index. Call
    /// only on files a checkpoint manifest has verified.
    pub(crate) fn load_partition(
        dir: &Path,
        name: &str,
        dimensions: usize,
        metric: DistanceMetric,
        quantization: Quantization,
        params: HnswParams,
        kind: IndexKind,
    ) -> VectorResult<Self> {
        let IndexKind::Ivf(ivf_params) = kind else {
            return Err(bad("not an IVF index"));
        };
        let bytes = std::fs::read(sidecar_path(dir, name))?;
        let s: Sidecar = bincode::deserialize(&bytes)
            .map_err(|e| bad(format!("IVF centroid deserialization error: {e}")))?;
        if s.format != FORMAT {
            return Err(bad("IVF centroids written by another format"));
        }
        if s.dimensions != dimensions
            || s.metric != metric
            || s.params.nlist != ivf_params.nlist
            || s.centroids.is_empty()
            || s.centroids
                .iter()
                .any(|c| c.len() != dimensions || c.iter().any(|x| !x.is_finite()))
        {
            return Err(bad("IVF centroids describe a different index"));
        }
        let mut index = Self::load_untrained(
            &dir.join(name),
            dimensions,
            metric,
            quantization,
            params,
            kind,
        )?;
        index.install(s.centroids, s.trained_on);
        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grid(n: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| {
                vec![
                    (i % 10) as f32 + (i / 100) as f32 * 0.01,
                    (i / 10 % 10) as f32,
                ]
            })
            .collect()
    }

    #[test]
    fn kmeans_is_deterministic_and_fills_every_list() {
        let data = grid(400);
        let a = kmeans(&data, 9, DistanceMetric::L2);
        let b = kmeans(&data, 9, DistanceMetric::L2);
        assert_eq!(a, b);
        assert_eq!(a.len(), 9);
        let mut counts = [0usize; 9];
        for v in &data {
            counts[nearest(&a, v, DistanceMetric::L2)] += 1;
        }
        assert!(counts.iter().all(|c| *c > 0), "{counts:?}");
    }

    #[test]
    fn kmeans_on_fewer_points_than_lists_returns_one_per_point() {
        let data = grid(3);
        assert_eq!(kmeans(&data, 10, DistanceMetric::L2).len(), 3);
        assert!(kmeans(&[], 4, DistanceMetric::L2).is_empty());
    }

    #[test]
    fn spherical_centroids_are_unit_vectors() {
        let data: Vec<Vec<f32>> = grid(300)
            .iter()
            .map(|v| normalized(&[v[0] + 1.0, v[1] + 1.0]))
            .collect();
        for c in kmeans(&data, 6, DistanceMetric::Cosine) {
            let norm = distance::dot(&c, &c).sqrt();
            assert!((norm - 1.0).abs() < 1e-4, "{norm}");
        }
    }

    #[test]
    fn defaults() {
        assert_eq!(auto_nlist(10_000), 100);
        assert_eq!(auto_nlist(0), 1);
        assert_eq!(auto_nprobe(100), 8);
        assert_eq!(auto_nprobe(1600), 100);
        assert_eq!(auto_nprobe(3), 3);
    }
}
