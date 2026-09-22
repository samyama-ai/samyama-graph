//! # HNSW Vector Index Implementation
//!
//! ## How HNSW works
//!
//! HNSW (Hierarchical Navigable Small World) builds a proximity graph with multiple
//! layers. Each node is assigned a random maximum layer (exponentially distributed —
//! most nodes live only on layer 0, few reach the top). Insertion connects the new
//! point to its nearest neighbors on each layer. Search starts at the top layer's
//! entry point and greedily descends, refining the candidate set at each level.
//!
//! ## Key parameters
//!
//! - **`m`** (max connections per node): Controls graph density. Higher m = better recall
//!   but more memory and slower insertion. Typical values: 12-48. Layer 0 uses `2*m`
//!   connections.
//! - **`ef_construction`** (search width during insertion): How many candidates to
//!   consider when connecting a new node. Higher = better graph quality but slower build.
//!   Typical values: 100-400.
//! - **`ef_search`** (search width during query): How many candidates to track during
//!   search. Higher = better recall but slower queries. Must be >= k (number of results).
//!   This is the main recall-vs-speed knob at query time.
//!
//! ## Distance trait
//!
//! Rust's trait system enables polymorphic distance computation. The `hnsw_rs` crate
//! defines a `Distance<T>` trait, and this module implements it with `CosineDistance`
//! and `InnerProductDistance` structs. This allows the same HNSW data structure to work
//! with different distance metrics without runtime dispatch overhead (monomorphization).
//!
//! ## Cosine distance formula
//!
//! `cosine_distance(a, b) = 1 - (a . b) / (||a|| * ||b||)`
//!
//! This measures angular distance between vectors:
//! - **0** = identical direction (parallel vectors)
//! - **1** = orthogonal (perpendicular, no similarity)
//! - **2** = opposite direction (anti-correlated)
//!
//! ## Persistence strategy
//!
//! HNSW indices (from `hnsw_rs`) don't expose an iterator over stored vectors.
//! To support persistence, all inserted vectors are also stored in a `Vec<StoredVector>`
//! alongside the HNSW structure. On serialization, this vector list is saved via
//! `bincode`. On load, a fresh HNSW index is constructed and all stored vectors are
//! re-inserted. This trades load-time speed for implementation simplicity.

use crate::graph::NodeId;
use half::f16;
use hnsw_rs::prelude::*;
use thiserror::Error;

/// Vector index errors
#[derive(Error, Debug)]
pub enum VectorError {
    #[error("Index error: {0}")]
    IndexError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // Naming only the two numbers left the caller reading it as "my embedding is
    // the wrong size", when the usual cause is an index configured with a
    // dimension nobody chose -- CREATE VECTOR INDEX defaults to 1536 when
    // `dimensions` is omitted (#474). Point at the index configuration, since
    // that is the side that is adjustable and the side that is usually wrong.
    #[error("Dimension mismatch: the index expects {expected}-dimensional vectors, got {got}. \
Set the index dimension with CREATE VECTOR INDEX ... OPTIONS {{dimensions: {got}}} \
(the default is 1536 when `dimensions` is omitted).")]
    DimensionMismatch { expected: usize, got: usize },

    #[error("Search failed: {0}")]
    SearchFailed(String),
}

pub type VectorResult<T> = Result<T, VectorError>;

/// Distance metric for vector search
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DistanceMetric {
    /// L2 (Euclidean) distance
    L2,
    /// Cosine similarity
    Cosine,
    /// Inner product
    InnerProduct,
}

/// A point in the vector space, associated with a NodeId
#[derive(Clone, Debug)]
pub struct VectorPoint {
    pub node_id: NodeId,
    pub vector: Vec<f32>,
}

/// Cosine distance implementation for hnsw_rs
#[derive(Clone, Copy, Debug, Default)]
pub struct CosineDistance;

impl Distance<f32> for CosineDistance {
    fn eval(&self, va: &[f32], vb: &[f32]) -> f32 {
        let mut dot = 0.0;
        let mut norm_a = 0.0;
        let mut norm_b = 0.0;
        
        for (a, b) in va.iter().zip(vb.iter()) {
            dot += a * b;
            norm_a += a * a;
            norm_b += b * b;
        }
        
        if norm_a <= 0.0 || norm_b <= 0.0 {
            return 1.0;
        }
        
        // Cosine distance = 1.0 - cosine similarity.
        // Cosine similarity is mathematically in [-1, 1], but floating-point
        // rounding on near-duplicate vectors can push it just past 1.0, yielding a
        // tiny NEGATIVE distance (or a tiny positive distance between two identical
        // vectors that should be exactly 0). Either violates hnsw_rs 0.2.1's internal
        // `dist_to_ref <= 0` invariant and panics mid-insert (hnsw.rs:938), aborting
        // the whole index rebuild. Clamp the similarity into range and floor the
        // distance at 0 so identical vectors give exactly 0 and the metric stays in
        // [0, 2].
        let sim = (dot / (norm_a.sqrt() * norm_b.sqrt())).clamp(-1.0, 1.0);
        (1.0 - sim).max(0.0)
    }
}

/// Cosine distance over f16-quantized vectors (NDS-09).
///
/// The arithmetic is done in f32. f16 has a 10-bit significand and a maximum
/// around 65504; accumulating a dot product of even a few hundred terms in f16
/// would overflow or lose the sum long before the distance mattered. What is
/// quantized is *storage* -- which is where the memory is -- and the comparison
/// is computed at full precision on the way past.
///
/// The clamp and the floor are the same as `CosineDistance` and for the same
/// reason: hnsw_rs panics on a negative distance, and quantization makes
/// near-duplicate vectors *more* likely to round past 1.0, not less.
#[derive(Clone, Copy, Debug, Default)]
pub struct CosineDistanceF16;

impl Distance<f16> for CosineDistanceF16 {
    fn eval(&self, va: &[f16], vb: &[f16]) -> f32 {
        let mut dot = 0.0f32;
        let mut norm_a = 0.0f32;
        let mut norm_b = 0.0f32;
        for (a, b) in va.iter().zip(vb.iter()) {
            let (a, b) = (a.to_f32(), b.to_f32());
            dot += a * b;
            norm_a += a * a;
            norm_b += b * b;
        }
        if norm_a <= 0.0 || norm_b <= 0.0 {
            return 1.0;
        }
        let sim = (dot / (norm_a.sqrt() * norm_b.sqrt())).clamp(-1.0, 1.0);
        (1.0 - sim).max(0.0)
    }
}

/// How vectors are stored in the index (NDS-09).
///
/// `Fp16` halves the bytes held per vector, in the HNSW's own copy **and** in
/// the copy kept for persistence. It costs recall, and how much is a property
/// of the corpus rather than a constant -- which is why
/// `tests/vector_quantization_recall.rs` measures it against the unquantized
/// index on the same vectors rather than asserting a figure here.
///
/// The on-disk format is unchanged: `dump` writes f32 whatever the index holds,
/// so a snapshot written by a quantized index loads into an unquantized one and
/// the reverse. Quantization is a runtime memory choice, not a file format.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Quantization {
    /// 32-bit floats, as stored before this existed.
    #[default]
    None,
    /// 16-bit floats. Half the memory, some recall.
    Fp16,
}

impl Quantization {
    /// Parse the `quantization` DDL option.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" | "f32" | "fp32" => Some(Self::None),
            "fp16" | "f16" | "half" => Some(Self::Fp16),
            _ => None,
        }
    }

    /// Bytes per dimension held per vector.
    pub fn bytes_per_value(self) -> usize {
        match self {
            Self::None => std::mem::size_of::<f32>(),
            Self::Fp16 => std::mem::size_of::<f16>(),
        }
    }
}

/// Inner Product distance implementation for hnsw_rs
#[derive(Clone, Copy, Debug, Default)]
pub struct InnerProductDistance;

impl Distance<f32> for InnerProductDistance {
    fn eval(&self, va: &[f32], vb: &[f32]) -> f32 {
        let mut dot = 0.0;
        for (a, b) in va.iter().zip(vb.iter()) {
            dot += a * b;
        }
        // Inner product distance = 1.0 - dot product (for normalized vectors)
        1.0 - dot
    }
}

/// Stored vector entry for persistence
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StoredVector {
    pub node_id: u64,
    pub vector: Vec<f32>,
}

/// The HNSW graph, holding whichever element type the index was built for.
///
/// Two variants rather than one generic index because `Hnsw` is typed on its
/// element: an index that converted f16 to f32 at the boundary would keep the
/// f32 copy inside the graph, and the memory saving -- the entire point of
/// NDS-09's quantization -- would be a claim about the persistence copy only.
enum Backend {
    F32(Hnsw<'static, f32, CosineDistance>),
    F16(Hnsw<'static, f16, CosineDistanceF16>),
}

/// Wrapper around HNSW index
pub struct VectorIndex {
    /// Number of dimensions
    dimensions: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// How values are stored (NDS-09).
    quantization: Quantization,
    /// The actual HNSW index
    backend: Backend,
    /// All inserted vectors (for persistence — HNSW doesn't expose iteration),
    /// held at the index's own precision so this copy is halved too.
    stored_f32: Vec<StoredVector>,
    stored_f16: Vec<(u64, Vec<f16>)>,
}

// Implement Debug manually because Hnsw doesn't implement it
impl std::fmt::Debug for VectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorIndex")
            .field("dimensions", &self.dimensions)
            .field("metric", &self.metric)
            .field("quantization", &self.quantization)
            .field("len", &self.len())
            .finish()
    }
}

impl VectorIndex {
    /// Create a new vector index
    pub fn new(dimensions: usize, metric: DistanceMetric) -> Self {
        Self::with_quantization(dimensions, metric, Quantization::None)
    }

    /// Create an index that stores its vectors at the given precision (NDS-09).
    pub fn with_quantization(
        dimensions: usize,
        metric: DistanceMetric,
        quantization: Quantization,
    ) -> Self {
        Self::build(dimensions, metric, quantization, 100_000)
    }

    fn build(
        dimensions: usize,
        metric: DistanceMetric,
        quantization: Quantization,
        max_elements: usize,
    ) -> Self {
        let m = 16;
        let ef_construction = 200;
        let backend = match quantization {
            Quantization::None => Backend::F32(Hnsw::new(
                m, max_elements, 16, ef_construction, CosineDistance,
            )),
            Quantization::Fp16 => Backend::F16(Hnsw::new(
                m, max_elements, 16, ef_construction, CosineDistanceF16,
            )),
        };
        Self {
            dimensions,
            metric,
            quantization,
            backend,
            stored_f32: Vec::new(),
            stored_f16: Vec::new(),
        }
    }

    /// How this index stores its values.
    pub fn quantization(&self) -> Quantization {
        self.quantization
    }

    /// Bytes held for the vectors themselves, across both copies the index
    /// keeps: the HNSW's and the one kept for persistence.
    ///
    /// Reported rather than asserted. A quantization that claims to halve
    /// memory has to be able to show it.
    pub fn vector_bytes(&self) -> usize {
        2 * self.len() * self.dimensions * self.quantization.bytes_per_value()
    }

    /// Add a vector to the index
    pub fn add(&mut self, node_id: NodeId, vector: &Vec<f32>) -> VectorResult<()> {
        if vector.len() != self.dimensions {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimensions,
                got: vector.len(),
            });
        }
        
        // Not wrapped in catch_unwind on purpose: hnsw_rs registers the point in its
        // layer index before doing any of the work that could panic, so catching would
        // leave a half-linked point live in the graph while this function returns Err
        // and skips the stored_vectors push below — desynchronising the HNSW from the
        // vector list that backs len(), dump() and the brute-force fallback. The
        // CosineDistance clamp keeps the metric in [0, 2], which is what the
        // search_layer assertions (hnsw.rs:937-938) actually require.
        match &mut self.backend {
            Backend::F32(h) => {
                h.insert((vector, node_id.0 as usize));
                self.stored_f32.push(StoredVector {
                    node_id: node_id.0,
                    vector: vector.clone(),
                });
            }
            Backend::F16(h) => {
                // Quantized once, here, and the graph is built on the values a
                // search will actually compare. Inserting f32 and quantizing
                // only the stored copy would build the graph on one set of
                // vectors and answer from another.
                let q: Vec<f16> = vector.iter().map(|v| f16::from_f32(*v)).collect();
                h.insert((&q, node_id.0 as usize));
                self.stored_f16.push((node_id.0, q));
            }
        }

        Ok(())
    }

    /// Search for nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> VectorResult<Vec<(NodeId, f32)>> {
        if query.len() != self.dimensions {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimensions,
                got: query.len(),
            });
        }
        
        // ef_search drives recall (HNSW only returns from the ef candidate set), so
        // floor it for small k. But it must NOT exceed the number of indexed vectors
        // or hnsw_rs panics in search_layer (hnsw_rs 0.2.1 hnsw.rs:938); clamp both ef
        // and k into the index size. An empty index returns no neighbours rather than
        // searching a malformed graph.
        let n = self.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        // Below this size, search exactly. HNSW is an approximation that earns its keep at
        // scale; on a tiny index its randomised layer assignment can produce a graph whose
        // search returns NOTHING for a vector that is present — observed on a 1-vector
        // index roughly one run in six, silently (#382). A linear scan over a handful of
        // vectors costs microseconds and is always right, so there is nothing to trade.
        const EXACT_SEARCH_MAX: usize = 128;
        if n <= EXACT_SEARCH_MAX {
            return Ok(self.brute_force_search(query, k.min(n)));
        }

        let ef_search = (k * 2).max(64).min(n);
        // hnsw_rs 0.2.1 can panic deep in search_layer (hnsw.rs:938,
        // `return_points.peek().unwrap()`) on certain graphs. A panic here would
        // unwind across the await point and take the whole server down, so a single
        // HTTP search must never be able to crash the process — contain it and
        // surface a clean error instead.
        let results = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            match &self.backend {
                Backend::F32(h) => h.search(query, k.min(n), ef_search),
                // The query is quantized to the stored precision before the
                // search, so the graph is walked with the same arithmetic it
                // was built with.
                Backend::F16(h) => {
                    let q: Vec<f16> = query.iter().map(|v| f16::from_f32(*v)).collect();
                    h.search(&q, k.min(n), ef_search)
                }
            }
        })) {
            Ok(r) => r,
            Err(_) => {
                // hnsw_rs 0.2.1 panics in multi-layer search_layer for some graphs
                // (large indexes). Rather than crash, fall back to an exact linear
                // scan — O(n·dim), trivial for typical indexes and always correct.
                eprintln!(
                    "[vector] HNSW search panicked on {}-vector index; using exact brute-force fallback",
                    n
                );
                return Ok(self.brute_force_search(query, k.min(n)));
            }
        };

        let mut neighbors = Vec::new();
        for res in results {
            neighbors.push((NodeId::new(res.d_id as u64), res.distance));
        }

        // An empty result from a non-empty index is not an answer, it is a failure of the
        // approximation — and unlike a panic it is silent. Fall back rather than report
        // "no matches" for data that is present.
        if neighbors.is_empty() {
            eprintln!(
                "[vector] HNSW returned no neighbours from a {n}-vector index; using exact fallback"
            );
            return Ok(self.brute_force_search(query, k.min(n)));
        }

        Ok(neighbors)
    }

    /// Every stored vector, at f32, whichever precision the index holds.
    /// A quantized value widens back exactly -- f16 to f32 is lossless -- so
    /// this is the original value only when the index is unquantized, and the
    /// rounded one otherwise. That is what was stored, and what a reload must
    /// reproduce.
    fn iter_stored(&self) -> impl Iterator<Item = StoredVector> + '_ {
        self.stored_f32.iter().cloned().chain(
            self.stored_f16.iter().map(|(id, v)| StoredVector {
                node_id: *id,
                vector: v.iter().map(|x| x.to_f32()).collect(),
            }),
        )
    }

    /// Exact nearest-neighbour search by linear scan over stored vectors.
    /// Used as a fallback when the HNSW index search panics. The index uses
    /// cosine distance, so this matches it; non-finite distances are skipped.
    fn brute_force_search(&self, query: &[f32], k: usize) -> Vec<(NodeId, f32)> {
        // The query is quantized to match what is stored, so the exact search
        // answers the same question the graph does. Comparing a full-precision
        // query against quantized vectors would make the fallback disagree with
        // the index it is standing in for, which is worse than either.
        let mut scored: Vec<(NodeId, f32)> = match self.quantization {
            Quantization::None => self
                .stored_f32
                .iter()
                .map(|sv| (NodeId::new(sv.node_id), CosineDistance.eval(query, &sv.vector)))
                .collect(),
            Quantization::Fp16 => {
                let q: Vec<f16> = query.iter().map(|v| f16::from_f32(*v)).collect();
                self.stored_f16
                    .iter()
                    .map(|(id, v)| (NodeId::new(*id), CosineDistanceF16.eval(&q, v)))
                    .collect()
            }
        };
        let mut scored: Vec<(NodeId, f32)> =
            scored.drain(..).filter(|(_, d)| d.is_finite()).collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    /// Get dimensions
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Get metric
    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    /// Get count of stored vectors
    pub fn len(&self) -> usize {
        self.stored_f32.len() + self.stored_f16.len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Save index to disk by serializing stored vectors via bincode.
    /// On load, vectors are re-inserted into a fresh HNSW index.
    pub fn dump(&self, path: &std::path::Path) -> VectorResult<()> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        // Always f32 on disk, whatever the index holds. Quantization is a
        // runtime memory choice, not a file format: a snapshot written by a
        // quantized index has to load into an unquantized one and the reverse.
        let as_f32: Vec<StoredVector> = self.iter_stored().collect();
        bincode::serialize_into(writer, &as_f32)
            .map_err(|e| VectorError::IndexError(format!("serialization error: {}", e)))?;
        Ok(())
    }

    /// Load index from disk: deserialize stored vectors and re-insert into HNSW.
    pub fn load(
        path: &std::path::Path,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> VectorResult<Self> {
        Self::load_with_quantization(path, dimensions, metric, Quantization::None)
    }

    /// Load, re-quantizing to the requested precision (NDS-09).
    ///
    /// The file is f32 either way, so this is the only place the choice is
    /// made: an index reloaded as `Fp16` re-rounds on the way in, and one
    /// reloaded as `None` from a file a quantized index wrote gets the rounded
    /// values back at full width. Neither is a migration.
    pub fn load_with_quantization(
        path: &std::path::Path,
        dimensions: usize,
        metric: DistanceMetric,
        quantization: Quantization,
    ) -> VectorResult<Self> {
        if !path.exists() {
            return Ok(Self::with_quantization(dimensions, metric, quantization));
        }
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let stored_vectors: Vec<StoredVector> = bincode::deserialize_from(reader)
            .map_err(|e| VectorError::IndexError(format!("deserialization error: {}", e)))?;

        let max_elements = (stored_vectors.len() + 10_000).max(100_000);
        let mut index = Self::build(dimensions, metric, quantization, max_elements);
        for sv in &stored_vectors {
            index.add(NodeId::new(sv.node_id), &sv.vector)?;
        }
        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_index_basic() {
        let mut index = VectorIndex::new(3, DistanceMetric::Cosine);
        
        // Add some vectors
        index.add(NodeId::new(1), &vec![1.0, 0.0, 0.0]).unwrap();
        index.add(NodeId::new(2), &vec![0.0, 1.0, 0.0]).unwrap();
        index.add(NodeId::new(3), &vec![0.0, 0.1, 0.9]).unwrap();
        
        // Search — HNSW is approximate and may return fewer than k results on very small graphs
        let results = index.search(&[1.0, 0.1, 0.0], 2).unwrap();
        assert!(results.len() >= 1 && results.len() <= 2);
        assert_eq!(results[0].0, NodeId::new(1));
    }

    #[test]
    fn test_vector_index_persistence() {
        let dir = tempfile::TempDir::new().unwrap();
        let dump_path = dir.path().join("test_vectors.bin");

        // Create and populate index
        let mut index = VectorIndex::new(3, DistanceMetric::Cosine);
        index.add(NodeId::new(1), &vec![1.0, 0.0, 0.0]).unwrap();
        index.add(NodeId::new(2), &vec![0.0, 1.0, 0.0]).unwrap();
        index.add(NodeId::new(3), &vec![0.0, 0.1, 0.9]).unwrap();
        assert_eq!(index.len(), 3);

        // Dump to disk
        index.dump(&dump_path).unwrap();

        // Load from disk
        let loaded = VectorIndex::load(&dump_path, 3, DistanceMetric::Cosine).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.dimensions(), 3);

        // Verify search still works after reload
        let results = loaded.search(&[1.0, 0.1, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, NodeId::new(1));
    }

    #[test]
    fn test_distance_metrics() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![0.0, 1.0];
        let v3 = vec![1.0, 1.0]; // Not normalized

        let cosine = CosineDistance;
        // Orthogonal
        assert!((cosine.eval(&v1, &v2) - 1.0).abs() < 1e-6); 
        // Same
        assert!((cosine.eval(&v1, &v1) - 0.0).abs() < 1e-6);
        
        let inner = InnerProductDistance;
        // Dot product = 0
        assert!((inner.eval(&v1, &v2) - 1.0).abs() < 1e-6); // 1.0 - 0.0
    }
}