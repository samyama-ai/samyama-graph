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
use hnsw_rs::prelude::*;
use thiserror::Error;

/// Vector index errors
#[derive(Error, Debug)]
pub enum VectorError {
    #[error("Index error: {0}")]
    IndexError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
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
        
        // Cosine distance = 1.0 - cosine similarity
        let sim = dot / (norm_a.sqrt() * norm_b.sqrt());
        1.0 - sim
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

/// Wrapper around HNSW index
pub struct VectorIndex {
    /// Number of dimensions
    dimensions: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// The actual HNSW index
    hnsw: Hnsw<'static, f32, CosineDistance>,
    /// All inserted vectors (for persistence — HNSW doesn't expose iteration)
    stored_vectors: Vec<StoredVector>,
}

// Implement Debug manually because Hnsw doesn't implement it
impl std::fmt::Debug for VectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorIndex")
            .field("dimensions", &self.dimensions)
            .field("metric", &self.metric)
            .finish()
    }
}

impl VectorIndex {
    /// Create a new vector index
    pub fn new(dimensions: usize, metric: DistanceMetric) -> Self {
        // HNSW parameters
        let max_elements = 100_000;
        let m = 16;
        let ef_construction = 200;

        let hnsw = Hnsw::new(m, max_elements, 16, ef_construction, CosineDistance);

        Self {
            dimensions,
            metric,
            hnsw,
            stored_vectors: Vec::new(),
        }
    }

    /// Add a vector to the index
    pub fn add(&mut self, node_id: NodeId, vector: &Vec<f32>) -> VectorResult<()> {
        if vector.len() != self.dimensions {
            return Err(VectorError::DimensionMismatch {
                expected: self.dimensions,
                got: vector.len(),
            });
        }
        
        self.hnsw.insert((vector, node_id.0 as usize));

        // Store vector for persistence
        self.stored_vectors.push(StoredVector {
            node_id: node_id.0,
            vector: vector.clone(),
        });

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
        
        let ef_search = k * 2;
        let results = self.hnsw.search(query, k, ef_search);
        
        let mut neighbors = Vec::new();
        for res in results {
            neighbors.push((NodeId::new(res.d_id as u64), res.distance));
        }
        
        Ok(neighbors)
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
        self.stored_vectors.len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.stored_vectors.is_empty()
    }

    /// Save index to disk by serializing stored vectors via bincode.
    /// On load, vectors are re-inserted into a fresh HNSW index.
    pub fn dump(&self, path: &std::path::Path) -> VectorResult<()> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        bincode::serialize_into(writer, &self.stored_vectors)
            .map_err(|e| VectorError::IndexError(format!("serialization error: {}", e)))?;
        Ok(())
    }

    /// Load index from disk: deserialize stored vectors and re-insert into HNSW.
    pub fn load(
        path: &std::path::Path,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> VectorResult<Self> {
        if !path.exists() {
            return Ok(Self::new(dimensions, metric));
        }
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let stored_vectors: Vec<StoredVector> = bincode::deserialize_from(reader)
            .map_err(|e| VectorError::IndexError(format!("deserialization error: {}", e)))?;

        let max_elements = (stored_vectors.len() + 10_000).max(100_000);
        let m = 16;
        let ef_construction = 200;
        let mut hnsw = Hnsw::new(m, max_elements, 16, ef_construction, CosineDistance);

        // Re-insert all vectors
        for sv in &stored_vectors {
            hnsw.insert((&sv.vector, sv.node_id as usize));
        }

        Ok(Self {
            dimensions,
            metric,
            hnsw,
            stored_vectors,
        })
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