//! Vector index implementation using HNSW
//!
//! This module provides a wrapper around the hnsw_rs library for
//! high-performance approximate nearest neighbor search.

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

/// Wrapper around HNSW index
pub struct VectorIndex {
    /// Number of dimensions
    dimensions: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// The actual HNSW index
    hnsw: Hnsw<'static, f32, CosineDistance>,
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

    /// Save index to disk
    pub fn dump(&self, _path: &std::path::Path) -> VectorResult<()> {
        // TODO: Implement actual serialization. 
        // hnsw_rs 0.2.1 Hnsw does not implement dump/load directly without specialized setup.
        Ok(())
    }

    /// Load index from disk
    pub fn load(
        _path: &std::path::Path,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> VectorResult<Self> {
        // TODO: Implement actual deserialization
        Ok(Self::new(dimensions, metric))
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
        
        // Search
        let results = index.search(&[1.0, 0.1, 0.0], 2).unwrap();
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