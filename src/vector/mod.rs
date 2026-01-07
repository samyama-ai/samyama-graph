//! Vector search and AI integration (Phase 6)
//!
//! This module provides support for vector embeddings and
//! approximate nearest neighbor search using HNSW.

pub mod index;
pub mod manager;

pub use index::{VectorIndex, DistanceMetric, VectorError, VectorResult};
pub use manager::{VectorIndexManager, IndexKey};
