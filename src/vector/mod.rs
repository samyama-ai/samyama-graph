//! # Vector Search and AI Integration
//!
//! ## What are vector embeddings?
//!
//! Vector embeddings are dense numerical representations of data (text, images, graph
//! nodes) in high-dimensional space — typically 128 to 1536 dimensions. The key insight
//! is that **similar items have nearby vectors**: the sentence "happy dog" will be closer
//! to "joyful puppy" than to "stock market crash" in embedding space. This is the
//! foundation of modern AI search, recommendation systems, and retrieval-augmented
//! generation (RAG).
//!
//! ## Approximate Nearest Neighbor (ANN) search
//!
//! Given a query vector, we want to find the k closest vectors in a collection. Exact
//! (brute-force) search is O(n*d) where n = number of vectors and d = dimensions — too
//! slow for millions of vectors. ANN algorithms trade a small amount of accuracy (recall)
//! for dramatically faster search, often achieving 95%+ recall at 100x+ speedup.
//!
//! ## HNSW (Hierarchical Navigable Small World)
//!
//! This module uses HNSW, a graph-based ANN algorithm by Malkov & Yashunin (2018). It
//! builds a multi-layer proximity graph:
//! - **Upper layers** have few nodes with long-range connections for fast coarse navigation
//! - **Lower layers** have more nodes with short-range connections for precise search
//! - **Layer 0** contains all points
//!
//! Search starts at the top layer and greedily navigates to the nearest neighbor, then
//! descends to the next layer. This yields O(log n) search time with high recall.
//!
//! ## Distance metrics
//!
//! - **L2 (Euclidean)**: straight-line distance in space. Good for dense, normalized vectors.
//! - **Cosine**: measures the angle between vectors (direction only, ignores magnitude).
//!   `distance = 1 - cos(theta)`. Most common for text embeddings.
//! - **Inner Product (dot product)**: combines magnitude and direction. Used when vector
//!   norms carry meaning (e.g., popularity-weighted embeddings).
//!
//! ## Integration with graph queries
//!
//! Vector search is exposed as a Cypher procedure:
//! ```cypher
//! CALL db.index.vector.queryNodes('embedding_index', 10, [0.1, 0.2, ...])
//! YIELD node, score
//! RETURN node.name, score
//! ```
//! This enables hybrid queries combining graph traversal with semantic similarity.

pub mod index;
pub mod manager;

pub use index::{DistanceMetric, VectorError, VectorIndex, VectorResult};
pub use manager::{IndexKey, VectorIndexManager};
