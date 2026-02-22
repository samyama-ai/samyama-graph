//! VectorClient — extension trait for vector search operations (EmbeddedClient only)
//!
//! Provides vector index creation, insertion, and k-NN search via the
//! `VectorClient` trait. Only `EmbeddedClient` implements this trait
//! since vector operations require direct in-process access to the graph store.

use async_trait::async_trait;

use samyama::graph::NodeId;
use samyama::vector::DistanceMetric;

use crate::embedded::EmbeddedClient;
use crate::error::{SamyamaError, SamyamaResult};

/// Extension trait for vector search operations.
///
/// Only implemented by `EmbeddedClient` since vector ops need direct store access.
#[async_trait]
pub trait VectorClient {
    /// Create a vector index for a given label and property.
    async fn create_vector_index(
        &self,
        label: &str,
        property: &str,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> SamyamaResult<()>;

    /// Add a vector to the index for a given node.
    async fn add_vector(
        &self,
        label: &str,
        property: &str,
        node_id: NodeId,
        vector: &[f32],
    ) -> SamyamaResult<()>;

    /// Search for the k nearest neighbors to a query vector.
    async fn vector_search(
        &self,
        label: &str,
        property: &str,
        query_vec: &[f32],
        k: usize,
    ) -> SamyamaResult<Vec<(NodeId, f32)>>;
}

#[async_trait]
impl VectorClient for EmbeddedClient {
    async fn create_vector_index(
        &self,
        label: &str,
        property: &str,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> SamyamaResult<()> {
        let store = self.store.read().await;
        store.create_vector_index(label, property, dimensions, metric)
            .map_err(|e| SamyamaError::VectorError(e.to_string()))
    }

    async fn add_vector(
        &self,
        label: &str,
        property: &str,
        node_id: NodeId,
        vector: &[f32],
    ) -> SamyamaResult<()> {
        let store = self.store.read().await;
        store.vector_index
            .add_vector(label, property, node_id, &vector.to_vec())
            .map_err(|e| SamyamaError::VectorError(e.to_string()))
    }

    async fn vector_search(
        &self,
        label: &str,
        property: &str,
        query_vec: &[f32],
        k: usize,
    ) -> SamyamaResult<Vec<(NodeId, f32)>> {
        let store = self.store.read().await;
        store.vector_search(label, property, query_vec, k)
            .map_err(|e| SamyamaError::VectorError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EmbeddedClient, SamyamaClient};

    #[tokio::test]
    async fn test_vector_index_create_and_search() {
        let client = EmbeddedClient::new();

        // Create index
        client.create_vector_index("Doc", "embedding", 4, DistanceMetric::Cosine)
            .await.unwrap();

        // Create some nodes
        client.query("default", r#"CREATE (d:Doc {title: "Alpha"})"#).await.unwrap();
        client.query("default", r#"CREATE (d:Doc {title: "Beta"})"#).await.unwrap();

        // Add vectors
        let store = client.store().read().await;
        let nodes: Vec<_> = store.all_nodes().iter().map(|n| n.id).collect();
        drop(store);

        client.add_vector("Doc", "embedding", nodes[0], &[1.0, 0.0, 0.0, 0.0]).await.unwrap();
        client.add_vector("Doc", "embedding", nodes[1], &[0.0, 1.0, 0.0, 0.0]).await.unwrap();

        // Search
        let results = client.vector_search("Doc", "embedding", &[1.0, 0.1, 0.0, 0.0], 2).await.unwrap();
        assert_eq!(results.len(), 2);
        // First result should be closest to query
        assert_eq!(results[0].0, nodes[0]);
    }
}
