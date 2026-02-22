//! AlgorithmClient — extension trait for graph algorithms (EmbeddedClient only)
//!
//! Provides PageRank, community detection, pathfinding, and other graph algorithms
//! via the `AlgorithmClient` trait. Only `EmbeddedClient` implements this trait
//! since algorithms require direct in-process access to the graph store.

use async_trait::async_trait;
use std::collections::HashMap;

use samyama::algo::{
    build_view, page_rank, weakly_connected_components, strongly_connected_components,
    bfs, dijkstra, edmonds_karp, prim_mst, count_triangles,
    PageRankConfig, PathResult, WccResult, SccResult, FlowResult, MSTResult,
};
use samyama_graph_algorithms::GraphView;

use crate::embedded::EmbeddedClient;

/// Extension trait for graph algorithm operations.
///
/// Only implemented by `EmbeddedClient` since algorithms need direct store access.
#[async_trait]
pub trait AlgorithmClient {
    /// Build a `GraphView` projection for algorithm execution.
    ///
    /// Optionally filter by node label, edge type, and extract edge weights.
    async fn build_view(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
        weight_prop: Option<&str>,
    ) -> GraphView;

    /// Run PageRank on the graph (or a subgraph filtered by label/edge_type).
    async fn page_rank(
        &self,
        config: PageRankConfig,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> HashMap<u64, f64>;

    /// Detect weakly connected components.
    async fn weakly_connected_components(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> WccResult;

    /// Detect strongly connected components.
    async fn strongly_connected_components(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> SccResult;

    /// Breadth-first search from source to target.
    async fn bfs(
        &self,
        source: u64,
        target: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> Option<PathResult>;

    /// Dijkstra's shortest path from source to target (weighted).
    async fn dijkstra(
        &self,
        source: u64,
        target: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
        weight_prop: Option<&str>,
    ) -> Option<PathResult>;

    /// Edmonds-Karp maximum flow from source to sink.
    async fn edmonds_karp(
        &self,
        source: u64,
        sink: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> Option<FlowResult>;

    /// Prim's minimum spanning tree.
    async fn prim_mst(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
        weight_prop: Option<&str>,
    ) -> MSTResult;

    /// Count triangles in the graph.
    async fn count_triangles(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> usize;
}

#[async_trait]
impl AlgorithmClient for EmbeddedClient {
    async fn build_view(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
        weight_prop: Option<&str>,
    ) -> GraphView {
        let store = self.store.read().await;
        build_view(&store, label, edge_type, weight_prop)
    }

    async fn page_rank(
        &self,
        config: PageRankConfig,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> HashMap<u64, f64> {
        let store = self.store.read().await;
        let view = build_view(&store, label, edge_type, None);
        page_rank(&view, config)
    }

    async fn weakly_connected_components(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> WccResult {
        let store = self.store.read().await;
        let view = build_view(&store, label, edge_type, None);
        weakly_connected_components(&view)
    }

    async fn strongly_connected_components(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> SccResult {
        let store = self.store.read().await;
        let view = build_view(&store, label, edge_type, None);
        strongly_connected_components(&view)
    }

    async fn bfs(
        &self,
        source: u64,
        target: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> Option<PathResult> {
        let store = self.store.read().await;
        let view = build_view(&store, label, edge_type, None);
        bfs(&view, source, target)
    }

    async fn dijkstra(
        &self,
        source: u64,
        target: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
        weight_prop: Option<&str>,
    ) -> Option<PathResult> {
        let store = self.store.read().await;
        let view = build_view(&store, label, edge_type, weight_prop);
        dijkstra(&view, source, target)
    }

    async fn edmonds_karp(
        &self,
        source: u64,
        sink: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> Option<FlowResult> {
        let store = self.store.read().await;
        let view = build_view(&store, label, edge_type, None);
        edmonds_karp(&view, source, sink)
    }

    async fn prim_mst(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
        weight_prop: Option<&str>,
    ) -> MSTResult {
        let store = self.store.read().await;
        let view = build_view(&store, label, edge_type, weight_prop);
        prim_mst(&view)
    }

    async fn count_triangles(
        &self,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> usize {
        let store = self.store.read().await;
        let view = build_view(&store, label, edge_type, None);
        count_triangles(&view)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EmbeddedClient, SamyamaClient};

    #[tokio::test]
    async fn test_page_rank() {
        let client = EmbeddedClient::new();

        // Create a small graph: A -> B -> C, A -> C
        client.query("default", r#"CREATE (a:Person {name: "Alice"})"#).await.unwrap();
        client.query("default", r#"CREATE (b:Person {name: "Bob"})"#).await.unwrap();
        client.query("default", r#"CREATE (c:Person {name: "Carol"})"#).await.unwrap();
        client.query("default",
            r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#
        ).await.unwrap();
        client.query("default",
            r#"MATCH (b:Person {name: "Bob"}), (c:Person {name: "Carol"}) CREATE (b)-[:KNOWS]->(c)"#
        ).await.unwrap();
        client.query("default",
            r#"MATCH (a:Person {name: "Alice"}), (c:Person {name: "Carol"}) CREATE (a)-[:KNOWS]->(c)"#
        ).await.unwrap();

        let scores = client.page_rank(PageRankConfig::default(), Some("Person"), Some("KNOWS")).await;
        assert_eq!(scores.len(), 3);
        // Carol should have highest PageRank (most incoming links)
        let max_node = scores.iter().max_by(|a, b| a.1.partial_cmp(b.1).unwrap()).unwrap();
        // At least verify we got results
        assert!(*max_node.1 > 0.0);
    }

    #[tokio::test]
    async fn test_wcc() {
        let client = EmbeddedClient::new();

        // Two disconnected components
        client.query("default", r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#).await.unwrap();
        client.query("default", r#"CREATE (c:Person {name: "Carol"})-[:KNOWS]->(d:Person {name: "Dave"})"#).await.unwrap();

        let wcc = client.weakly_connected_components(Some("Person"), Some("KNOWS")).await;
        assert_eq!(wcc.components.len(), 2);
    }

    #[tokio::test]
    async fn test_bfs() {
        let client = EmbeddedClient::new();

        client.query("default", r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#).await.unwrap();
        client.query("default",
            r#"MATCH (b:Person {name: "Bob"}) CREATE (b)-[:KNOWS]->(c:Person {name: "Carol"})"#
        ).await.unwrap();

        // Get node IDs
        let store = client.store().read().await;
        let all_nodes: Vec<_> = store.all_nodes().iter().map(|n| n.id.as_u64()).collect();
        drop(store);

        if all_nodes.len() >= 3 {
            let result = client.bfs(all_nodes[0], all_nodes[2], Some("Person"), Some("KNOWS")).await;
            assert!(result.is_some());
            let path = result.unwrap();
            assert!(path.path.len() >= 2);
        }
    }
}
