//! EmbeddedClient — in-process graph database client
//!
//! Uses GraphStore and QueryEngine directly, no network needed.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

use samyama::graph::GraphStore;
use samyama::query::{QueryEngine, Value, RecordBatch};

use crate::client::SamyamaClient;
use crate::error::{SamyamaError, SamyamaResult};
use crate::models::{QueryResult, SdkNode, SdkEdge, ServerStatus, StorageStats};

/// In-process client that wraps a GraphStore directly.
///
/// No network overhead — queries execute in the same process.
/// Ideal for examples, tests, and embedded applications.
pub struct EmbeddedClient {
    pub(crate) store: Arc<RwLock<GraphStore>>,
    engine: QueryEngine,
}

impl EmbeddedClient {
    /// Create a new EmbeddedClient with a fresh empty graph store
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: QueryEngine::new(),
        }
    }

    /// Create an EmbeddedClient wrapping an existing store
    pub fn with_store(store: Arc<RwLock<GraphStore>>) -> Self {
        Self {
            store,
            engine: QueryEngine::new(),
        }
    }

    /// Get a reference to the underlying store (for direct graph manipulation)
    pub fn store(&self) -> &Arc<RwLock<GraphStore>> {
        &self.store
    }

    /// Acquire a read lock on the store.
    ///
    /// Use for direct read-only access to graph data (node/edge lookups, iterations).
    pub async fn store_read(&self) -> tokio::sync::RwLockReadGuard<'_, GraphStore> {
        self.store.read().await
    }

    /// Acquire a write lock on the store.
    ///
    /// Use for direct mutation (create_node, set_property, etc.).
    pub async fn store_write(&self) -> tokio::sync::RwLockWriteGuard<'_, GraphStore> {
        self.store.write().await
    }

    /// Create an NLQ pipeline for natural language → Cypher translation.
    pub fn nlq_pipeline(
        &self,
        config: samyama::persistence::tenant::NLQConfig,
    ) -> Result<samyama::NLQPipeline, samyama::NLQError> {
        samyama::NLQPipeline::new(config)
    }

    /// Create an agent runtime for agentic enrichment workflows.
    pub fn agent_runtime(
        &self,
        config: samyama::persistence::tenant::AgentConfig,
    ) -> samyama::agent::AgentRuntime {
        samyama::agent::AgentRuntime::new(config)
    }

    /// Create a persistence manager for durable storage.
    pub fn persistence_manager(
        &self,
        base_path: impl AsRef<std::path::Path>,
    ) -> Result<samyama::PersistenceManager, samyama::PersistenceError> {
        samyama::PersistenceManager::new(base_path)
    }

    /// Create a tenant manager for multi-tenancy.
    pub fn tenant_manager(&self) -> samyama::TenantManager {
        samyama::TenantManager::new()
    }

    /// Return AST cache statistics (hits, misses).
    pub fn cache_stats(&self) -> &samyama::query::CacheStats {
        self.engine.cache_stats()
    }

    /// Export a snapshot of the current graph store to a file.
    pub async fn export_snapshot(
        &self,
        _tenant: &str,
        path: &std::path::Path,
    ) -> Result<samyama::snapshot::format::ExportStats, Box<dyn std::error::Error>> {
        let store_guard = self.store.read().await;
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        let stats = samyama::snapshot::export_tenant(&store_guard, writer)?;
        Ok(stats)
    }

    /// Import a snapshot into the current graph store from a file.
    pub async fn import_snapshot(
        &self,
        _tenant: &str,
        path: &std::path::Path,
    ) -> Result<samyama::snapshot::format::ImportStats, Box<dyn std::error::Error>> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut store_guard = self.store.write().await;
        let stats = samyama::snapshot::import_tenant(&mut store_guard, reader)?;
        Ok(stats)
    }

    /// Import a snapshot with entity deduplication.
    ///
    /// `dedup_keys` specifies which node properties should be used to detect
    /// duplicates across snapshots. For example, `&["iso_code", "drugbank_id"]`
    /// will merge Country and Drug nodes that share the same key value.
    pub async fn import_snapshot_dedup(
        &self,
        _tenant: &str,
        path: &std::path::Path,
        dedup_keys: &[&str],
    ) -> Result<samyama::snapshot::format::ImportStats, Box<dyn std::error::Error>> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut store_guard = self.store.write().await;
        let stats = samyama::snapshot::import_tenant_with_dedup(&mut store_guard, reader, dedup_keys)?;
        Ok(stats)
    }
}

impl Default for EmbeddedClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a RecordBatch from the query engine into an SDK QueryResult.
fn record_batch_to_query_result(batch: &RecordBatch, store: &GraphStore) -> QueryResult {
    let mut nodes_map: HashMap<String, SdkNode> = HashMap::new();
    let mut edges_map: HashMap<String, SdkEdge> = HashMap::new();
    let mut records = Vec::new();

    for record in &batch.records {
        let mut row = Vec::new();
        for col in &batch.columns {
            let val = match record.get(col) {
                Some(v) => v,
                None => {
                    row.push(serde_json::Value::Null);
                    continue;
                }
            };

            match val {
                Value::Node(id, node) => {
                    let mut properties = serde_json::Map::new();
                    for (k, v) in &node.properties {
                        properties.insert(k.clone(), v.to_json());
                    }
                    let id_str = id.as_u64().to_string();
                    let labels: Vec<String> = node.labels.iter().map(|l| l.as_str().to_string()).collect();

                    let node_json = serde_json::json!({
                        "id": id_str,
                        "labels": labels,
                        "properties": properties,
                    });

                    nodes_map.entry(id_str.clone()).or_insert_with(|| SdkNode {
                        id: id_str,
                        labels,
                        properties: properties.into_iter().collect(),
                    });

                    row.push(node_json);
                }
                Value::NodeRef(id) => {
                    let id_str = id.as_u64().to_string();
                    // Try to resolve from store
                    let (labels, properties, node_json) = if let Some(node) = store.get_node(*id) {
                        let mut props = serde_json::Map::new();
                        for (k, v) in &node.properties {
                            props.insert(k.clone(), v.to_json());
                        }
                        let lbls: Vec<String> = node.labels.iter().map(|l| l.as_str().to_string()).collect();
                        let json = serde_json::json!({
                            "id": id_str,
                            "labels": lbls,
                            "properties": props,
                        });
                        (lbls, props.into_iter().collect(), json)
                    } else {
                        let json = serde_json::json!({ "id": id_str, "labels": [], "properties": {} });
                        (vec![], HashMap::new(), json)
                    };

                    nodes_map.entry(id_str.clone()).or_insert_with(|| SdkNode {
                        id: id_str,
                        labels,
                        properties,
                    });

                    row.push(node_json);
                }
                Value::Edge(id, edge) => {
                    let mut properties = serde_json::Map::new();
                    for (k, v) in &edge.properties {
                        properties.insert(k.clone(), v.to_json());
                    }
                    let id_str = id.as_u64().to_string();
                    let edge_json = serde_json::json!({
                        "id": id_str,
                        "source": edge.source.as_u64().to_string(),
                        "target": edge.target.as_u64().to_string(),
                        "type": edge.edge_type.as_str(),
                        "properties": properties,
                    });

                    edges_map.entry(id_str.clone()).or_insert_with(|| SdkEdge {
                        id: id_str,
                        source: edge.source.as_u64().to_string(),
                        target: edge.target.as_u64().to_string(),
                        edge_type: edge.edge_type.as_str().to_string(),
                        properties: properties.into_iter().collect(),
                    });

                    row.push(edge_json);
                }
                Value::EdgeRef(id, src, tgt, et) => {
                    let id_str = id.as_u64().to_string();
                    let edge_json = serde_json::json!({
                        "id": id_str,
                        "source": src.as_u64().to_string(),
                        "target": tgt.as_u64().to_string(),
                        "type": et.as_str(),
                        "properties": {},
                    });

                    edges_map.entry(id_str.clone()).or_insert_with(|| SdkEdge {
                        id: id_str,
                        source: src.as_u64().to_string(),
                        target: tgt.as_u64().to_string(),
                        edge_type: et.as_str().to_string(),
                        properties: HashMap::new(),
                    });

                    row.push(edge_json);
                }
                Value::Property(p) => {
                    row.push(p.to_json());
                }
                Value::Path { nodes: path_nodes, edges: path_edges } => {
                    row.push(serde_json::json!({
                        "nodes": path_nodes.iter().map(|n| n.as_u64().to_string()).collect::<Vec<_>>(),
                        "edges": path_edges.iter().map(|e| e.as_u64().to_string()).collect::<Vec<_>>(),
                        "length": path_edges.len(),
                    }));
                }
                Value::Null => {
                    row.push(serde_json::Value::Null);
                }
            }
        }
        records.push(row);
    }

    QueryResult {
        nodes: nodes_map.into_values().collect(),
        edges: edges_map.into_values().collect(),
        columns: batch.columns.clone(),
        records,
    }
}

fn is_write_query(cypher: &str) -> bool {
    let upper = cypher.trim().to_uppercase();
    upper.starts_with("CREATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("SET")
        || upper.starts_with("MERGE")
        || upper.starts_with("CALL")
        || upper.contains(" CREATE ")
        || upper.contains(" DELETE ")
        || upper.contains(" SET ")
        || upper.contains(" MERGE ")
        || upper.contains(" CALL ")
}

#[async_trait]
impl SamyamaClient for EmbeddedClient {
    async fn query(&self, graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        if is_write_query(cypher) {
            let mut store_guard = self.store.write().await;
            let batch = self.engine.execute_mut(cypher, &mut *store_guard, graph)
                .map_err(|e| SamyamaError::QueryError(e.to_string()))?;
            Ok(record_batch_to_query_result(&batch, &*store_guard))
        } else {
            let store_guard = self.store.read().await;
            let batch = self.engine.execute(cypher, &*store_guard)
                .map_err(|e| SamyamaError::QueryError(e.to_string()))?;
            Ok(record_batch_to_query_result(&batch, &*store_guard))
        }
    }

    async fn query_readonly(&self, _graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        let store_guard = self.store.read().await;
        let batch = self.engine.execute(cypher, &*store_guard)
            .map_err(|e| SamyamaError::QueryError(e.to_string()))?;
        Ok(record_batch_to_query_result(&batch, &*store_guard))
    }

    async fn delete_graph(&self, _graph: &str) -> SamyamaResult<()> {
        let mut store_guard = self.store.write().await;
        store_guard.clear();
        Ok(())
    }

    async fn list_graphs(&self) -> SamyamaResult<Vec<String>> {
        Ok(vec!["default".to_string()])
    }

    async fn status(&self) -> SamyamaResult<ServerStatus> {
        let store_guard = self.store.read().await;
        Ok(ServerStatus {
            status: "healthy".to_string(),
            version: samyama::VERSION.to_string(),
            storage: StorageStats {
                nodes: store_guard.node_count() as u64,
                edges: store_guard.edge_count() as u64,
            },
        })
    }

    async fn ping(&self) -> SamyamaResult<String> {
        Ok("PONG".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_embedded_ping() {
        let client = EmbeddedClient::new();
        let result = client.ping().await.unwrap();
        assert_eq!(result, "PONG");
    }

    #[tokio::test]
    async fn test_embedded_status() {
        let client = EmbeddedClient::new();
        let status = client.status().await.unwrap();
        assert_eq!(status.status, "healthy");
        assert_eq!(status.storage.nodes, 0);
    }

    #[tokio::test]
    async fn test_embedded_create_and_query() {
        let client = EmbeddedClient::new();

        // Create nodes
        client.query("default", r#"CREATE (n:Person {name: "Alice", age: 30})"#)
            .await.unwrap();
        client.query("default", r#"CREATE (n:Person {name: "Bob", age: 25})"#)
            .await.unwrap();

        // Query
        let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name, n.age")
            .await.unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.records.len(), 2);

        // Status should reflect 2 nodes
        let status = client.status().await.unwrap();
        assert_eq!(status.storage.nodes, 2);
    }

    #[tokio::test]
    async fn test_embedded_delete_graph() {
        let client = EmbeddedClient::new();

        client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
            .await.unwrap();

        let status = client.status().await.unwrap();
        assert_eq!(status.storage.nodes, 1);

        client.delete_graph("default").await.unwrap();

        let status = client.status().await.unwrap();
        assert_eq!(status.storage.nodes, 0);
    }

    #[tokio::test]
    async fn test_embedded_list_graphs() {
        let client = EmbeddedClient::new();
        let graphs = client.list_graphs().await.unwrap();
        assert_eq!(graphs, vec!["default"]);
    }

    #[tokio::test]
    async fn test_embedded_query_with_edges() {
        let client = EmbeddedClient::new();

        client.query("default",
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#
        ).await.unwrap();

        let result = client.query_readonly("default",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
        ).await.unwrap();

        assert_eq!(result.records.len(), 1);
    }

    #[tokio::test]
    async fn test_embedded_with_existing_store() {
        let mut store = GraphStore::new();
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let store = Arc::new(RwLock::new(store));
        let client = EmbeddedClient::with_store(store);

        let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name")
            .await.unwrap();
        assert_eq!(result.records.len(), 1);
    }

    // ========== Additional Embedded Client Coverage Tests ==========

    #[test]
    fn test_embedded_default() {
        let client = EmbeddedClient::default();
        // Default should produce a valid, empty client
        let store = client.store();
        assert!(Arc::strong_count(store) >= 1);
    }

    #[tokio::test]
    async fn test_embedded_store_read() {
        let client = EmbeddedClient::new();
        client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
            .await.unwrap();

        let guard = client.store_read().await;
        assert_eq!(guard.node_count(), 1);
    }

    #[tokio::test]
    async fn test_embedded_store_write() {
        let client = EmbeddedClient::new();
        {
            let mut guard = client.store_write().await;
            let id = guard.create_node("Person");
            if let Some(node) = guard.get_node_mut(id) {
                node.set_property("name", "DirectWrite");
            }
        }

        let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name")
            .await.unwrap();
        assert_eq!(result.records.len(), 1);
    }

    #[tokio::test]
    async fn test_embedded_tenant_manager() {
        let client = EmbeddedClient::new();
        let tm = client.tenant_manager();
        let tenants = tm.list_tenants();
        // Default tenant should exist
        assert!(!tenants.is_empty());
        assert!(tm.is_tenant_enabled("default"));
    }

    #[tokio::test]
    async fn test_embedded_cache_stats() {
        let client = EmbeddedClient::new();
        let stats = client.cache_stats();
        // Initially no queries, so hits should be 0
        assert_eq!(stats.hits(), 0);
    }

    #[tokio::test]
    async fn test_embedded_cache_stats_after_queries() {
        let client = EmbeddedClient::new();
        client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
            .await.unwrap();
        // Same query twice should potentially hit cache
        client.query_readonly("default", "MATCH (n:Person) RETURN n.name")
            .await.unwrap();
        client.query_readonly("default", "MATCH (n:Person) RETURN n.name")
            .await.unwrap();

        let stats = client.cache_stats();
        // At least one miss for the first time, then a hit
        assert!(stats.hits() + stats.misses() >= 2);
    }

    #[tokio::test]
    async fn test_embedded_query_readonly_error() {
        let client = EmbeddedClient::new();
        // Invalid Cypher syntax should produce an error
        let result = client.query_readonly("default", "INVALID SYNTAX !!!").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_embedded_query_write_error() {
        let client = EmbeddedClient::new();
        // Invalid write query should produce an error
        let result = client.query("default", "CREATE INVALID").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_embedded_version_in_status() {
        let client = EmbeddedClient::new();
        let status = client.status().await.unwrap();
        // Version should be non-empty
        assert!(!status.version.is_empty());
    }

    #[tokio::test]
    async fn test_embedded_query_returns_nodes() {
        let client = EmbeddedClient::new();
        client.query("default", r#"CREATE (n:Person {name: "Alice", age: 30})"#)
            .await.unwrap();

        let result = client.query_readonly("default", "MATCH (n:Person) RETURN n")
            .await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert!(!result.nodes.is_empty());
        // Check that the node has properties
        let node = &result.nodes[0];
        assert!(node.labels.contains(&"Person".to_string()));
    }

    #[tokio::test]
    async fn test_embedded_query_returns_edges() {
        let client = EmbeddedClient::new();
        client.query("default",
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS {since: 2020}]->(b:Person {name: "Bob"})"#
        ).await.unwrap();

        let result = client.query_readonly("default",
            "MATCH (a)-[r:KNOWS]->(b) RETURN r"
        ).await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert!(!result.edges.is_empty());
        let edge = &result.edges[0];
        assert_eq!(edge.edge_type, "KNOWS");
    }

    #[tokio::test]
    async fn test_embedded_query_returns_null() {
        let client = EmbeddedClient::new();
        client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
            .await.unwrap();

        // Query for a property that does not exist
        let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.missing")
            .await.unwrap();
        assert_eq!(result.records.len(), 1);
        // The value should be JSON null
        assert_eq!(result.records[0][0], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_embedded_multiple_writes_and_reads() {
        let client = EmbeddedClient::new();

        for i in 0..5 {
            client.query("default",
                &format!(r#"CREATE (n:Item {{id: {}}})"#, i)
            ).await.unwrap();
        }

        let result = client.query_readonly("default", "MATCH (n:Item) RETURN n.id")
            .await.unwrap();
        assert_eq!(result.records.len(), 5);
    }

    #[tokio::test]
    async fn test_embedded_delete_graph_and_recreate() {
        let client = EmbeddedClient::new();

        client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
            .await.unwrap();
        assert_eq!(client.status().await.unwrap().storage.nodes, 1);

        client.delete_graph("default").await.unwrap();
        assert_eq!(client.status().await.unwrap().storage.nodes, 0);

        // Recreate
        client.query("default", r#"CREATE (n:Person {name: "Bob"})"#)
            .await.unwrap();
        assert_eq!(client.status().await.unwrap().storage.nodes, 1);
    }

    #[tokio::test]
    async fn test_embedded_with_store_shares_state() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let client = EmbeddedClient::with_store(Arc::clone(&store));

        client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
            .await.unwrap();

        // Store should reflect the changes made via client
        let guard = store.read().await;
        assert_eq!(guard.node_count(), 1);
    }

    #[test]
    fn test_is_write_query_variants() {
        assert!(is_write_query("CREATE (n:Person)"));
        assert!(is_write_query("DELETE n"));
        assert!(is_write_query("SET n.name = 'x'"));
        assert!(is_write_query("MERGE (n:Person)"));
        assert!(is_write_query("CALL db.something()"));
        assert!(is_write_query("MATCH (n) CREATE (m)"));
        assert!(is_write_query("MATCH (n) DELETE n"));
        assert!(is_write_query("MATCH (n) SET n.x = 1"));
        assert!(is_write_query("MATCH (n) MERGE (m)"));
        assert!(is_write_query("MATCH (n) CALL db.x()"));

        assert!(!is_write_query("MATCH (n) RETURN n"));
        assert!(!is_write_query("MATCH (n:Person) RETURN n.name"));
        assert!(!is_write_query("RETURN 1 + 2"));
    }

    #[tokio::test]
    async fn test_embedded_query_property_values() {
        let client = EmbeddedClient::new();
        client.query("default",
            r#"CREATE (n:Person {name: "Alice", age: 30, score: 95.5, active: true})"#
        ).await.unwrap();

        let result = client.query_readonly("default",
            "MATCH (n:Person) RETURN n.name, n.age, n.score, n.active"
        ).await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.columns.len(), 4);
    }

    #[tokio::test]
    async fn test_embedded_store_accessor() {
        let client = EmbeddedClient::new();
        let store_ref = client.store();
        // Should be able to clone the Arc
        let _cloned = Arc::clone(store_ref);
        assert!(Arc::strong_count(store_ref) >= 2);
    }
}
