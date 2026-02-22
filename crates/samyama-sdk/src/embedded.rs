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
        || upper.contains(" CREATE ")
        || upper.contains(" DELETE ")
        || upper.contains(" SET ")
        || upper.contains(" MERGE ")
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
}
