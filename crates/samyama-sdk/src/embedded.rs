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

/// The NLQ settings for one [`EmbeddedClient::nlq_with`] call (#2092).
///
/// Each field that is set takes the place of an environment variable, and
/// each one left `None` is read from the environment as before:
///
/// | field          | replaces                                        |
/// |----------------|-------------------------------------------------|
/// | `provider`     | `NLQ_PROVIDER`                                  |
/// | `model`        | `NLQ_MODEL` (default `gpt-4o`)                  |
/// | `api_key`      | the provider's own key variable, e.g. `OPENAI_API_KEY` or `GEMINI_API_KEY` |
/// | `api_base_url` | `NLQ_API_BASE_URL`                              |
///
/// The result is checked by the same reader the server uses
/// (`samyama::nlq::config_from`), so the two refuse the same things with the
/// same messages: no provider, an unknown one, one that is not implemented,
/// and one that needs a key it was not given.
#[derive(Clone, Debug, Default)]
pub struct NlqSettings {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub api_base_url: Option<String>,
}

impl NlqSettings {
    /// The [`samyama::NLQConfig`] these settings and the process environment
    /// describe.
    pub fn resolve(&self) -> Result<samyama::NLQConfig, String> {
        self.resolve_from(|var| std::env::var(var).ok())
    }

    /// As [`Self::resolve`], reading the environment through `env`.
    pub(crate) fn resolve_from(
        &self,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<samyama::NLQConfig, String> {
        let provider_name = self.provider.clone().or_else(|| env("NLQ_PROVIDER"));
        let provider = samyama::LLMProvider::parse(provider_name.as_deref().unwrap_or_default())?;
        let key_var = samyama::nlq::api_key_var(&provider);
        // A key given for a provider that takes none would be dropped
        // without a word, so it is refused: either the provider is not the
        // one the caller meant, or the key is going nowhere.
        if self.api_key.is_some() && key_var.is_none() {
            return Err(format!(
                "api_key was given, but the NLQ provider {provider:?} takes no API key"
            ));
        }
        samyama::nlq::config_from(|var| {
            let given = match var {
                "NLQ_PROVIDER" => provider_name.clone(),
                "NLQ_MODEL" => self.model.clone(),
                "NLQ_API_BASE_URL" => self.api_base_url.clone(),
                v if Some(v) == key_var => self.api_key.clone(),
                _ => None,
            };
            given.or_else(|| env(var))
        })
    }
}

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

    /// Translate a natural-language question into Cypher, in-process (#2092).
    ///
    /// The embedded counterpart of [`crate::RemoteClient::nlq`], doing what
    /// the server's `POST /api/nlq` does: the provider comes from the
    /// `NLQ_*` environment variables, the model is shown this graph's schema
    /// summary, and a generated query that writes is refused before it is
    /// returned. It returns the query and does **not** run it. Pass it to
    /// [`SamyamaClient::query_readonly`] for results.
    ///
    /// No server is involved, but the provider still is: `openai` and
    /// `gemini` send the question and the schema summary to that service,
    /// and `ollama` to the Ollama server. Only `mock` stays in the process.
    /// Errors arrive as [`SamyamaError::QueryError`], as they do from a
    /// server.
    pub async fn nlq(&self, question: &str) -> SamyamaResult<String> {
        self.nlq_with(question, &NlqSettings::default()).await
    }

    /// As [`Self::nlq`], with `settings` taking the place of the matching
    /// `NLQ_*` environment variables for this call.
    ///
    /// # Example
    /// ```
    /// # use samyama_sdk::{EmbeddedClient, NlqSettings, SamyamaClient};
    /// # #[tokio::main(flavor = "current_thread")] async fn main() -> samyama_sdk::SamyamaResult<()> {
    /// let client = EmbeddedClient::new();
    /// let settings = NlqSettings { provider: Some("mock".into()), ..Default::default() };
    /// let cypher = client.nlq_with("Show me some nodes", &settings).await?;
    /// let result = client.query_readonly("default", &cypher).await?;
    /// # Ok(()) }
    /// ```
    pub async fn nlq_with(&self, question: &str, settings: &NlqSettings) -> SamyamaResult<String> {
        let config = settings.resolve().map_err(SamyamaError::QueryError)?;
        let pipeline = samyama::NLQPipeline::new(config)
            .map_err(|e| SamyamaError::QueryError(e.to_string()))?;
        // Take the summary and let go of the read lock before the model call,
        // which can take seconds, as the server's handler does.
        let schema = self.store.read().await.schema_summary();
        pipeline
            .translate(question, &schema)
            .await
            .map(|t| t.cypher)
            .map_err(|e| SamyamaError::QueryError(e.to_string()))
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
        self.import_snapshot_dedup_options(_tenant, path, dedup_keys, false).await
    }

    /// Import with explicit permission to merge conflicting identifiers.
    pub async fn import_snapshot_dedup_options(
        &self,
        _tenant: &str,
        path: &std::path::Path,
        dedup_keys: &[&str],
        allow_conflicts: bool,
    ) -> Result<samyama::snapshot::format::ImportStats, Box<dyn std::error::Error>> {
        self.import_snapshot_merge_policy(_tenant, path, dedup_keys, allow_conflicts, samyama::snapshot::MergeEdges::default()).await
    }

    /// Import with an explicit policy for parallel edges introduced by merging.
    pub async fn import_snapshot_merge_policy(
        &self,
        _tenant: &str,
        path: &std::path::Path,
        dedup_keys: &[&str],
        allow_conflicts: bool,
        merge_edges: samyama::snapshot::MergeEdges,
    ) -> Result<samyama::snapshot::format::ImportStats, Box<dyn std::error::Error>> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut store_guard = self.store.write().await;
        let stats = samyama::snapshot::import_tenant_with_merge_policy(&mut store_guard, reader, dedup_keys, allow_conflicts, merge_edges)?;
        Ok(stats)
    }

    /// The node holding an external key -- a primary key or business id
    /// imported from another system -- under a unique constraint (#542).
    ///
    /// Answered from the constraint's index; see
    /// [`GraphStore::find_node_by_unique`]. `Ok(None)` means no node holds the
    /// key. Without `CREATE CONSTRAINT FOR (n:Label) REQUIRE n.property IS
    /// UNIQUE` it is an error, not a scan.
    ///
    /// # Example
    /// ```
    /// # use samyama_sdk::{EmbeddedClient, SamyamaClient};
    /// # #[tokio::main] async fn main() -> samyama_sdk::SamyamaResult<()> {
    /// let client = EmbeddedClient::new();
    /// client.query("default", "CREATE CONSTRAINT FOR (a:Acct) REQUIRE a.krid IS UNIQUE").await?;
    /// client.query("default", "CREATE (:Acct {krid: 42})").await?;
    /// let id = client.find_node_by_unique("default", "Acct", "krid", 42i64).await?;
    /// assert!(id.is_some());
    /// # Ok(()) }
    /// ```
    pub async fn find_node_by_unique(
        &self,
        _graph: &str,
        label: &str,
        property: &str,
        value: impl Into<samyama::graph::PropertyValue>,
    ) -> SamyamaResult<Option<samyama::graph::NodeId>> {
        let value = value.into();
        let store_guard = self.store.read().await;
        store_guard
            .find_node_by_unique(&samyama::graph::Label::new(label), property, &value)
            .map_err(|e| SamyamaError::QueryError(e.to_string()))
    }
}

impl Default for EmbeddedClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a RecordBatch from the query engine into an SDK QueryResult.
pub(crate) fn record_batch_to_query_result(batch: &RecordBatch, store: &GraphStore) -> QueryResult {
    let mut nodes_map: HashMap<String, SdkNode> = HashMap::new();
    let mut edges_map: HashMap<String, SdkEdge> = HashMap::new();
    let mut records = Vec::new();

    for record in &batch.records {
        let mut row = Vec::new();
        for col in &batch.columns {
            match record.get(col) {
                Some(v) => row.push(value_to_json(v, store, &mut nodes_map, &mut edges_map)),
                None => row.push(serde_json::Value::Null),
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

/// One result value as JSON, registering any entity it contains.
///
/// **Recursive, which is the point.** This used to be inlined in the loop
/// above, and the `List` and `Map` arms rendered their elements with
/// `format!("{v:?}")` — the Rust `Debug` of the internal enum. So an entity
/// inside a collection reached SDK consumers as
/// `"NodeRef(NodeId(1))"` or
/// `"EdgeRef(EdgeId(1), NodeId(1), NodeId(2), EdgeType(\"R\"))"`, labels and
/// properties gone and an escaped quote embedded in a JSON string, while the
/// very same node in a bare column came back as `{id, labels, properties}`.
///
/// Scalars were unaffected, which is what made it easy to miss: `collect(n.name)`
/// was right and `collect(n)` was not — and `collect`, `nodes` and
/// `relationships` are everyday Cypher.
fn value_to_json(
    val: &Value,
    store: &GraphStore,
    nodes_map: &mut HashMap<String, SdkNode>,
    edges_map: &mut HashMap<String, SdkEdge>,
) -> serde_json::Value {
    match val {
        Value::Map(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v, store, nodes_map, edges_map)))
                .collect(),
        ),
        Value::List(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|i| value_to_json(i, store, nodes_map, edges_map))
                .collect(),
        ),
        Value::Node(id, node) => {
            // The store's merged view, column first (#1188): the row a node
            // value was cloned from holds no property a store write made. The
            // value's own map is the fallback for a node no longer in the store,
            // such as one returned by a DELETE.
            let mut properties = serde_json::Map::new();
            let merged;
            let source = if store.get_node(*id).is_some() {
                merged = store.node_properties_merged(*id);
                &merged
            } else {
                &node.properties
            };
            for (k, v) in source {
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

            node_json
        }
        Value::NodeRef(id) => {
            let id_str = id.as_u64().to_string();
            // Materialised, not the row: store writes leave the row empty and
            // hold every value in the column (#1188).
            let (labels, properties, node_json) = if let Some(node) = store.node_materialized(*id) {
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

            node_json
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

            edge_json
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

            edge_json
        }
        Value::Property(p) => p.to_json(),
        Value::Path { nodes: path_nodes, edges: path_edges } => serde_json::json!({
            "nodes": path_nodes.iter().map(|n| n.as_u64().to_string()).collect::<Vec<_>>(),
            "edges": path_edges.iter().map(|e| e.as_u64().to_string()).collect::<Vec<_>>(),
            "length": path_edges.len(),
        }),
        Value::Null => serde_json::Value::Null,
    }
}

/// The one graph an embedded client serves, as the servers spell it.
const DEFAULT_GRAPH: &str = "default";

/// Refuse a `graph` this client cannot honour.
///
/// The embedded client holds exactly one `GraphStore` and the read path has no
/// tenant parameter, so there is nothing to scope a foreign name to. Ignoring
/// the argument is the one option that is actually wrong: a caller asking for
/// `analytics` was being served `default`'s rows and told nothing (#2139).
///
/// Refusing is what the HTTP server does (`reject_foreign_graph`,
/// `src/http/handler.rs:100`) and what the RESP server does before
/// `GRAPH.DELETE` (`src/protocol/command.rs:586`), for the reason #1110
/// records: the name used to be parsed and thrown away, so
/// `GRAPH.DELETE analytics` emptied `default`.
fn reject_foreign_graph(graph: &str) -> SamyamaResult<()> {
    if graph == DEFAULT_GRAPH {
        return Ok(());
    }
    Err(SamyamaError::QueryError(format!(
        "This build serves a single graph ('{DEFAULT_GRAPH}'); the requested graph \
         '{graph}' does not exist and the argument cannot be honoured."
    )))
}

#[async_trait]
impl SamyamaClient for EmbeddedClient {
    async fn query(&self, graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        reject_foreign_graph(graph)?;
        // The AST decides, not the text. Matching strings disagreed with the
        // servers and was whitespace-sensitive: `" MERGE "` does not occur in
        // `MATCH (a:X)\nMERGE (b:X)`, so a write was routed to the read path
        // and failed with WriteInReadTransaction (#2139, and #1111 for the
        // same defect in the servers).
        //
        // A statement that does not parse is not classified; the caller gets
        // the parse error, which is what it would have got a beat later.
        let is_write = self
            .engine
            .statement_is_write(cypher)
            .map_err(|e| SamyamaError::QueryError(e.to_string()))?;
        if is_write {
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

    async fn query_readonly(&self, graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        reject_foreign_graph(graph)?;
        let store_guard = self.store.read().await;
        let batch = self.engine.execute(cypher, &*store_guard)
            .map_err(|e| SamyamaError::QueryError(e.to_string()))?;
        Ok(record_batch_to_query_result(&batch, &*store_guard))
    }

    async fn delete_graph(&self, graph: &str) -> SamyamaResult<()> {
        // Was `_graph`, and cleared the whole store whatever name it was given
        // -- #1110's defect, still live here after the servers fixed it.
        reject_foreign_graph(graph)?;
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
    use samyama::LLMProvider;

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

    /// The classification the old string matcher got right, kept so the move
    /// to the AST is not a regression. These all read as writes on one line.
    #[tokio::test]
    async fn a_write_on_one_line_still_routes_to_the_write_path() {
        let client = EmbeddedClient::new();
        for cypher in [
            "CREATE (n:Person)",
            "MATCH (n) CREATE (m:Person)",
            "MERGE (n:Person {name: 'a'})",
            "MATCH (n:Person) SET n.x = 1",
            "MATCH (n:Person) DETACH DELETE n",
        ] {
            client
                .query("default", cypher)
                .await
                .unwrap_or_else(|e| panic!("{cypher:?} was refused: {e}"));
        }
    }

    /// The reported failure. `" MERGE "` does not occur in these -- the newline
    /// sits where the matcher wanted a space -- so each was classified as a
    /// read, sent to the read path, and failed with
    /// `Samyama.ClientError.Statement.WriteInReadTransaction`.
    #[tokio::test]
    async fn a_write_split_over_lines_is_not_mistaken_for_a_read() {
        let client = EmbeddedClient::new();
        for cypher in [
            "MATCH (a:Person)\nMERGE (b:Person {name: 'b'})",
            "MATCH (a:Person)\nCREATE (b:Person {name: 'b'})",
            "UNWIND [1, 2] AS r\nMERGE (n:Row {v: r})",
            "MATCH (a:Person)\n  SET a.seen = true",
        ] {
            client
                .query("default", cypher)
                .await
                .unwrap_or_else(|e| panic!("{cypher:?} was treated as a read: {e}"));
        }
    }

    #[tokio::test]
    async fn a_read_is_still_a_read() {
        let client = EmbeddedClient::new();
        for cypher in ["MATCH (n) RETURN n", "RETURN 1 + 2"] {
            client.query("default", cypher).await.unwrap();
            client.query_readonly("default", cypher).await.unwrap();
        }
    }

    /// A statement that does not parse is not classified. The caller gets the
    /// parse error rather than a guess about which path to take.
    #[tokio::test]
    async fn an_unparseable_statement_reports_its_parse_error() {
        let client = EmbeddedClient::new();
        assert!(client.query("default", "MATCH ((((").await.is_err());
    }

    /// Reads used to ignore `graph` entirely, so a caller asking for another
    /// tenant was served `default`'s rows and told nothing.
    #[tokio::test]
    async fn a_foreign_graph_is_refused_on_every_surface() {
        let client = EmbeddedClient::new();
        client.query("default", "CREATE (n:Person {name: 'a'})").await.unwrap();

        for graph in ["analytics", "DEFAULT", ""] {
            assert!(
                client.query(graph, "MATCH (n) RETURN n").await.is_err(),
                "query({graph:?}) was served default's rows"
            );
            assert!(
                client.query_readonly(graph, "MATCH (n) RETURN n").await.is_err(),
                "query_readonly({graph:?}) was served default's rows"
            );
            assert!(
                client.delete_graph(graph).await.is_err(),
                "delete_graph({graph:?}) was accepted"
            );
        }

        // The refusals left the one real graph alone -- #1110's defect was
        // that a foreign name cleared it.
        let rows = client.query("default", "MATCH (n) RETURN n").await.unwrap();
        assert_eq!(rows.records.len(), 1, "a refused call still touched default");
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

    // --- NlqSettings (#2092): read against a made-up environment ------------

    fn env_of(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |var| pairs.iter().find(|(k, _)| *k == var).map(|(_, v)| v.to_string())
    }

    fn settings(provider: Option<&str>, api_key: Option<&str>) -> NlqSettings {
        NlqSettings {
            provider: provider.map(str::to_string),
            api_key: api_key.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn nlq_settings_left_empty_read_the_environment() {
        let env = env_of(&[
            ("NLQ_PROVIDER", "openai"),
            ("NLQ_MODEL", "gpt-env"),
            ("OPENAI_API_KEY", "sk-env"),
            ("NLQ_API_BASE_URL", "http://env.invalid/v1"),
        ]);
        let c = NlqSettings::default().resolve_from(env).unwrap();
        assert_eq!(c.provider, LLMProvider::OpenAI);
        assert_eq!(c.model, "gpt-env");
        assert_eq!(c.api_key.as_deref(), Some("sk-env"));
        assert_eq!(c.api_base_url.as_deref(), Some("http://env.invalid/v1"));
        assert!(c.enabled);
    }

    #[test]
    fn nlq_settings_that_are_set_win_over_the_environment() {
        let env = env_of(&[
            ("NLQ_PROVIDER", "ollama"),
            ("NLQ_MODEL", "llama-env"),
            ("OPENAI_API_KEY", "sk-env"),
            ("NLQ_API_BASE_URL", "http://env.invalid"),
        ]);
        let given = NlqSettings {
            provider: Some("openai".into()),
            model: Some("gpt-given".into()),
            api_key: Some("sk-given".into()),
            api_base_url: Some("http://given.invalid/v1".into()),
        };
        let c = given.resolve_from(env).unwrap();
        assert_eq!(c.provider, LLMProvider::OpenAI);
        assert_eq!(c.model, "gpt-given");
        assert_eq!(c.api_key.as_deref(), Some("sk-given"));
        assert_eq!(c.api_base_url.as_deref(), Some("http://given.invalid/v1"));
    }

    #[test]
    fn nlq_settings_default_the_model_as_the_server_does() {
        let c = settings(Some("mock"), None).resolve_from(env_of(&[])).unwrap();
        assert_eq!(c.model, "gpt-4o");
        assert_eq!(c.api_key, None);
        assert_eq!(c.api_base_url, None);
    }

    #[test]
    fn nlq_settings_give_the_key_to_the_provider_it_was_given_for() {
        // The OpenAI key in the environment must not reach Google (#438).
        let env = env_of(&[("OPENAI_API_KEY", "sk-openai")]);
        let c = settings(Some("gemini"), Some("g-key")).resolve_from(&env).unwrap();
        assert_eq!(c.provider, LLMProvider::Gemini);
        assert_eq!(c.api_key.as_deref(), Some("g-key"));
        let e = settings(Some("gemini"), None).resolve_from(&env).unwrap_err();
        assert!(e.contains("GEMINI_API_KEY"), "{e}");
    }

    #[test]
    fn nlq_settings_refuse_what_the_server_refuses() {
        let none = env_of(&[]);
        let e = NlqSettings::default().resolve_from(&none).unwrap_err();
        assert!(e.contains("NLQ_PROVIDER is not set"), "{e}");
        let e = settings(Some("openai"), None).resolve_from(&none).unwrap_err();
        assert!(e.contains("OPENAI_API_KEY"), "{e}");
        let e = settings(Some("nope"), None).resolve_from(&none).unwrap_err();
        assert!(e.contains("unknown NLQ_PROVIDER \"nope\""), "{e}");
        let e = settings(Some("anthropic"), Some("k")).resolve_from(&none).unwrap_err();
        assert!(e.contains("not implemented"), "{e}");
    }

    #[test]
    fn nlq_settings_refuse_a_key_the_provider_would_drop() {
        for provider in ["mock", "ollama", "claudecode"] {
            let e = settings(Some(provider), Some("sk-x")).resolve_from(env_of(&[])).unwrap_err();
            assert!(e.contains("takes no API key"), "{provider}: {e}");
        }
        // The same provider from the environment is refused the same way.
        let env = env_of(&[("NLQ_PROVIDER", "mock")]);
        assert!(settings(None, Some("sk-x")).resolve_from(env).is_err());
    }
}
