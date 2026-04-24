//! Command handler for GRAPH.* commands
//!
//! Implements REQ-REDIS-004 (Redis-compatible graph commands)
//! Now with persistence support - writes are persisted to disk when enabled

use crate::graph::GraphStore;
use crate::persistence::{PersistenceManager, TenantManager};
use crate::protocol::resp::RespValue;
use crate::query::{QueryEngine, Value};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// Command handler for processing GRAPH.* commands
pub struct CommandHandler {
    query_engine: QueryEngine,
    /// Optional persistence manager - when Some, writes are persisted to disk
    persistence: Option<Arc<PersistenceManager>>,
    /// Shared tenant registry — HA-09 unifies HTTP + RESP views
    tenant_manager: Arc<TenantManager>,
}

impl CommandHandler {
    /// Create a new command handler
    /// Pass Some(persistence) to enable persistence, or None for in-memory only.
    /// When persistence is Some, its TenantManager is reused; otherwise a fresh
    /// one is created. For tests/integrations that need to share a registry
    /// across HTTP+RESP, use `new_with_tenants` instead.
    pub fn new(persistence: Option<Arc<PersistenceManager>>) -> Self {
        let tenant_manager = persistence
            .as_ref()
            .map(|p| p.tenants_arc())
            .unwrap_or_else(|| Arc::new(TenantManager::new()));
        Self {
            query_engine: QueryEngine::new(),
            persistence,
            tenant_manager,
        }
    }

    /// Create a command handler that shares a pre-existing `TenantManager`.
    /// Both the RESP path and the HTTP tenant routes must see the same `Arc`
    /// to satisfy HA-09 (cross-registry visibility).
    pub fn new_with_tenants(
        persistence: Option<Arc<PersistenceManager>>,
        tenant_manager: Arc<TenantManager>,
    ) -> Self {
        Self {
            query_engine: QueryEngine::new(),
            persistence,
            tenant_manager,
        }
    }

    /// Access the shared tenant registry (for HTTP wiring in main).
    pub fn tenant_manager(&self) -> Arc<TenantManager> {
        Arc::clone(&self.tenant_manager)
    }

    /// Handle a RESP command
    pub async fn handle_command(
        &self,
        value: &RespValue,
        store: &Arc<RwLock<GraphStore>>,
    ) -> RespValue {
        // Parse command from RESP array
        let args = match value.as_array() {
            Ok(arr) => arr,
            Err(e) => {
                return RespValue::Error(format!("ERR {}", e));
            }
        };

        if args.is_empty() {
            return RespValue::Error("ERR empty command".to_string());
        }

        // Extract command name
        let cmd_name = match args[0].as_string() {
            Ok(Some(s)) => s.to_uppercase(),
            Ok(None) => {
                return RespValue::Error("ERR null command".to_string());
            }
            Err(e) => {
                return RespValue::Error(format!("ERR {}", e));
            }
        };

        debug!("Received command: {}", cmd_name);

        // Route to appropriate handler
        match cmd_name.as_str() {
            "GRAPH.QUERY" => self.handle_graph_query(args, store).await,
            "GRAPH.RO_QUERY" => self.handle_graph_ro_query(args, store).await,
            "GRAPH.DELETE" => self.handle_graph_delete(args, store).await,
            "GRAPH.LIST" => self.handle_graph_list(args, store).await,
            "PING" => self.handle_ping(args),
            "ECHO" => self.handle_echo(args),
            "INFO" => self.handle_info(args),
            _ => RespValue::Error(format!("ERR unknown command '{}'", cmd_name)),
        }
    }

    /// Handle GRAPH.QUERY command
    /// Format: GRAPH.QUERY graph_name "MATCH (n) RETURN n"
    async fn handle_graph_query(
        &self,
        args: &[RespValue],
        store: &Arc<RwLock<GraphStore>>,
    ) -> RespValue {
        if args.len() < 3 {
            return RespValue::Error("ERR wrong number of arguments for 'GRAPH.QUERY' command".to_string());
        }

        // Extract graph name (for future multi-tenancy)
        let graph_name = match args[1].as_string() {
            Ok(Some(s)) => s,
            Ok(None) => return RespValue::Error("ERR null graph name".to_string()),
            Err(e) => return RespValue::Error(format!("ERR {}", e)),
        };

        // Extract query string
        let query_str = match args[2].as_string() {
            Ok(Some(s)) => s,
            Ok(None) => return RespValue::Error("ERR null query".to_string()),
            Err(e) => return RespValue::Error(format!("ERR {}", e)),
        };

        debug!("Executing query: {}", query_str);

        // Check if this is a write query (CREATE, DELETE, SET, MERGE)
        let query_upper = query_str.trim().to_uppercase();
        let is_write_query = query_upper.starts_with("CREATE")
            || query_upper.starts_with("DELETE")
            || query_upper.starts_with("SET")
            || query_upper.starts_with("MERGE")
            || query_upper.contains(" CREATE ")
            || query_upper.contains(" DELETE ")
            || query_upper.contains(" SET ")
            || query_upper.contains(" MERGE ");

        // Execute query with appropriate method
        let result = if is_write_query {
            let mut store_guard = store.write().await;
            
            // Set current tenant for indexing events
            // In a more complex architecture, the store_guard would be isolated
            let res = self.query_engine.execute_mut(&query_str, &mut *store_guard, &graph_name);

            // If write succeeded and persistence is enabled, persist the changes
            if let (Ok(ref batch), Some(ref persist_mgr)) = (&res, &self.persistence) {
                // Extract created nodes/edges from the result and persist them
                // The RecordBatch contains Node and Edge values that were created
                for record in &batch.records {
                    for (_col, value) in record.bindings().iter() {
                        match value {
                            Value::Node(node_id, node) => {
                                // Persist the created node
                                if let Err(e) = persist_mgr.persist_create_node(&graph_name, node) {
                                    warn!("Failed to persist node {:?}: {}", node_id, e);
                                }
                            }
                            Value::Edge(edge_id, edge) => {
                                // Persist the created edge
                                if let Err(e) = persist_mgr.persist_create_edge(&graph_name, edge) {
                                    warn!("Failed to persist edge {:?}: {}", edge_id, e);
                                }
                            }
                            Value::NodeRef(_) | Value::EdgeRef(..) => {
                                // Refs from read queries — nothing to persist
                            }
                            _ => {} // Other value types don't need persistence
                        }
                    }
                }
                debug!("Write query persisted successfully");
            }

            drop(store_guard);
            res
        } else {
            let store_guard = store.read().await;
            let res = self.query_engine.execute(&query_str, &*store_guard);
            drop(store_guard);
            res
        };

        match result {
            Ok(batch) => {
                // Format result as RESP array
                self.format_query_result(batch)
            }
            Err(e) => {
                error!("Query error: {}", e);
                RespValue::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle GRAPH.RO_QUERY (read-only query)
    async fn handle_graph_ro_query(
        &self,
        args: &[RespValue],
        store: &Arc<RwLock<GraphStore>>,
    ) -> RespValue {
        // For now, same as GRAPH.QUERY (we don't enforce read-only yet)
        self.handle_graph_query(args, store).await
    }

    /// Handle GRAPH.DELETE command
    async fn handle_graph_delete(
        &self,
        args: &[RespValue],
        store: &Arc<RwLock<GraphStore>>,
    ) -> RespValue {
        if args.len() < 2 {
            return RespValue::Error("ERR wrong number of arguments for 'GRAPH.DELETE' command".to_string());
        }

        let _graph_name = match args[1].as_string() {
            Ok(Some(s)) => s,
            Ok(None) => return RespValue::Error("ERR null graph name".to_string()),
            Err(e) => return RespValue::Error(format!("ERR {}", e)),
        };

        // Clear the graph
        let mut store_guard = store.write().await;
        store_guard.clear();
        drop(store_guard);

        RespValue::SimpleString("OK".to_string())
    }

    /// Handle GRAPH.LIST command — reads from the shared TenantManager (HA-09).
    async fn handle_graph_list(
        &self,
        _args: &[RespValue],
        _store: &Arc<RwLock<GraphStore>>,
    ) -> RespValue {
        let mut ids: Vec<String> = self
            .tenant_manager
            .list_tenants()
            .into_iter()
            .map(|t| t.id)
            .collect();
        ids.sort();
        RespValue::Array(
            ids.into_iter()
                .map(|id| RespValue::BulkString(Some(id.into_bytes())))
                .collect(),
        )
    }

    /// Handle PING command
    fn handle_ping(&self, args: &[RespValue]) -> RespValue {
        if args.len() > 1 {
            // PING with message - echo it back
            match args[1].as_string() {
                Ok(Some(s)) => RespValue::BulkString(Some(s.into_bytes())),
                Ok(None) => RespValue::BulkString(None),
                Err(_) => RespValue::BulkString(Some(b"PONG".to_vec())),
            }
        } else {
            RespValue::SimpleString("PONG".to_string())
        }
    }

    /// Handle ECHO command
    fn handle_echo(&self, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::Error("ERR wrong number of arguments for 'ECHO' command".to_string());
        }

        match args[1].as_bulk_string() {
            Ok(Some(data)) => RespValue::BulkString(Some(data.to_vec())),
            Ok(None) => RespValue::BulkString(None),
            Err(e) => RespValue::Error(format!("ERR {}", e)),
        }
    }

    /// Handle INFO command
    fn handle_info(&self, _args: &[RespValue]) -> RespValue {
        let info = format!(
            "# Server\r\n\
             samyama_version:{}\r\n\
             redis_mode:standalone\r\n\
             # Clients\r\n\
             connected_clients:1\r\n\
             # Memory\r\n\
             used_memory:0\r\n",
            crate::VERSION
        );
        RespValue::BulkString(Some(info.into_bytes()))
    }

    /// Format query results as RESP value
    fn format_query_result(&self, batch: crate::query::RecordBatch) -> RespValue {
        let mut result_rows = Vec::new();

        // Add header row with column names
        let mut header = Vec::new();
        for col in &batch.columns {
            header.push(RespValue::BulkString(Some(col.clone().into_bytes())));
        }
        result_rows.push(RespValue::Array(header));

        // Add data rows
        for record in &batch.records {
            let mut row = Vec::new();
            for col_name in &batch.columns {
                if let Some(value) = record.get(col_name) {
                    row.push(self.format_value(value));
                } else {
                    row.push(RespValue::Null);
                }
            }
            result_rows.push(RespValue::Array(row));
        }

        // Return array of [header, data1, data2, ...]
        RespValue::Array(result_rows)
    }

    /// Format a query value as RESP
    fn format_value(&self, value: &Value) -> RespValue {
        match value {
            // _node prefixed with underscore - node data available but not used in
            // simple string formatting (only showing id for RESP compatibility)
            Value::Node(id, _node) => {
                // Format node as JSON-like string
                let node_str = format!("Node({:?})", id);
                RespValue::BulkString(Some(node_str.into_bytes()))
            }
            Value::NodeRef(id) => {
                let node_str = format!("Node({:?})", id);
                RespValue::BulkString(Some(node_str.into_bytes()))
            }
            Value::Edge(id, edge) => {
                // Format edge as JSON-like string
                let edge_str = format!("Edge({:?}, {} -> {})", id, edge.source, edge.target);
                RespValue::BulkString(Some(edge_str.into_bytes()))
            }
            Value::EdgeRef(id, src, tgt, _) => {
                let edge_str = format!("Edge({:?}, {} -> {})", id, src, tgt);
                RespValue::BulkString(Some(edge_str.into_bytes()))
            }
            Value::Property(prop) => {
                // Format property value
                match prop {
                    crate::graph::PropertyValue::String(s) => {
                        RespValue::BulkString(Some(s.clone().into_bytes()))
                    }
                    crate::graph::PropertyValue::Integer(i) => {
                        RespValue::Integer(*i)
                    }
                    crate::graph::PropertyValue::Float(f) => {
                        RespValue::BulkString(Some(f.to_string().into_bytes()))
                    }
                    crate::graph::PropertyValue::Boolean(b) => {
                        RespValue::BulkString(Some(b.to_string().into_bytes()))
                    }
                    _ => RespValue::BulkString(Some(format!("{:?}", prop).into_bytes())),
                }
            }
            Value::Path { nodes, edges } => {
                let path_str = format!("Path(nodes: {:?}, edges: {:?})", nodes, edges);
                RespValue::BulkString(Some(path_str.into_bytes()))
            }
            Value::Null => RespValue::Null,
        }
    }
}

impl Default for CommandHandler {
    fn default() -> Self {
        Self::new(None)  // Default is in-memory only (no persistence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping() {
        let handler = CommandHandler::new(None);  // No persistence for tests
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"PING".to_vec())),
        ]);

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let response = handler.handle_command(&cmd, &store).await;

        assert_eq!(response, RespValue::SimpleString("PONG".to_string()));
    }

    #[tokio::test]
    async fn test_echo() {
        let handler = CommandHandler::new(None);  // No persistence for tests
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"ECHO".to_vec())),
            RespValue::BulkString(Some(b"hello".to_vec())),
        ]);

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let response = handler.handle_command(&cmd, &store).await;

        assert_eq!(response, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[tokio::test]
    async fn test_graph_query() {
        let handler = CommandHandler::new(None);  // No persistence for tests

        // Create test data
        let mut graph_store = GraphStore::new();
        let alice = graph_store.create_node("Person");
        if let Some(node) = graph_store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let store = Arc::new(RwLock::new(graph_store));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
            RespValue::BulkString(Some(b"mygraph".to_vec())),
            RespValue::BulkString(Some(b"MATCH (n:Person) RETURN n".to_vec())),
        ]);

        let response = handler.handle_command(&cmd, &store).await;

        // Should return an array (results)
        assert!(matches!(response, RespValue::Array(_)));
    }

    // ========== Batch 6: Additional Command Tests ==========

    #[tokio::test]
    async fn test_command_handler_default() {
        let handler = CommandHandler::default();
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"PING".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(response, RespValue::SimpleString("PONG".to_string()));
    }

    #[tokio::test]
    async fn test_graph_ro_query() {
        let handler = CommandHandler::new(None);
        let mut graph_store = GraphStore::new();
        let n = graph_store.create_node("Person");
        if let Some(node) = graph_store.get_node_mut(n) {
            node.set_property("name", "Bob");
        }
        let store = Arc::new(RwLock::new(graph_store));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.RO_QUERY".to_vec())),
            RespValue::BulkString(Some(b"mygraph".to_vec())),
            RespValue::BulkString(Some(b"MATCH (n:Person) RETURN n.name".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert!(matches!(response, RespValue::Array(_)));
    }

    #[tokio::test]
    async fn test_graph_delete() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.DELETE".to_vec())),
            RespValue::BulkString(Some(b"mygraph".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        // Should return OK or similar
        assert!(!matches!(response, RespValue::Null));
    }

    #[tokio::test]
    async fn test_graph_list() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.LIST".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert!(matches!(response, RespValue::Array(_)));
    }

    #[tokio::test]
    async fn test_info_command() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"INFO".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        // Should return a bulk string with info
        assert!(matches!(response, RespValue::BulkString(_)));
    }

    #[tokio::test]
    async fn test_unknown_command() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"NONEXISTENT".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        // Should return an error
        assert!(matches!(response, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_empty_command() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![]);
        let response = handler.handle_command(&cmd, &store).await;
        assert!(matches!(response, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_graph_query_create() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
            RespValue::BulkString(Some(b"mygraph".to_vec())),
            RespValue::BulkString(Some(b"CREATE (n:Person {name: 'Alice'})".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert!(matches!(response, RespValue::Array(_)));
    }

    // ========== Coverage expansion tests ==========

    #[tokio::test]
    async fn test_graph_query_wrong_args() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        // Only 2 args (command + graph name, missing query)
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
            RespValue::BulkString(Some(b"mygraph".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::Error("ERR wrong number of arguments for 'GRAPH.QUERY' command".to_string())
        );

        // Only 1 arg (command only)
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::Error("ERR wrong number of arguments for 'GRAPH.QUERY' command".to_string())
        );
    }

    #[tokio::test]
    async fn test_graph_query_null_graph_name() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
            RespValue::BulkString(None), // null graph name
            RespValue::BulkString(Some(b"MATCH (n) RETURN n".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::Error("ERR null graph name".to_string())
        );
    }

    #[tokio::test]
    async fn test_graph_query_null_query() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
            RespValue::BulkString(Some(b"mygraph".to_vec())),
            RespValue::BulkString(None), // null query
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::Error("ERR null query".to_string())
        );
    }

    #[tokio::test]
    async fn test_graph_delete_wrong_args() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        // Only 1 arg (command only, missing graph name)
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.DELETE".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::Error("ERR wrong number of arguments for 'GRAPH.DELETE' command".to_string())
        );
    }

    #[tokio::test]
    async fn test_echo_wrong_args() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        // Only 1 arg (command only, missing message)
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"ECHO".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::Error("ERR wrong number of arguments for 'ECHO' command".to_string())
        );
    }

    #[tokio::test]
    async fn test_ping_with_message() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"PING".to_vec())),
            RespValue::BulkString(Some(b"hello world".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::BulkString(Some(b"hello world".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_ping_with_null_message() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"PING".to_vec())),
            RespValue::BulkString(None), // null message
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(response, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_ping_with_non_string_message() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        // PING with an Integer argument (not a bulk string)
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"PING".to_vec())),
            RespValue::Integer(42),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        // Should fall back to PONG when as_string fails
        assert_eq!(
            response,
            RespValue::BulkString(Some(b"PONG".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_format_value_node() {
        use crate::graph::{Node, NodeId};

        let handler = CommandHandler::new(None);
        let node = Node::new(NodeId::new(1), "Person");
        let value = Value::Node(NodeId::new(1), node);
        let result = handler.format_value(&value);
        match result {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8(bytes).unwrap();
                assert!(s.contains("Node("));
                assert!(s.contains("NodeId(1)"));
            }
            _ => panic!("Expected BulkString for Node"),
        }
    }

    #[tokio::test]
    async fn test_format_value_node_ref() {
        use crate::graph::NodeId;

        let handler = CommandHandler::new(None);
        let value = Value::NodeRef(NodeId::new(42));
        let result = handler.format_value(&value);
        match result {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8(bytes).unwrap();
                assert!(s.contains("Node("));
                assert!(s.contains("NodeId(42)"));
            }
            _ => panic!("Expected BulkString for NodeRef"),
        }
    }

    #[tokio::test]
    async fn test_format_value_edge() {
        use crate::graph::{Edge, EdgeId, NodeId};

        let handler = CommandHandler::new(None);
        let edge = Edge::new(EdgeId::new(10), NodeId::new(1), NodeId::new(2), "KNOWS");
        let value = Value::Edge(EdgeId::new(10), edge);
        let result = handler.format_value(&value);
        match result {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8(bytes).unwrap();
                assert!(s.contains("Edge("));
                assert!(s.contains("NodeId(1)"));
                assert!(s.contains("NodeId(2)"));
            }
            _ => panic!("Expected BulkString for Edge"),
        }
    }

    #[tokio::test]
    async fn test_format_value_edge_ref() {
        use crate::graph::{EdgeId, EdgeType, NodeId};

        let handler = CommandHandler::new(None);
        let value = Value::EdgeRef(
            EdgeId::new(5),
            NodeId::new(10),
            NodeId::new(20),
            EdgeType::new("FOLLOWS"),
        );
        let result = handler.format_value(&value);
        match result {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8(bytes).unwrap();
                assert!(s.contains("Edge("));
                assert!(s.contains("NodeId(10)"));
                assert!(s.contains("NodeId(20)"));
            }
            _ => panic!("Expected BulkString for EdgeRef"),
        }
    }

    #[tokio::test]
    async fn test_format_value_property_integer() {
        use crate::graph::PropertyValue;

        let handler = CommandHandler::new(None);
        let value = Value::Property(PropertyValue::Integer(42));
        let result = handler.format_value(&value);
        assert_eq!(result, RespValue::Integer(42));
    }

    #[tokio::test]
    async fn test_format_value_property_float() {
        use crate::graph::PropertyValue;

        let handler = CommandHandler::new(None);
        let value = Value::Property(PropertyValue::Float(3.14));
        let result = handler.format_value(&value);
        match result {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8(bytes).unwrap();
                assert!(s.contains("3.14"));
            }
            _ => panic!("Expected BulkString for Float"),
        }
    }

    #[tokio::test]
    async fn test_format_value_property_boolean() {
        use crate::graph::PropertyValue;

        let handler = CommandHandler::new(None);
        let value_true = Value::Property(PropertyValue::Boolean(true));
        let result = handler.format_value(&value_true);
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"true".to_vec()))
        );

        let value_false = Value::Property(PropertyValue::Boolean(false));
        let result = handler.format_value(&value_false);
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"false".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_format_value_property_other() {
        use crate::graph::PropertyValue;

        let handler = CommandHandler::new(None);
        // DateTime is one of the "other" property variants (not String/Integer/Float/Boolean)
        let value = Value::Property(PropertyValue::DateTime(1709712000000));
        let result = handler.format_value(&value);
        match result {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8(bytes).unwrap();
                assert!(s.contains("DateTime"));
            }
            _ => panic!("Expected BulkString for DateTime property"),
        }
    }

    #[tokio::test]
    async fn test_format_value_path() {
        use crate::graph::{EdgeId, NodeId};

        let handler = CommandHandler::new(None);
        let value = Value::Path {
            nodes: vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            edges: vec![EdgeId::new(10), EdgeId::new(20)],
        };
        let result = handler.format_value(&value);
        match result {
            RespValue::BulkString(Some(bytes)) => {
                let s = String::from_utf8(bytes).unwrap();
                assert!(s.contains("Path("));
                assert!(s.contains("nodes:"));
                assert!(s.contains("edges:"));
            }
            _ => panic!("Expected BulkString for Path"),
        }
    }

    #[tokio::test]
    async fn test_format_value_null() {
        let handler = CommandHandler::new(None);
        let value = Value::Null;
        let result = handler.format_value(&value);
        assert_eq!(result, RespValue::Null);
    }

    #[tokio::test]
    async fn test_format_query_result_multiple_columns_rows() {
        use crate::graph::PropertyValue;
        use crate::query::{Record, RecordBatch};

        let handler = CommandHandler::new(None);

        let mut batch = RecordBatch::new(vec!["name".to_string(), "age".to_string()]);

        let mut r1 = Record::new();
        r1.bind("name".to_string(), Value::Property(PropertyValue::String("Alice".to_string())));
        r1.bind("age".to_string(), Value::Property(PropertyValue::Integer(30)));
        batch.push(r1);

        let mut r2 = Record::new();
        r2.bind("name".to_string(), Value::Property(PropertyValue::String("Bob".to_string())));
        r2.bind("age".to_string(), Value::Property(PropertyValue::Integer(25)));
        batch.push(r2);

        let result = handler.format_query_result(batch);
        match result {
            RespValue::Array(rows) => {
                // First row is the header
                assert_eq!(rows.len(), 3); // 1 header + 2 data rows
                // Check header
                match &rows[0] {
                    RespValue::Array(header) => {
                        assert_eq!(header.len(), 2);
                        assert_eq!(header[0], RespValue::BulkString(Some(b"name".to_vec())));
                        assert_eq!(header[1], RespValue::BulkString(Some(b"age".to_vec())));
                    }
                    _ => panic!("Expected Array for header row"),
                }
                // Check first data row
                match &rows[1] {
                    RespValue::Array(data) => {
                        assert_eq!(data.len(), 2);
                        assert_eq!(data[0], RespValue::BulkString(Some(b"Alice".to_vec())));
                        assert_eq!(data[1], RespValue::Integer(30));
                    }
                    _ => panic!("Expected Array for first data row"),
                }
                // Check second data row
                match &rows[2] {
                    RespValue::Array(data) => {
                        assert_eq!(data.len(), 2);
                        assert_eq!(data[0], RespValue::BulkString(Some(b"Bob".to_vec())));
                        assert_eq!(data[1], RespValue::Integer(25));
                    }
                    _ => panic!("Expected Array for second data row"),
                }
            }
            _ => panic!("Expected Array for format_query_result"),
        }
    }

    #[tokio::test]
    async fn test_format_query_result_missing_column_value() {
        use crate::graph::PropertyValue;
        use crate::query::{Record, RecordBatch};

        let handler = CommandHandler::new(None);

        // Record only has "name" bound, but batch declares "name" + "age" columns
        let mut batch = RecordBatch::new(vec!["name".to_string(), "age".to_string()]);
        let mut r = Record::new();
        r.bind("name".to_string(), Value::Property(PropertyValue::String("Alice".to_string())));
        // "age" is NOT bound
        batch.push(r);

        let result = handler.format_query_result(batch);
        match result {
            RespValue::Array(rows) => {
                assert_eq!(rows.len(), 2); // 1 header + 1 data row
                match &rows[1] {
                    RespValue::Array(data) => {
                        assert_eq!(data.len(), 2);
                        assert_eq!(data[0], RespValue::BulkString(Some(b"Alice".to_vec())));
                        assert_eq!(data[1], RespValue::Null); // missing column -> Null
                    }
                    _ => panic!("Expected Array for data row"),
                }
            }
            _ => panic!("Expected Array for format_query_result"),
        }
    }

    #[tokio::test]
    async fn test_non_array_command() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        // Send a SimpleString instead of an Array
        let cmd = RespValue::SimpleString("PING".to_string());
        let response = handler.handle_command(&cmd, &store).await;
        match response {
            RespValue::Error(msg) => {
                assert!(msg.contains("ERR"));
            }
            _ => panic!("Expected Error for non-array command"),
        }
    }

    #[tokio::test]
    async fn test_command_with_null_first_element() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        // Array with a null bulk string as the first element
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(None), // null command name
            RespValue::BulkString(Some(b"arg".to_vec())),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::Error("ERR null command".to_string())
        );
    }

    #[tokio::test]
    async fn test_command_with_non_string_first_element() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        // Array where first element is an Integer (not a BulkString)
        let cmd = RespValue::Array(vec![
            RespValue::Integer(123),
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        match response {
            RespValue::Error(msg) => {
                assert!(msg.contains("ERR"));
            }
            _ => panic!("Expected Error for non-string first element"),
        }
    }

    #[tokio::test]
    async fn test_graph_delete_null_graph_name() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.DELETE".to_vec())),
            RespValue::BulkString(None), // null graph name
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(
            response,
            RespValue::Error("ERR null graph name".to_string())
        );
    }

    #[tokio::test]
    async fn test_echo_with_null_message() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"ECHO".to_vec())),
            RespValue::BulkString(None), // null message
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        assert_eq!(response, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_echo_with_non_string_arg() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"ECHO".to_vec())),
            RespValue::Integer(99), // not a bulk string
        ]);
        let response = handler.handle_command(&cmd, &store).await;
        match response {
            RespValue::Error(msg) => {
                assert!(msg.contains("ERR"));
            }
            _ => panic!("Expected Error for non-string ECHO arg"),
        }
    }

    #[tokio::test]
    async fn test_graph_query_with_write_keywords_in_middle() {
        let handler = CommandHandler::new(None);
        let store = Arc::new(RwLock::new(GraphStore::new()));

        // Test a query that has write keywords in the middle (e.g., MATCH ... SET ...)
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GRAPH.QUERY".to_vec())),
            RespValue::BulkString(Some(b"mygraph".to_vec())),
            RespValue::BulkString(Some(b"MATCH (n:Person {name: 'Alice'}) SET n.age = 30 RETURN n".to_vec())),
        ]);
        // Should detect " SET " and treat as write query
        let response = handler.handle_command(&cmd, &store).await;
        // May error since no Alice exists, but the important thing is it routes through the write path
        // without panicking
        assert!(matches!(response, RespValue::Array(_) | RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_format_value_property_string() {
        use crate::graph::PropertyValue;

        let handler = CommandHandler::new(None);
        let value = Value::Property(PropertyValue::String("hello".to_string()));
        let result = handler.format_value(&value);
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));
    }
}
