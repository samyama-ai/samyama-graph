//! Command handler for GRAPH.* commands
//!
//! Implements REQ-REDIS-004 (Redis-compatible graph commands)

use crate::graph::GraphStore;
use crate::protocol::resp::{RespValue, RespError};
use crate::query::{QueryEngine, Value};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// Command handler for processing GRAPH.* commands
pub struct CommandHandler {
    query_engine: QueryEngine,
}

impl CommandHandler {
    /// Create a new command handler
    pub fn new() -> Self {
        Self {
            query_engine: QueryEngine::new(),
        }
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
        let _graph_name = match args[1].as_string() {
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

        // Check if this is a mutation query
        let needs_mutation = match self.query_engine.needs_mutation(&query_str) {
            Ok(needs) => needs,
            Err(e) => {
                error!("Parse error: {}", e);
                return RespValue::Error(format!("ERR {}", e));
            }
        };

        // Execute query with appropriate lock
        let result = if needs_mutation {
            let mut store_guard = store.write().await;
            let result = self.query_engine.execute_mutation(&query_str, &mut *store_guard);
            drop(store_guard);
            result
        } else {
            let store_guard = store.read().await;
            let result = self.query_engine.execute(&query_str, &*store_guard);
            drop(store_guard);
            result
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

    /// Handle GRAPH.LIST command
    async fn handle_graph_list(
        &self,
        _args: &[RespValue],
        _store: &Arc<RwLock<GraphStore>>,
    ) -> RespValue {
        // For single-graph mode, return a single graph
        RespValue::Array(vec![
            RespValue::BulkString(Some(b"default".to_vec())),
        ])
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
            Value::Node(id, node) => {
                // Format node as JSON-like string
                let node_str = format!("Node({:?})", id);
                RespValue::BulkString(Some(node_str.into_bytes()))
            }
            Value::Edge(id, edge) => {
                // Format edge as JSON-like string
                let edge_str = format!("Edge({:?}, {} -> {})", id, edge.source, edge.target);
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
            Value::Null => RespValue::Null,
        }
    }
}

impl Default for CommandHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping() {
        let handler = CommandHandler::new();
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(b"PING".to_vec())),
        ]);

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let response = handler.handle_command(&cmd, &store).await;

        assert_eq!(response, RespValue::SimpleString("PONG".to_string()));
    }

    #[tokio::test]
    async fn test_echo() {
        let handler = CommandHandler::new();
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
        let handler = CommandHandler::new();

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
}
