//! Query processing module
//!
//! Implements OpenCypher query language support:
//! - REQ-CYPHER-001: OpenCypher query language
//! - REQ-CYPHER-002: Pattern matching
//! - REQ-CYPHER-003: CRUD operations
//! - REQ-CYPHER-007: WHERE clauses
//! - REQ-CYPHER-008: ORDER BY and LIMIT
//! - REQ-CYPHER-009: Query optimization
//!
//! Architecture follows ADR-007 (Volcano Iterator Model)

pub mod ast;
pub mod parser;
pub mod executor;

// Re-export main types
pub use ast::Query;
pub use parser::{parse_query, ParseError, ParseResult};
pub use executor::{
    QueryExecutor, ExecutionError, ExecutionResult,
    Record, RecordBatch, Value,
};

/// Query engine - high-level interface for executing queries
pub struct QueryEngine {
    // Future: add query cache, prepared statements, etc.
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new() -> Self {
        Self {}
    }

    /// Parse a query and check if it requires mutation (CREATE, DELETE, etc.)
    pub fn needs_mutation(&self, query_str: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let query = parse_query(query_str)?;
        Ok(!query.is_read_only())
    }

    /// Parse and execute a read-only Cypher query
    pub fn execute(
        &self,
        query_str: &str,
        store: &crate::graph::GraphStore,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        // Parse query
        let query = parse_query(query_str)?;

        // If it's a mutation query, return an error
        if !query.is_read_only() {
            return Err("Use execute_mutation for CREATE/DELETE/MERGE queries".into());
        }

        // Execute read-only query
        let executor = QueryExecutor::new(store);
        let result = executor.execute(&query)?;

        Ok(result)
    }

    /// Parse and execute a mutation query (CREATE, DELETE, MERGE)
    pub fn execute_mutation(
        &self,
        query_str: &str,
        store: &mut crate::graph::GraphStore,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        // Parse query
        let query = parse_query(query_str)?;

        // Handle CREATE clause
        if let Some(ref create_clause) = query.create_clause {
            let mut created_nodes: std::collections::HashMap<String, crate::graph::NodeId> =
                std::collections::HashMap::new();

            for path in &create_clause.pattern.paths {
                // Create start node
                let start_node_id = if !path.start.labels.is_empty() {
                    let label = &path.start.labels[0];
                    let node_id = store.create_node(label.clone());

                    // Set properties if any
                    if let Some(ref props) = path.start.properties {
                        if let Some(node) = store.get_node_mut(node_id) {
                            for (key, value) in props {
                                node.set_property(key.clone(), value.clone());
                            }
                        }
                    }

                    // Track variable for edge creation
                    if let Some(ref var) = path.start.variable {
                        created_nodes.insert(var.clone(), node_id);
                    }

                    Some(node_id)
                } else {
                    None
                };

                // Create segments (edges and nodes)
                let mut prev_node_id = start_node_id;
                for segment in &path.segments {
                    // Create target node
                    let target_node_id = if !segment.node.labels.is_empty() {
                        let label = &segment.node.labels[0];
                        let node_id = store.create_node(label.clone());

                        // Set properties if any
                        if let Some(ref props) = segment.node.properties {
                            if let Some(node) = store.get_node_mut(node_id) {
                                for (key, value) in props {
                                    node.set_property(key.clone(), value.clone());
                                }
                            }
                        }

                        // Track variable
                        if let Some(ref var) = segment.node.variable {
                            created_nodes.insert(var.clone(), node_id);
                        }

                        Some(node_id)
                    } else {
                        None
                    };

                    // Create edge if both nodes exist
                    if let (Some(src), Some(tgt)) = (prev_node_id, target_node_id) {
                        if !segment.edge.types.is_empty() {
                            let edge_type = &segment.edge.types[0];
                            let _ = store.create_edge(src, tgt, edge_type.clone());
                        }
                    }

                    prev_node_id = target_node_id;
                }
            }

            // Return empty result for CREATE
            return Ok(RecordBatch {
                records: vec![],
                columns: vec!["created".to_string()],
            });
        }

        // If there's also a MATCH clause, use the normal executor path
        if !query.match_clauses.is_empty() {
            let executor = QueryExecutor::new(store);
            return Ok(executor.execute(&query)?);
        }

        Err("Query must have either MATCH or CREATE clause".into())
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphStore, Label};

    #[test]
    fn test_query_engine_creation() {
        let engine = QueryEngine::new();
        drop(engine);
    }

    #[test]
    fn test_end_to_end_simple_query() {
        let mut store = GraphStore::new();

        // Create test data
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 25i64);
        }

        // Execute query
        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.columns.len(), 1);
        assert_eq!(batch.columns[0], "n");
    }

    #[test]
    fn test_query_with_filter() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 25i64);
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) WHERE n.age > 28 RETURN n", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1); // Only Alice
    }

    #[test]
    fn test_query_with_limit() {
        let mut store = GraphStore::new();

        for i in 0..10 {
            let node = store.create_node("Person");
            if let Some(n) = store.get_node_mut(node) {
                n.set_property("id", i as i64);
            }
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n LIMIT 5", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 5);
    }

    #[test]
    fn test_query_with_edge_traversal() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        store.create_edge(alice, bob, "KNOWS").unwrap();

        let engine = QueryEngine::new();
        let result = engine.execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
            &store
        );

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.columns.len(), 2);
    }

    #[test]
    fn test_property_projection() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n.name, n.age", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.columns.len(), 2);
        assert_eq!(batch.columns[0], "n.name");
        assert_eq!(batch.columns[1], "n.age");
    }
}
