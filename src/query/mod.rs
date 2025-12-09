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

    /// Parse and execute a Cypher query
    pub fn execute(
        &self,
        query_str: &str,
        store: &crate::graph::GraphStore,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        // Parse query
        let query = parse_query(query_str)?;

        // Execute query
        let executor = QueryExecutor::new(store);
        let result = executor.execute(&query)?;

        Ok(result)
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
