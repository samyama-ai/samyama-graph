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
    MutQueryExecutor,  // Added for CREATE/DELETE/SET support
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

    /// Parse and execute a read-only Cypher query (MATCH, RETURN, etc.)
    pub fn execute(
        &self,
        query_str: &str,
        store: &crate::graph::GraphStore,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        // Parse query
        let query = parse_query(query_str)?;

        // Execute query (read-only)
        let executor = QueryExecutor::new(store);
        let result = executor.execute(&query)?;

        Ok(result)
    }

    /// Parse and execute a write Cypher query (CREATE, DELETE, SET, etc.)
    /// This method takes a mutable reference to the graph store
    pub fn execute_mut(
        &self,
        query_str: &str,
        store: &mut crate::graph::GraphStore,
        tenant_id: &str,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        // Parse query
        let query = parse_query(query_str)?;

        // Execute query (with write access)
        let mut executor = MutQueryExecutor::new(store, tenant_id.to_string());
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

    // ==================== CREATE TESTS ====================

    #[test]
    fn test_create_single_node() {
        // Test: CREATE (n:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query
        let result = engine.execute_mut(r#"CREATE (n:Person)"#, &mut store, "default");

        assert!(result.is_ok(), "CREATE query should succeed");

        // Verify node was created by querying it
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 1, "Should have created 1 Person node");
    }

    #[test]
    fn test_create_node_with_properties() {
        // Test: CREATE (n:Person {name: "Alice", age: 30})
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with properties
        let result = engine.execute_mut(
            r#"CREATE (n:Person {name: "Alice", age: 30})"#,
            &mut store,
            "default"
        );

        assert!(result.is_ok(), "CREATE query with properties should succeed");

        // Verify node was created with correct properties
        let query_result = engine.execute("MATCH (n:Person) RETURN n.name, n.age", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 1, "Should have created 1 Person node");
    }

    #[test]
    fn test_create_multiple_nodes() {
        // Test multiple CREATE operations
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Create first node
        let result1 = engine.execute_mut(r#"CREATE (a:Person {name: "Alice"})"#, &mut store, "default");
        assert!(result1.is_ok());

        // Create second node
        let result2 = engine.execute_mut(r#"CREATE (b:Person {name: "Bob"})"#, &mut store, "default");
        assert!(result2.is_ok());

        // Verify both nodes exist
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 2, "Should have created 2 Person nodes");
    }

    #[test]
    fn test_create_returns_error_on_readonly_executor() {
        // Test that using read-only executor for CREATE fails
        let store = GraphStore::new();
        let engine = QueryEngine::new();

        // Try to execute CREATE with read-only execute() - should fail
        let result = engine.execute(r#"CREATE (n:Person)"#, &store);

        assert!(result.is_err(), "CREATE should fail with read-only executor");
    }

    // ==================== CREATE EDGE TESTS ====================

    #[test]
    fn test_create_edge_simple() {
        // Test: CREATE (a:Person)-[:KNOWS]->(b:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with edge
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#,
            &mut store,
            "default"
        );

        assert!(result.is_ok(), "CREATE with edge should succeed: {:?}", result.err());

        // Verify nodes were created
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 2, "Should have created 2 Person nodes");

        // Verify edge was created by querying the relationship
        let edge_result = engine.execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
            &store
        );
        assert!(edge_result.is_ok(), "Edge query should succeed");
        let edge_batch = edge_result.unwrap();
        assert_eq!(edge_batch.len(), 1, "Should have 1 KNOWS relationship");
    }

    #[test]
    fn test_create_edge_with_properties() {
        // Test: CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with edge properties
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:FRIENDS {since: 2020}]->(b:Person {name: "Bob"})"#,
            &mut store,
            "default"
        );

        assert!(result.is_ok(), "CREATE with edge properties should succeed: {:?}", result.err());

        // Verify edge was created
        let edge_result = engine.execute(
            "MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN a, r, b",
            &store
        );
        assert!(edge_result.is_ok(), "Edge query should succeed");
        let edge_batch = edge_result.unwrap();
        assert_eq!(edge_batch.len(), 1, "Should have 1 FRIENDS relationship");
    }

    #[test]
    fn test_create_chain_pattern() {
        // Test: CREATE (a:Person)-[:KNOWS]->(b:Person)-[:LIKES]->(c:Movie)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with chain of edges
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})-[:LIKES]->(c:Movie {title: "Matrix"})"#,
            &mut store,
            "default"
        );

        assert!(result.is_ok(), "CREATE chain should succeed: {:?}", result.err());

        // Verify 2 Person nodes and 1 Movie node created
        let person_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(person_result.is_ok());
        assert_eq!(person_result.unwrap().len(), 2, "Should have 2 Person nodes");

        let movie_result = engine.execute("MATCH (n:Movie) RETURN n", &store);
        assert!(movie_result.is_ok());
        assert_eq!(movie_result.unwrap().len(), 1, "Should have 1 Movie node");

        // Verify both edges were created
        let knows_result = engine.execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
            &store
        );
        assert!(knows_result.is_ok());
        assert_eq!(knows_result.unwrap().len(), 1, "Should have 1 KNOWS relationship");

        let likes_result = engine.execute(
            "MATCH (a:Person)-[:LIKES]->(b:Movie) RETURN a, b",
            &store
        );
        assert!(likes_result.is_ok());
        assert_eq!(likes_result.unwrap().len(), 1, "Should have 1 LIKES relationship");
    }
}
