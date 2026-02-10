//! Query execution engine using Volcano iterator model
//!
//! Implements REQ-CYPHER-009 (query optimization) and ADR-007

pub mod operator;
pub mod planner;
pub mod record;

// Export operators - added CreateNodeOperator, CreateEdgeOperator, CartesianProductOperator for CREATE support
pub use operator::{PhysicalOperator, OperatorBox, CreateNodeOperator, CreateEdgeOperator, MatchCreateEdgeOperator, CartesianProductOperator};
pub use planner::{QueryPlanner, ExecutionPlan};
pub use record::{Record, RecordBatch, Value};

use crate::graph::GraphStore;
use crate::query::ast::Query;
use thiserror::Error;

/// Execution errors
#[derive(Error, Debug)]
pub enum ExecutionError {
    /// Graph store error
    #[error("Graph error: {0}")]
    GraphError(String),

    /// Planning error
    #[error("Planning error: {0}")]
    PlanningError(String),

    /// Runtime error
    #[error("Runtime error: {0}")]
    RuntimeError(String),

    /// Type error
    #[error("Type error: {0}")]
    TypeError(String),

    /// Variable not found
    #[error("Variable not found: {0}")]
    VariableNotFound(String),
}

pub type ExecutionResult<T> = Result<T, ExecutionError>;

/// Query executor for read-only queries (MATCH, RETURN, etc.)
pub struct QueryExecutor<'a> {
    store: &'a GraphStore,
    planner: QueryPlanner,
}

impl<'a> QueryExecutor<'a> {
    /// Create a new query executor
    pub fn new(store: &'a GraphStore) -> Self {
        Self {
            store,
            planner: QueryPlanner::new(),
        }
    }

    /// Execute a read-only query and return results
    pub fn execute(&self, query: &Query) -> ExecutionResult<RecordBatch> {
        // Plan the query
        let plan = self.planner.plan(query, self.store)?;

        // Check if this is a write query - if so, error out
        if plan.is_write {
            return Err(ExecutionError::RuntimeError(
                "Cannot execute write query with read-only executor. Use MutQueryExecutor instead.".to_string()
            ));
        }

        // Execute the plan
        self.execute_plan(plan)
    }

    fn execute_plan(&self, mut plan: ExecutionPlan) -> ExecutionResult<RecordBatch> {
        let mut records = Vec::new();
        let batch_size = 1024;

        // Pull records from the root operator in batches (Vectorized Execution)
        while let Some(batch) = plan.root.next_batch(self.store, batch_size)? {
            records.extend(batch.records);
        }

        Ok(RecordBatch {
            records,
            columns: plan.output_columns,
        })
    }
}

/// Query executor for write queries (CREATE, DELETE, SET, etc.)
/// Takes mutable reference to GraphStore to allow modifications
pub struct MutQueryExecutor<'a> {
    store: &'a mut GraphStore,
    planner: QueryPlanner,
    tenant_id: String,
}

impl<'a> MutQueryExecutor<'a> {
    /// Create a new mutable query executor for write operations
    pub fn new(store: &'a mut GraphStore, tenant_id: String) -> Self {
        Self {
            store,
            planner: QueryPlanner::new(),
            tenant_id,
        }
    }

    /// Execute a query (read or write) and return results
    /// For CREATE queries, nodes/edges are created in the graph store
    pub fn execute(&mut self, query: &Query) -> ExecutionResult<RecordBatch> {
        // Plan the query (need immutable borrow temporarily)
        let plan = {
            let store_ref: &GraphStore = self.store;
            self.planner.plan(query, store_ref)?
        };

        // Execute the plan with mutable access
        self.execute_plan_mut(plan)
    }

    fn execute_plan_mut(&mut self, mut plan: ExecutionPlan) -> ExecutionResult<RecordBatch> {
        let mut records = Vec::new();
        let batch_size = 1024;

        // Pull records from the root operator in batches
        // Use next_batch_mut to allow operators to modify the graph store
        while let Some(batch) = plan.root.next_batch_mut(self.store, &self.tenant_id, batch_size)? {
            records.extend(batch.records);
        }

        Ok(RecordBatch {
            records,
            columns: plan.output_columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Label, PropertyValue};
    use crate::query::parser::parse_query;

    #[test]
    fn test_executor_creation() {
        let store = GraphStore::new();
        let executor = QueryExecutor::new(&store);
        // Executor should be created successfully
        drop(executor);
    }

    #[test]
    fn test_execute_simple_scan() {
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
        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 2);
    }

    #[test]
    fn test_execute_is_not_null_filter() {
        let mut store = GraphStore::new();

        // Alice has email, Bob does not
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("email", "alice@example.com");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            // no email property
        }

        // IS NOT NULL should return only Alice
        let query = parse_query("MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.name").unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "IS NOT NULL query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1, "Expected 1 result, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_is_null_filter() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("email", "alice@example.com");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        // IS NULL should return only Bob (no email)
        let query = parse_query("MATCH (n:Person) WHERE n.email IS NULL RETURN n.name").unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "IS NULL query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1, "Expected 1 result, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_case_expression() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 25i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 15i64);
        }

        // CASE WHEN expression
        let query = parse_query(
            "MATCH (n:Person) RETURN n.name, CASE WHEN n.age > 18 THEN \"adult\" ELSE \"minor\" END AS category"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "CASE query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 2);
    }

    #[test]
    fn test_execute_collect_aggregate() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("dept", "Engineering");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("dept", "Engineering");
        }

        // COLLECT aggregate
        let query = parse_query(
            "MATCH (n:Person) RETURN collect(n.name) AS names"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "COLLECT query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
    }

    #[test]
    fn test_execute_merge_create() {
        let mut store = GraphStore::new();

        // MERGE should create the node since it doesn't exist
        let query = parse_query(r#"MERGE (n:Person {name: "Alice"})"#).unwrap();
        let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
        let result = executor.execute(&query);
        assert!(result.is_ok(), "MERGE create failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);

        // Verify node was created
        let nodes = store.get_nodes_by_label(&Label::new("Person"));
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn test_execute_merge_match() {
        let mut store = GraphStore::new();

        // Pre-create node
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        // MERGE should match existing node, not create a new one
        let query = parse_query(r#"MERGE (n:Person {name: "Alice"})"#).unwrap();
        let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
        let result = executor.execute(&query);
        assert!(result.is_ok(), "MERGE match failed: {:?}", result.err());

        // Should still be 1 node
        let nodes = store.get_nodes_by_label(&Label::new("Person"));
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn test_execute_unwind() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("tags", PropertyValue::Array(vec![
                PropertyValue::String("dev".to_string()),
                PropertyValue::String("rust".to_string()),
            ]));
        }

        // UNWIND the tags array
        let query = parse_query(
            "MATCH (n:Person) UNWIND n.tags AS tag RETURN tag"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "UNWIND query failed: {:?}", result.err());
        let batch = result.unwrap();
        // Should get 2 rows (one per tag)
        assert_eq!(batch.records.len(), 2, "Expected 2 rows from UNWIND, got {}", batch.records.len());
    }
}