//! Query execution engine using Volcano iterator model
//!
//! Implements REQ-CYPHER-009 (query optimization) and ADR-007

pub mod operator;
pub mod planner;
pub mod record;

// Export operators - added CreateNodeOperator, CreateEdgeOperator, CartesianProductOperator for CREATE support
pub use operator::{PhysicalOperator, OperatorBox, OperatorDescription, CreateNodeOperator, CreateEdgeOperator, MatchCreateEdgeOperator, CartesianProductOperator};
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

        // Handle EXPLAIN - return plan description instead of executing
        if query.explain {
            return Ok(Self::explain_plan_with_stats(&plan, Some(self.store)));
        }

        // Check if this is a write query - if so, error out
        if plan.is_write {
            return Err(ExecutionError::RuntimeError(
                "Cannot execute write query with read-only executor. Use MutQueryExecutor instead.".to_string()
            ));
        }

        // Execute the plan
        self.execute_plan(plan)
    }

    /// Generate EXPLAIN output from an execution plan, optionally with graph statistics
    fn explain_plan(plan: &ExecutionPlan) -> RecordBatch {
        Self::explain_plan_with_stats(plan, None)
    }

    fn explain_plan_with_stats(plan: &ExecutionPlan, store: Option<&GraphStore>) -> RecordBatch {
        use crate::graph::PropertyValue;

        let description = plan.root.describe();
        let mut plan_text = description.format(0);

        // Append statistics summary if store is available
        if let Some(store) = store {
            let stats = store.compute_statistics();
            plan_text.push_str("\n--- Statistics ---\n");
            plan_text.push_str(&stats.format());
        }

        let mut record = Record::new();
        record.bind("plan".to_string(), Value::Property(PropertyValue::String(plan_text)));

        RecordBatch {
            records: vec![record],
            columns: vec!["plan".to_string()],
        }
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

        // Handle EXPLAIN - return plan description instead of executing
        if query.explain {
            let store_ref: &GraphStore = self.store;
            return Ok(QueryExecutor::explain_plan_with_stats(&plan, Some(store_ref)));
        }

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

    #[test]
    fn test_execute_exists_subquery() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        let company = store.create_node("Company");
        if let Some(node) = store.get_node_mut(company) {
            node.set_property("name", "Acme");
        }

        // Alice works at Acme, Bob doesn't
        store.create_edge(alice, company, "WORKS_AT").unwrap();

        let query = parse_query(
            "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:WORKS_AT]->(:Company) } RETURN n.name"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "EXISTS query failed: {:?}", result.err());
        let batch = result.unwrap();
        // Only Alice has a WORKS_AT edge
        assert_eq!(batch.records.len(), 1, "Expected 1 result from EXISTS, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_is_null() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("email", "alice@example.com");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            // Bob has no email
        }

        // IS NULL - should return Bob
        let query = parse_query(
            "MATCH (n:Person) WHERE n.email IS NULL RETURN n.name"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "IS NULL query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1, "Expected 1 result from IS NULL, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_is_not_null() {
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

        // IS NOT NULL - should return Alice
        let query = parse_query(
            "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.name"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "IS NOT NULL query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1, "Expected 1 result from IS NOT NULL, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_foreach_set() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("scores", PropertyValue::Array(vec![
                PropertyValue::Integer(90),
                PropertyValue::Integer(85),
            ]));
        }

        let query = parse_query(
            "MATCH (n:Person) FOREACH (s IN n.scores | SET n.processed = TRUE)"
        ).unwrap();
        let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
        let result = executor.execute(&query);
        assert!(result.is_ok(), "FOREACH query failed: {:?}", result.err());

        // The node should have been processed
        let node = store.get_node(alice).unwrap();
        assert_eq!(
            node.properties.get("processed"),
            Some(&PropertyValue::Boolean(true))
        );
    }

    #[test]
    fn test_execute_list_comprehension() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("scores", PropertyValue::Array(vec![
                PropertyValue::Integer(10),
                PropertyValue::Integer(20),
                PropertyValue::Integer(30),
            ]));
        }

        // Simple list comprehension: [x IN n.scores | x]
        let query = parse_query(
            "MATCH (n:Person) RETURN [x IN n.scores | x]"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "List comprehension query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
    }

    #[test]
    fn test_execute_multiple_match_patterns() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        let acme = store.create_node("Company");
        if let Some(node) = store.get_node_mut(acme) {
            node.set_property("name", "Acme");
        }

        // Multiple MATCH with comma-separated patterns
        let query = parse_query(
            "MATCH (p:Person), (c:Company) RETURN p.name, c.name"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "Multi-pattern MATCH failed: {:?}", result.err());
        let batch = result.unwrap();
        // 2 persons × 1 company = 2 results
        assert_eq!(batch.records.len(), 2, "Expected 2 results from multi-pattern, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_optional_match() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        let company = store.create_node("Company");
        if let Some(node) = store.get_node_mut(company) {
            node.set_property("name", "Acme");
        }

        // Only Alice works at a company
        store.create_edge(alice, company, "WORKS_AT").unwrap();

        // OPTIONAL MATCH - both persons should be returned
        let query = parse_query(
            "MATCH (n:Person) OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company) RETURN n.name"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "OPTIONAL MATCH failed: {:?}", result.err());
        let batch = result.unwrap();
        // Both Alice and Bob should be in results
        assert_eq!(batch.records.len(), 2, "Expected 2 results from OPTIONAL MATCH, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_with_clause_passthrough() {
        // WITH clause is parsed and passes through MATCH results
        // Full WITH projection support is planned for a future release
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

        // WITH n passes through the variable binding
        let query = parse_query(
            "MATCH (n:Person) WITH n RETURN n.name"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "WITH clause query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 2);
    }

    #[test]
    fn test_execute_delete() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        assert_eq!(store.get_nodes_by_label(&Label::new("Person")).len(), 2);

        let query = parse_query(
            "MATCH (n:Person) WHERE n.name = 'Alice' DELETE n"
        ).unwrap();
        let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
        let result = executor.execute(&query);
        assert!(result.is_ok(), "DELETE query failed: {:?}", result.err());

        // Alice should be deleted
        assert_eq!(store.get_nodes_by_label(&Label::new("Person")).len(), 1);
    }

    #[test]
    fn test_execute_set_property() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let query = parse_query(
            "MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31"
        ).unwrap();
        let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
        let result = executor.execute(&query);
        assert!(result.is_ok(), "SET query failed: {:?}", result.err());

        let node = store.get_node(alice).unwrap();
        assert_eq!(node.properties.get("age"), Some(&PropertyValue::Integer(31)));
    }

    #[test]
    fn test_execute_remove_property() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("temp", "temporary");
        }

        assert!(store.get_node(alice).unwrap().properties.contains_key("temp"));

        let query = parse_query(
            "MATCH (n:Person) WHERE n.name = 'Alice' REMOVE n.temp"
        ).unwrap();
        let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
        let result = executor.execute(&query);
        assert!(result.is_ok(), "REMOVE query failed: {:?}", result.err());

        let node = store.get_node(alice).unwrap();
        assert!(!node.properties.contains_key("temp"));
    }

    #[test]
    fn test_execute_arithmetic() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
            node.set_property("bonus", 5i64);
        }

        let query = parse_query(
            "MATCH (n:Person) RETURN n.age + n.bonus"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "Arithmetic query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
    }

    #[test]
    fn test_execute_string_functions() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let query = parse_query(
            "MATCH (n:Person) RETURN toUpper(n.name)"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "String function query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
        let val = batch.records[0].get("toUpper(n.name)").unwrap();
        assert_eq!(*val, Value::Property(PropertyValue::String("ALICE".to_string())));
    }

    #[test]
    fn test_execute_regex_match() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("email", "alice@example.com");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("email", "bob@test.org");
        }

        let query = parse_query(
            r#"MATCH (n:Person) WHERE n.email =~ ".*@example\.com" RETURN n.name"#
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "Regex query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1, "Expected 1 regex match, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_in_operator() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        let charlie = store.create_node("Person");
        if let Some(node) = store.get_node_mut(charlie) {
            node.set_property("name", "Charlie");
        }

        let query = parse_query(
            r#"MATCH (n:Person) WHERE n.name IN ["Alice", "Charlie"] RETURN n.name"#
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "IN operator query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 2, "Expected 2 IN results, got {}", batch.records.len());
    }

    #[test]
    fn test_execute_skip_and_limit() {
        let mut store = GraphStore::new();

        for i in 0..10 {
            let id = store.create_node("Person");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Person{}", i));
                node.set_property("idx", i as i64);
            }
        }

        // SKIP 3 LIMIT 2
        let query = parse_query(
            "MATCH (n:Person) RETURN n.name SKIP 3 LIMIT 2"
        ).unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "SKIP/LIMIT query failed: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 2, "Expected 2 results from SKIP 3 LIMIT 2, got {}", batch.records.len());
    }

    #[test]
    fn test_explain_simple_scan() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        // EXPLAIN should return the plan, not execute the query
        let query = parse_query("EXPLAIN MATCH (n:Person) WHERE n.age > 30 RETURN n.name").unwrap();
        assert!(query.explain);

        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "EXPLAIN query failed: {:?}", result.err());

        let batch = result.unwrap();
        assert_eq!(batch.columns, vec!["plan".to_string()]);
        assert_eq!(batch.records.len(), 1);

        // The plan should contain operator names
        if let Some(Value::Property(PropertyValue::String(plan_text))) = batch.records[0].get("plan") {
            assert!(plan_text.contains("Project"), "Plan should contain Project operator, got: {}", plan_text);
            assert!(plan_text.contains("Filter"), "Plan should contain Filter operator, got: {}", plan_text);
            assert!(plan_text.contains("NodeScan"), "Plan should contain NodeScan operator, got: {}", plan_text);
        } else {
            panic!("Expected plan text in result");
        }
    }

    #[test]
    fn test_explain_traversal() {
        let store = GraphStore::new();

        let query = parse_query("EXPLAIN MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "EXPLAIN traversal failed: {:?}", result.err());

        let batch = result.unwrap();
        if let Some(Value::Property(PropertyValue::String(plan_text))) = batch.records[0].get("plan") {
            assert!(plan_text.contains("Expand"), "Plan should contain Expand operator, got: {}", plan_text);
            assert!(plan_text.contains("NodeScan"), "Plan should contain NodeScan operator, got: {}", plan_text);
        } else {
            panic!("Expected plan text in result");
        }
    }

    #[test]
    fn test_explain_aggregation() {
        let store = GraphStore::new();

        let query = parse_query("EXPLAIN MATCH (n:Person) RETURN n.dept, count(n)").unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query);
        assert!(result.is_ok(), "EXPLAIN aggregation failed: {:?}", result.err());

        let batch = result.unwrap();
        if let Some(Value::Property(PropertyValue::String(plan_text))) = batch.records[0].get("plan") {
            assert!(plan_text.contains("Aggregate"), "Plan should contain Aggregate operator, got: {}", plan_text);
        } else {
            panic!("Expected plan text in result");
        }
    }

    #[test]
    fn test_explain_with_statistics() {
        let mut store = GraphStore::new();

        // Create some data so statistics are meaningful
        for i in 0..50 {
            let id = store.create_node("Person");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Person{}", i));
                node.set_property("age", (20 + i % 60) as i64);
            }
        }
        for i in 0..10 {
            let id = store.create_node("Company");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Company{}", i));
            }
        }

        let query = parse_query("EXPLAIN MATCH (n:Person) RETURN n.name").unwrap();
        let executor = QueryExecutor::new(&store);
        let result = executor.execute(&query).unwrap();

        if let Some(Value::Property(PropertyValue::String(plan_text))) = result.records[0].get("plan") {
            // Should contain statistics section
            assert!(plan_text.contains("Statistics"), "Plan should contain Statistics, got: {}", plan_text);
            assert!(plan_text.contains("Person"), "Statistics should mention Person label, got: {}", plan_text);
            assert!(plan_text.contains("Company"), "Statistics should mention Company label, got: {}", plan_text);
        } else {
            panic!("Expected plan text in result");
        }
    }

    #[test]
    fn test_graph_statistics() {
        let mut store = GraphStore::new();

        for i in 0..100 {
            let id = store.create_node("Person");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Person{}", i));
                node.set_property("city", if i % 2 == 0 { "NYC" } else { "LA" });
            }
        }
        for i in 0..20 {
            let id = store.create_node("Company");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Company{}", i));
            }
        }

        let stats = store.compute_statistics();
        assert_eq!(stats.total_nodes, 120);
        assert_eq!(*stats.label_counts.get(&Label::new("Person")).unwrap(), 100);
        assert_eq!(*stats.label_counts.get(&Label::new("Company")).unwrap(), 20);
        assert_eq!(stats.estimate_label_scan(&Label::new("Person")), 100);
        assert_eq!(stats.estimate_label_scan(&Label::new("Company")), 20);

        // Selectivity for "city" on Person — should be ~0.5 (2 distinct values)
        let city_sel = stats.estimate_equality_selectivity(&Label::new("Person"), "city");
        assert!(city_sel > 0.3 && city_sel < 0.7, "City selectivity should be ~0.5, got {}", city_sel);
    }

    #[test]
    fn test_cross_type_numeric_comparison() {
        let mut store = GraphStore::new();
        // Create nodes with Float properties
        for i in 0..5 {
            let id = store.create_node("Sensor");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Sensor{}", i));
                node.set_property("value", PropertyValue::Float(10.0 * (i as f64) + 5.0));
            }
        }
        // Query: compare Float property to Integer literal (value > 20)
        let executor = QueryExecutor::new(&store);
        let query = parse_query("MATCH (s:Sensor) WHERE s.value > 20 RETURN s.name").unwrap();
        let result = executor.execute(&query).unwrap();
        // Sensor0=5.0, Sensor1=15.0, Sensor2=25.0, Sensor3=35.0, Sensor4=45.0
        assert_eq!(result.records.len(), 3, "Expected 3 sensors with value > 20");
    }

    #[test]
    fn test_cross_type_string_boolean_eq() {
        let mut store = GraphStore::new();
        for i in 0..4 {
            let id = store.create_node("Item");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Item{}", i));
                node.set_property("active", PropertyValue::Boolean(i % 2 == 0));
            }
        }
        // Query: compare Boolean property to String literal 'true'
        let executor = QueryExecutor::new(&store);
        let query = parse_query("MATCH (i:Item) WHERE i.active = 'true' RETURN i.name").unwrap();
        let result = executor.execute(&query).unwrap();
        // Item0=true, Item1=false, Item2=true, Item3=false
        assert_eq!(result.records.len(), 2, "Expected 2 active items");
    }
}