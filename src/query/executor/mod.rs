//! Query execution engine using Volcano iterator model
//!
//! Implements REQ-CYPHER-009 (query optimization) and ADR-007

pub mod operator;
pub mod planner;
pub mod record;

// Export operators - added CreateNodeOperator and CreateEdgeOperator for CREATE support
pub use operator::{PhysicalOperator, OperatorBox, CreateNodeOperator, CreateEdgeOperator};
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

        // Pull records from the root operator (read-only)
        while let Some(record) = plan.root.next(self.store)? {
            records.push(record);
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
}

impl<'a> MutQueryExecutor<'a> {
    /// Create a new mutable query executor for write operations
    pub fn new(store: &'a mut GraphStore) -> Self {
        Self {
            store,
            planner: QueryPlanner::new(),
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

        // Pull records from the root operator
        // Use next_mut() which allows operators to modify the graph store
        while let Some(record) = plan.root.next_mut(self.store)? {
            records.push(record);
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
    use crate::graph::Label;
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
}
