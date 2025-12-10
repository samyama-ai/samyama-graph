//! Query planner - converts AST to execution plan
//!
//! Implements basic query optimization (REQ-CYPHER-009)

use crate::graph::GraphStore;
use crate::graph::{Label, PropertyValue};  // Added for CREATE support
use crate::query::ast::*;
use crate::query::executor::{
    ExecutionError, ExecutionResult, OperatorBox,
    // Added CreateNodeOperator and CreateNodesAndEdgesOperator for CREATE statement support
    operator::{NodeScanOperator, FilterOperator, ExpandOperator, ProjectOperator, LimitOperator, CreateNodeOperator, CreateNodesAndEdgesOperator},
};
use crate::graph::EdgeType;  // Added for CREATE edge support
use std::collections::HashMap;  // Added for CREATE properties

/// Execution plan - a tree of physical operators
pub struct ExecutionPlan {
    /// Root operator
    pub root: OperatorBox,
    /// Output column names
    pub output_columns: Vec<String>,
    /// Whether this plan contains write operations (CREATE/DELETE/SET)
    /// If true, executor must use next_mut() with mutable GraphStore
    pub is_write: bool,
}

/// Query planner
pub struct QueryPlanner {
    /// Enable optimization (future)
    _optimize: bool,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            _optimize: true,
        }
    }

    /// Plan a query
    pub fn plan(&self, query: &Query, _store: &GraphStore) -> ExecutionResult<ExecutionPlan> {
        // Handle CREATE-only queries (no MATCH required)
        // Example: CREATE (n:Person {name: "Alice"})
        if query.match_clauses.is_empty() {
            if let Some(create_clause) = &query.create_clause {
                // Route to CREATE-specific planning
                return self.plan_create_only(create_clause);
            }
            return Err(ExecutionError::PlanningError(
                "Query must have at least one MATCH or CREATE clause".to_string()
            ));
        }

        let match_clause = &query.match_clauses[0];

        // Build operator tree bottom-up
        let mut operator = self.plan_match(match_clause)?;

        // Add WHERE clause if present
        if let Some(where_clause) = &query.where_clause {
            operator = Box::new(FilterOperator::new(operator, where_clause.predicate.clone()));
        }

        // Determine output columns
        let mut output_columns = Vec::new();

        // Add RETURN clause if present
        if let Some(return_clause) = &query.return_clause {
            let projections = self.plan_return(return_clause, &mut output_columns)?;
            operator = Box::new(ProjectOperator::new(operator, projections));
        } else {
            // No explicit RETURN - return all matched variables
            // For now, extract variables from match clause
            let pattern = &match_clause.pattern;
            for path in &pattern.paths {
                if let Some(var) = &path.start.variable {
                    output_columns.push(var.clone());
                }
                for segment in &path.segments {
                    if let Some(var) = &segment.node.variable {
                        output_columns.push(var.clone());
                    }
                }
            }
        }

        // Add LIMIT if present
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        // Return execution plan
        // MATCH-only queries are read-only (is_write: false)
        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write: false,
        })
    }

    fn plan_match(&self, match_clause: &MatchClause) -> ExecutionResult<OperatorBox> {
        let pattern = &match_clause.pattern;

        if pattern.paths.is_empty() {
            return Err(ExecutionError::PlanningError("Match pattern has no paths".to_string()));
        }

        // For now, handle single path patterns
        let path = &pattern.paths[0];

        // Start with node scan
        let start_var = path.start.variable.as_ref()
            .ok_or_else(|| ExecutionError::PlanningError("Start node must have a variable".to_string()))?
            .clone();

        let mut operator: OperatorBox = Box::new(NodeScanOperator::new(
            start_var.clone(),
            path.start.labels.clone(),
        ));

        // Add expand operators for each segment
        for segment in &path.segments {
            let target_var = segment.node.variable.as_ref()
                .ok_or_else(|| ExecutionError::PlanningError("Target node must have a variable".to_string()))?
                .clone();

            let edge_var = segment.edge.variable.clone();

            let edge_types: Vec<String> = segment.edge.types.iter()
                .map(|t| t.as_str().to_string())
                .collect();

            operator = Box::new(ExpandOperator::new(
                operator,
                start_var.clone(),
                target_var.clone(),
                edge_var,
                edge_types,
                segment.edge.direction.clone(),
            ));

            // For multi-hop paths, update source variable
            // This is simplified - real implementation would handle this better
        }

        Ok(operator)
    }

    fn plan_return(&self, return_clause: &ReturnClause, output_columns: &mut Vec<String>) -> ExecutionResult<Vec<(Expression, String)>> {
        let mut projections = Vec::new();

        for (idx, item) in return_clause.items.iter().enumerate() {
            let alias = if let Some(alias) = &item.alias {
                alias.clone()
            } else {
                // Generate alias from expression
                match &item.expression {
                    Expression::Variable(var) => var.clone(),
                    Expression::Property { variable, property } => {
                        format!("{}.{}", variable, property)
                    }
                    Expression::Function { name, .. } => {
                        format!("{}_{}", name, idx)
                    }
                    _ => format!("col_{}", idx),
                }
            };

            output_columns.push(alias.clone());
            projections.push((item.expression.clone(), alias));
        }

        Ok(projections)
    }

    /// Plan a CREATE-only query (no MATCH clause)
    /// Supports:
    /// - CREATE (n:Person {name: "Alice", age: 30})
    /// - CREATE (a:Person)-[:KNOWS]->(b:Person)
    /// - CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
    fn plan_create_only(&self, create_clause: &CreateClause) -> ExecutionResult<ExecutionPlan> {
        let pattern = &create_clause.pattern;

        // Collect all nodes to create from the pattern
        // Each node has: (labels, properties, variable_name)
        let mut nodes_to_create: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>)> = Vec::new();
        let mut output_columns: Vec<String> = Vec::new();

        // Collect edges to create: (source_var, target_var, edge_type, properties, edge_var)
        let mut edges_to_create: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)> = Vec::new();

        for path in &pattern.paths {
            // Add start node
            let start = &path.start;
            let labels: Vec<Label> = start.labels.clone();
            let properties: HashMap<String, PropertyValue> = start.properties.clone().unwrap_or_default();
            let variable = start.variable.clone();

            // Track output column if variable exists
            if let Some(ref var) = variable {
                output_columns.push(var.clone());
            }

            nodes_to_create.push((labels, properties, variable.clone()));

            // Track current source variable for edge creation
            let mut current_source_var = variable;

            // Add nodes and edges from path segments (if any)
            // Example: CREATE (a:Person)-[:KNOWS]->(b:Person)
            for segment in &path.segments {
                let node = &segment.node;
                let node_labels: Vec<Label> = node.labels.clone();
                let node_properties: HashMap<String, PropertyValue> = node.properties.clone().unwrap_or_default();
                let node_variable = node.variable.clone();

                if let Some(ref var) = node_variable {
                    output_columns.push(var.clone());
                }

                nodes_to_create.push((node_labels, node_properties, node_variable.clone()));

                // Extract edge information
                let edge = &segment.edge;
                let edge_type = edge.types.first()
                    .cloned()
                    .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                let edge_properties: HashMap<String, PropertyValue> = edge.properties.clone().unwrap_or_default();
                let edge_variable = edge.variable.clone();

                // Create edge between source and target nodes
                // For CREATE, we need both variables to be defined
                if let (Some(source_var), Some(target_var)) = (&current_source_var, &node_variable) {
                    edges_to_create.push((
                        source_var.clone(),
                        target_var.clone(),
                        edge_type,
                        edge_properties,
                        edge_variable,
                    ));
                }

                // Update source variable for next segment
                current_source_var = node_variable;
            }
        }

        // Build the operator chain
        // First: CreateNodeOperator to create all nodes
        let node_operator: OperatorBox = Box::new(CreateNodeOperator::new(nodes_to_create));

        // If there are edges to create, chain CreateEdgeOperator
        let final_operator: OperatorBox = if edges_to_create.is_empty() {
            node_operator
        } else {
            // Create edges after nodes are created
            // We need a special combined operator that creates nodes first, then edges
            Box::new(CreateNodesAndEdgesOperator::new(node_operator, edges_to_create))
        };

        // Return execution plan with is_write: true (this mutates the graph)
        Ok(ExecutionPlan {
            root: final_operator,
            output_columns,
            is_write: true,
        })
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse_query;

    #[test]
    fn test_plan_simple_match() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let result = planner.plan(&query, &store);

        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 1);
        assert_eq!(plan.output_columns[0], "n");
    }

    #[test]
    fn test_plan_with_where() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WHERE n.age > 30 RETURN n").unwrap();
        let result = planner.plan(&query, &store);

        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_with_limit() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n LIMIT 10").unwrap();
        let result = planner.plan(&query, &store);

        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_with_edge() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        let result = planner.plan(&query, &store);

        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 2);
    }
}
