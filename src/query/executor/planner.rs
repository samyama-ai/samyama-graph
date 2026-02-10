//! Query planner - converts AST to execution plan
//!
//! Implements basic query optimization (REQ-CYPHER-009)

use crate::graph::GraphStore;
use crate::graph::{Label, PropertyValue};  // Added for CREATE support
use crate::query::ast::*;
use crate::query::executor::{
    ExecutionError, ExecutionResult, OperatorBox,
    // Added CreateNodeOperator and CreateNodesAndEdgesOperator for CREATE statement support
    operator::{NodeScanOperator, FilterOperator, ExpandOperator, ProjectOperator, LimitOperator, SkipOperator, CreateNodeOperator, CreateNodesAndEdgesOperator, CartesianProductOperator, VectorSearchOperator, JoinOperator, CreateVectorIndexOperator, CreateIndexOperator, AlgorithmOperator, IndexScanOperator, AggregateOperator, AggregateType, AggregateFunction, SortOperator, DeleteOperator, SetPropertyOperator, RemovePropertyOperator, UnwindOperator, MergeOperator, ForeachOperator},
};
use crate::graph::EdgeType;  // Added for CREATE edge support
use std::collections::{HashMap, HashSet};  // Added for CREATE properties and JOIN logic

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
        // Handle CREATE VECTOR INDEX
        if let Some(clause) = &query.create_vector_index_clause {
            return Ok(ExecutionPlan {
                root: Box::new(CreateVectorIndexOperator::new(
                    clause.label.clone(),
                    clause.property_key.clone(),
                    clause.dimensions,
                    clause.similarity.clone(),
                )),
                output_columns: vec![],
                is_write: true,
            });
        }

        // Handle CREATE INDEX
        if let Some(clause) = &query.create_index_clause {
            return Ok(ExecutionPlan {
                root: Box::new(CreateIndexOperator::new(
                    clause.label.clone(),
                    clause.property.clone(),
                )),
                output_columns: vec![],
                is_write: true,
            });
        }

        // Handle MERGE-only statement (no MATCH needed)
        if query.match_clauses.is_empty() && query.call_clause.is_none() {
            if let Some(merge_clause) = &query.merge_clause {
                let on_create: Vec<(String, String, Expression)> = merge_clause.on_create_set.iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();
                let on_match: Vec<(String, String, Expression)> = merge_clause.on_match_set.iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();

                let mut operator: OperatorBox = Box::new(MergeOperator::new(
                    merge_clause.pattern.clone(),
                    on_create,
                    on_match,
                ));

                let mut output_columns = Vec::new();
                if let Some(return_clause) = &query.return_clause {
                    let projections: Vec<(Expression, String)> = return_clause.items.iter().enumerate().map(|(i, item)| {
                        let alias = item.alias.clone().unwrap_or_else(|| match &item.expression {
                            Expression::Variable(v) => v.clone(),
                            Expression::Property { variable, property } => format!("{}.{}", variable, property),
                            _ => format!("col_{}", i),
                        });
                        output_columns.push(alias.clone());
                        (item.expression.clone(), alias)
                    }).collect();
                    operator = Box::new(ProjectOperator::new(operator, projections));
                }

                return Ok(ExecutionPlan {
                    root: operator,
                    output_columns,
                    is_write: true,
                });
            }
        }

        // Handle CREATE-only queries (no MATCH/CALL required)
        if query.match_clauses.is_empty() && query.call_clause.is_none() {
            if let Some(create_clause) = &query.create_clause {
                return self.plan_create_only(create_clause);
            }
            return Err(ExecutionError::PlanningError(
                "Query must have at least one MATCH, CALL or CREATE clause".to_string()
            ));
        }

        let mut operator: Option<OperatorBox> = None;

        // 1. Handle MATCH clauses — combine multiple with CartesianProduct
        for match_clause in &query.match_clauses {
            let match_op = self.plan_match(match_clause, query.where_clause.as_ref(), _store)?;
            operator = Some(match operator {
                Some(existing) => Box::new(CartesianProductOperator::new(existing, match_op)) as OperatorBox,
                None => match_op,
            });
        }

        // 2. Handle CALL if present
        if let Some(call_clause) = &query.call_clause {
            let call_op = self.plan_call(call_clause)?;
            if let Some(existing_op) = operator {
                // Check for shared variables to decide between Join and Cartesian Product
                let mut shared_vars = Vec::new();
                
                // Collect variables from all MATCH clauses
                let mut match_vars = HashSet::new();
                for mc in &query.match_clauses {
                    for path in &mc.pattern.paths {
                        if let Some(v) = &path.start.variable { match_vars.insert(v.clone()); }
                        for seg in &path.segments {
                            if let Some(v) = &seg.node.variable { match_vars.insert(v.clone()); }
                            if let Some(v) = &seg.edge.variable { match_vars.insert(v.clone()); }
                        }
                    }
                }

                // Check against CALL yield items
                for item in &call_clause.yield_items {
                    let var_name = item.alias.as_ref().unwrap_or(&item.name);
                    if match_vars.contains(var_name) {
                        shared_vars.push(var_name.clone());
                    }
                }

                if !shared_vars.is_empty() {
                    // Use JoinOperator on the first shared variable
                    operator = Some(Box::new(JoinOperator::new(existing_op, call_op, shared_vars[0].clone())));
                } else {
                    // Fallback to Cartesian Product
                    operator = Some(Box::new(CartesianProductOperator::new(existing_op, call_op)));
                }
            } else {
                operator = Some(call_op);
            }
        }

        let mut operator = operator.unwrap();

        // Add WHERE clause if present
        if let Some(where_clause) = &query.where_clause {
            operator = Box::new(FilterOperator::new(operator, where_clause.predicate.clone()));
        }

        // Add UNWIND clause if present
        if let Some(unwind_clause) = &query.unwind_clause {
            operator = Box::new(UnwindOperator::new(
                operator,
                unwind_clause.expression.clone(),
                unwind_clause.variable.clone(),
            ));
        }

        // Determine output columns
        let mut output_columns = Vec::new();

        // Check if this is a MATCH...CREATE query (create edges between matched nodes)
        let is_write = if let Some(create_clause) = &query.create_clause {
            // Extract edge creation info from CREATE pattern
            // Example: MATCH (a:Trial), (b:Condition) CREATE (a)-[:STUDIES]->(b)
            let create_pattern = &create_clause.pattern;

            // Collect edges to create from the CREATE pattern
            let mut edges_to_create: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)> = Vec::new();

            for path in &create_pattern.paths {
                let mut current_var = path.start.variable.clone();

                for segment in &path.segments {
                    let target_var = segment.node.variable.clone();
                    let edge = &segment.edge;
                    let edge_type = edge.types.first()
                        .cloned()
                        .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                    let edge_properties = edge.properties.clone().unwrap_or_default();
                    let edge_variable = edge.variable.clone();

                    if let (Some(src), Some(tgt)) = (&current_var, &target_var) {
                        edges_to_create.push((
                            src.clone(),
                            tgt.clone(),
                            edge_type,
                            edge_properties,
                            edge_variable,
                        ));
                    }

                    current_var = target_var;
                }
            }

            // Wrap the match operator with edge creation
            if !edges_to_create.is_empty() {
                use crate::query::executor::operator::MatchCreateEdgeOperator;
                operator = Box::new(MatchCreateEdgeOperator::new(operator, edges_to_create));
            }

            true // This is a write query
        } else {
            false
        };

        // Handle DELETE clause
        let is_write = if let Some(delete_clause) = &query.delete_clause {
            let vars: Vec<String> = delete_clause.expressions.iter().filter_map(|e| {
                if let Expression::Variable(v) = e { Some(v.clone()) } else { None }
            }).collect();
            operator = Box::new(DeleteOperator::new(operator, vars, delete_clause.detach));
            true
        } else {
            is_write
        };

        // Handle SET clauses
        let is_write = if !query.set_clauses.is_empty() {
            let mut items = Vec::new();
            for set_clause in &query.set_clauses {
                for item in &set_clause.items {
                    items.push((item.variable.clone(), item.property.clone(), item.value.clone()));
                }
            }
            operator = Box::new(SetPropertyOperator::new(operator, items));
            true
        } else {
            is_write
        };

        // Handle REMOVE clauses
        let is_write = if !query.remove_clauses.is_empty() {
            let mut items = Vec::new();
            for remove_clause in &query.remove_clauses {
                for item in &remove_clause.items {
                    if let RemoveItem::Property { variable, property } = item {
                        items.push((variable.clone(), property.clone()));
                    }
                }
            }
            if !items.is_empty() {
                operator = Box::new(RemovePropertyOperator::new(operator, items));
            }
            true
        } else {
            is_write
        };

        // Handle FOREACH clause
        let is_write = if let Some(foreach_clause) = &query.foreach_clause {
            let mut set_items = Vec::new();
            for set_clause in &foreach_clause.set_clauses {
                for item in &set_clause.items {
                    set_items.push((item.variable.clone(), item.property.clone(), item.value.clone()));
                }
            }
            let create_patterns: Vec<Pattern> = foreach_clause.create_clauses.iter()
                .map(|c| c.pattern.clone())
                .collect();
            operator = Box::new(ForeachOperator::new(
                operator,
                foreach_clause.variable.clone(),
                foreach_clause.expression.clone(),
                set_items,
                create_patterns,
            ));
            true
        } else {
            is_write
        };

        // Add RETURN clause if present
        if let Some(return_clause) = &query.return_clause {
            let mut aggregates = Vec::new();
            let mut group_by = Vec::new();
            let mut projections = Vec::new();
            let mut has_aggregation = false;

            for (idx, item) in return_clause.items.iter().enumerate() {
                let alias = item.alias.clone().unwrap_or_else(|| {
                    match &item.expression {
                        Expression::Variable(var) => var.clone(),
                        Expression::Property { variable, property } => format!("{}.{}", variable, property),
                        Expression::Function { name, args } => {
                            let arg_strs: Vec<String> = args.iter().map(|a| match a {
                                Expression::Variable(v) => v.clone(),
                                Expression::Property { variable, property } => format!("{}.{}", variable, property),
                                _ => "?".to_string(),
                            }).collect();
                            format!("{}({})", name, arg_strs.join(", "))
                        },
                        _ => format!("col_{}", idx),
                    }
                });

                output_columns.push(alias.clone());

                // Detect Aggregation
                let mut is_agg_func = false;
                if let Expression::Function { name, args } = &item.expression {
                    let func_type = match name.to_lowercase().as_str() {
                        "count" => Some(AggregateType::Count),
                        "sum" => Some(AggregateType::Sum),
                        "avg" => Some(AggregateType::Avg),
                        "min" => Some(AggregateType::Min),
                        "max" => Some(AggregateType::Max),
                        "collect" => Some(AggregateType::Collect),
                        _ => None,
                    };

                    if let Some(func) = func_type {
                        is_agg_func = true;
                        has_aggregation = true;
                        let arg_expr = args.first().cloned().unwrap_or(Expression::Literal(PropertyValue::Null));
                        aggregates.push(AggregateFunction {
                            func,
                            expr: arg_expr,
                            alias: alias.clone(),
                        });
                    }
                }

                if !is_agg_func {
                    group_by.push((item.expression.clone(), alias.clone()));
                    projections.push((item.expression.clone(), alias.clone()));
                }
            }

            if has_aggregation {
                operator = Box::new(AggregateOperator::new(operator, group_by, aggregates));
                
                // Sort after aggregation
                if let Some(order_by) = &query.order_by {
                    let mut sort_items = Vec::new();
                    for item in &order_by.items {
                        sort_items.push((item.expression.clone(), item.ascending));
                    }
                    operator = Box::new(SortOperator::new(operator, sort_items));
                }
            } else {
                // Non-aggregation: Sort -> Project
                if let Some(order_by) = &query.order_by {
                    let mut sort_items = Vec::new();
                    for item in &order_by.items {
                        sort_items.push((item.expression.clone(), item.ascending));
                    }
                    operator = Box::new(SortOperator::new(operator, sort_items));
                }
                
                operator = Box::new(ProjectOperator::new(operator, projections));
            }
        } else {
            // No explicit RETURN - return all matched/yielded variables
            for mc in &query.match_clauses {
                for path in &mc.pattern.paths {
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
            
            if let Some(call_clause) = &query.call_clause {
                for item in &call_clause.yield_items {
                    output_columns.push(item.alias.clone().unwrap_or_else(|| item.name.clone()));
                }
            }
        }

        // Add SKIP if present
        if let Some(skip) = query.skip {
            operator = Box::new(SkipOperator::new(operator, skip));
        }

        // Add LIMIT if present
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        // Return execution plan
        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write,
        })
    }

    fn plan_call(&self, call_clause: &CallClause) -> ExecutionResult<OperatorBox> {
        if call_clause.procedure_name == "db.index.vector.queryNodes" {
            // CALL db.index.vector.queryNodes(label, property, vector, k) YIELD node, score
            if call_clause.arguments.len() < 4 {
                return Err(ExecutionError::PlanningError(
                    "db.index.vector.queryNodes requires 4 arguments: (label, property, query_vector, k)".to_string()
                ));
            }

            let label = match &call_clause.arguments[0] {
                Expression::Literal(PropertyValue::String(s)) => s.clone(),
                _ => return Err(ExecutionError::PlanningError("First argument (label) must be a string literal".to_string())),
            };

            let property = match &call_clause.arguments[1] {
                Expression::Literal(PropertyValue::String(s)) => s.clone(),
                _ => return Err(ExecutionError::PlanningError("Second argument (property) must be a string literal".to_string())),
            };

            let query_vector = match &call_clause.arguments[2] {
                Expression::Literal(PropertyValue::Vector(v)) => v.clone(),
                _ => return Err(ExecutionError::PlanningError("Third argument (vector) must be a vector literal".to_string())),
            };

            let k = match &call_clause.arguments[3] {
                Expression::Literal(PropertyValue::Integer(i)) => *i as usize,
                _ => return Err(ExecutionError::PlanningError("Fourth argument (k) must be an integer literal".to_string())),
            };

            let mut node_var = "node".to_string();
            let mut score_var = None;

            for item in &call_clause.yield_items {
                if item.name == "node" {
                    node_var = item.alias.clone().unwrap_or_else(|| item.name.clone());
                } else if item.name == "score" {
                    score_var = Some(item.alias.clone().unwrap_or_else(|| item.name.clone()));
                }
            }

            Ok(Box::new(VectorSearchOperator::new(
                label,
                property,
                query_vector,
                k,
                node_var,
                score_var,
            )))
        } else if call_clause.procedure_name.starts_with("algo.") {
            Ok(Box::new(AlgorithmOperator::new(
                call_clause.procedure_name.clone(),
                call_clause.arguments.clone(),
            )))
        } else {
            Err(ExecutionError::PlanningError(format!("Unknown procedure: {}", call_clause.procedure_name)))
        }
    }

    fn plan_match(&self, match_clause: &MatchClause, where_clause: Option<&WhereClause>, store: &GraphStore) -> ExecutionResult<OperatorBox> {
        let pattern = &match_clause.pattern;

        if pattern.paths.is_empty() {
            return Err(ExecutionError::PlanningError("Match pattern has no paths".to_string()));
        }

        // Handle multiple paths with CartesianProductOperator
        // Example: MATCH (a:Trial), (b:Condition) -> CartesianProduct of two node scans
        let mut operators: Vec<OperatorBox> = Vec::new();

        for path in &pattern.paths {
            // Start with node scan for this path
            let start_var = path.start.variable.as_ref()
                .ok_or_else(|| ExecutionError::PlanningError("Start node must have a variable".to_string()))?
                .clone();

            // Optimization: Check for index usage
            let mut index_op: Option<OperatorBox> = None;
            if let Some(wc) = where_clause {
                // Simple case: WHERE n.prop OP literal
                // TODO: Handle AND chains
                if let Expression::Binary { left, op, right } = &wc.predicate {
                    if let (Expression::Property { variable, property }, Expression::Literal(val)) = (left.as_ref(), right.as_ref()) {
                        if variable == &start_var {
                            // Check if any label has an index on this property
                            for label in &path.start.labels {
                                if store.property_index.has_index(label, property) {
                                    // Found index!
                                    // Only support =, >, >=, <, <=
                                    match op {
                                        BinaryOp::Eq | BinaryOp::Gt | BinaryOp::Ge | BinaryOp::Lt | BinaryOp::Le => {
                                            index_op = Some(Box::new(IndexScanOperator::new(
                                                start_var.clone(),
                                                label.clone(),
                                                property.clone(),
                                                op.clone(),
                                                val.clone()
                                            )));
                                        },
                                        _ => {}
                                    }
                                    if index_op.is_some() { break; }
                                }
                            }
                        }
                    }
                }
            }

            let mut path_operator = index_op.unwrap_or_else(|| {
                Box::new(NodeScanOperator::new(
                    start_var.clone(),
                    path.start.labels.clone(),
                ))
            });

            // Add property filter for start node if properties are specified
            if let Some(ref props) = path.start.properties {
                if !props.is_empty() {
                    let filter_expr = self.build_property_filter(&start_var, props);
                    path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
                }
            }

            // Add expand operators for each segment in this path
            let mut current_var = start_var.clone();
            for segment in &path.segments {
                let target_var = segment.node.variable.as_ref()
                    .ok_or_else(|| ExecutionError::PlanningError("Target node must have a variable".to_string()))?
                    .clone();

                let edge_var = segment.edge.variable.clone();
                let edge_types: Vec<String> = segment.edge.types.iter()
                    .map(|t| t.as_str().to_string())
                    .collect();

                path_operator = Box::new(ExpandOperator::new(
                    path_operator,
                    current_var.clone(),
                    target_var.clone(),
                    edge_var,
                    edge_types,
                    segment.edge.direction.clone(),
                ));

                current_var = target_var;
            }

            operators.push(path_operator);
        }

        // Combine operators: single path returns directly, multiple paths use CartesianProduct
        let mut result = operators.remove(0);
        for op in operators {
            result = Box::new(CartesianProductOperator::new(result, op));
        }

        Ok(result)
    }

    /// Build a filter expression from node properties.
    /// Converts {name: "Alice", age: 30} into (n.name = "Alice" AND n.age = 30)
    fn build_property_filter(&self, var: &str, props: &HashMap<String, PropertyValue>) -> Expression {
        let mut conditions: Vec<Expression> = Vec::new();

        for (prop_name, prop_value) in props {
            let condition = Expression::Binary {
                left: Box::new(Expression::Property {
                    variable: var.to_string(),
                    property: prop_name.clone(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(Expression::Literal(prop_value.clone())),
            };
            conditions.push(condition);
        }

        // Combine with AND if multiple properties
        if conditions.len() == 1 {
            conditions.remove(0)
        } else {
            let mut result = conditions.remove(0);
            for condition in conditions {
                result = Expression::Binary {
                    left: Box::new(result),
                    op: BinaryOp::And,
                    right: Box::new(condition),
                };
            }
            result
        }
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
