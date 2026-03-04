//! Query planner - converts AST to execution plan
//!
//! Implements basic query optimization (REQ-CYPHER-009)

use crate::graph::GraphStore;
use crate::graph::{Label, PropertyValue};  // Added for CREATE support
use crate::query::ast::*;
use std::sync::Mutex;
use crate::query::executor::{
    ExecutionError, ExecutionResult, OperatorBox,
    // Added CreateNodeOperator and CreateNodesAndEdgesOperator for CREATE statement support
    operator::{NodeScanOperator, FilterOperator, ExpandOperator, ProjectOperator, LimitOperator, SkipOperator, CreateNodeOperator, CreateNodesAndEdgesOperator, CartesianProductOperator, VectorSearchOperator, JoinOperator, LeftOuterJoinOperator, CreateVectorIndexOperator, CreateIndexOperator, CompositeCreateIndexOperator, CreateConstraintOperator, DropIndexOperator, ShowIndexesOperator, ShowConstraintsOperator, AlgorithmOperator, IndexScanOperator, AggregateOperator, AggregateType, AggregateFunction, SortOperator, DeleteOperator, SetPropertyOperator, RemovePropertyOperator, UnwindOperator, MergeOperator, ForeachOperator, ShortestPathOperator, WithBarrierOperator},
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

/// Simple plan cache entry storing planning metadata
struct PlanCacheEntry {
    /// Timestamp when entry was created
    created_at: std::time::Instant,
    /// Which index to use (if any): (label, property, op)
    index_hint: Option<(Label, String)>,
}

/// Query planner
pub struct QueryPlanner {
    /// Enable optimization
    _optimize: bool,
    /// Plan cache: query string hash → planning metadata
    plan_cache: Mutex<HashMap<u64, PlanCacheEntry>>,
    /// Cache generation counter (incremented on schema changes)
    cache_generation: std::sync::atomic::AtomicU64,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            _optimize: true,
            plan_cache: Mutex::new(HashMap::new()),
            cache_generation: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Invalidate the plan cache (e.g., after CREATE INDEX or schema change)
    pub fn invalidate_cache(&self) {
        self.cache_generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.plan_cache.lock().unwrap().clear();
    }

    /// Plan a query
    pub fn plan(&self, query: &Query, _store: &GraphStore) -> ExecutionResult<ExecutionPlan> {
        // Handle SHOW INDEXES
        if query.show_indexes {
            return Ok(ExecutionPlan {
                root: Box::new(ShowIndexesOperator::new()),
                output_columns: vec!["label".to_string(), "property".to_string(), "type".to_string()],
                is_write: false,
            });
        }

        // Handle SHOW CONSTRAINTS
        if query.show_constraints {
            return Ok(ExecutionPlan {
                root: Box::new(ShowConstraintsOperator::new()),
                output_columns: vec!["label".to_string(), "property".to_string(), "type".to_string()],
                is_write: false,
            });
        }

        // Handle CREATE CONSTRAINT
        if let Some(clause) = &query.create_constraint_clause {
            return Ok(ExecutionPlan {
                root: Box::new(CreateConstraintOperator::new(
                    clause.label.clone(),
                    clause.property.clone(),
                )),
                output_columns: vec![],
                is_write: true,
            });
        }

        // Handle DROP INDEX
        if let Some(clause) = &query.drop_index_clause {
            return Ok(ExecutionPlan {
                root: Box::new(DropIndexOperator::new(
                    clause.label.clone(),
                    clause.property.clone(),
                )),
                output_columns: vec![],
                is_write: true,
            });
        }

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

        // Handle CREATE INDEX (supports composite indexes)
        if let Some(clause) = &query.create_index_clause {
            // For composite indexes, create individual indexes for each property
            // The first property gets a dedicated CreateIndexOperator
            // Additional properties are also indexed
            if clause.additional_properties.is_empty() {
                return Ok(ExecutionPlan {
                    root: Box::new(CreateIndexOperator::new(
                        clause.label.clone(),
                        clause.property.clone(),
                    )),
                    output_columns: vec![],
                    is_write: true,
                });
            } else {
                // Composite index: create operator for first property
                // Additional properties are created in sequence
                return Ok(ExecutionPlan {
                    root: Box::new(CompositeCreateIndexOperator::new(
                        clause.label.clone(),
                        std::iter::once(clause.property.clone())
                            .chain(clause.additional_properties.iter().cloned())
                            .collect(),
                    )),
                    output_columns: vec![],
                    is_write: true,
                });
            }
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
        let mut known_vars: HashSet<String> = HashSet::new();

        // Determine split point for WITH barrier
        let split = query.with_split_index.unwrap_or(query.match_clauses.len());
        let pre_with_clauses = &query.match_clauses[..split];
        let post_with_clauses = &query.match_clauses[split..];

        // 1a. Handle pre-WITH MATCH clauses
        for match_clause in pre_with_clauses {
            let match_op = self.plan_match(match_clause, query.where_clause.as_ref(), _store)?;

            let mut clause_vars = HashSet::new();
            for path in &match_clause.pattern.paths {
                if let Some(v) = &path.start.variable { clause_vars.insert(v.clone()); }
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable { clause_vars.insert(v.clone()); }
                    if let Some(v) = &seg.edge.variable { clause_vars.insert(v.clone()); }
                }
            }

            operator = Some(match operator {
                Some(existing) => {
                    let shared: Vec<String> = known_vars.intersection(&clause_vars).cloned().collect();
                    if !shared.is_empty() {
                        if match_clause.optional {
                            let right_only: Vec<String> = clause_vars.difference(&known_vars).cloned().collect();
                            Box::new(LeftOuterJoinOperator::new(existing, match_op, shared[0].clone(), right_only)) as OperatorBox
                        } else {
                            Box::new(JoinOperator::new(existing, match_op, shared[0].clone())) as OperatorBox
                        }
                    } else {
                        Box::new(CartesianProductOperator::new(existing, match_op)) as OperatorBox
                    }
                }
                None => match_op,
            });
            known_vars.extend(clause_vars);
        }

        // 1b. Insert WITH barrier if WITH clause is present and has post-WITH clauses
        if let Some(with_clause) = &query.with_clause {
            if let Some(op) = operator {
                // Parse WITH items into projections and aggregations
                let mut items = Vec::new();
                let mut aggregates = Vec::new();
                let mut group_by = Vec::new();
                let mut has_aggregation = false;

                for (idx, item) in with_clause.items.iter().enumerate() {
                    let alias = item.alias.clone().unwrap_or_else(|| {
                        match &item.expression {
                            Expression::Variable(var) => var.clone(),
                            Expression::Property { variable, property } => format!("{}.{}", variable, property),
                            Expression::Function { name, args, distinct } => {
                                let arg_strs: Vec<String> = args.iter().map(|a| match a {
                                    Expression::Variable(v) => v.clone(),
                                    Expression::Property { variable, property } => format!("{}.{}", variable, property),
                                    _ => "?".to_string(),
                                }).collect();
                                if *distinct {
                                    format!("{}(DISTINCT {})", name, arg_strs.join(", "))
                                } else {
                                    format!("{}({})", name, arg_strs.join(", "))
                                }
                            },
                            _ => format!("col_{}", idx),
                        }
                    });

                    items.push((item.expression.clone(), alias.clone()));

                    let mut is_agg_func = false;
                    if let Expression::Function { name, args, distinct } = &item.expression {
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
                            let arg_expr = args.first().cloned()
                                .unwrap_or(Expression::Literal(PropertyValue::Null));
                            aggregates.push(AggregateFunction {
                                func,
                                expr: arg_expr,
                                alias: alias.clone(),
                                distinct: *distinct,
                            });
                        }
                    }

                    if !is_agg_func {
                        group_by.push((item.expression.clone(), alias));
                    }
                }

                // Parse WITH ORDER BY
                let sort_items: Vec<(Expression, bool)> = with_clause.order_by.as_ref()
                    .map(|ob| ob.items.iter().map(|i| (i.expression.clone(), i.ascending)).collect())
                    .unwrap_or_default();

                // Parse WITH WHERE
                let where_predicate = with_clause.where_clause.as_ref()
                    .map(|wc| wc.predicate.clone());

                operator = Some(Box::new(WithBarrierOperator::new(
                    op,
                    items.clone(),
                    aggregates,
                    group_by,
                    has_aggregation,
                    with_clause.distinct,
                    where_predicate,
                    sort_items,
                    with_clause.skip,
                    with_clause.limit,
                )));

                // Reset known_vars to only WITH output aliases
                known_vars.clear();
                for (_, alias) in &items {
                    known_vars.insert(alias.clone());
                }
            }
        }

        // 1c. Handle post-WITH MATCH clauses (join on variables from WITH output)
        for match_clause in post_with_clauses {
            // Post-WITH clauses do NOT use the original WHERE (it was applied before WITH)
            let match_op = self.plan_match(match_clause, None, _store)?;

            let mut clause_vars = HashSet::new();
            for path in &match_clause.pattern.paths {
                if let Some(v) = &path.start.variable { clause_vars.insert(v.clone()); }
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable { clause_vars.insert(v.clone()); }
                    if let Some(v) = &seg.edge.variable { clause_vars.insert(v.clone()); }
                }
            }

            operator = Some(match operator {
                Some(existing) => {
                    let shared: Vec<String> = known_vars.intersection(&clause_vars).cloned().collect();
                    if !shared.is_empty() {
                        if match_clause.optional {
                            let right_only: Vec<String> = clause_vars.difference(&known_vars).cloned().collect();
                            Box::new(LeftOuterJoinOperator::new(existing, match_op, shared[0].clone(), right_only)) as OperatorBox
                        } else {
                            Box::new(JoinOperator::new(existing, match_op, shared[0].clone())) as OperatorBox
                        }
                    } else {
                        Box::new(CartesianProductOperator::new(existing, match_op)) as OperatorBox
                    }
                }
                None => match_op,
            });
            known_vars.extend(clause_vars);
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
                        Expression::Function { name, args, distinct } => {
                            let arg_strs: Vec<String> = args.iter().map(|a| match a {
                                Expression::Variable(v) => v.clone(),
                                Expression::Property { variable, property } => format!("{}.{}", variable, property),
                                _ => "?".to_string(),
                            }).collect();
                            if *distinct {
                                format!("{}(DISTINCT {})", name, arg_strs.join(", "))
                            } else {
                                format!("{}({})", name, arg_strs.join(", "))
                            }
                        },
                        _ => format!("col_{}", idx),
                    }
                });

                output_columns.push(alias.clone());

                // Detect Aggregation
                let mut is_agg_func = false;
                if let Expression::Function { name, args, distinct } = &item.expression {
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
                            distinct: *distinct,
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

        // QP-01: Predicate pushdown is handled inline during plan_match() via AND-chain decomposition
        // QP-02: Cost-based plan selection uses GraphStatistics to pick indexes over scans
        // QP-04: Early LIMIT propagation — done when NodeScanOperator gets early_limit set

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

        // QP-02/QP-03: Cost-based optimization — reorder paths by estimated cardinality (smallest first)
        let stats = store.compute_statistics();
        let mut paths_with_cost: Vec<(usize, f64)> = pattern.paths.iter().enumerate().map(|(i, path)| {
            let cost = if let Some(label) = path.start.labels.first() {
                stats.estimate_label_scan(label) as f64
            } else {
                f64::MAX // All-nodes scan is most expensive
            };
            (i, cost)
        }).collect();
        paths_with_cost.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Handle multiple paths — use JoinOperator when paths share variables,
        // CartesianProductOperator otherwise.
        let mut operators: Vec<OperatorBox> = Vec::new();
        let mut path_vars: Vec<HashSet<String>> = Vec::new();

        for &(path_idx, _) in &paths_with_cost {
            let path = &pattern.paths[path_idx];
            // Start with node scan for this path
            let start_var = path.start.variable.as_ref()
                .ok_or_else(|| ExecutionError::PlanningError("Start node must have a variable".to_string()))?
                .clone();

            // Optimization: Check for index usage (supports AND-chain predicates)
            let mut index_op: Option<OperatorBox> = None;
            let mut remaining_predicates: Vec<Expression> = Vec::new();
            if let Some(wc) = where_clause {
                // Flatten AND-chain into individual predicates
                let predicates = flatten_and_predicates(&wc.predicate);
                let mut used_index = false;

                for pred in &predicates {
                    if used_index {
                        // Already found an index — push remaining predicates to filter
                        remaining_predicates.push(pred.clone());
                        continue;
                    }
                    if let Expression::Binary { left, op, right } = pred {
                        if let (Expression::Property { variable, property }, Expression::Literal(val)) = (left.as_ref(), right.as_ref()) {
                            if variable == &start_var {
                                for label in &path.start.labels {
                                    if store.property_index.has_index(label, property) {
                                        match op {
                                            BinaryOp::Eq | BinaryOp::Gt | BinaryOp::Ge | BinaryOp::Lt | BinaryOp::Le => {
                                                index_op = Some(Box::new(IndexScanOperator::new(
                                                    start_var.clone(),
                                                    label.clone(),
                                                    property.clone(),
                                                    op.clone(),
                                                    val.clone()
                                                )));
                                                used_index = true;
                                            },
                                            _ => {}
                                        }
                                        if used_index { break; }
                                    }
                                }
                            }
                        }
                    }
                    if !used_index {
                        remaining_predicates.push(pred.clone());
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

            // Add remaining non-indexed predicates from AND-chain decomposition
            if !remaining_predicates.is_empty() {
                let filter_expr = remaining_predicates.into_iter().reduce(|acc, pred| {
                    Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    }
                }).unwrap();
                path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
            }

            // Check for shortestPath / allShortestPaths
            if matches!(path.path_type, PathType::Shortest | PathType::AllShortest) && !path.segments.is_empty() {
                // shortestPath: use BFS-based ShortestPathOperator
                let last_segment = path.segments.last().unwrap();
                let target_var = last_segment.node.variable.as_ref()
                    .ok_or_else(|| ExecutionError::PlanningError("shortestPath target must have a variable".to_string()))?
                    .clone();
                let edge_types: Vec<String> = last_segment.edge.types.iter()
                    .map(|t| t.as_str().to_string())
                    .collect();
                let all_paths = matches!(path.path_type, PathType::AllShortest);

                // We need the target node to be scanned too — create a CartesianProduct with target scan
                let target_scan: OperatorBox = Box::new(NodeScanOperator::new(
                    target_var.clone(),
                    last_segment.node.labels.clone(),
                ));
                // Add property filter for target node
                let target_op = if let Some(ref props) = last_segment.node.properties {
                    if !props.is_empty() {
                        let filter_expr = self.build_property_filter(&target_var, props);
                        Box::new(FilterOperator::new(target_scan, filter_expr)) as OperatorBox
                    } else {
                        target_scan
                    }
                } else {
                    target_scan
                };

                let combined = Box::new(CartesianProductOperator::new(path_operator, target_op));
                path_operator = Box::new(ShortestPathOperator::new(
                    combined,
                    start_var.clone(),
                    target_var.clone(),
                    path.path_variable.clone(),
                    edge_types,
                    last_segment.edge.direction.clone(),
                    all_paths,
                ));
            } else {
                // Normal path: use ExpandOperator for each segment
                let mut current_var = start_var.clone();
                for segment in &path.segments {
                    let target_var = segment.node.variable.as_ref()
                        .ok_or_else(|| ExecutionError::PlanningError("Target node must have a variable".to_string()))?
                        .clone();

                    let edge_var = segment.edge.variable.clone();
                    let edge_types: Vec<String> = segment.edge.types.iter()
                        .map(|t| t.as_str().to_string())
                        .collect();

                    let mut expand = ExpandOperator::new(
                        path_operator,
                        current_var.clone(),
                        target_var.clone(),
                        edge_var,
                        edge_types,
                        segment.edge.direction.clone(),
                    );

                    // CY-04: Set path variable for named path materialization
                    if let Some(ref pv) = path.path_variable {
                        expand = expand.with_path_variable(pv.clone());
                    }

                    // Add target label filter if labels specified on target node
                    path_operator = if !segment.node.labels.is_empty() {
                        Box::new(expand.with_target_labels(segment.node.labels.clone()))
                    } else {
                        Box::new(expand)
                    };

                    // Add property filter for target node if properties specified
                    if let Some(ref props) = segment.node.properties {
                        if !props.is_empty() {
                            let filter_expr = self.build_property_filter(&target_var, props);
                            path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
                        }
                    }

                    current_var = target_var;
                }
            }

            // Collect variables used in this path for join detection
            let mut vars = HashSet::new();
            if let Some(v) = &path.start.variable { vars.insert(v.clone()); }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable { vars.insert(v.clone()); }
                if let Some(v) = &seg.edge.variable { vars.insert(v.clone()); }
            }
            path_vars.push(vars);

            operators.push(path_operator);
        }

        // Combine operators: use JoinOperator when paths share a variable, CartesianProduct otherwise
        let mut result = operators.remove(0);
        let mut combined_vars = path_vars.remove(0);
        for (op, vars) in operators.into_iter().zip(path_vars.into_iter()) {
            let shared: Vec<String> = combined_vars.intersection(&vars).cloned().collect();
            if !shared.is_empty() {
                result = Box::new(JoinOperator::new(result, op, shared[0].clone()));
            } else {
                result = Box::new(CartesianProductOperator::new(result, op));
            }
            combined_vars.extend(vars);
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

    /// Collect variables referenced by an expression
    fn collect_expression_variables(expr: &Expression, vars: &mut HashSet<String>) {
        match expr {
            Expression::Variable(v) => { vars.insert(v.clone()); }
            Expression::Property { variable, .. } => { vars.insert(variable.clone()); }
            Expression::Binary { left, right, .. } => {
                Self::collect_expression_variables(left, vars);
                Self::collect_expression_variables(right, vars);
            }
            Expression::Unary { expr: e, .. } => { Self::collect_expression_variables(e, vars); }
            Expression::Function { args, .. } => {
                for arg in args { Self::collect_expression_variables(arg, vars); }
            }
            _ => {}
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

/// Flatten an AND-chain expression into a list of individual predicates.
/// E.g., `a AND b AND c` → `[a, b, c]`
fn flatten_and_predicates(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::Binary { left, op: BinaryOp::And, right } => {
            let mut result = flatten_and_predicates(left);
            result.extend(flatten_and_predicates(right));
            result
        }
        _ => vec![expr.clone()],
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
