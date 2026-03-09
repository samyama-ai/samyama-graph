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
    operator::{NodeScanOperator, FilterOperator, ExpandOperator, ProjectOperator, LimitOperator, SkipOperator, CreateNodeOperator, CreateNodesAndEdgesOperator, CartesianProductOperator, VectorSearchOperator, JoinOperator, LeftOuterJoinOperator, CreateVectorIndexOperator, CreateIndexOperator, CompositeCreateIndexOperator, CreateConstraintOperator, DropIndexOperator, ShowIndexesOperator, ShowConstraintsOperator, ShowLabelsOperator, ShowRelationshipTypesOperator, ShowPropertyKeysOperator, SchemaVisualizationOperator, AlgorithmOperator, IndexScanOperator, AggregateOperator, AggregateType, AggregateFunction, SortOperator, DeleteOperator, SetPropertyOperator, RemovePropertyOperator, UnwindOperator, MergeOperator, ForeachOperator, ShortestPathOperator, WithBarrierOperator},
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

/// Configuration for the query planner (ADR-015)
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// Enable the graph-native planner (default: false, uses legacy planner)
    pub graph_native: bool,
    /// Maximum number of candidate plans to evaluate (default: 64)
    pub max_candidate_plans: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            graph_native: false,
            max_candidate_plans: 64,
        }
    }
}

/// Query planner
pub struct QueryPlanner {
    /// Enable optimization
    _optimize: bool,
    /// Plan cache: query string hash → planning metadata
    plan_cache: Mutex<HashMap<u64, PlanCacheEntry>>,
    /// Cache generation counter (incremented on schema changes)
    cache_generation: std::sync::atomic::AtomicU64,
    /// Planner configuration (ADR-015)
    config: PlannerConfig,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            _optimize: true,
            plan_cache: Mutex::new(HashMap::new()),
            cache_generation: std::sync::atomic::AtomicU64::new(0),
            config: PlannerConfig::default(),
        }
    }

    /// Create a new query planner with configuration
    pub fn with_config(config: PlannerConfig) -> Self {
        Self {
            _optimize: true,
            plan_cache: Mutex::new(HashMap::new()),
            cache_generation: std::sync::atomic::AtomicU64::new(0),
            config,
        }
    }

    /// Get the current planner configuration
    pub fn config(&self) -> &PlannerConfig {
        &self.config
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

        // Pre-compute variable sets for each pre-WITH MATCH clause
        let pre_match_var_sets: Vec<HashSet<String>> = pre_with_clauses.iter().map(|mc| {
            let mut vars = HashSet::new();
            for path in &mc.pattern.paths {
                if let Some(v) = &path.start.variable { vars.insert(v.clone()); }
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable { vars.insert(v.clone()); }
                    if let Some(v) = &seg.edge.variable { vars.insert(v.clone()); }
                }
            }
            vars
        }).collect();

        // Decompose WHERE clause: assign predicates to MATCH clauses or cross-MATCH
        let pre_where_preds = query.where_clause.as_ref()
            .map(|wc| flatten_and_predicates(&wc.predicate))
            .unwrap_or_default();
        let mut per_match_where: Vec<Option<WhereClause>> = vec![None; pre_with_clauses.len()];
        let mut cross_match_predicates: Vec<Expression> = Vec::new();

        for pred in pre_where_preds {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(&pred, &mut pred_vars);

            let target = pre_match_var_sets.iter().position(|match_vars| {
                pred_vars.is_empty() || pred_vars.iter().all(|v| match_vars.contains(v))
            });
            if let Some(i) = target {
                match &mut per_match_where[i] {
                    Some(wc) => {
                        wc.predicate = Expression::Binary {
                            left: Box::new(wc.predicate.clone()),
                            op: BinaryOp::And,
                            right: Box::new(pred),
                        };
                    }
                    None => {
                        per_match_where[i] = Some(WhereClause { predicate: pred });
                    }
                }
            } else {
                cross_match_predicates.push(pred);
            }
        }

        // 1a. Handle pre-WITH MATCH clauses
        for (match_idx, match_clause) in pre_with_clauses.iter().enumerate() {
            let match_op = self.dispatch_plan_match(match_clause, per_match_where[match_idx].as_ref(), _store)?;

            let clause_vars = pre_match_var_sets[match_idx].clone();

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

        // Apply cross-MATCH predicates after all pre-WITH MATCH clauses are joined
        if !cross_match_predicates.is_empty() {
            if let Some(op) = operator {
                let filter_expr = cross_match_predicates.into_iter().reduce(|acc, pred| {
                    Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    }
                }).unwrap();
                operator = Some(Box::new(FilterOperator::new(op, filter_expr)));
            }
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
                            let arg_expr = if matches!(func, AggregateType::Count) && args.is_empty() {
                                // count(*) — use non-null literal so every row is counted
                                Expression::Literal(PropertyValue::Integer(1))
                            } else {
                                args.first().cloned()
                                    .unwrap_or(Expression::Literal(PropertyValue::Null))
                            };
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
        // Pre-compute variable sets for post-WITH MATCH clauses
        let post_match_var_sets: Vec<HashSet<String>> = post_with_clauses.iter().map(|mc| {
            let mut vars = HashSet::new();
            for path in &mc.pattern.paths {
                if let Some(v) = &path.start.variable { vars.insert(v.clone()); }
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable { vars.insert(v.clone()); }
                    if let Some(v) = &seg.edge.variable { vars.insert(v.clone()); }
                }
            }
            vars
        }).collect();

        // Decompose post-WITH WHERE clause: assign to MATCH clauses or cross-MATCH
        let post_where_preds = query.post_with_where_clause.as_ref()
            .map(|wc| flatten_and_predicates(&wc.predicate))
            .unwrap_or_default();
        let mut post_per_match_where: Vec<Option<WhereClause>> = vec![None; post_with_clauses.len()];
        let mut post_cross_match_preds: Vec<Expression> = Vec::new();

        for pred in post_where_preds {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(&pred, &mut pred_vars);

            let target = post_match_var_sets.iter().position(|match_vars| {
                pred_vars.is_empty() || pred_vars.iter().all(|v| match_vars.contains(v))
            });
            if let Some(i) = target {
                match &mut post_per_match_where[i] {
                    Some(wc) => {
                        wc.predicate = Expression::Binary {
                            left: Box::new(wc.predicate.clone()),
                            op: BinaryOp::And,
                            right: Box::new(pred),
                        };
                    }
                    None => {
                        post_per_match_where[i] = Some(WhereClause { predicate: pred });
                    }
                }
            } else {
                post_cross_match_preds.push(pred);
            }
        }

        for (match_idx, match_clause) in post_with_clauses.iter().enumerate() {
            let match_op = self.dispatch_plan_match(match_clause, post_per_match_where[match_idx].as_ref(), _store)?;

            let clause_vars = post_match_var_sets[match_idx].clone();

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

        // Apply post-WITH cross-MATCH predicates after all post-WITH MATCH clauses are joined
        if !post_cross_match_preds.is_empty() {
            if let Some(op) = operator {
                let filter_expr = post_cross_match_preds.into_iter().reduce(|acc, pred| {
                    Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    }
                }).unwrap();
                operator = Some(Box::new(FilterOperator::new(op, filter_expr)));
            }
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
                        let arg_expr = if matches!(func, AggregateType::Count) && args.is_empty() {
                            // count(*) — use non-null literal so every row is counted
                            Expression::Literal(PropertyValue::Integer(1))
                        } else {
                            args.first().cloned().unwrap_or(Expression::Literal(PropertyValue::Null))
                        };
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
        } else if call_clause.procedure_name == "db.labels" {
            Ok(Box::new(ShowLabelsOperator::new()))
        } else if call_clause.procedure_name == "db.relationshipTypes" {
            Ok(Box::new(ShowRelationshipTypesOperator::new()))
        } else if call_clause.procedure_name == "db.propertyKeys" {
            Ok(Box::new(ShowPropertyKeysOperator::new()))
        } else if call_clause.procedure_name == "db.schema.visualization" {
            Ok(Box::new(SchemaVisualizationOperator::new()))
        } else if call_clause.procedure_name.starts_with("algo.") {
            Ok(Box::new(AlgorithmOperator::new(
                call_clause.procedure_name.clone(),
                call_clause.arguments.clone(),
            )))
        } else {
            Err(ExecutionError::PlanningError(format!("Unknown procedure: {}", call_clause.procedure_name)))
        }
    }

    /// Dispatch to graph-native or legacy planner based on configuration
    fn dispatch_plan_match(&self, match_clause: &MatchClause, where_clause: Option<&WhereClause>, store: &GraphStore) -> ExecutionResult<OperatorBox> {
        if self.config.graph_native {
            self.plan_match_native(match_clause, where_clause, store)
        } else {
            self.plan_match(match_clause, where_clause, store)
        }
    }

    /// Graph-native planner (ADR-015): enumerate candidate plans, choose cheapest
    fn plan_match_native(&self, match_clause: &MatchClause, where_clause: Option<&WhereClause>, store: &GraphStore) -> ExecutionResult<OperatorBox> {
        use super::logical_plan::PatternGraph;
        use super::plan_enumerator::{enumerate_plans, EnumerationConfig};
        use super::physical_planner::logical_to_physical;

        let pattern = &match_clause.pattern;
        if pattern.paths.is_empty() {
            return Err(ExecutionError::PlanningError("Match pattern has no paths".to_string()));
        }

        let pg = PatternGraph::from_match_clause(match_clause);
        let catalog = store.catalog();
        let config = EnumerationConfig {
            max_candidate_plans: self.config.max_candidate_plans,
        };

        let candidates = enumerate_plans(&pg, where_clause, catalog, &config);
        if candidates.is_empty() {
            return Err(ExecutionError::PlanningError("No valid plans enumerated".to_string()));
        }

        // Pick the cheapest plan (first one — already sorted)
        let (best_plan, _best_cost) = candidates.into_iter().next().unwrap();
        let physical = logical_to_physical(&best_plan);

        Ok(physical)
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

        // Pre-compute variable sets for each path
        let path_var_sets: Vec<HashSet<String>> = pattern.paths.iter().map(|path| {
            let mut vars = HashSet::new();
            if let Some(v) = &path.start.variable { vars.insert(v.clone()); }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable { vars.insert(v.clone()); }
                if let Some(v) = &seg.edge.variable { vars.insert(v.clone()); }
            }
            vars
        }).collect();

        // Decompose WHERE clause: assign each predicate to the first path that contains
        // all its referenced variables. Cross-path predicates are applied after path join.
        let all_where_preds = where_clause
            .map(|wc| flatten_and_predicates(&wc.predicate))
            .unwrap_or_default();
        let mut per_path_preds: Vec<Vec<Expression>> = vec![Vec::new(); pattern.paths.len()];
        let mut cross_path_predicates: Vec<Expression> = Vec::new();

        for pred in all_where_preds {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(&pred, &mut pred_vars);

            let target_path = path_var_sets.iter().position(|pvars| {
                pred_vars.is_empty() || pred_vars.iter().all(|v| pvars.contains(v))
            });
            if let Some(i) = target_path {
                per_path_preds[i].push(pred);
            } else {
                cross_path_predicates.push(pred);
            }
        }

        let mut anon_counter: usize = 0;

        for &(path_idx, _) in &paths_with_cost {
            let path = &pattern.paths[path_idx];
            // Start with node scan for this path
            // Auto-generate variable names for anonymous nodes (e.g., `()` in patterns)
            let start_var = path.start.variable.clone().unwrap_or_else(|| {
                let name = format!("_anon_{}", anon_counter);
                anon_counter += 1;
                name
            });

            // Optimization: Check for index usage (using this path's assigned predicates)
            let mut index_op: Option<OperatorBox> = None;
            let mut remaining_predicates: Vec<Expression> = Vec::new();
            {
                let predicates = &per_path_preds[path_idx];
                let mut used_index = false;

                for pred in predicates {
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

            // Split remaining predicates: those referencing only start_var can be pushed
            // down now; those referencing later-path variables must be deferred until
            // after all ExpandOperators have materialized those variables.
            let mut early_predicates: Vec<Expression> = Vec::new();
            let mut deferred_predicates: Vec<Expression> = Vec::new();
            for pred in remaining_predicates {
                let mut pred_vars = HashSet::new();
                Self::collect_expression_variables(&pred, &mut pred_vars);
                // Push down only if predicate references exclusively the start variable
                // (or no variables at all, e.g., literal expressions)
                if pred_vars.is_empty() || pred_vars.iter().all(|v| v == &start_var) {
                    early_predicates.push(pred);
                } else {
                    deferred_predicates.push(pred);
                }
            }
            if !early_predicates.is_empty() {
                let filter_expr = early_predicates.into_iter().reduce(|acc, pred| {
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
                    let target_var = segment.node.variable.clone().unwrap_or_else(|| {
                        let name = format!("_anon_{}", anon_counter);
                        anon_counter += 1;
                        name
                    });

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

            // Apply deferred WHERE predicates after all path expansions
            if !deferred_predicates.is_empty() {
                let filter_expr = deferred_predicates.into_iter().reduce(|acc, pred| {
                    Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    }
                }).unwrap();
                path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
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

        // Apply cross-path predicates after all paths are joined
        if !cross_path_predicates.is_empty() {
            let filter_expr = cross_path_predicates.into_iter().reduce(|acc, pred| {
                Expression::Binary {
                    left: Box::new(acc),
                    op: BinaryOp::And,
                    right: Box::new(pred),
                }
            }).unwrap();
            result = Box::new(FilterOperator::new(result, filter_expr));
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

    // ========== Batch 5: Additional Planner Tests ==========

    #[test]
    fn test_plan_create_only() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE (n:Person {name: 'Alice'})").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for CREATE: {:?}", result.err());
    }

    #[test]
    fn test_plan_delete() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) DELETE n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for DELETE: {:?}", result.err());
    }

    #[test]
    fn test_plan_set() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) SET n.age = 30 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for SET: {:?}", result.err());
    }

    #[test]
    fn test_plan_merge() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MERGE (n:Person {name: 'Alice'})").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for MERGE: {:?}", result.err());
    }

    #[test]
    fn test_plan_unwind() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n) UNWIND [1,2,3] AS x RETURN x").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for UNWIND: {:?}", result.err());
    }

    #[test]
    fn test_plan_union() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:Company) RETURN m.name").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for UNION: {:?}", result.err());
    }

    #[test]
    fn test_plan_optional_match() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for OPTIONAL MATCH: {:?}", result.err());
    }

    #[test]
    fn test_plan_explain() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("EXPLAIN MATCH (n:Person) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for EXPLAIN: {:?}", result.err());
    }

    #[test]
    fn test_plan_profile() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("PROFILE MATCH (n:Person) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for PROFILE: {:?}", result.err());
    }

    #[test]
    fn test_plan_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.city, count(n) AS cnt").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for aggregation: {:?}", result.err());
    }

    #[test]
    fn test_plan_order_by_limit() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n ORDER BY n.name LIMIT 5").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for ORDER BY + LIMIT: {:?}", result.err());
    }

    #[test]
    fn test_plan_distinct() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN DISTINCT n.name").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for DISTINCT: {:?}", result.err());
    }

    #[test]
    fn test_plan_with_clause() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n.name AS name RETURN name").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for WITH: {:?}", result.err());
    }

    #[test]
    fn test_plan_create_index() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE INDEX ON :Person(name)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for CREATE INDEX: {:?}", result.err());
    }

    #[test]
    fn test_plan_drop_index() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("DROP INDEX ON :Person(name)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for DROP INDEX: {:?}", result.err());
    }

    #[test]
    fn test_plan_show_indexes() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("SHOW INDEXES").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for SHOW INDEXES: {:?}", result.err());
    }

    #[test]
    fn test_plan_show_constraints() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("SHOW CONSTRAINTS").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for SHOW CONSTRAINTS: {:?}", result.err());
    }

    #[test]
    fn test_plan_create_constraint() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for CREATE CONSTRAINT: {:?}", result.err());
    }

    #[test]
    fn test_plan_call_algorithm() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CALL algo.pageRank({maxIterations: 20}) YIELD node, score").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for CALL algo: {:?}", result.err());
    }

    #[test]
    fn test_plan_multiple_return_items() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.name, n.age, id(n)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 3);
    }

    #[test]
    fn test_plan_with_populated_store() {
        let mut store = GraphStore::new();
        // Populate with data so statistics-based planning kicks in
        for i in 0..100 {
            let id = store.create_node("Person");
            store.get_node_mut(id).unwrap().set_property(
                "name".to_string(),
                crate::graph::PropertyValue::String(format!("Person{}", i)),
            );
        }
        for i in 0..20 {
            let id = store.create_node("Company");
            store.get_node_mut(id).unwrap().set_property(
                "name".to_string(),
                crate::graph::PropertyValue::String(format!("Company{}", i)),
            );
        }

        let planner = QueryPlanner::new();
        let query = parse_query("MATCH (n:Person) WHERE n.name = 'Person50' RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_detach_delete() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) DETACH DELETE n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for DETACH DELETE: {:?}", result.err());
    }

    // ========== Coverage Enhancement Tests ==========

    #[test]
    fn test_planner_default_impl() {
        let planner = QueryPlanner::default();
        let store = GraphStore::new();
        let query = parse_query("MATCH (n) RETURN n").unwrap();
        assert!(planner.plan(&query, &store).is_ok());
    }

    #[test]
    fn test_plan_cache_invalidation() {
        let planner = QueryPlanner::new();
        let store = GraphStore::new();
        // Plan a query to populate cache
        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        planner.plan(&query, &store).unwrap();
        // Invalidate should not cause errors
        planner.invalidate_cache();
        // Re-planning should still work
        let result = planner.plan(&query, &store);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_create_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE (n:Person {name: 'Alice'})").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "CREATE should be a write plan");
    }

    #[test]
    fn test_plan_delete_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) DELETE n").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "DELETE should be a write plan");
    }

    #[test]
    fn test_plan_set_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) SET n.age = 30 RETURN n").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "SET should be a write plan");
    }

    #[test]
    fn test_plan_merge_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MERGE (n:Person {name: 'Alice'})").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "MERGE should be a write plan");
    }

    #[test]
    fn test_plan_read_is_not_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(!plan.is_write, "MATCH...RETURN should not be a write plan");
    }

    #[test]
    fn test_plan_create_index_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE INDEX ON :Person(name)").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "CREATE INDEX should be a write plan");
    }

    #[test]
    fn test_plan_drop_index_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("DROP INDEX ON :Person(name)").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "DROP INDEX should be a write plan");
    }

    #[test]
    fn test_plan_show_indexes_not_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("SHOW INDEXES").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(!plan.is_write, "SHOW INDEXES should not be a write plan");
        assert!(plan.output_columns.contains(&"label".to_string()));
        assert!(plan.output_columns.contains(&"property".to_string()));
        assert!(plan.output_columns.contains(&"type".to_string()));
    }

    #[test]
    fn test_plan_show_constraints_not_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("SHOW CONSTRAINTS").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(!plan.is_write, "SHOW CONSTRAINTS should not be a write plan");
    }

    #[test]
    fn test_plan_constraint_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "CREATE CONSTRAINT should be a write plan");
    }

    #[test]
    fn test_plan_create_with_edge() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write);
        // Both variables should appear in output columns
        assert!(plan.output_columns.contains(&"a".to_string()));
        assert!(plan.output_columns.contains(&"b".to_string()));
    }

    #[test]
    fn test_plan_match_create_edge() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (a:Person), (b:Company) CREATE (a)-[:WORKS_AT]->(b)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "MATCH...CREATE should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert!(plan.is_write);
    }

    #[test]
    fn test_plan_skip() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n SKIP 5").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "SKIP should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_skip_and_limit() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n SKIP 5 LIMIT 10").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "SKIP + LIMIT should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_remove_property() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) REMOVE n.age RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "REMOVE should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert!(plan.is_write, "REMOVE should be a write plan");
    }

    #[test]
    fn test_plan_index_scan_selection() {
        let mut store = GraphStore::new();
        // Create nodes and an index so the planner can choose IndexScan
        for i in 0..100 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "name", crate::graph::PropertyValue::String(format!("Person{}", i))).unwrap();
        }
        // Create a property index
        store.property_index.create_index(crate::graph::Label::new("Person"), "name".to_string());

        let planner = QueryPlanner::new();
        let query = parse_query("MATCH (n:Person) WHERE n.name = 'Person50' RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Index scan planning failed: {:?}", result.err());
    }

    #[test]
    fn test_plan_composite_create_index() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE INDEX ON :Person(name, age)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Composite CREATE INDEX should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert!(plan.is_write);
    }

    #[test]
    fn test_plan_multiple_match_cartesian_product() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Two independent patterns produce CartesianProduct
        let query = parse_query("MATCH (a:Person), (b:Company) RETURN a, b").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Multiple MATCH patterns should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 2);
    }

    #[test]
    fn test_plan_optional_match_output_columns() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert_eq!(plan.output_columns.len(), 2);
        assert!(plan.output_columns.contains(&"n".to_string()));
        assert!(plan.output_columns.contains(&"m".to_string()));
    }

    #[test]
    fn test_plan_with_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n.city AS city, count(n) AS cnt RETURN city, cnt").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "WITH + aggregation should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_with_order_by_limit() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n ORDER BY n.name LIMIT 10 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "WITH ORDER BY LIMIT should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_with_distinct() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH DISTINCT n.city AS city RETURN city").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "WITH DISTINCT should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_multiple_aggregations() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN count(n) AS cnt, sum(n.age) AS total_age, avg(n.age) AS avg_age").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Multiple aggregations should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 3);
        assert!(plan.output_columns.contains(&"cnt".to_string()));
        assert!(plan.output_columns.contains(&"total_age".to_string()));
        assert!(plan.output_columns.contains(&"avg_age".to_string()));
    }

    #[test]
    fn test_plan_collect_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN collect(n.name) AS names").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "collect() aggregation should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_min_max_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN min(n.age) AS youngest, max(n.age) AS oldest").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "min/max aggregation should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_where_complex_and_chain() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WHERE n.age > 18 AND n.city = 'NYC' AND n.active = true RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Complex AND chain WHERE should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_where_or_predicate() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WHERE n.age > 18 OR n.name = 'Admin' RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "OR predicate should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_no_match_no_create_errors() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Build a query manually with no MATCH and no CREATE
        let query = crate::query::ast::Query {
            match_clauses: vec![],
            where_clause: None,
            return_clause: None,
            create_clause: None,
            order_by: None,
            limit: None,
            skip: None,
            call_clause: None,
            call_subquery: None,
            delete_clause: None,
            set_clauses: vec![],
            remove_clauses: vec![],
            with_clause: None,
            create_vector_index_clause: None,
            create_index_clause: None,
            drop_index_clause: None,
            create_constraint_clause: None,
            show_indexes: false,
            show_constraints: false,
            profile: false,
            params: std::collections::HashMap::new(),
            foreach_clause: None,
            unwind_clause: None,
            merge_clause: None,
            union_queries: vec![],
            explain: false,
            with_split_index: None,
            post_with_where_clause: None,
        };
        let result = planner.plan(&query, &store);
        assert!(result.is_err());
        if let Err(e) = result {
            let msg = format!("{}", e);
            assert!(msg.contains("MATCH") || msg.contains("CALL") || msg.contains("CREATE"),
                "Error should mention required clauses: {}", msg);
        }
    }

    #[test]
    fn test_plan_match_with_edge_variable() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Edge variable should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 3);
    }

    #[test]
    fn test_plan_return_expressions() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.name AS name, n.age AS age, id(n) AS node_id").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert_eq!(plan.output_columns, vec!["name", "age", "node_id"]);
    }

    #[test]
    fn test_plan_return_without_alias() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.name, n.age").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        // Without alias, the output column should be "variable.property"
        assert!(plan.output_columns.contains(&"n.name".to_string()));
        assert!(plan.output_columns.contains(&"n.age".to_string()));
    }

    #[test]
    fn test_plan_no_return_clause() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // DELETE without RETURN — should still plan successfully
        let query = parse_query("MATCH (n:Person) DELETE n").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        // Output columns come from MATCH variables
        assert!(plan.output_columns.contains(&"n".to_string()));
    }

    #[test]
    fn test_plan_order_by_with_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.city, count(n) AS cnt ORDER BY cnt").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "ORDER BY with aggregation should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_unwind_with_return() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n) UNWIND [1, 2, 3] AS x RETURN x, n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "UNWIND with RETURN should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert!(plan.output_columns.contains(&"x".to_string()));
        assert!(plan.output_columns.contains(&"n".to_string()));
    }

    #[test]
    fn test_plan_merge_with_return() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MERGE (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "MERGE with RETURN should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert!(plan.is_write);
        assert!(plan.output_columns.contains(&"n".to_string()));
    }

    #[test]
    fn test_plan_with_where_filter() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n WHERE n.age > 30 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "WITH WHERE should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_with_skip() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n SKIP 5 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "WITH SKIP should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_with_resets_known_vars() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // WITH clause should project only selected variables
        let query = parse_query("MATCH (n:Person) WITH n.name AS name RETURN name").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.output_columns.contains(&"name".to_string()));
    }

    #[test]
    fn test_plan_match_with_node_properties() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Node with inline properties should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_edge_direction() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Forward direction
        let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        assert!(planner.plan(&query, &store).is_ok());

        // Backward direction
        let query = parse_query("MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a, b").unwrap();
        assert!(planner.plan(&query, &store).is_ok());
    }

    #[test]
    fn test_plan_multi_hop_path() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:LIVES_IN]->(c:City) RETURN a, b, c").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Multi-hop path should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 3);
    }

    #[test]
    fn test_plan_index_scan_with_gt_operator() {
        let mut store = GraphStore::new();
        for i in 0..50 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "age", crate::graph::PropertyValue::Integer(i as i64)).unwrap();
        }
        store.property_index.create_index(crate::graph::Label::new("Person"), "age".to_string());

        let planner = QueryPlanner::new();
        let query = parse_query("MATCH (n:Person) WHERE n.age > 25 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Index scan with > should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_index_scan_with_lt_operator() {
        let mut store = GraphStore::new();
        for i in 0..50 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "age", crate::graph::PropertyValue::Integer(i as i64)).unwrap();
        }
        store.property_index.create_index(crate::graph::Label::new("Person"), "age".to_string());

        let planner = QueryPlanner::new();
        let query = parse_query("MATCH (n:Person) WHERE n.age < 25 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Index scan with < should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_cross_match_where_predicate() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // WHERE predicate references variables from different MATCH patterns
        let query = parse_query("MATCH (a:Person), (b:Company) WHERE a.company = b.name RETURN a, b").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Cross-match WHERE should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_match_all_nodes() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Match without label — all node scan
        let query = parse_query("MATCH (n) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "All-node scan should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_function_alias_generation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Function without alias should auto-generate column name
        let query = parse_query("MATCH (n:Person) RETURN count(n)").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert_eq!(plan.output_columns.len(), 1);
        // Auto-generated alias should be like "count(n)"
        assert!(plan.output_columns[0].contains("count"));
    }

    #[test]
    fn test_plan_collect_distinct() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN collect(DISTINCT n.name) AS unique_names").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "collect(DISTINCT) should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_with_multiple_aggregations() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n.city AS city, count(n) AS cnt, collect(n.name) AS names RETURN city, cnt, names").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "WITH multiple aggregations should plan: {:?}", result.err());
    }

    // ============================
    // ADR-015: Graph-native planner integration tests
    // ============================

    #[test]
    fn test_planner_config_default() {
        let config = PlannerConfig::default();
        assert!(!config.graph_native);
        assert_eq!(config.max_candidate_plans, 64);
    }

    #[test]
    fn test_planner_with_config() {
        let config = PlannerConfig {
            graph_native: true,
            max_candidate_plans: 32,
        };
        let planner = QueryPlanner::with_config(config);
        assert!(planner.config().graph_native);
        assert_eq!(planner.config().max_candidate_plans, 32);
    }

    #[test]
    fn test_plan_match_native_simple() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        store.get_node_mut(n1).unwrap().set_property("name", PropertyValue::String("Alice".to_string()));

        let planner = QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        });
        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Graph-native planner should handle simple MATCH: {:?}", result.err());
    }

    #[test]
    fn test_plan_match_native_with_expand() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("Person");
        store.create_edge(a, b, "KNOWS").unwrap();

        let planner = QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        });
        let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Graph-native planner should handle expand: {:?}", result.err());
    }

    #[test]
    fn test_ab_correctness_simple_scan() {
        // A/B test: both planners should produce identical results
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        store.get_node_mut(n1).unwrap().set_property("name", PropertyValue::String("Alice".to_string()));
        let n2 = store.create_node("Person");
        store.get_node_mut(n2).unwrap().set_property("name", PropertyValue::String("Bob".to_string()));
        store.create_node("Company"); // should not appear

        let query = parse_query("MATCH (n:Person) RETURN n.name").unwrap();

        // Legacy planner
        let legacy = QueryPlanner::new();
        let legacy_plan = legacy.plan(&query, &store).unwrap();
        let mut legacy_op = legacy_plan.root;
        let mut legacy_results = Vec::new();
        while let Some(record) = legacy_op.next(&store).unwrap() {
            if let Some(val) = record.get("n.name") {
                legacy_results.push(format!("{:?}", val));
            }
        }
        legacy_results.sort();

        // Graph-native planner
        let native = QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        });
        let native_plan = native.plan(&query, &store).unwrap();
        let mut native_op = native_plan.root;
        let mut native_results = Vec::new();
        while let Some(record) = native_op.next(&store).unwrap() {
            if let Some(val) = record.get("n.name") {
                native_results.push(format!("{:?}", val));
            }
        }
        native_results.sort();

        assert_eq!(legacy_results, native_results,
            "Legacy and native planners must produce identical results.\nLegacy: {:?}\nNative: {:?}", legacy_results, native_results);
    }

    #[test]
    fn test_ab_correctness_expand() {
        // A/B: ALL candidate plans must produce identical results to legacy
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        store.get_node_mut(a).unwrap().set_property("name", PropertyValue::String("Alice".to_string()));
        let b = store.create_node("Person");
        store.get_node_mut(b).unwrap().set_property("name", PropertyValue::String("Bob".to_string()));
        let c = store.create_node("Person");
        store.get_node_mut(c).unwrap().set_property("name", PropertyValue::String("Charlie".to_string()));
        store.create_edge(a, b, "KNOWS").unwrap();
        store.create_edge(a, c, "KNOWS").unwrap();

        let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();

        // Legacy planner results
        let legacy = QueryPlanner::new();
        let legacy_plan = legacy.plan(&query, &store).unwrap();
        let mut legacy_results: Vec<String> = Vec::new();
        let mut op = legacy_plan.root;
        while let Some(record) = op.next(&store).unwrap() {
            let a_name = record.get("a.name").map(|v| format!("{:?}", v)).unwrap_or_default();
            let b_name = record.get("b.name").map(|v| format!("{:?}", v)).unwrap_or_default();
            legacy_results.push(format!("{}->{}", a_name, b_name));
        }
        legacy_results.sort();

        // Graph-native planner — verify ALL candidate plans produce correct results
        use super::super::logical_plan::PatternGraph;
        use super::super::plan_enumerator::{enumerate_plans, EnumerationConfig};
        use super::super::physical_planner::logical_to_physical;

        let match_clause = &query.match_clauses[0];
        let pg = PatternGraph::from_match_clause(match_clause);
        let catalog = store.catalog();
        let config = EnumerationConfig { max_candidate_plans: 64 };
        let candidates = enumerate_plans(&pg, query.where_clause.as_ref(), catalog, &config);
        assert!(candidates.len() >= 2, "Should have at least 2 candidate plans");

        for (plan_idx, (logical_plan, cost)) in candidates.iter().enumerate() {
            let physical = logical_to_physical(logical_plan);
            let projections = vec![
                (Expression::Property { variable: "a".to_string(), property: "name".to_string() }, "a.name".to_string()),
                (Expression::Property { variable: "b".to_string(), property: "name".to_string() }, "b.name".to_string()),
            ];
            let mut op: OperatorBox = Box::new(super::super::operator::ProjectOperator::new(physical, projections));

            let mut native_results: Vec<String> = Vec::new();
            while let Some(record) = op.next(&store).unwrap() {
                let a_name = record.get("a.name").map(|v| format!("{:?}", v)).unwrap_or_default();
                let b_name = record.get("b.name").map(|v| format!("{:?}", v)).unwrap_or_default();
                native_results.push(format!("{}->{}", a_name, b_name));
            }
            native_results.sort();

            assert_eq!(legacy_results, native_results,
                "Plan candidate #{} (cost={}) produces different results.\nLegacy: {:?}\nNative: {:?}",
                plan_idx, cost, legacy_results, native_results);
        }
    }

    #[test]
    fn test_ab_correctness_with_where() {
        // A/B: MATCH with WHERE filter
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        store.get_node_mut(n1).unwrap().set_property("age", PropertyValue::Integer(25));
        store.get_node_mut(n1).unwrap().set_property("name", PropertyValue::String("Alice".to_string()));
        let n2 = store.create_node("Person");
        store.get_node_mut(n2).unwrap().set_property("age", PropertyValue::Integer(35));
        store.get_node_mut(n2).unwrap().set_property("name", PropertyValue::String("Bob".to_string()));
        let n3 = store.create_node("Person");
        store.get_node_mut(n3).unwrap().set_property("age", PropertyValue::Integer(45));
        store.get_node_mut(n3).unwrap().set_property("name", PropertyValue::String("Charlie".to_string()));

        let query = parse_query("MATCH (n:Person) WHERE n.age > 30 RETURN n.name").unwrap();

        let legacy = QueryPlanner::new();
        let native = QueryPlanner::with_config(PlannerConfig { graph_native: true, max_candidate_plans: 64 });

        let legacy_plan = legacy.plan(&query, &store).unwrap();
        let native_plan = native.plan(&query, &store).unwrap();

        let mut legacy_results: Vec<String> = Vec::new();
        let mut op = legacy_plan.root;
        while let Some(record) = op.next(&store).unwrap() {
            if let Some(val) = record.get("n.name") {
                legacy_results.push(format!("{:?}", val));
            }
        }
        legacy_results.sort();

        let mut native_results: Vec<String> = Vec::new();
        let mut op = native_plan.root;
        while let Some(record) = op.next(&store).unwrap() {
            if let Some(val) = record.get("n.name") {
                native_results.push(format!("{:?}", val));
            }
        }
        native_results.sort();

        assert_eq!(legacy_results, native_results,
            "WHERE filter results differ.\nLegacy: {:?}\nNative: {:?}", legacy_results, native_results);
    }
}
