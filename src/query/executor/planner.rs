//! # Query Planner: From Declarative Query to Imperative Execution
//!
//! The planner is the heart of the query engine. It transforms a **declarative** query
//! ("find all people who know Alice") into an **imperative** execution plan ("scan Person
//! nodes, expand along KNOWS edges, filter where name = 'Alice'"). This transformation is
//! the most important optimization opportunity in any database -- the same query can have
//! dozens of valid execution plans, and the best one can be orders of magnitude faster
//! than the worst.
//!
//! ## Cost-Based Optimization (ADR-015)
//!
//! Like PostgreSQL, MySQL, and other mature databases, Samyama uses **cost-based
//! optimization**. The planner:
//! 1. **Enumerates** candidate plans (different join orders, scan strategies, traversal
//!    directions)
//! 2. **Estimates** the cost of each plan using **cardinality estimation** -- statistical
//!    models that predict how many records each operator will produce (e.g., "there are
//!    10,000 Person nodes, 0.1% have name = 'Alice', so an equality filter produces ~10
//!    records")
//! 3. **Picks** the cheapest plan
//!
//! The statistics come from [`GraphStore::compute_statistics()`] which samples property
//! distributions, counts labels, and measures average degree.
//!
//! ## Key Optimization Techniques
//!
//! - **Predicate pushdown**: move WHERE filters as close to the scan as possible. Filtering
//!   1 million nodes down to 100 *before* expanding edges is vastly cheaper than expanding
//!   all edges and filtering afterward.
//! - **Index selection**: when a WHERE clause matches an indexed property (`WHERE n.email = $x`
//!   and an index exists on `:Person(email)`), use `IndexScanOperator` instead of
//!   `NodeScanOperator + FilterOperator`. This turns O(n) scans into O(log n) lookups.
//! - **Join ordering**: for multi-pattern MATCH clauses, the order in which patterns are
//!   joined matters enormously. Joining a 10-row result with a 1M-row result is fast;
//!   joining two 1M-row results is catastrophic.
//! - **Early LIMIT propagation**: push LIMIT down into the operator tree so that scans
//!   stop after producing enough records.
//!
//! ## Plan Cache
//!
//! Planning is not free -- enumerating plans and computing cost estimates takes time. For
//! repeated queries (common in applications), the planner caches planning metadata (index
//! hints, cost estimates) keyed by a hash of the query string. A **generation counter**
//! (`AtomicU64`) is incremented on schema changes (CREATE INDEX, DROP INDEX) to invalidate
//! stale cache entries. This uses `AtomicU64` with `Ordering::Relaxed` because exact
//! ordering is not required -- a stale read just causes one extra re-plan.
//!
//! ## Rust Concepts
//!
//! - **`Mutex<HashMap<u64, PlanCacheEntry>>`**: the plan cache is shared across threads
//!   (the query engine is `Send + Sync`). `Mutex` provides mutual exclusion -- only one
//!   thread can read/write the cache at a time. `HashMap<u64, _>` uses a pre-computed hash
//!   of the query string as the key.
//! - **`AtomicU64`**: a lock-free atomic integer for the generation counter. Atomics are
//!   cheaper than mutexes for simple counters because they use CPU-level atomic instructions
//!   (e.g., `LOCK CMPXCHG` on x86) instead of OS-level locks.

use crate::graph::GraphStore;
use crate::graph::{Label, PropertyValue};  // Added for CREATE support
use crate::query::ast::*;
use std::sync::Mutex;
use crate::query::executor::{
    ExecutionError, ExecutionResult, OperatorBox,
    // Added CreateNodeOperator and CreateNodesAndEdgesOperator for CREATE statement support
    operator::{NodeScanOperator, NodeByIdOperator, FilterOperator, ExpandOperator, ProjectOperator, LimitOperator, SkipOperator, CreateNodeOperator, CreateNodesAndEdgesOperator, CartesianProductOperator, VectorSearchOperator, JoinOperator, LeftOuterJoinOperator, CreateVectorIndexOperator, CreateIndexOperator, CompositeCreateIndexOperator, CreateConstraintOperator, DropIndexOperator, ShowIndexesOperator, ShowConstraintsOperator, DistinctOperator, ShowLabelsOperator, ShowRelationshipTypesOperator, ShowPropertyKeysOperator, SchemaVisualizationOperator, AlgorithmOperator, IndexScanOperator, AggregateOperator, AggregateType, AggregateFunction, AlgorithmOperator as _AlgoOp, SortOperator, DeleteOperator, SetPropertyOperator, RemovePropertyOperator, LabelMutationOperator, UnwindOperator, MergeOperator, ForeachOperator, ShortestPathOperator, VarLengthExpandOperator, WithBarrierOperator, LabelCountOperator, EdgeTypeCountOperator, EdgeCountOperator},
};
use crate::graph::EdgeType;  // Added for CREATE edge support
use std::collections::{HashMap, HashSet};  // Added for CREATE properties and JOIN logic
use std::cell::RefCell;

/// Diagnostics from the graph-native planner (ADR-015), used by EXPLAIN
#[derive(Debug, Clone)]
pub struct PlanDiagnostics {
    pub candidates_evaluated: usize,
    pub chosen_plan_cost: f64,
    pub candidate_costs: Vec<(String, f64)>,
}

thread_local! {
    /// Thread-local storage for planner diagnostics, consumed by EXPLAIN
    pub static PLAN_DIAGNOSTICS: RefCell<Option<PlanDiagnostics>> = RefCell::new(None);
}

/// Recursively extract aggregate function calls (sum, avg, count, min, max, collect)
/// from an expression tree, replacing each with a `Variable("__agg_N")` reference.
///
/// Returns the rewritten expression and the list of extracted aggregates.
/// This enables expressions like `round(sum(b.runs) * 100 / sum(b.balls))` where
/// aggregate calls are nested inside arithmetic or scalar function calls.
/// Rewrite an `ORDER BY` expression so it refers to the projection's aliases.
///
/// Cypher lets a sort key be written either as the alias or as a repeat of the
/// projected expression:
///
/// ```cypher
/// WITH a.num2 % 3 AS mod, sum(a.num) AS total ORDER BY sum(a.num)   -- this
/// WITH a.num2 % 3 AS mod, sum(a.num) AS total ORDER BY total        -- and this
/// ```
///
/// are the same query. The second worked; the first did not, and failed in the
/// worst available way. After the aggregation barrier the rows hold `mod` and
/// `total` — there is no `a` to evaluate `sum(a.num)` against — so the sort key
/// evaluated to null for every row, the sort became a no-op, and any `LIMIT`
/// then took an arbitrary prefix of whatever order the group hash map happened
/// to produce. Not a stable wrong answer: the same query over the same data
/// returned **five different results across 100 runs**, of which 36 were right.
///
/// The rewrite is by structural equality against the projected expressions,
/// recursing through compound keys so `ORDER BY sum(x) + 1` resolves too. An
/// expression that matches nothing is left alone — it may legitimately name a
/// grouping key that is still in scope, and rewriting it would break that.
fn rewrite_sort_key(expr: &Expression, projections: &[(Expression, String)]) -> Expression {
    if let Some((_, alias)) = projections.iter().find(|(projected, _)| projected == expr) {
        return Expression::Variable(alias.clone());
    }
    match expr {
        Expression::Binary { left, op, right } => Expression::Binary {
            left: Box::new(rewrite_sort_key(left, projections)),
            op: op.clone(),
            right: Box::new(rewrite_sort_key(right, projections)),
        },
        Expression::Unary { op, expr } => Expression::Unary {
            op: op.clone(),
            expr: Box::new(rewrite_sort_key(expr, projections)),
        },
        other => other.clone(),
    }
}

fn extract_nested_aggregates(
    expr: &Expression,
    counter: &mut usize,
) -> (Expression, Vec<AggregateFunction>) {
    let mut aggregates = Vec::new();
    let rewritten = extract_agg_inner(expr, counter, &mut aggregates);
    (rewritten, aggregates)
}

fn extract_agg_inner(
    expr: &Expression,
    counter: &mut usize,
    aggs: &mut Vec<AggregateFunction>,
) -> Expression {
    match expr {
        Expression::Function { name, args, distinct } => {
            let func_type = match name.to_lowercase().as_str() {
                "count" => Some(AggregateType::Count),
                "sum" => Some(AggregateType::Sum),
                "avg" => Some(AggregateType::Avg),
                "min" => Some(AggregateType::Min),
                "max" => Some(AggregateType::Max),
                "collect" => Some(AggregateType::Collect),
                "percentilecont" => Some(AggregateType::PercentileCont),
                "percentiledisc" => Some(AggregateType::PercentileDisc),
                "stdev" => Some(AggregateType::StDev),
                "stdevp" => Some(AggregateType::StDevP),
                _ => None,
            };

            if let Some(func) = func_type {
                let alias = format!("__agg_{}", *counter);
                *counter += 1;

                let arg_expr = if matches!(func, AggregateType::Count) && args.is_empty() {
                    Expression::Literal(PropertyValue::Integer(1))
                } else {
                    args.first().cloned()
                        .unwrap_or(Expression::Literal(PropertyValue::Null))
                };

                // `percentileCont`/`percentileDisc` take the percentile as
                // their **second** argument. It was dropped here, so the
                // aggregator's `pct` never moved off its initial 0.5 and every
                // percentile call returned the median (#871).
                let percentile = if matches!(
                    func,
                    AggregateType::PercentileCont | AggregateType::PercentileDisc
                ) {
                    args.get(1).cloned()
                } else {
                    None
                };

                aggs.push(AggregateFunction {
                    func,
                    expr: arg_expr,
                    alias: alias.clone(),
                    distinct: *distinct,
                    percentile,
                });

                Expression::Variable(alias)
            } else {
                // Non-aggregate function — recurse into args
                Expression::Function {
                    name: name.clone(),
                    args: args.iter().map(|a| extract_agg_inner(a, counter, aggs)).collect(),
                    distinct: *distinct,
                }
            }
        }
        Expression::Binary { left, op, right } => {
            Expression::Binary {
                left: Box::new(extract_agg_inner(left, counter, aggs)),
                op: op.clone(),
                right: Box::new(extract_agg_inner(right, counter, aggs)),
            }
        }
        Expression::Unary { op, expr: inner } => {
            Expression::Unary {
                op: op.clone(),
                expr: Box::new(extract_agg_inner(inner, counter, aggs)),
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            Expression::Case {
                operand: operand.as_ref().map(|e| Box::new(extract_agg_inner(e, counter, aggs))),
                when_clauses: when_clauses.iter().map(|(cond, then)| {
                    (extract_agg_inner(cond, counter, aggs), extract_agg_inner(then, counter, aggs))
                }).collect(),
                else_result: else_result.as_ref().map(|e| Box::new(extract_agg_inner(e, counter, aggs))),
            }
        }
        // An aggregate can appear inside a collection literal or a
        // comprehension's source: `[v IN collect(a.n) | v]`,
        // `{k: collect(a)}`. Not recursing here left those to the scalar
        // evaluator, which has no aggregate handling, so they failed with
        // "Unknown function: collect" — an error naming one of Cypher's most
        // ordinary functions (#670).
        //
        // Only the *source* of a comprehension is rewritten. Its body runs
        // once per element with the loop variable bound, so an aggregate there
        // is a different question and is left alone rather than silently
        // hoisted out of the loop.
        Expression::ListExpr(items) => Expression::ListExpr(
            items.iter().map(|e| extract_agg_inner(e, counter, aggs)).collect(),
        ),
        Expression::MapExpr(entries) => Expression::MapExpr(
            entries
                .iter()
                .map(|(k, e)| (k.clone(), extract_agg_inner(e, counter, aggs)))
                .collect(),
        ),
        Expression::ListComprehension { variable, list_expr, filter, map_expr } => {
            Expression::ListComprehension {
                variable: variable.clone(),
                list_expr: Box::new(extract_agg_inner(list_expr, counter, aggs)),
                filter: filter.clone(),
                map_expr: map_expr.clone(),
            }
        }
        // `ALL(ok IN collect(x) WHERE ok)`. The same class as the collection
        // literal above, in the sibling that fix did not reach: the aggregate
        // sits in the *list* a predicate iterates, so leaving it here handed
        // `collect` to the scalar evaluator and produced "Unknown function:
        // collect" — the identical error #670 was raised for, one variant
        // over (#997).
        //
        // The predicate itself is left alone, for the same reason a
        // comprehension's body is: it runs once per element with the loop
        // variable bound, so an aggregate there is a different question and
        // must not be silently hoisted out of the loop.
        Expression::PredicateFunction { name, variable, list_expr, predicate } => {
            Expression::PredicateFunction {
                name: name.clone(),
                variable: variable.clone(),
                list_expr: Box::new(extract_agg_inner(list_expr, counter, aggs)),
                predicate: predicate.clone(),
            }
        }
        // `reduce(t = 0, x IN collect(n.v) | t + x)`. The seed and the list are
        // evaluated once, so both hoist; the body is per-element and does not.
        Expression::Reduce { accumulator, init, variable, list_expr, expression } => {
            Expression::Reduce {
                accumulator: accumulator.clone(),
                init: Box::new(extract_agg_inner(init, counter, aggs)),
                variable: variable.clone(),
                list_expr: Box::new(extract_agg_inner(list_expr, counter, aggs)),
                expression: expression.clone(),
            }
        }
        // `collect(x)[0]`, `collect(x)[1..3]` — structurally the same as the
        // binary arm above, and missed for the same reason.
        Expression::Index { expr, index } => Expression::Index {
            expr: Box::new(extract_agg_inner(expr, counter, aggs)),
            index: Box::new(extract_agg_inner(index, counter, aggs)),
        },
        Expression::ListSlice { expr, start, end } => Expression::ListSlice {
            expr: Box::new(extract_agg_inner(expr, counter, aggs)),
            start: start.as_ref().map(|e| Box::new(extract_agg_inner(e, counter, aggs))),
            end: end.as_ref().map(|e| Box::new(extract_agg_inner(e, counter, aggs))),
        },
        // Leaf expressions and others — no aggregates possible
        other => other.clone(),
    }
}

/// An execution plan: a tree of physical operators ready to execute.
///
/// Where the sort sits relative to the projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortPosition {
    /// Sort runs before `ProjectOperator`, so only the source variables are bound.
    BeforeProjection,
    /// Sort runs after `ProjectOperator`, so only the projected aliases are bound.
    AfterProjection,
}

/// Rewrite an `ORDER BY` key so it resolves where the sort actually runs.
///
/// openCypher lets `ORDER BY` name either a projected alias or an expression over the
/// variables still in scope. The planner places the sort on one side of the projection or
/// the other depending on whether the query aggregates, and `ProjectOperator` builds a
/// *fresh* record containing only the projected aliases. So at the point the sort runs,
/// exactly one of those two spellings is bound and the other evaluates to null for every
/// row — which sorts nothing, silently. That is one defect wearing two faces: the alias
/// form was dead on plain projections (#356) and the expression form was dead on
/// aggregates (#345).
///
/// Moving the sort would only swap which spelling breaks. Instead the key is translated
/// into whichever form is bound where it lands:
///
/// - [`SortPosition::BeforeProjection`] — an alias is replaced by the expression it was
///   defined as, so `RETURN p.salary AS v ORDER BY v` sorts on `p.salary`.
/// - [`SortPosition::AfterProjection`] — an expression matching a projected item is
///   replaced by that item's alias, so `RETURN sum(p.x) AS v ORDER BY sum(p.x)` sorts on
///   the already-computed `v` rather than re-deriving an aggregate over discarded rows.
///
/// A key that matches nothing is returned unchanged: it may legitimately reference a
/// variable that is still in scope, and rewriting it would be worse than leaving it.
/// Replace RETURN aliases in a sort key with the expressions they name.
///
/// A sort placed *below* the projection sees the pre-projection record, where
/// the alias does not exist yet — only the expression behind it does. Handling
/// the bare `ORDER BY alias` case but not `ORDER BY alias.property` made this
/// silently wrong:
///
/// ```cypher
/// MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY rel.id DESC
/// ```
///
/// `rel` was unbound at sort time, so the key was null on every row, the sort
/// was a no-op, and the rows came back in scan order — which for a small
/// fixture is often *already* the ascending order, so the bug reads as a
/// correct answer until you ask for `DESC` and get the same rows back.
///
/// It surfaced through the TCK as a scenario that passed or failed depending
/// on the process, because after a `WITH` that aggregates, "scan order"
/// becomes hash order.
///
/// Only aliases naming a variable can be substituted into a property access:
/// if `rel` aliases `count(*)` then `rel.id` means nothing and is left alone
/// for the executor to reject rather than silently rewritten into something
/// else.
fn substitute_aliases(key: &Expression, return_items: &[(Expression, String)]) -> Expression {
    let aliased = |name: &str| -> Option<&Expression> {
        return_items.iter().find(|(_, alias)| alias == name).map(|(expr, _)| expr)
    };
    match key {
        Expression::Variable(name) => match aliased(name) {
            Some(expr) => expr.clone(),
            None => key.clone(),
        },
        Expression::Property { variable, property } => match aliased(variable) {
            Some(Expression::Variable(underlying)) => Expression::Property {
                variable: underlying.clone(),
                property: property.clone(),
            },
            _ => key.clone(),
        },
        Expression::Binary { left, op, right } => Expression::Binary {
            left: Box::new(substitute_aliases(left, return_items)),
            op: op.clone(),
            right: Box::new(substitute_aliases(right, return_items)),
        },
        Expression::Unary { op, expr } => Expression::Unary {
            op: op.clone(),
            expr: Box::new(substitute_aliases(expr, return_items)),
        },
        Expression::Function { name, args, distinct } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(|a| substitute_aliases(a, return_items)).collect(),
            distinct: *distinct,
        },
        other => other.clone(),
    }
}

/// Can the query see how many times a var-length target is reached?
///
/// The BFS in `VarLengthExpandOperator` marks a node visited at the depth it is
/// first reached, so `(a)-[:R*1..2]-(x)` over a triangle answers `b, c` where
/// openCypher answers `b, b, c, c`. Enumerating trails is the correct walk and
/// is far more expensive, so it is used only where the difference is
/// observable (#710).
///
/// **Conservative by construction: it answers `false` only for shapes it can
/// prove are insensitive.** Getting this wrong in one direction is a wrong
/// answer and in the other is LDBC IC1 enumerating every trail within three
/// hops of a person, so anything unrecognised enumerates.
///
/// The provably-insensitive shape is a `DISTINCT` that dedups *before*
/// anything counts, orders or truncates:
///
/// * `RETURN DISTINCT …` with no aggregate among the items and no `WITH`
///   pipeline in between (IC1, IC11), or
/// * the first `WITH` is `WITH DISTINCT …` with no aggregate among its items
///   (IC5's `WITH DISTINCT friend`, IC6's `WITH DISTINCT post`).
///
/// A `DISTINCT` that comes *after* an aggregate does not help — in
/// `WITH count(x) AS n RETURN DISTINCT n` the count has already seen the
/// duplicates — which is why the position matters and a plain "does the query
/// contain DISTINCT" test would be unsound.
fn multiplicity_is_observable(query: &Query) -> bool {
    fn has_aggregate(items: &[crate::query::ast::ReturnItem]) -> bool {
        items.iter().any(|i| expression_has_aggregate(&i.expression))
    }

    // The first WITH in the pipeline, if any, is the first thing that can
    // dedup. `stages` holds the pipeline; `with_clause` the single-WITH form.
    // A query parsed as a **clause sequence** leaves every by-kind field empty —
    // `return_clause` is `None` even for `RETURN DISTINCT` — so reading only
    // those fields sends LDBC IC11 down the enumerating path and costs it 10%
    // at SF10. Walk `clauses` when it is populated.
    if !query.clauses.is_empty() {
        for clause in &query.clauses {
            match clause {
                // The first projection decides: a DISTINCT here dedups before
                // anything downstream can count the duplicates.
                crate::query::ast::Clause::With(w) => {
                    return !(w.distinct && !has_aggregate(&w.items));
                }
                crate::query::ast::Clause::Return(r) => {
                    return !(r.distinct && !has_aggregate(&r.items));
                }
                _ => {}
            }
        }
        return true;
    }

    // `with_clause` is the first WITH; `extra_with_stages` holds any that
    // follow. Only the first can dedup before anything else sees the rows.
    let first_with = query
        .with_clause
        .as_ref()
        .or_else(|| query.extra_with_stages.first().map(|st| &st.0));

    if let Some(w) = first_with {
        // A WITH that dedups before counting absorbs the multiplicity.
        return !(w.distinct && !has_aggregate(&w.items));
    }

    match &query.return_clause {
        Some(r) => !(r.distinct && !has_aggregate(&r.items)),
        None => true,
    }
}

/// Whether an expression contains an aggregate call, at any depth.
fn expression_has_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Function { name, args, .. } => {
            const AGGREGATES: &[&str] = &[
                "count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp",
                "percentilecont", "percentiledisc",
            ];
            if AGGREGATES.contains(&name.to_lowercase().as_str()) {
                return true;
            }
            args.iter().any(expression_has_aggregate)
        }
        Expression::Binary { left, right, .. } => {
            expression_has_aggregate(left) || expression_has_aggregate(right)
        }
        Expression::Unary { expr, .. } => expression_has_aggregate(expr),
        // The detector has to agree with `extract_agg_inner` above, arm for
        // arm. It decides whether an `AggregateOperator` is planned at all,
        // so a shape the extractor can rewrite but this cannot see is never
        // given the chance (#997).
        //
        // Loop bodies are excluded on both sides for the same reason.
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_has_aggregate(e))
                || when_clauses.iter().any(|(c, t)| {
                    expression_has_aggregate(c) || expression_has_aggregate(t)
                })
                || else_result.as_ref().is_some_and(|e| expression_has_aggregate(e))
        }
        Expression::ListExpr(items) => items.iter().any(expression_has_aggregate),
        Expression::MapExpr(entries) => {
            entries.iter().any(|(_, e)| expression_has_aggregate(e))
        }
        Expression::ListComprehension { list_expr, .. }
        | Expression::PredicateFunction { list_expr, .. } => {
            expression_has_aggregate(list_expr)
        }
        Expression::Reduce { init, list_expr, .. } => {
            expression_has_aggregate(init) || expression_has_aggregate(list_expr)
        }
        Expression::Index { expr, index } => {
            expression_has_aggregate(expr) || expression_has_aggregate(index)
        }
        Expression::ListSlice { expr, start, end } => {
            expression_has_aggregate(expr)
                || start.as_ref().is_some_and(|e| expression_has_aggregate(e))
                || end.as_ref().is_some_and(|e| expression_has_aggregate(e))
        }
        _ => false,
    }
}

fn resolve_sort_key(
    key: &Expression,
    return_items: &[(Expression, String)],
    position: SortPosition,
) -> Expression {
    match position {
        SortPosition::BeforeProjection => substitute_aliases(key, return_items),
        SortPosition::AfterProjection => {
            if let Some((_, alias)) = return_items.iter().find(|(expr, _)| expr == key) {
                return Expression::Variable(alias.clone());
            }
            key.clone()
        }
    }
}

/// The `root` field holds the top-level operator (typically a `ProjectOperator` or
/// `LimitOperator`). Calling `root.next(store)` begins the Volcano pull-based execution,
/// cascading `next()` calls down the operator tree until a leaf scan produces a record.
///
/// `output_columns` lists the variable names that appear in the RETURN clause, used to
/// construct the final `RecordBatch` column headers.
///
/// `is_write` distinguishes read plans from write plans. When `true`, the executor must
/// use `next_mut(&mut store)` instead of `next(&store)`, and the caller must hold an
/// exclusive (`&mut`) reference to the `GraphStore`. This flag is set by the planner
/// when it encounters CREATE, DELETE, SET, MERGE, or schema-modification clauses.
pub struct ExecutionPlan {
    /// Root operator
    pub root: OperatorBox,
    /// Output column names
    pub output_columns: Vec<String>,
    /// Whether this plan contains write operations (CREATE/DELETE/SET)
    /// If true, executor must use next_mut() with mutable GraphStore
    pub is_write: bool,
    /// Planner diagnostics: number of candidate plans evaluated (0 = legacy planner)
    #[allow(dead_code)]
    pub candidates_evaluated: usize,
    /// Planner diagnostics: cost of chosen plan (0.0 = not computed)
    #[allow(dead_code)]
    pub chosen_plan_cost: f64,
    /// Planner diagnostics: summary of each candidate (description, cost), sorted ascending
    #[allow(dead_code)]
    pub candidate_costs: Vec<(String, f64)>,
}

impl ExecutionPlan {
    /// Create a plan without planner diagnostics (used by legacy planner paths)
    pub fn new(root: OperatorBox, output_columns: Vec<String>, is_write: bool) -> Self {
        Self {
            root,
            output_columns,
            is_write,
            candidates_evaluated: 0,
            chosen_plan_cost: 0.0,
            candidate_costs: Vec::new(),
        }
    }
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
    /// Whether the query being planned can observe var-length multiplicity.
    ///
    /// Set once per `plan` call from `multiplicity_is_observable` and read at
    /// the three sites that build a `VarLengthExpandOperator`, which sit in
    /// functions that never see the `Query`. Threading a bool through all of
    /// them would touch far more code than the decision is worth; an atomic
    /// keeps `&self` and stays `Sync` (#710).
    trail_enumeration: std::sync::atomic::AtomicBool,
}

/// Project a `RETURN` list, building an aggregation when any item aggregates.
///
/// The write paths projected their return items directly, with no aggregate
/// handling at all, so `CREATE (a) RETURN count(*)` and `MERGE (a) RETURN
/// count(*)` died on `Unknown function: count` -- an everyday query, failing
/// because the *planner* never routed `count` to the operator that implements
/// it. Adding `WITH a` in the middle made it work, which is the tell: the
/// aggregation was there, only unreachable from this branch.
///
/// This is the small, shared version. The read path keeps its own assembly
/// because it also chooses between O(1) shortcuts (label count, edge count)
/// that need the MATCH to decide, and none of those apply after a write.
fn plan_return_projection(
    input: OperatorBox,
    return_clause: &crate::query::ast::ReturnClause,
) -> (OperatorBox, Vec<String>) {
    let mut output_columns = Vec::new();
    let mut aggregates = Vec::new();
    let mut group_by: Vec<(Expression, String)> = Vec::new();
    let mut projections: Vec<(Expression, String)> = Vec::new();
    let mut post_projections: Vec<(Expression, String)> = Vec::new();
    let mut has_aggregation = false;
    let mut agg_counter = 0usize;

    for (idx, item) in return_clause.items.iter().enumerate() {
        let alias = item.column_name(idx);
        output_columns.push(alias.clone());
        let (rewritten, extracted) = extract_nested_aggregates(&item.expression, &mut agg_counter);
        if extracted.is_empty() {
            group_by.push((item.expression.clone(), alias.clone()));
            projections.push((item.expression.clone(), alias.clone()));
            post_projections.push((Expression::Variable(alias.clone()), alias.clone()));
        } else {
            has_aggregation = true;
            aggregates.extend(extracted);
            post_projections.push((rewritten, alias.clone()));
        }
    }

    let operator: OperatorBox = if has_aggregation {
        let aggregated = Box::new(AggregateOperator::new(input, group_by, aggregates));
        Box::new(ProjectOperator::new(aggregated, post_projections))
    } else {
        Box::new(ProjectOperator::new(input, projections))
    };
    (operator, output_columns)
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            _optimize: true,
            plan_cache: Mutex::new(HashMap::new()),
            cache_generation: std::sync::atomic::AtomicU64::new(0),
            config: PlannerConfig::default(),
            trail_enumeration: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create a new query planner with configuration
    pub fn with_config(config: PlannerConfig) -> Self {
        Self {
            _optimize: true,
            plan_cache: Mutex::new(HashMap::new()),
            cache_generation: std::sync::atomic::AtomicU64::new(0),
            config,
            trail_enumeration: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Get the current planner configuration
    pub fn config(&self) -> &PlannerConfig {
        &self.config
    }

    /// Build the physical plan for a detected hierarchy rewrite (ADR-035 §8).
    ///
    /// Both shapes cost far less than the expansion they replace, so the cost recorded
    /// here is the index cost rather than a placeholder: a roll-up is O(log n) in
    /// nested-set mode, and a descendant scan is one output row per descendant with no
    /// per-edge work and no visited-set.
    fn plan_hierarchy_rewrite(
        &self,
        rewrite: super::hierarchy_detector::HierarchyRewrite,
    ) -> ExecutionPlan {
        use super::hierarchy_detector::{DrivenOutput, HierarchyRewrite, OrderTestOutput};
        match rewrite {
            HierarchyRewrite::HierarchyDriven {
                index_name,
                root,
                hier_var,
                fact_var,
                fact_labels,
                edge_type,
                to_fact,
                output,
            } => {
                // Enumerate the subtree from the index, then walk the relationship
                // backwards into the fact table. Facts outside the subtree are never
                // visited, where the default plan scans all of them and discards most.
                let scan: OperatorBox =
                    Box::new(super::hierarchy_ops::HierarchyDescendantScanOperator::new(
                        index_name,
                        root,
                        hier_var.clone(),
                    ));
                let expand: OperatorBox = Box::new(
                    ExpandOperator::new(
                        scan,
                        hier_var,
                        fact_var.clone(),
                        None,
                        vec![edge_type],
                        to_fact,
                    )
                    .with_target_labels(fact_labels),
                );
                let (agg, alias) = match output {
                    DrivenOutput::Count { alias, distinct } => (
                        AggregateFunction {
                            func: AggregateType::Count,
                            expr: Expression::Variable(fact_var),
                            alias: alias.clone(),
                            distinct,
                            percentile: None,
                        },
                        alias,
                    ),
                    DrivenOutput::Sum { alias, property } => (
                        AggregateFunction {
                            func: AggregateType::Sum,
                            expr: Expression::Property {
                                variable: fact_var,
                                property,
                            },
                            alias: alias.clone(),
                            distinct: false,
                            percentile: None,
                        },
                        alias,
                    ),
                };
                ExecutionPlan {
                    root: Box::new(AggregateOperator::new(expand, Vec::new(), vec![agg])),
                    output_columns: vec![alias],
                    is_write: false,
                    candidates_evaluated: 1,
                    chosen_plan_cost: super::cost_model::HIERARCHY_DESCENDANT_SCAN_COST,
                    candidate_costs: Vec::new(),
                }
            }
            HierarchyRewrite::OrderTest {
                index_name,
                root,
                var,
                labels,
                negated,
                output,
            } => {
                // Scan the tested side once and filter it with an O(1) interval check,
                // instead of evaluating `subsumes()` as a generic expression per row.
                let scan: OperatorBox = Box::new(NodeScanOperator::new(var.clone(), labels));
                let filtered: OperatorBox =
                    Box::new(super::hierarchy_ops::HierarchyOrderTestOperator::new(
                        scan,
                        index_name,
                        var.clone(),
                        root,
                        negated,
                    ));
                match output {
                    OrderTestOutput::Count(alias) => ExecutionPlan {
                        root: Box::new(AggregateOperator::new(
                            filtered,
                            Vec::new(),
                            vec![AggregateFunction {
                                func: AggregateType::Count,
                                expr: Expression::Variable(var),
                                alias: alias.clone(),
                                distinct: false,
                                percentile: None,
                            }],
                        )),
                        output_columns: vec![alias],
                        is_write: false,
                        candidates_evaluated: 1,
                        chosen_plan_cost: super::cost_model::HIERARCHY_DESCENDANT_SCAN_COST,
                        candidate_costs: Vec::new(),
                    },
                    OrderTestOutput::Nodes => ExecutionPlan {
                        root: filtered,
                        output_columns: vec![var],
                        is_write: false,
                        candidates_evaluated: 1,
                        chosen_plan_cost: super::cost_model::HIERARCHY_DESCENDANT_SCAN_COST,
                        candidate_costs: Vec::new(),
                    },
                }
            }
            HierarchyRewrite::Rollup {
                index_name,
                root,
                op,
                alias,
            } => ExecutionPlan {
                root: Box::new(super::hierarchy_ops::HierarchyRollupOperator::new(
                    index_name,
                    root,
                    op,
                    alias.clone(),
                )),
                output_columns: vec![alias],
                is_write: false,
                candidates_evaluated: 1,
                chosen_plan_cost: super::cost_model::HIERARCHY_ROLLUP_COST,
                candidate_costs: Vec::new(),
            },
            HierarchyRewrite::DescendantScan {
                index_name,
                root,
                var,
            } => ExecutionPlan {
                root: Box::new(super::hierarchy_ops::HierarchyDescendantScanOperator::new(
                    index_name,
                    root,
                    var.clone(),
                )),
                output_columns: vec![var],
                is_write: false,
                candidates_evaluated: 1,
                chosen_plan_cost: super::cost_model::HIERARCHY_DESCENDANT_SCAN_COST,
                candidate_costs: Vec::new(),
            },
        }
    }

    /// Invalidate the plan cache (e.g., after CREATE INDEX or schema change)
    pub fn invalidate_cache(&self) {
        self.cache_generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.plan_cache.lock().unwrap().clear();
    }

    /// Plan a query
    /// Plan a query, then apply `RETURN DISTINCT` if the query asked for it.
    ///
    /// The deduplication is applied here, at the single point every planning path funnels
    /// through, rather than inside the dozen-odd places that build a `ProjectOperator`.
    /// That matters because several of those paths are specialized fast plans
    /// (adjacency-count aggregation, hierarchy rewrites, label-count caches) that were
    /// each individually capable of dropping the flag on the floor — which is how
    /// `RETURN DISTINCT` came to be a complete no-op (#311). One choke point cannot
    /// forget.
    /// Reject non-literal property values where they are not yet evaluated.
    ///
    /// CREATE evaluates them per row; MATCH and MERGE patterns do not. Until they do, a
    /// pattern carrying one has to be an error: silently dropping the constraint means
    /// `MATCH (p:P {n: x})` returns every `:P` rather than the matching one, which looks
    /// like a working query returning too much.
    fn reject_unevaluated_property_exprs(query: &Query) -> ExecutionResult<()> {
        let complain = |clause: &str, key: &str| {
            ExecutionError::PlanningError(format!(
                "{clause} does not yet support a non-literal property value (`{key}`); use a WHERE comparison instead"
            ))
        };

        for mc in &query.match_clauses {
            for path in &mc.pattern.paths {
                if let Some(key) = path.start.property_exprs.as_ref().and_then(|e| e.keys().next()) {
                    return Err(complain("MATCH", key));
                }
                for seg in &path.segments {
                    if let Some(key) = seg.node.property_exprs.as_ref().and_then(|e| e.keys().next()) {
                        return Err(complain("MATCH", key));
                    }
                    if let Some(key) = seg.edge.property_exprs.as_ref().and_then(|e| e.keys().next()) {
                        return Err(complain("MATCH", key));
                    }
                }
            }
        }

        // A clause-pipeline query keeps its clauses here and leaves the fields
        // above empty, so the checks above see nothing. Missing this let
        // `UNWIND ['a','b','a'] AS x MERGE (n:N {v: x})` create **one** node
        // instead of two: the property is an expression, MERGE matched on the
        // label alone, and every row found the first `:N`. The established
        // path had refused that query for exactly this reason since #311's
        // lesson — a new path does not inherit an old guard.
        for clause in &query.clauses {
            // MERGE resolves its property expressions against the row now
            // (#642), so only MATCH is still restricted here.
            let (label, pattern) = match clause {
                crate::query::ast::Clause::Match(m) => ("MATCH", &m.pattern),
                _ => continue,
            };
            for path in &pattern.paths {
                if let Some(key) = path.start.property_exprs.as_ref().and_then(|e| e.keys().next()) {
                    return Err(complain(label, key));
                }
                for seg in &path.segments {
                    if let Some(key) = seg.node.property_exprs.as_ref().and_then(|e| e.keys().next()) {
                        return Err(complain(label, key));
                    }
                    if let Some(key) = seg.edge.property_exprs.as_ref().and_then(|e| e.keys().next()) {
                        return Err(complain(label, key));
                    }
                }
            }
        }

        Ok(())
    }

    /// Variables that exist only *above* the match pipeline, and so cannot be referenced by
    /// a predicate evaluated during match planning.
    ///
    /// Two sources, both of which bind their variables in an operator that sits on top of
    /// the matches: a leading UNWIND, and a `CALL ... YIELD`. A predicate mentioning one of
    /// these must be left to the top-level WHERE filter, which runs after both are joined
    /// in. Assigning it to a MATCH instead puts the filter underneath the operator that
    /// binds the variable, and the query dies with "Variable not found" even though the
    /// same variable projects fine in RETURN (#429).
    /// Whether the query has an `UNWIND` anywhere, in either slot the parser
    /// uses for one.
    ///
    /// A leading `UNWIND` is `query.unwind_clause` when a single `WITH`
    /// follows, and the *first extra stage's* unwind when two or more do. Four
    /// early branches -- the CREATE-only path, the standalone-RETURN path, the
    /// leading-FOREACH path and the "no clauses at all" error -- tested only
    /// the first slot, so `UNWIND [1,2] AS x WITH x WHERE x > 1 WITH ... RETURN`
    /// was planned as a standalone RETURN and the `UNWIND` vanished from the
    /// plan entirely (#572).
    fn has_any_unwind(query: &Query) -> bool {
        query.unwind_clause.is_some()
            || query
                .extra_with_stages
                .iter()
                .any(|(_, unwind, _, _)| unwind.is_some())
    }

    fn late_bound_variables(query: &Query) -> HashSet<String> {
        let mut vars = HashSet::new();
        // Not gated on `unwind_leading`. That flag says the *query* opens with
        // UNWIND; the question here is whether the variable is bound above the
        // matches, and an `unwind_clause` always is -- the parser only puts a
        // pre-WITH unwind there. Gating on the wrong flag left
        // `MATCH (n) UNWIND [1,2,3] AS x WHERE x > 1 WITH x RETURN x` with its
        // predicate decomposed into match planning, filtering on an `x` that
        // nothing had bound yet (#927).
        if let Some(u) = &query.unwind_clause {
            vars.insert(u.variable.clone());
        }
        for extra in &query.extra_unwind_clauses {
            vars.insert(extra.variable.clone());
        }
        if let Some(call) = &query.call_clause {
            for item in &call.yield_items {
                vars.insert(item.alias.clone().unwrap_or_else(|| item.name.clone()));
            }
        }
        vars
    }

    pub fn plan(&self, query: &Query, store: &GraphStore) -> ExecutionResult<ExecutionPlan> {
        // Decide once, here, whether a var-length walk must enumerate trails.
        // See `multiplicity_is_observable`: the BFS answers `b, c` where
        // openCypher answers `b, b, c, c`, and enumerating is only affordable
        // where a `DISTINCT` makes the difference invisible (#710).
        self.trail_enumeration.store(
            multiplicity_is_observable(query),
            std::sync::atomic::Ordering::Relaxed,
        );
        // A query parsed as a clause sequence has empty by-kind fields — it is
        // there precisely because they cannot represent it. Planning it through
        // the established path would read those empty fields as "no MATCH, no
        // CREATE" and answer a different query, which is worse than the parse
        // error it replaced. Until `plan_clause_pipeline` covers a shape, the
        // engine says so.
        // Checked for *both* paths. It reads `query.clauses` as well as the
        // by-kind fields, so a pipeline query cannot slip a pattern carrying an
        // unevaluated property expression past it.
        Self::reject_unevaluated_property_exprs(query)?;
        if query.needs_clause_pipeline {
            return self.plan_clause_pipeline(query, store);
        }
        // `DISTINCT` is inserted inside `plan_inner`, below SKIP and LIMIT.
        // Wrapping the finished plan here put it *above* them, so `LIMIT n`
        // took n duplicate-bearing rows and DISTINCT then collapsed them --
        // `RETURN DISTINCT p.city LIMIT 3` returned one row (#522).
        self.plan_inner(query, store)
    }

    fn plan_inner(&self, query: &Query, store: &GraphStore) -> ExecutionResult<ExecutionPlan> {
        // Handle SHOW INDEXES
        if query.show_indexes {
            return Ok(ExecutionPlan {
                root: Box::new(ShowIndexesOperator::new()),
                output_columns: vec!["label".to_string(), "property".to_string(), "type".to_string()],
                is_write: false, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
            });
        }

        // Handle SHOW HIERARCHY INDEXES (ADR-035)
        if query.show_hierarchy_indexes {
            return Ok(ExecutionPlan {
                root: Box::new(super::hierarchy_ops::ShowHierarchyIndexesOperator::new()),
                output_columns: super::hierarchy_ops::hierarchy_info_columns(),
                is_write: false, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
            });
        }

        // Handle CREATE HIERARCHY INDEX (ADR-035)
        if let Some(clause) = &query.create_hierarchy_index_clause {
            let mut spec = crate::index::hierarchy::HierarchySpec::new(
                clause.name.clone(),
                clause
                    .edge_types
                    .iter()
                    .map(|t| crate::graph::types::EdgeType::new(t.as_str()))
                    .collect(),
            );
            spec.reverse = clause.reverse;
            if let Some(prop) = &clause.measure_property {
                // An unrecognized aggregate name is rejected here rather than silently
                // dropped: a user who asks for `AGGREGATE avg` should learn that the index
                // does not support it, not get an index that quietly lacks it.
                let mut ops = Vec::new();
                for name in &clause.aggregates {
                    match crate::index::hierarchy::RollupOp::parse(name) {
                        Some(op) => ops.push(op),
                        None => {
                            return Err(ExecutionError::RuntimeError(format!(
                                "unsupported hierarchy aggregate '{}': expected sum, count, min or max",
                                name
                            )))
                        }
                    }
                }
                if ops.is_empty() {
                    ops.push(crate::index::hierarchy::RollupOp::Sum);
                }
                spec = spec.with_measure(
                    clause
                        .measure_label
                        .as_ref()
                        .map(|l| crate::graph::types::Label::new(l.as_str())),
                    prop.clone(),
                    ops,
                );
            }
            return Ok(ExecutionPlan {
                root: Box::new(super::hierarchy_ops::CreateHierarchyIndexOperator::new(spec)),
                output_columns: super::hierarchy_ops::hierarchy_info_columns(),
                is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
            });
        }

        // Handle DROP HIERARCHY INDEX
        if let Some(name) = &query.drop_hierarchy_index {
            return Ok(ExecutionPlan {
                root: Box::new(super::hierarchy_ops::DropHierarchyIndexOperator::new(name.clone())),
                output_columns: vec![],
                is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
            });
        }

        // Handle REBUILD HIERARCHY INDEX
        if let Some(name) = &query.rebuild_hierarchy_index {
            return Ok(ExecutionPlan {
                root: Box::new(super::hierarchy_ops::RebuildHierarchyIndexOperator::new(name.clone())),
                output_columns: super::hierarchy_ops::hierarchy_info_columns(),
                is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
            });
        }

        // Handle SHOW CONSTRAINTS
        if query.show_constraints {
            return Ok(ExecutionPlan {
                root: Box::new(ShowConstraintsOperator::new()),
                output_columns: vec!["label".to_string(), "property".to_string(), "type".to_string()],
                is_write: false, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
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
                is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
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
                is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
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
                is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
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
                    is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
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
                    is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
                });
            }
        }

        // ADR-035: answer a reflexive subtree aggregate or enumeration from the OEH
        // hierarchy index. The detector is conservative — when it returns Some, the
        // rewrite is answer-preserving and only the cost changes.
        if let Some(rewrite) = super::hierarchy_detector::detect(query, store) {
            return Ok(self.plan_hierarchy_rewrite(rewrite));
        }

        // ADR-017 Phase 1: recognize the adjacency-count-aggregate shape before
        // the generic planner runs. The detector's constraints are conservative
        // enough that when it returns Some, the specialized plan is always
        // correct for the query. The specialized plan short-circuits the
        // Expand→Aggregate path that causes MB049/MB054 to time out.
        if let Some(pat) = super::adjacency_agg_detector::detect(query, store) {
            return self.plan_adjacency_count_aggregate(query, pat);
        }
        // ADR-017 Phase 3a: the WITH-bound variant handles MB053 and EX49,
        // where an explicit pre-WITH LIMIT caps the work per group but the
        // second MATCH would otherwise spill billions of edges into the
        // generic aggregate.
        if let Some(pat) = super::adjacency_agg_detector::detect_with_binding(query) {
            return self.plan_adjacency_count_aggregate_with_binding(query, pat);
        }
        // Phase 4 (PR-P2.8): aggregate-then-expand. CT20 shape.
        if let Some(pat) = super::adjacency_agg_detector::detect_aggregate_then_expand(query, store) {
            return self.plan_aggregate_then_expand(query, pat);
        }

        // Handle MERGE-only statement (no MATCH needed).
        //
        // A leading UNWIND is excluded for the same reason as the CREATE-only
        // branch below: `UNWIND [...] AS x MERGE (n:N {v: x})` has no MATCH but
        // is still row-driven, and planning it here runs the MERGE once with
        // nothing bound. It falls through to the general path, where the
        // Unwind feeds the merge.
        if query.match_clauses.is_empty()
            && query.call_clause.is_none()
            && !Self::has_any_unwind(query)
        {
            if let Some(merge_clause) = &query.merge_clause {
                let on_create: Vec<(String, String, Expression)> = merge_clause.on_create_set.iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();
                let on_match: Vec<(String, String, Expression)> = merge_clause.on_match_set.iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();
                let on_create_labels: Vec<(String, Vec<Label>)> = merge_clause.on_create_labels.iter()
                    .map(|l| (l.variable.clone(), l.labels.clone()))
                    .collect();
                let on_match_labels: Vec<(String, Vec<Label>)> = merge_clause.on_match_labels.iter()
                    .map(|l| (l.variable.clone(), l.labels.clone()))
                    .collect();

                // `ON CREATE SET n = {…}` / `n += {…}` (#874).
                let on_create_entity: Vec<(String, bool, Expression)> = merge_clause
                    .on_create_entity_set
                    .iter()
                    .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                    .collect();
                let on_match_entity: Vec<(String, bool, Expression)> = merge_clause
                    .on_match_entity_set
                    .iter()
                    .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                    .collect();
                let mut operator: OperatorBox = Box::new(
                    MergeOperator::new(
                        merge_clause.pattern.clone(),
                        on_create,
                        on_match,
                        on_create_labels,
                        on_match_labels,
                    )
                    .with_entity_sets(on_create_entity, on_match_entity),
                );

                // `MERGE p = (...)` binds `p` (#876).
                let merge_paths = named_path_handles(&merge_clause.pattern);
                if !merge_paths.is_empty() {
                    operator = Box::new(crate::query::executor::operator::BindPathOperator::new(
                        operator,
                        merge_paths,
                    ));
                }

                // A bare `SET` after MERGE applies on both branches, unlike ON CREATE /
                // ON MATCH. It parsed but was dropped here, so `MERGE (m) SET m.x = 1`
                // silently left the property unset.
                if !query.set_clauses.is_empty() {
                    let items: Vec<(String, String, Expression)> = query
                        .set_clauses
                        .iter()
                        .flat_map(|sc| sc.items.iter())
                        .map(|item| {
                            (item.variable.clone(), item.property.clone(), item.value.clone())
                        })
                        .collect();
                    operator = Box::new(SetPropertyOperator::new(operator, items));
                }

                let mut output_columns = Vec::new();
                if let Some(return_clause) = &query.return_clause {
                    let (projected, columns) = plan_return_projection(operator, return_clause);
                    operator = projected;
                    output_columns = columns;
                }

                return Ok(ExecutionPlan {
                    root: operator,
                    output_columns,
                    is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
                });
            }
        }

        // Handle CREATE-only queries (no MATCH/CALL required).
        //
        // A leading UNWIND is excluded: `UNWIND $rows AS row CREATE (:N {id: row.id})` has
        // no MATCH but is still row-driven, and planning it as a CREATE-only statement runs
        // it once with nothing bound. It falls through to the general pipeline, where the
        // Unwind feeds the create.
        if query.match_clauses.is_empty()
            && query.call_clause.is_none()
            && !Self::has_any_unwind(query)
        {
            if let Some(create_clause) = &query.create_clause {
                let mut plan = self.plan_create_only(create_clause)?;
                // CY-12: Wrap with ProjectOperator if RETURN clause is present
                if let Some(return_clause) = &query.return_clause {
                    let (projected, columns) = plan_return_projection(plan.root, return_clause);
                    plan.root = projected;
                    plan.output_columns = columns;

                    // `ORDER BY`, `SKIP` and `LIMIT` after a bare `CREATE`.
                    //
                    // This path returned straight after projecting, so
                    // `CREATE (n:N) RETURN n LIMIT 0` produced a row -- the one
                    // shape where the clause was silently dropped. It works
                    // wherever the create has input rows
                    // (`UNWIND [1,2,3] AS x CREATE ... RETURN n LIMIT 2` is
                    // correct), because that goes through a different planner
                    // path which applies them.
                    //
                    // The side effects still happen: the nodes are created and
                    // only the *result set* is trimmed, which is exactly what
                    // `Create6` asserts (#866).
                    if let Some(order_by) = &query.order_by {
                        let sort_items: Vec<(Expression, bool)> = order_by
                            .items
                            .iter()
                            .map(|i| (i.expression.clone(), i.ascending))
                            .collect();
                        plan.root = Box::new(SortOperator::new(plan.root, sort_items));
                    }
                    // `SKIP`/`LIMIT` go through an **eager** barrier that does
                    // the trimming itself.
                    //
                    // A `LimitOperator(0)` returns without pulling, so a lazy
                    // plan would never run the create beneath it: the query
                    // reported success and changed nothing. Cypher trims the
                    // result set and not the side effects -- `CREATE (n:N)
                    // RETURN n LIMIT 0` still creates the node (#866).
                    if query.skip.is_some() || query.limit.is_some() {
                        plan.root = Box::new(
                            crate::query::executor::operator::EagerOperator::new(
                                plan.root,
                                query.skip.unwrap_or(0),
                                query.limit,
                            ),
                        );
                    }
                }
                return Ok(plan);
            }
            // CY-32: Standalone WITH...RETURN (e.g., WITH datetime() AS dt RETURN dt.year)
            if let (Some(with_clause), Some(return_clause)) = (&query.with_clause, &query.return_clause) {
                use crate::query::executor::operator::SingleRowOperator;

                // WITH projection: bind expressions to aliases
                let with_projections: Vec<(Expression, String)> = with_clause.items.iter().enumerate().map(|(i, item)| {
                    let alias = item.column_name(i);
                    (item.expression.clone(), alias)
                }).collect();

                let with_op: OperatorBox = Box::new(ProjectOperator::new(
                    Box::new(SingleRowOperator::new()),
                    with_projections,
                ));

                // RETURN projection: project from WITH-bound variables
                let mut output_columns = Vec::new();
                let return_projections: Vec<(Expression, String)> = return_clause.items.iter().enumerate().map(|(i, item)| {
                    let alias = item.column_name(i);
                    output_columns.push(alias.clone());
                    (item.expression.clone(), alias)
                }).collect();

                let root: OperatorBox = Box::new(ProjectOperator::new(with_op, return_projections));

                return Ok(ExecutionPlan {
                    root,
                    output_columns,
                    is_write: false, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
                });
            }

            // CY-30: Standalone RETURN without MATCH/CREATE (e.g., RETURN 1+2, RETURN sin(0.5)).
            // A leading UNWIND also has no MATCH, but it is *not* a single-row projection --
            // it must fall through to the general pipeline so the Unwind, and any
            // aggregation over it, get planned.
            if !Self::has_any_unwind(query) {
            if let Some(return_clause) = &query.return_clause {
                // Single-row operator that emits one empty record for projection
                use crate::query::executor::operator::SingleRowOperator;
                let mut output_columns = Vec::new();
                let projections: Vec<(Expression, String)> = return_clause.items.iter().enumerate().map(|(i, item)| {
                    let alias = item.column_name(i);
                    output_columns.push(alias.clone());
                    (item.expression.clone(), alias)
                }).collect();

                let root: OperatorBox = Box::new(ProjectOperator::new(
                    Box::new(SingleRowOperator::new()),
                    projections,
                ));

                return Ok(ExecutionPlan {
                    root,
                    output_columns,
                    is_write: false, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
                });
            }
            }

            // A leading FOREACH has no pattern to drive it, so it runs against a
            // single empty row -- the same way a bare RETURN does. The loop
            // variable is bound per element inside ForeachOperator, so nothing
            // upstream needs to supply bindings.
            if query.foreach_clause.is_some() && !Self::has_any_unwind(query) {
                let foreach_clause = query.foreach_clause.as_ref().expect("checked above");
                let mut set_items = Vec::new();
                for set_clause in &foreach_clause.set_clauses {
                    for item in &set_clause.items {
                        set_items.push((item.variable.clone(), item.property.clone(), item.value.clone()));
                    }
                }
                let create_patterns: Vec<Pattern> = foreach_clause
                    .create_clauses
                    .iter()
                    .map(|c| c.pattern.clone())
                    .collect();
                let root: OperatorBox = Box::new(ForeachOperator::new(
                    Box::new(crate::query::executor::operator::SingleRowOperator::new()),
                    foreach_clause.variable.clone(),
                    foreach_clause.expression.clone(),
                    set_items,
                    create_patterns,
                ));
                return Ok(ExecutionPlan {
                    root,
                    output_columns: Vec::new(),
                    is_write: true,
                    candidates_evaluated: 0,
                    chosen_plan_cost: 0.0,
                    candidate_costs: Vec::new(),
                });
            }

            if !Self::has_any_unwind(query) {
                return Err(ExecutionError::PlanningError(
                    "Query must have at least one MATCH, CALL, CREATE, or RETURN clause".to_string()
                ));
            }
        }

        let mut operator: Option<OperatorBox> = None;
        let mut known_vars: HashSet<String> = HashSet::new();

        // Determine split point for WITH barrier
        let split = query.with_split_index.unwrap_or(query.match_clauses.len());
        // Propagate node labels/inline properties for variables shared across multiple
        // MATCH clauses in this pre-WITH group, e.g.
        //   MATCH (m:Post {id: 1})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
        //   MATCH (m)-[:HAS_CREATOR]->(op:Person)
        // Without this, the second clause's bare `(m)` has no known label or index
        // predicate and each MATCH clause is planned independently, so it falls back to
        // an unrestricted all-nodes scan even though `m` is already typed (and pinned to
        // one id) earlier in the same query. Same variable = same node, so inheriting
        // constraints declared anywhere in the query is correctness-preserving, not a guess.
        let enriched_pre_with_clauses = propagate_shared_variable_labels(&query.match_clauses[..split]);
        let pre_with_clauses: &[MatchClause] = &enriched_pre_with_clauses;
        let post_with_clauses = &query.match_clauses[split..];

        // Pre-compute variable sets for each pre-WITH MATCH clause
        let pre_match_var_sets: Vec<HashSet<String>> = pre_with_clauses
            .iter()
            .map(|mc| Self::clause_variables(&mc.pattern))
            .collect();

        // Decompose WHERE clause: assign predicates to MATCH clauses or cross-MATCH
        let pre_where_preds = query.where_clause.as_ref()
            .map(|wc| flatten_and_predicates(&wc.predicate))
            .unwrap_or_default();
        let mut per_match_where: Vec<Option<WhereClause>> = vec![None; pre_with_clauses.len()];
        let mut cross_match_predicates: Vec<Expression> = Vec::new();

        // See the matching guard in the WITH-stage decomposition below: a predicate that
        // references a leading UNWIND's variable cannot run during match planning, because
        // the Unwind operator that binds it sits above the matches. The top-level WHERE
        // filter re-applies the full predicate after the Unwind, so dropping it here loses
        // nothing.
        let late_bound_pre = Self::late_bound_variables(query);

        // Predicates deferred past match planning because they name a leading
        // UNWIND's variable. Kept rather than dropped: the claim above -- that
        // the top-level WHERE re-applies them -- holds only when there is no
        // WITH, because that filter is skipped once a barrier exists. With a
        // WITH they were being discarded, so
        // `UNWIND [1,2,3] AS x MATCH (p) WHERE p.n = x WITH ... RETURN ...`
        // returned a cross product instead of the join (#572).
        let mut late_bound_predicates: Vec<Expression> = Vec::new();
        // Predicates that become join conditions on an OPTIONAL MATCH (#667).
        let mut optional_join_predicates: Vec<Option<Expression>> =
            vec![None; pre_with_clauses.len()];

        for pred in pre_where_preds {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(&pred, &mut pred_vars);
            if pred_vars.iter().any(|v| late_bound_pre.contains(v)) {
                late_bound_predicates.push(pred);
                continue;
            }

            // A predicate mentioning an OPTIONAL MATCH's variables belongs to
            // that clause, not above the join. Cypher scopes the WHERE after an
            // OPTIONAL MATCH to the optional match itself, so a row failing it
            // keeps the left side and nulls the right — filtering above the
            // join deletes the row entirely, which is what
            // `OPTIONAL MATCH (x)-[:E1]->(y) WHERE y.val > 4` did: one row
            // returned where Cypher returns three (#667).
            //
            // Only for predicates that cannot be pushed *into* the optional
            // clause's own plan, i.e. ones also referencing an outer variable.
            // Any predicate naming a variable the optional clause introduces.
            // "Introduces" is the operative word: the clause's own set includes
            // the join variable it shares with the outer match, so testing
            // against the whole set catches predicates that belong entirely to
            // the outer side.
            let optional_target = pre_with_clauses.iter().enumerate().find(|(i, mc)| {
                if !mc.optional || pred_vars.is_empty() {
                    return false;
                }
                let own = &pre_match_var_sets[*i];
                // Must *span* the join: name something this clause introduces
                // **and** something it does not.
                //
                // A predicate naming only the optional clause's own variables
                // is pushed inside that clause instead, where it can anchor the
                // scan — `OPTIONAL MATCH (b:N) WHERE id(b) = 6` has to stay an
                // id lookup. Routing those here cost that anchor and
                // `anchor_coverage` caught it. They are already handled
                // correctly by the existing per-clause decomposition: a filter
                // inside the optional side leaves unmatched rows null, which is
                // what Cypher asks for.
                let earlier: HashSet<&String> = pre_match_var_sets[..*i]
                    .iter()
                    .flat_map(|s| s.iter())
                    .collect();
                let introduced: HashSet<&String> =
                    own.iter().filter(|v| !earlier.contains(*v)).collect();
                let touches_optional = pred_vars.iter().any(|v| introduced.contains(v));
                let touches_outer = pred_vars.iter().any(|v| !introduced.contains(v));
                touches_optional && touches_outer
            });
            if let Some((i, _)) = optional_target {
                optional_join_predicates[i] = Some(match optional_join_predicates[i].take() {
                    Some(existing) => Expression::Binary {
                        left: Box::new(existing),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    },
                    None => pred,
                });
                continue;
            }

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

        // Names for anonymous pattern variables, shared by both MATCH loops so
        // a name minted before the first `WITH` cannot collide with one minted
        // after it.
        let mut anon_counter: usize = 0;

        // 1a. Handle pre-WITH MATCH clauses
        for (match_idx, match_clause) in pre_with_clauses.iter().enumerate() {
            // A clause whose paths all start from a variable the pipeline has
            // already bound is an expansion, not a second scan to join against.
            // Only the post-`WITH` loop did this, so a query whose clauses all
            // precede the first `WITH` — most queries — planned each one
            // standalone and joined: `MATCH (m:Post {id: $id})-[:HAS_CREATOR]->(op)
            // MATCH (op)-[:KNOWS]-(f) RETURN count(f)` scanned every `:Person`
            // and hash-joined back to the one node already resolved (#711).
            if Self::can_pushdown_match(match_clause, &known_vars) && operator.is_some() {
                let upstream = operator.take().unwrap();
                let (current_op, new_vars) = self.plan_pushed_down_match(
                    match_clause,
                    per_match_where[match_idx].as_ref(),
                    &pre_match_var_sets[match_idx],
                    upstream,
                    &mut anon_counter,
                )?;
                operator = Some(current_op);
                known_vars.extend(new_vars);
                continue;
            }

            // The same idea for `OPTIONAL MATCH`, which `can_pushdown_match`
            // declines outright because it has to emit a null-filled row when
            // nothing matches. A single-segment optional clause hanging off a
            // bound variable can do that inside the expand, and the difference
            // is not marginal: `OPTIONAL MATCH (op)-[k:KNOWS]-(author)` with
            // both ends bound scanned every `:Person` and cost 422 ms where
            // the equivalent `EXISTS` cost 0.02 (#726).
            //
            // A `WHERE` on the clause keeps the join: in `OPTIONAL MATCH` the
            // predicate is part of the optional pattern, so a row failing it
            // must still emit nulls, and a filter above the expand would
            // delete exactly that row.
            //
            // `per_match_where` is not the only place that `WHERE` can land.
            // A predicate spanning the optional side and an outer variable --
            // `OPTIONAL MATCH (b)-[r2]-(c) WHERE r <> r2` -- is classified as
            // a *join* predicate above, which leaves `per_match_where` empty.
            // Checking only `per_match_where` let exactly those clauses push
            // down, and the pushdown has nowhere to put a join predicate, so
            // it was dropped: `r <> r2` stopped being applied at all and the
            // relationship a row was matched on came back as its own
            // `r2` (#982). Both stores have to be empty to push down.
            if operator.is_some()
                && per_match_where[match_idx].is_none()
                && optional_join_predicates[match_idx].is_none()
            {
                if let Some(null_vars) =
                    Self::optional_pushdown_vars(match_clause, &known_vars)
                {
                    let upstream = operator.take().unwrap();
                    let path = &match_clause.pattern.paths[0];
                    let start_var = path.start.variable.clone().unwrap();
                    operator = Some(self.plan_optional_expand(
                        path,
                        &start_var,
                        null_vars,
                        &known_vars,
                        upstream,
                        store,
                    ));
                    known_vars.extend(pre_match_var_sets[match_idx].iter().cloned());
                    continue;
                }
            }

            let match_op = self.dispatch_plan_match(match_clause, per_match_where[match_idx].as_ref(), store)?;

            let clause_vars = pre_match_var_sets[match_idx].clone();

            operator = Some(match operator {
                Some(existing) => {
                    // ALL shared variables form the join key. Taking one of them left the
                    // rest uncorrelated — a silent cartesian product — and since this comes
                    // from a HashSet intersection, which one varied between runs (#360).
                    let mut shared: Vec<String> =
                        known_vars.intersection(&clause_vars).cloned().collect();
                    shared.sort();
                    if !shared.is_empty() {
                        if match_clause.optional {
                            let right_only: Vec<String> = clause_vars.difference(&known_vars).cloned().collect();
                            let mut join =
                                LeftOuterJoinOperator::new(existing, match_op, shared.clone(), right_only);
                            if let Some(pred) = optional_join_predicates[match_idx].clone() {
                                join = join.with_join_predicate(pred);
                            }
                            Box::new(join) as OperatorBox
                        } else {
                            Box::new(JoinOperator::new(existing, match_op, shared.clone())) as OperatorBox
                        }
                    } else if match_clause.optional {
                        // A *disjoint* OPTIONAL MATCH -- one sharing no
                        // variable with anything bound so far -- used to fall
                        // to a cartesian product, which ignores `optional`
                        // entirely. A cartesian with an empty right side
                        // yields nothing, so
                        // `MATCH (f:Exists) OPTIONAL MATCH (n:DoesNotExist)`
                        // destroyed every row the MATCH had found and
                        // `count(f)` answered 0 instead of 3 (#954): a left
                        // outer join behaving as an inner one, which is the
                        // one property OPTIONAL MATCH exists to provide.
                        //
                        // The join operator already does the right thing with
                        // no join variables. `key_of` over an empty list is
                        // `Some(vec![])` for every record on both sides, so a
                        // non-empty right still produces the full cartesian
                        // product and an empty one null-fills the left.
                        let right_only: Vec<String> =
                            clause_vars.difference(&known_vars).cloned().collect();
                        let mut join =
                            LeftOuterJoinOperator::new(existing, match_op, Vec::new(), right_only);
                        if let Some(pred) = optional_join_predicates[match_idx].clone() {
                            join = join.with_join_predicate(pred);
                        }
                        Box::new(join) as OperatorBox
                    } else {
                        Box::new(CartesianProductOperator::new(existing, match_op)) as OperatorBox
                    }
                }
                // A *leading* OPTIONAL MATCH has no left side, so there was
                // nothing to null-fill and a query matching nothing returned
                // **no rows** instead of one null row —
                // `OPTIONAL MATCH (a:Nope) RETURN a` came back empty (#671).
                // Joining against a single empty row gives the outer join the
                // left side the clause implies.
                None if match_clause.optional => {
                    use crate::query::executor::operator::SingleRowOperator;
                    let right_only: Vec<String> = clause_vars.iter().cloned().collect();
                    Box::new(LeftOuterJoinOperator::new(
                        Box::new(SingleRowOperator::new()),
                        match_op,
                        Vec::new(),
                        right_only,
                    )) as OperatorBox
                }
                None => match_op,
            });
            known_vars.extend(clause_vars);
        }

        // A leading UNWIND binds its variable before anything downstream reads it,
        // and that has to happen *before* the first WITH barrier.
        //
        // A barrier projects the variables the WITH names, so with the UNWIND
        // applied after it there is nothing bound for it to project, and
        // `UNWIND [1,2,3] AS x WITH x WHERE x > 1 RETURN x` failed with
        // `VariableNotFound("x")` -- as did every other shape, whatever the
        // value type. Since `WHERE` cannot follow `UNWIND` directly, that left
        // no way to filter an unwound list at all (#572).
        //
        // It also has to happen before the cross-MATCH predicates below, which
        // may reference it: `UNWIND [1,2] AS x MATCH (p) WHERE p.n = x` puts
        // `x` in a predicate spanning the unwound variable and a match
        // variable. Applied after that filter, the predicate was silently
        // dropped rather than failing, and the query returned a cross product.
        //
        // A statement that begins with UNWIND has no MATCH to build a pipeline
        // on, so it is seeded with a single empty row here for the same reason
        // it was seeded further down: the rest of the planner -- filters,
        // aggregation, ORDER BY, SKIP/LIMIT -- then applies unchanged.
        //
        // The leading UNWIND is always `query.unwind_clause`. It used to be
        // read from `extra_with_stages[0].1` when there were two or more
        // stages, because the parser moved it there -- but that slot also
        // holds a stage's *own* trailing unwind, and nothing distinguished the
        // two. With both present the stage's unwind was hoisted to the head
        // and read a variable its WITH had not projected yet (#785).
        //
        // `unwind_leading` says the *query* opens with UNWIND. It does not say
        // which side of the WITH the unwind is on, and that is the question
        // the planner has to answer. The parser already settles it: it only
        // fills `unwind_clause` while no WITH has been seen, and puts anything
        // after one in `post_with_unwind_clauses`. So an `unwind_clause` is
        // *always* written before the WITH, leading or not.
        //
        // Gating this block on `unwind_leading` therefore sent
        // `MATCH ... UNWIND ... WITH ...` down the trailing path, where the
        // barrier runs first and the unwind's list expression then reads
        // variables the WITH has already dropped: `MATCH (n) UNWIND [n] AS x
        // WITH x RETURN x` died with VariableNotFound("n") (#927).
        let leading_unwind: Option<&UnwindClause> = query.unwind_clause.as_ref();

        // Applied here only when a WITH follows. Without one there are no
        // barriers to be on the wrong side of, and the trailing site below
        // already places it after the filter so the cross product is built
        // from narrowed rows -- doing both ran the unwind twice and turned ten
        // rows into a hundred, which is the second half of the #785 trap.
        let unwind_before_barrier = query.unwind_leading
            || query.with_clause.is_some()
            || !query.extra_with_stages.is_empty();

        if unwind_before_barrier {
            if let Some(unwind) = leading_unwind {
                use crate::query::executor::operator::SingleRowOperator;
                let base: OperatorBox = match operator.take() {
                    Some(op) => op,
                    None => Box::new(SingleRowOperator::new()),
                };
                operator = Some(Box::new(UnwindOperator::new(
                    base,
                    unwind.expression.clone(),
                    unwind.variable.clone(),
                )));
                known_vars.insert(unwind.variable.clone());

                // A run of UNWINDs at the head of the query: each expands the
                // rows the previous produced, giving the cross product the TCK
                // uses to enumerate three-variable truth tables.
                for extra in &query.extra_unwind_clauses {
                    let base = operator.take().expect("previous UNWIND produced an operator");
                    operator = Some(Box::new(UnwindOperator::new(
                        base,
                        extra.expression.clone(),
                        extra.variable.clone(),
                    )));
                    known_vars.insert(extra.variable.clone());
                }
            }
        }

        // The predicates held back above, now that the UNWIND has bound its
        // variable. Only when a WITH follows: without one, the top-level WHERE
        // applies the full predicate and doing it here as well would just cost
        // a second evaluation per row.
        if query.with_clause.is_some() && !late_bound_predicates.is_empty() {
            if let Some(op) = operator.take() {
                let filter_expr = late_bound_predicates
                    .clone()
                    .into_iter()
                    .reduce(|acc, pred| Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    })
                    .unwrap();
                operator = Some(Box::new(FilterOperator::new(op, filter_expr)));
            }
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

        // `MATCH p = (a)` binds `p` to a path of one node and no relationships.
        //
        // A named path is bound by the expand that walks it, so a pattern with
        // no segments had no expand and nothing bound `p`: `MATCH p = (a)
        // RETURN p` parsed and then failed with VariableNotFound. The
        // zero-length path is the one case where there is nothing to walk, and
        // it is exactly the case the walking code cannot reach (#909).
        //
        // **Below the WITH barriers**, not above them. Bound above, the
        // barrier never saw `p` and projected a null for it, so
        // `MATCH p = (a) WITH p RETURN p` answered `null` where
        // `MATCH p = (a) RETURN p` answered the path -- the variable existed
        // right up until something asked the WITH to carry it (#964).
        //
        // `named_path_handles` already produces the right handle for a
        // segment-less path -- it is what MERGE uses -- so this is the same
        // description, bound by the same operator.
        {
            let mut zero_length: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();
            for mc in &query.match_clauses {
                for path in &mc.pattern.paths {
                    if path.segments.is_empty() && path.path_variable.is_some() {
                        let single = crate::query::ast::Pattern { paths: vec![path.clone()] };
                        zero_length.extend(named_path_handles(&single));
                    }
                }
            }
            if !zero_length.is_empty() {
                if let Some(op) = operator.take() {
                    operator = Some(Box::new(
                        crate::query::executor::operator::BindPathOperator::new(op, zero_length),
                    ));
                }
            }
        }

        // 1b. Build ordered list of WITH stages, then apply barriers + post-WITH matches in sequence.
        // extra_with_stages contains earlier WITH stages; query.with_clause is the last one.
        // Each stage: (with_clause, unwind, post_match_clauses, post_where_clause)
        let mut all_with_stages: Vec<(&WithClause, Option<&UnwindClause>, Vec<&MatchClause>, Option<&WhereClause>)> = Vec::new();

        for (idx, (wc, uw, mcs, wh)) in query.extra_with_stages.iter().enumerate() {
            // Every stage keeps its own trailing UNWIND. Stage 0 used to be
            // suppressed whenever the query had a leading UNWIND, to undo the
            // parser storing that leading unwind here; now that it does not,
            // suppressing stage 0 would simply drop a real clause (#785).
            let _ = idx;
            all_with_stages.push((wc, uw.as_ref(), mcs.iter().collect(), wh.as_ref()));
        }
        if let Some(wc) = &query.with_clause {
            // `query.unwind_clause` is applied above, before the barriers,
            // because the parser only ever puts a *pre-WITH* unwind there --
            // see the comment at that block. Applying it here as well ran it
            // twice; applying it only here ran it on the wrong side of the
            // barrier (#927). A post-WITH unwind is a different field.
            let stage_unwind: Option<&UnwindClause> = None;
            all_with_stages.push((wc, stage_unwind, post_with_clauses.iter().collect(), query.post_with_where_clause.as_ref()));
        }

        for (stage_idx, (with_clause, stage_unwind, stage_matches, stage_where)) in all_with_stages.iter().enumerate() {
            // Apply the WITH barrier
            if let Some(op) = operator {
                let barrier = self.build_with_barrier(op, with_clause, store)?;
                operator = Some(barrier);

                // Reset known_vars to only WITH output aliases
                known_vars.clear();
                for item in &with_clause.items {
                    let alias = item.alias.clone().unwrap_or_else(|| {
                        match &item.expression {
                            Expression::Variable(var) => var.clone(),
                            Expression::Property { variable, property } => format!("{}.{}", variable, property),
                            _ => "?".to_string(),
                        }
                    });
                    known_vars.insert(alias);
                }
            }

            // Apply UNWIND for this stage
            if let Some(unwind) = stage_unwind {
                if let Some(op) = operator {
                    operator = Some(Box::new(UnwindOperator::new(
                        op,
                        unwind.expression.clone(),
                        unwind.variable.clone(),
                    )));
                    known_vars.insert(unwind.variable.clone());
                }
            }

            // UNWINDs written after the final WITH. They belong here, above the
            // barrier, because they read what the WITH projected -- applying
            // them with the leading run gave `VariableNotFound` on a variable
            // the WITH defines one line earlier (#785).
            if stage_idx + 1 == all_with_stages.len() {
                for extra in &query.post_with_unwind_clauses {
                    if let Some(op) = operator {
                        operator = Some(Box::new(UnwindOperator::new(
                            op,
                            extra.expression.clone(),
                            extra.variable.clone(),
                        )));
                        known_vars.insert(extra.variable.clone());
                    }
                }
            }

            // Process post-WITH MATCH clauses for this stage
            // Pre-compute variable sets
            let match_var_sets: Vec<HashSet<String>> = stage_matches.iter().map(|mc| {
                self.extract_match_vars(mc)
            }).collect();

            // Decompose WHERE predicates per MATCH clause
            let where_preds = stage_where
                .map(|wc| flatten_and_predicates(&wc.predicate))
                .unwrap_or_default();
            let mut per_match_where: Vec<Option<WhereClause>> = vec![None; stage_matches.len()];
            let mut cross_match_preds: Vec<Expression> = Vec::new();
            // Predicates that become join conditions on an OPTIONAL MATCH.
            let mut stage_optional_join_predicates: Vec<Option<Expression>> =
                vec![None; stage_matches.len()];

            // A predicate referring to a leading UNWIND's variable cannot be evaluated
            // during match planning -- the variable is not bound until the Unwind operator
            // runs, which sits above the matches. Leaving it here produced a filter under
            // the Unwind and the query died with "Variable not found". Dropping it from the
            // decomposition is safe because the top-level WHERE filter, applied after the
            // Unwind, evaluates the full predicate anyway.
            let late_bound = Self::late_bound_variables(query);

            for pred in where_preds {
                let mut pred_vars = HashSet::new();
                Self::collect_expression_variables(&pred, &mut pred_vars);
                if pred_vars.iter().any(|v| late_bound.contains(v)) {
                    continue;
                }
                // A predicate spanning an OPTIONAL MATCH's own variables and
                // an outer one is a **join condition**, not a filter above the
                // join. Cypher scopes the WHERE after an OPTIONAL MATCH to the
                // optional match, so a row failing it keeps the left side and
                // nulls the right; filtering above the join deletes the row.
                //
                // #667 established this for the pre-WITH decomposition and
                // this path never inherited it, so
                // `… WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2)
                //  WHERE a1 = a2` returned **no rows** where Cypher returns one
                // with nulls (#978). The same rule, in the copy that did not
                // have it.
                let optional_target = stage_matches.iter().enumerate().find(|(i, mc)| {
                    if !mc.optional || pred_vars.is_empty() {
                        return false;
                    }
                    let own = &match_var_sets[*i];
                    let earlier: HashSet<&String> = match_var_sets[..*i]
                        .iter()
                        .flat_map(|s| s.iter())
                        .chain(known_vars.iter())
                        .collect();
                    let introduced: HashSet<&String> =
                        own.iter().filter(|v| !earlier.contains(*v)).collect();
                    let touches_optional = pred_vars.iter().any(|v| introduced.contains(v));
                    let touches_outer = pred_vars.iter().any(|v| !introduced.contains(v));
                    touches_optional && touches_outer
                });
                if let Some((i, _)) = optional_target {
                    stage_optional_join_predicates[i] =
                        Some(match stage_optional_join_predicates[i].take() {
                            Some(existing) => Expression::Binary {
                                left: Box::new(existing),
                                op: BinaryOp::And,
                                right: Box::new(pred),
                            },
                            None => pred,
                        });
                    continue;
                }

                let target = match_var_sets.iter().position(|match_vars| {
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
                    cross_match_preds.push(pred);
                }
            }

            // Plan each MATCH clause
            for (match_idx, match_clause) in stage_matches.iter().enumerate() {
                if Self::can_pushdown_match(match_clause, &known_vars) && operator.is_some() {
                    let upstream = operator.take().unwrap();
                    let (current_op, new_vars) = self.plan_pushed_down_match(
                        match_clause,
                        per_match_where[match_idx].as_ref(),
                        &match_var_sets[match_idx],
                        upstream,
                        &mut anon_counter,
                    )?;
                    operator = Some(current_op);
                    known_vars.extend(new_vars);
                } else {
                    // Fallback: independent plan + join
                    let match_op = self.dispatch_plan_match(match_clause, per_match_where[match_idx].as_ref(), store)?;
                    let clause_vars = match_var_sets[match_idx].clone();

                    operator = Some(match operator {
                        Some(existing) => {
                            // ALL shared variables form the join key. Taking one of them left the
                    // rest uncorrelated — a silent cartesian product — and since this comes
                    // from a HashSet intersection, which one varied between runs (#360).
                    let mut shared: Vec<String> =
                        known_vars.intersection(&clause_vars).cloned().collect();
                    shared.sort();
                            if !shared.is_empty() {
                                if match_clause.optional {
                                    let right_only: Vec<String> = clause_vars.difference(&known_vars).cloned().collect();
                                    let mut join = LeftOuterJoinOperator::new(
                                        existing, match_op, shared.clone(), right_only);
                                    if let Some(pred) =
                                        stage_optional_join_predicates[match_idx].clone()
                                    {
                                        join = join.with_join_predicate(pred);
                                    }
                                    Box::new(join) as OperatorBox
                                } else {
                                    Box::new(JoinOperator::new(existing, match_op, shared.clone())) as OperatorBox
                                }
                            } else if match_clause.optional {
                                // Disjoint OPTIONAL MATCH, post-WITH. Same as
                                // the pre-WITH site: a cartesian product
                                // ignores `optional` and an empty right side
                                // then deletes every left row (#954).
                                let right_only: Vec<String> =
                                    clause_vars.difference(&known_vars).cloned().collect();
                                let mut join = LeftOuterJoinOperator::new(
                                    existing,
                                    match_op,
                                    Vec::new(),
                                    right_only,
                                );
                                if let Some(pred) =
                                    stage_optional_join_predicates[match_idx].clone()
                                {
                                    join = join.with_join_predicate(pred);
                                }
                                Box::new(join) as OperatorBox
                            } else {
                                Box::new(CartesianProductOperator::new(existing, match_op)) as OperatorBox
                            }
                        }
                        None => match_op,
                    });
                    known_vars.extend(clause_vars);
                }
            }

            // Apply cross-match predicates
            if !cross_match_preds.is_empty() {
                if let Some(op) = operator {
                    let filter_expr = cross_match_preds.into_iter().reduce(|acc, pred| {
                        Expression::Binary {
                            left: Box::new(acc),
                            op: BinaryOp::And,
                            right: Box::new(pred),
                        }
                    }).unwrap();
                    operator = Some(Box::new(FilterOperator::new(op, filter_expr)));
                }
            }
        }

        // (post-WITH MATCH clauses are now handled in the unified WITH stage loop above)

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
                    // Join on every shared variable, not just the first — see #360.
                    shared_vars.sort();
                    operator = Some(Box::new(JoinOperator::new(existing_op, call_op, shared_vars.clone())));
                } else {
                    // Fallback to Cartesian Product
                    operator = Some(Box::new(CartesianProductOperator::new(existing_op, call_op)));
                }
            } else {
                operator = Some(call_op);
            }
        }

        // A statement that begins with UNWIND has no MATCH to build a pipeline on, so seed
        // it with a single empty row and let the rest of the planner -- filters,
        // aggregation, ORDER BY, SKIP/LIMIT -- apply unchanged. Hand-building a plan here
        // instead would have to re-implement all of that; the first attempt did, and
        // promptly failed on `UNWIND [...] AS x RETURN count(x)`.
        if operator.is_none() && Self::has_any_unwind(query) {
            use crate::query::executor::operator::SingleRowOperator;
            operator = Some(Box::new(SingleRowOperator::new()));
        }

        let mut operator = operator.unwrap();

        // `MATCH p = (a)` binds `p` to a path of one node and no relationships.
        //
        // A named path is bound by the expand that walks it, so a pattern with
        // no segments had no expand and nothing bound `p`: `MATCH p = (a)
        // RETURN p` parsed and then failed with VariableNotFound. The
        // zero-length path is the one case where there is nothing to walk, and
        // it is exactly the case the walking code cannot reach (#909).
        //
        // `named_path_handles` already produces the right handle for a
        // segment-less path -- it is what MERGE uses -- so this is the same
        // description, bound by the same operator.
        // A *leading* UNWIND is planned before the WITH barriers, above, so that
        // a following WITH has its variable bound. It still lands below the
        // WHERE, which is what `UNWIND [1,2] AS x MATCH (p) WHERE p.n = x`
        // needs -- the predicate references the unwound variable.
        let leading_unwind = query.unwind_leading;

        // A trailing UNWIND whose variable the WHERE mentions has to bind it
        // *before* the filter runs.
        //
        // The unwind normally goes below, after the filter, so the cross
        // product is built from already-narrowed rows -- a real saving, and
        // correct as long as the predicate does not name the unwound variable.
        // When it does, the filter below re-applies the whole WHERE (see the
        // `has_late_bound` reasoning there) above an Unwind that has not run
        // yet, and `MATCH (n) UNWIND [1,2,3] AS x WHERE x > 1 RETURN x` died
        // with VariableNotFound("x") (#927).
        //
        // Reordered only in that case, so the ordinary shape keeps the saving.
        let mut trailing_unwind_hoisted = false;
        if !unwind_before_barrier && query.with_clause.is_none() {
            if let (Some(unwind_clause), Some(where_clause)) =
                (&query.unwind_clause, &query.where_clause)
            {
                let mut where_vars = HashSet::new();
                Self::collect_expression_variables(&where_clause.predicate, &mut where_vars);
                let unwound: Vec<&String> = std::iter::once(&unwind_clause.variable)
                    .chain(query.extra_unwind_clauses.iter().map(|u| &u.variable))
                    .collect();
                if unwound.iter().any(|v| where_vars.contains(*v)) {
                    operator = Box::new(UnwindOperator::new(
                        operator,
                        unwind_clause.expression.clone(),
                        unwind_clause.variable.clone(),
                    ));
                    for extra in &query.extra_unwind_clauses {
                        operator = Box::new(UnwindOperator::new(
                            operator,
                            extra.expression.clone(),
                            extra.variable.clone(),
                        ));
                    }
                    trailing_unwind_hoisted = true;
                }
            }
        }

        // Add WHERE clause if present.
        // When a WITH clause exists, WHERE predicates were already decomposed and
        // pushed into per-MATCH/cross-MATCH filters above. Applying them again here
        // would fail because the WithBarrier projects away referenced variables.
        if query.with_clause.is_none() {
            if let Some(where_clause) = &query.where_clause {
                // Apply only the conjuncts the plan below is not already
                // applying.
                //
                // The decomposition above hands each MATCH the conjuncts that
                // reference only its own variables, and the match planners
                // attach them as filters inside the subplan -- often at the
                // scan, which is the whole point of pushing them down. This
                // line then re-applied the *entire* WHERE on top. For
                // `WHERE p.age > 10 AND f.age < 40` over `(p)-[:KNOWS]->(f)`
                // the result was three evaluations of the same work:
                //
                //     Filter (p.age > 10 AND f.age < 40)     <- here
                //       Filter (f.age < 40)
                //         Expand
                //           Filter (p.age > 10)
                //             NodeScan
                //
                // On LDBC IC9 the redundant pass cost ~130 ms over 389,461
                // rows and removed nothing (#519).
                //
                // What is dropped is decided by reading the plan that was
                // actually built, not by trusting that the planner pushed what
                // it was given: there are several planning paths and none of
                // them promises to attach every predicate it receives.
                //
                // Two shapes make re-application load-bearing rather than
                // redundant, and both are excluded rather than reasoned about:
                //
                //   * OPTIONAL MATCH -- a left outer join can leave a variable
                //     NULL for unmatched rows. A filter pushed inside the
                //     optional side never sees those rows; the top-level one
                //     does, and rejects them. Dropping it would admit rows
                //     that should have been excluded.
                //
                //     A predicate made a *join condition* on the outer join is
                //     the exception, and the one this reasoning did not
                //     anticipate. Cypher scopes the WHERE after an OPTIONAL
                //     MATCH to the optional match, so a row failing it keeps
                //     the left side and nulls the right. Re-applying such a
                //     conjunct here deletes exactly the rows the OPTIONAL
                //     MATCH exists to produce:
                //     `MATCH (x:X) OPTIONAL MATCH (x)-[:E1]->(y) WHERE y.val > 4`
                //     returned one row where Cypher returns three (#667).
                //     Those conjuncts are subtracted below by name.
                //   * a leading UNWIND -- predicates on its variable are
                //     deliberately left out of the decomposition, because the
                //     Unwind that binds them sits above the matches. They are
                //     not in any descendant filter, so they survive the
                //     subtraction anyway; excluding the case as well makes the
                //     reasoning independent of that.
                //
                // With those excluded, every operator between a pushed filter
                // and this point only *adds* bindings -- Expand, ExpandInto,
                // Join on shared equal values, CartesianProduct -- so a
                // conjunct proved below stays true here.
                let has_optional = query.match_clauses.iter().any(|mc| mc.optional);
                let has_late_bound = !Self::late_bound_variables(query).is_empty();

                // Conjuncts scoped to an OPTIONAL MATCH. Subtracted even in
                // the OPTIONAL MATCH case, because re-applying one of these is
                // not redundant -- it changes the answer.
                //
                // Two ways a conjunct gets that scope, and both must be
                // subtracted:
                //
                //   * it spans the join and became a join condition;
                //   * it names only the optional clause's own variables and was
                //     pushed inside that clause, where it anchors the scan.
                //
                // The second is the common case and needs no new machinery at
                // all -- the decomposition already puts it in the right place.
                // The bug was purely that this filter then put it back on top,
                // and a filter above a left outer join deletes the null-filled
                // rows the OPTIONAL MATCH exists to produce (#667).
                let mut optional_scoped: Vec<Expression> = optional_join_predicates
                    .iter()
                    .flatten()
                    .flat_map(flatten_and_predicates)
                    .collect();
                for (i, mc) in pre_with_clauses.iter().enumerate() {
                    if !mc.optional {
                        continue;
                    }
                    if let Some(wc) = &per_match_where[i] {
                        optional_scoped.extend(flatten_and_predicates(&wc.predicate));
                    }
                }
                let as_join_conditions = optional_scoped;

                let predicate = if has_optional || has_late_bound {
                    let remaining: Vec<Expression> =
                        flatten_and_predicates(&where_clause.predicate)
                            .into_iter()
                            .filter(|c| !as_join_conditions.contains(c))
                            .collect();
                    remaining.into_iter().reduce(|acc, pred| Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    })
                } else {
                    let mut applied = Vec::new();
                    Self::collect_applied_predicates(&mut operator, &mut applied);
                    let remaining: Vec<Expression> = flatten_and_predicates(&where_clause.predicate)
                        .into_iter()
                        .filter(|conjunct| !applied.contains(conjunct))
                        .collect();
                    remaining.into_iter().reduce(|acc, pred| Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    })
                };

                if let Some(predicate) = predicate {
                    operator = Box::new(FilterOperator::new(operator, predicate));
                }
            }
        }

        // Extra WITH stages and UNWIND are now handled in the unified loop above.

        // Add standalone UNWIND clause (only when no WITH clause handles it, and not
        // already applied above as a leading UNWIND). A trailing UNWIND stays here, after
        // the filter, so the cross product is built from the already-narrowed rows.
        if !unwind_before_barrier && !trailing_unwind_hoisted {
            if let Some(unwind_clause) = &query.unwind_clause {
                operator = Box::new(UnwindOperator::new(
                    operator,
                    unwind_clause.expression.clone(),
                    unwind_clause.variable.clone(),
                ));
                // Consecutive UNWINDs stack: each one expands the rows the
                // previous produced, so `UNWIND [1,2] AS a UNWIND [3,4] AS b`
                // is four rows and not two.
                for extra in &query.extra_unwind_clauses {
                    operator = Box::new(UnwindOperator::new(
                        operator,
                        extra.expression.clone(),
                        extra.variable.clone(),
                    ));
                }
            }
        }

        // Determine output columns
        let mut output_columns = Vec::new();

        // Check if this is a MATCH...CREATE query (create edges between matched nodes)
        let is_write = if let Some(create_clause) = &query.create_clause {
            // Extract edge creation info from CREATE pattern
            // Example: MATCH (a:Trial), (b:Condition) CREATE (a)-[:STUDIES]->(b)
            let create_pattern = &create_clause.pattern;

            // Collect edges to create from the CREATE pattern
            let mut edges_to_create: Vec<crate::query::executor::operator::EdgeToCreate> = Vec::new();

            // Variables the MATCH already bound. Anything else in the CREATE pattern is a
            // *new* node: previously such nodes were dropped on the floor, so
            // `MATCH (p) CREATE (p)-[:R]->(c:C {..})` created neither node nor edge and
            // still reported success.
            let mut matched_vars: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            // A WITH re-scopes: what it projects is all that exists after it,
            // under the names it gives them. Reading the MATCH clauses instead
            // meant `MATCH (n) WITH n AS a CREATE (a)-[:T]->(b)` did not
            // recognise `a` and created a *fresh* node for it, so a query that
            // should add one node added two and returned the new blank one
            // where the caller expected the matched one (#940).
            //
            // Only the last WITH matters here: this CREATE runs after all of
            // them, and each stage's scope replaces the one before it.
            let scoping_with = query
                .with_clause
                .as_ref()
                .or_else(|| query.extra_with_stages.last().map(|(wc, ..)| wc));
            if let Some(wc) = scoping_with {
                for item in &wc.items {
                    if let Some(alias) = &item.alias {
                        matched_vars.insert(alias.clone());
                    } else if let Expression::Variable(v) = &item.expression {
                        matched_vars.insert(v.clone());
                    }
                }
                // A MATCH written after the WITH binds on top of what it
                // projected.
                let split = query
                    .with_split_index
                    .unwrap_or(query.match_clauses.len());
                for mc in &query.match_clauses[split..] {
                    for v in Self::clause_variables(&mc.pattern) {
                        matched_vars.insert(v);
                    }
                }
            } else {
                for mc in &query.match_clauses {
                    for path in &mc.pattern.paths {
                        if let Some(v) = &path.start.variable {
                            matched_vars.insert(v.clone());
                        }
                        for seg in &path.segments {
                            if let Some(v) = &seg.node.variable {
                                matched_vars.insert(v.clone());
                            }
                        }
                    }
                }
            }

            // Nodes to create per matched row: (handle, labels, properties)
            let mut nodes_to_create: Vec<(
                String,
                Vec<Label>,
                HashMap<String, PropertyValue>,
                Option<HashMap<String, Expression>>,
            )> = Vec::new();
            let mut anon_seq = 0usize;

            // Assign a handle to a CREATE-pattern node, registering it for creation when
            // the MATCH did not bind it. Anonymous nodes get a synthetic handle so an edge
            // can still be wired to them.
            let mut handle_for = |node: &crate::query::ast::NodePattern,
                                  nodes_to_create: &mut Vec<(
                String,
                Vec<Label>,
                HashMap<String, PropertyValue>,
                Option<HashMap<String, Expression>>,
            )>,
                                  anon_seq: &mut usize|
             -> String {
                match &node.variable {
                    Some(v) if matched_vars.contains(v) => v.clone(),
                    // Already registered by an earlier path in this same
                    // CREATE — reuse it rather than creating a second node.
                    Some(v) if nodes_to_create.iter().any(|(h, ..)| h == v) => v.clone(),
                    Some(v) => {
                        nodes_to_create.push((
                            v.clone(),
                            node.labels.clone(),
                            node.properties.clone().unwrap_or_default(),
                            node.property_exprs.clone(),
                        ));
                        v.clone()
                    }
                    None => {
                        let h = format!("__anon_mcreate_{anon_seq}");
                        *anon_seq += 1;
                        nodes_to_create.push((
                            h.clone(),
                            node.labels.clone(),
                            node.properties.clone().unwrap_or_default(),
                            node.property_exprs.clone(),
                        ));
                        h
                    }
                }
            };

            for path in &create_pattern.paths {
                let mut current_var =
                    handle_for(&path.start, &mut nodes_to_create, &mut anon_seq);

                for segment in &path.segments {
                    let target_var =
                        handle_for(&segment.node, &mut nodes_to_create, &mut anon_seq);
                    let edge = &segment.edge;
                    let edge_type = edge.types.first()
                        .cloned()
                        .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                    let edge_properties = edge.properties.clone().unwrap_or_default();
                    let edge_variable = edge.variable.clone();

                    // Direction comes from the pattern, not from write order.
                    let (from, to) = match segment.edge.direction {
                        Direction::Incoming => (target_var.clone(), current_var.clone()),
                        Direction::Outgoing | Direction::Both => {
                            (current_var.clone(), target_var.clone())
                        }
                    };
                    edges_to_create.push((
                        from,
                        to,
                        edge_type,
                        edge_properties,
                        edge_variable,
                        edge.property_exprs.clone(),
                    ));

                    current_var = target_var;
                }
            }

            // Wrap the match operator with node+edge creation
            if !edges_to_create.is_empty() || !nodes_to_create.is_empty() {
                use crate::query::executor::operator::MatchCreateEdgeOperator;
                operator = Box::new(MatchCreateEdgeOperator::with_nodes(
                    operator,
                    nodes_to_create,
                    edges_to_create,
                ));
            }

            true // This is a write query
        } else {
            false
        };

        // Handle DELETE clause
        let is_write = if let Some(delete_clause) = &query.delete_clause {
            // The read is fully materialised before the delete touches
            // anything. `MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*)`
            // counted 1: the first row's delete removed the edge, and the
            // lazy expansion re-read adjacency to produce the second row and
            // found nothing left. Cypher's rule is that a write does not
            // un-produce rows the read had already matched (#899).
            operator = Box::new(crate::query::executor::operator::EagerOperator::new(
                operator, 0, None,
            ));
            operator = Box::new(DeleteOperator::new(
                operator,
                delete_clause.expressions.clone(),
                delete_clause.detach,
            ));
            // ...and the delete is fully applied before anything reads the
            // graph again. `MATCH (a:A) DELETE a MERGE (a2:A)` matched a node
            // the DELETE had already removed: rows were pulled one at a time,
            // so the first row's MERGE ran when only the first node was gone
            // and matched the second, which was about to be deleted. The
            // scenario is named for exactly that -- "merges should not be able
            // to match on deleted nodes" (#994).
            //
            // The barrier below materialises the delete's *input*; this one
            // drains its *output*, which is what makes every deletion happen
            // before the next clause begins. Both are needed and they solve
            // opposite halves: #899 stopped a write from un-producing rows the
            // read had matched, and this stops a later read from seeing rows
            // the write was about to remove.
            operator = Box::new(crate::query::executor::operator::EagerOperator::new(
                operator, 0, None,
            ));
            true
        } else {
            is_write
        };

        // Handle SET clauses
        let is_write = if !query.set_clauses.is_empty() {
            let mut items = Vec::new();
            let mut label_adds = Vec::new();
            let mut entity_items = Vec::new();
            for set_clause in &query.set_clauses {
                for item in &set_clause.items {
                    items.push((item.variable.clone(), item.property.clone(), item.value.clone()));
                }
                for item in &set_clause.label_items {
                    for label in &item.labels {
                        label_adds.push((item.variable.clone(), label.clone()));
                    }
                }
                for item in &set_clause.entity_items {
                    entity_items.push((item.variable.clone(), item.merge, item.value.clone()));
                }
            }
            if !items.is_empty() || !entity_items.is_empty() {
                operator = Box::new(SetPropertyOperator::with_entity_items(
                    operator, items, entity_items,
                ));
            }
            if !label_adds.is_empty() {
                operator = Box::new(LabelMutationOperator::new(operator, label_adds, Vec::new()));
            }
            true
        } else {
            is_write
        };

        // Handle REMOVE clauses
        let is_write = if !query.remove_clauses.is_empty() {
            let mut items = Vec::new();
            let mut label_removes = Vec::new();
            for remove_clause in &query.remove_clauses {
                for item in &remove_clause.items {
                    match item {
                        RemoveItem::Property { variable, property } => {
                            items.push((variable.clone(), property.clone()));
                        }
                        // Previously dropped here while the statement still
                        // reported a successful write, so `REMOVE n:Label` was
                        // a silent no-op (#596).
                        RemoveItem::Label { variable, label } => {
                            label_removes.push((variable.clone(), label.clone()));
                        }
                    }
                }
            }
            if !items.is_empty() {
                operator = Box::new(RemovePropertyOperator::new(operator, items));
            }
            if !label_removes.is_empty() {
                operator = Box::new(LabelMutationOperator::new(operator, Vec::new(), label_removes));
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

        // Handle MERGE clause in MATCH context (CY-13: edge MERGE with bound variables)
        let is_write = if let Some(merge_clause) = &query.merge_clause {
            let on_create: Vec<(String, String, Expression)> = merge_clause.on_create_set.iter()
                .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                .collect();
            let on_match: Vec<(String, String, Expression)> = merge_clause.on_match_set.iter()
                .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                .collect();

            // Extract edge patterns from MERGE clause
            let mut edges_to_merge = Vec::new();
            // `MERGE p = (a)-[:R]->(b)` binds `p` (#876). An anonymous
            // relationship inside a named path is given a synthetic handle, for
            // the same reason `CREATE` gives one to an anonymous node: the path
            // has to reference it afterwards.
            let mut merge_named_paths: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();
            let mut anon_seq = 0usize;
            for path in &merge_clause.pattern.paths {
                let mut current_var = path.start.variable.clone();
                let mut path_nodes: Vec<String> = current_var.iter().cloned().collect();
                let mut path_edges: Vec<String> = Vec::new();
                let mut complete = current_var.is_some();
                for segment in &path.segments {
                    let edge = &segment.edge;
                    let edge_type = edge.types.first().cloned()
                        .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                    let edge_props = edge.properties.clone().unwrap_or_default();
                    let edge_var = match (&edge.variable, &path.path_variable) {
                        (None, Some(_)) => {
                            anon_seq += 1;
                            Some(format!("__merge_path_edge_{anon_seq}"))
                        }
                        (other, _) => other.clone(),
                    };
                    let target_var = segment.node.variable.clone();

                    match (&target_var, &edge_var) {
                        (Some(t), Some(e)) => {
                            path_nodes.push(t.clone());
                            path_edges.push(e.clone());
                        }
                        _ => complete = false,
                    }

                    if let (Some(src), Some(tgt)) = (&current_var, &target_var) {
                        // `-[r:T]-` matches a relationship either way round.
                        // Without this the operator only ever looked for
                        // `src -> tgt`, so an existing `tgt -> src` did not
                        // match and MERGE wrote a duplicate beside it (#938).
                        let undirected =
                            matches!(edge.direction, crate::query::ast::Direction::Both);
                        edges_to_merge.push((
                            src.clone(),
                            tgt.clone(),
                            edge_type,
                            edge_props,
                            edge_var,
                            undirected,
                        ));
                    }
                    current_var = target_var;
                }
                // A path with an unnameable position is left unbound rather
                // than bound to a shorter path that looks plausible.
                if let (Some(pv), true) = (&path.path_variable, complete) {
                    merge_named_paths.push((pv.clone(), path_nodes, path_edges));
                }
            }

            // `MatchMergeEdgeOperator` wires an edge between endpoints that
            // are **already bound**, which is what this branch is for -- the
            // `MATCH (a), (b) MERGE (a)-[:R]->(b)` shape. Without a MATCH the
            // endpoints are not bound by anything, so it wired nothing and
            // `UNWIND [...] AS i MERGE (:A {id: i})-[:R]->(:B {id: i})`
            // silently created no nodes and no edges. That case is a
            // whole-pattern merge, which `MergeOperator` already does (#642).
            // `MatchMergeEdgeOperator` wires an edge between endpoints that are
            // **already bound** -- that is its whole contract, and it is better
            // at that job than the general path: it binds the relationship
            // variable, matches an undirected pattern both ways, and runs
            // ON CREATE / ON MATCH against the relationship.
            //
            // The guard was `a MATCH exists`, not `the endpoints are bound`, so
            // `MATCH (a:A) MERGE (a)-[:T]->(b:B)` -- where `b` is bound by
            // nothing -- wired an edge between one endpoint and no other, and
            // the whole MERGE became a silent no-op returning zero rows (#894).
            let bound_by_match = {
                let mut scope: Vec<String> = Vec::new();
                crate::query::star::bind_match(&mut scope, &query.match_clauses);
                scope
            };
            let all_endpoints_bound = edges_to_merge
                .iter()
                .all(|(src, tgt, ..)| {
                    bound_by_match.iter().any(|v| v == src) && bound_by_match.iter().any(|v| v == tgt)
                });
            if !edges_to_merge.is_empty() && all_endpoints_bound {
                // Edge MERGE: use MatchMergeEdgeOperator
                use crate::query::executor::operator::MatchMergeEdgeOperator;
                // `ON CREATE SET n = {…}` / `n += {…}` (#874).
                let on_create_entity: Vec<(String, bool, Expression)> = merge_clause
                    .on_create_entity_set
                    .iter()
                    .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                    .collect();
                let on_match_entity: Vec<(String, bool, Expression)> = merge_clause
                    .on_match_entity_set
                    .iter()
                    .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                    .collect();
                operator = Box::new(
                    MatchMergeEdgeOperator::new(operator, edges_to_merge, on_create, on_match)
                        .with_entity_sets(on_create_entity, on_match_entity),
                );
                if !merge_named_paths.is_empty() {
                    operator = Box::new(crate::query::executor::operator::BindPathOperator::new(
                        operator,
                        merge_named_paths.clone(),
                    ));
                }
            } else {
                // Node-only MERGE, or a whole-pattern MERGE with nothing bound
                // to hang it off, running once per upstream row.
                //
                // The comment here used to say "with input" while the code
                // assigned over `operator` and threw the input away, so the
                // MERGE ran exactly once no matter what fed it -- and, more to
                // the point, could not see the row. That is why
                // `UNWIND [...] AS x MERGE (n:N {v: x})` had nowhere to read
                // `x` from (#642).
                let on_create_labels: Vec<(String, Vec<Label>)> = merge_clause
                    .on_create_labels
                    .iter()
                    .map(|l| (l.variable.clone(), l.labels.clone()))
                    .collect();
                let on_match_labels: Vec<(String, Vec<Label>)> = merge_clause
                    .on_match_labels
                    .iter()
                    .map(|l| (l.variable.clone(), l.labels.clone()))
                    .collect();
                operator = Box::new(
                    MergeOperator::new(
                        merge_clause.pattern.clone(),
                        on_create,
                        on_match,
                        on_create_labels,
                        on_match_labels,
                    )
                    .with_entity_sets(
                        merge_clause
                            .on_create_entity_set
                            .iter()
                            .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                            .collect(),
                        merge_clause
                            .on_match_entity_set
                            .iter()
                            .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                            .collect(),
                    )
                    .with_input(operator),
                );
                if !merge_named_paths.is_empty() {
                    operator = Box::new(crate::query::executor::operator::BindPathOperator::new(
                        operator,
                        merge_named_paths.clone(),
                    ));
                }
            }
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
            let mut agg_counter = 0usize;
            let mut return_item_aliases: Vec<(Expression, String)> = Vec::new();
            // Post-projection items: after aggregation, compute final expressions
            // from aggregate aliases (e.g. round(__agg_0 * 100 / __agg_1) AS strike_rate)
            let mut post_projections: Vec<(Expression, String)> = Vec::new();

            for (idx, item) in return_clause.items.iter().enumerate() {
                // `column_name` first: it uses the text the user wrote, which
                // the reconstruction below cannot recover. `count(*)` came out
                // as `count()` here because `*` is not an argument expression,
                // and a column nobody can name by writing the query again is
                // not a usable result (#635).
                let alias = item.column_name(idx);

                output_columns.push(alias.clone());
                // Kept so ORDER BY can be translated between alias and expression form,
                // whichever is bound where the sort lands.
                return_item_aliases.push((item.expression.clone(), alias.clone()));

                // Extract nested aggregates from expressions like round(sum(x) / sum(y))
                let (rewritten, extracted) = extract_nested_aggregates(&item.expression, &mut agg_counter);

                if !extracted.is_empty() {
                    has_aggregation = true;
                    aggregates.extend(extracted);
                    post_projections.push((rewritten, alias.clone()));
                } else {
                    group_by.push((item.expression.clone(), alias.clone()));
                    projections.push((item.expression.clone(), alias.clone()));
                    // Use Variable(alias) for post-projection since after aggregation
                    // the record only has the alias bound, not the original expression
                    post_projections.push((Expression::Variable(alias.clone()), alias.clone()));
                }
            }

            // Label count cache: O(1) shortcut for MATCH (n:Label) RETURN count(n)
            // Detect: single count aggregate, no group-by, no WHERE, no edges, single MATCH
            // **No shortcut answers a query that writes.** Each of the three
            // below replaces the whole plan with a metadata read, discarding
            // the operator built above -- which is where DELETE, SET, REMOVE,
            // MERGE and FOREACH live. `MATCH (a:A) DELETE a RETURN count(*)`
            // therefore returned 2 and **deleted nothing**: a fast, confident
            // number, and the caller's data still there (#993).
            //
            // That is the same rule the inline-property guard below already
            // states -- the shortcut may only fire when the query says nothing
            // the metadata cannot express -- and a write is the largest such
            // thing there is. `is_write` is already computed above and covers
            // every write clause, so it is the whole condition.
            let use_label_count = has_aggregation
                && !is_write
                && aggregates.len() == 1
                && group_by.is_empty()
                && matches!(aggregates[0].func, AggregateType::Count)
                && !aggregates[0].distinct
                // O(1) label count answers "how many rows", which is `count(*)` or
                // `count(var)`. `count(x.prop)` counts non-null *values*, and returning the
                // label count for it reported every node as having the property (#358).
                && matches!(
                    aggregates[0].expr,
                    Expression::Literal(_) | Expression::Variable(_)
                )
                && query.where_clause.is_none()
                && query.with_clause.is_none()
                && query.match_clauses.len() == 1
                && query.match_clauses[0].pattern.paths.len() == 1
                && query.match_clauses[0].pattern.paths[0].segments.is_empty()
                // An inline property is a filter, and the label count knows nothing about
                // it. Without this, `MATCH (p:P {name: "alice"}) RETURN count(p)` returned
                // the count of *every* :P -- a fast, confident, wrong number, while the
                // same pattern with `RETURN p` returned the single correct row. Same shape
                // as #358 above: the shortcut may only fire when the pattern says nothing
                // the metadata cannot express.
                && query.match_clauses[0].pattern.paths[0]
                    .start
                    .properties
                    .as_ref()
                    .is_none_or(|p| p.is_empty())
                && query.match_clauses[0].pattern.paths[0]
                    .start
                    .property_exprs
                    .is_none()
                && !query.match_clauses[0].pattern.paths[0].start.labels.is_empty();

            // Edge type count cache: O(1) shortcut for MATCH ()-[r]->() RETURN type(r), count(r)
            // Detect: one count aggregate, one group-by with type() function, single edge path, no WHERE
            let use_edge_type_count = has_aggregation
                && !is_write
                && aggregates.len() == 1
                && group_by.len() == 1
                && matches!(aggregates[0].func, AggregateType::Count)
                && !aggregates[0].distinct
                && query.where_clause.is_none()
                && query.with_clause.is_none()
                && query.match_clauses.len() == 1
                && query.match_clauses[0].pattern.paths.len() == 1
                && query.match_clauses[0].pattern.paths[0].segments.len() == 1
                && query.match_clauses[0].pattern.paths[0].start.labels.is_empty()
                && query.match_clauses[0].pattern.paths[0].segments[0].node.labels.is_empty()
                && matches!(&group_by[0].0, Expression::Function { name, args, .. }
                    if name == "type" && args.len() == 1 && matches!(&args[0], Expression::Variable(_)))
                // Directed only — see the note on `use_edge_count` below.
                && !matches!(
                    query.match_clauses[0].pattern.paths[0].segments[0].edge.direction,
                    Direction::Both
                )
                // And distinct endpoints, for the same reason: this count is
                // also blind to `(n)-[r]->(n)` (#962).
                && {
                    let path = &query.match_clauses[0].pattern.paths[0];
                    match (&path.start.variable, &path.segments[0].node.variable) {
                        (Some(a), Some(b)) => a != b,
                        _ => true,
                    }
                };

            // O(1) count for a single edge type (or all edges): the metadata that already
            // answers `type(r), count(r)` and node label counts can answer this too, but
            // this shape fell through to a full Expand + Aggregate -- on a billion-edge
            // graph, a timeout for a question the statistics already hold (#304).
            //
            // Requires the counted variable to be the *edge* (or `count(*)`): counting a
            // node variable over the same expand is a different question once the pattern
            // has labels, and the label-free case is handled below anyway.
            let edge_var = if query.match_clauses.len() == 1
                && query.match_clauses[0].pattern.paths.len() == 1
                && query.match_clauses[0].pattern.paths[0].segments.len() == 1
            {
                query.match_clauses[0].pattern.paths[0].segments[0]
                    .edge
                    .variable
                    .clone()
            } else {
                None
            };
            let use_edge_count = has_aggregation
                && !is_write
                && aggregates.len() == 1
                && group_by.is_empty()
                && matches!(aggregates[0].func, AggregateType::Count)
                && !aggregates[0].distinct
                && query.where_clause.is_none()
                && query.with_clause.is_none()
                && query.order_by.is_none()
                && query.match_clauses.len() == 1
                && query.match_clauses[0].pattern.paths.len() == 1
                && query.match_clauses[0].pattern.paths[0].segments.len() == 1
                && query.match_clauses[0].pattern.paths[0].start.labels.is_empty()
                && query.match_clauses[0].pattern.paths[0].start.properties.as_ref().is_none_or(|p| p.is_empty())
                && query.match_clauses[0].pattern.paths[0].segments[0].node.labels.is_empty()
                && query.match_clauses[0].pattern.paths[0].segments[0].node.properties.as_ref().is_none_or(|p| p.is_empty())
                && query.match_clauses[0].pattern.paths[0].segments[0].edge.length.is_none()
                && query.match_clauses[0].pattern.paths[0].segments[0].edge.types.len() <= 1
                && match &aggregates[0].expr {
                    Expression::Literal(_) => true,
                    Expression::Variable(v) => Some(v) == edge_var.as_ref(),
                    _ => false,
                }
                // Last, because it indexes into the pattern and every check
                // that guarantees those indices exist is above it. Placing it
                // earlier panicked on `UNWIND [1,2,3] AS x RETURN max(x)`,
                // which has no match clause at all.
                //
                // Both fast paths read the edge count straight off the store,
                // which counts each edge once. An **undirected** pattern
                // matches every edge twice — once from each end — so
                // `MATCH (a)--(b) RETURN count(*)` over two edges is 4, not 2.
                // Doubling here would then have to reason about self-loops, so
                // the fast path is restricted to directed patterns and the
                // general operator answers the rest.
                && !matches!(
                    query.match_clauses[0].pattern.paths[0].segments[0].edge.direction,
                    Direction::Both
                )
                // And the two endpoints must be *different* variables.
                // `MATCH (n)-[r]->(n)` asks for self-loops only, and the
                // store's edge count knows nothing about that constraint --
                // the fast path answered the **total** edge count, 2 over a
                // graph with one loop and one ordinary edge, where `RETURN r`
                // on the same pattern gives one row (#962).
                //
                // The same query answering two different numbers depending on
                // whether it counts is what a fast path has to be checked
                // against: an optimisation may skip *work*, never a predicate.
                && {
                    let path = &query.match_clauses[0].pattern.paths[0];
                    match (&path.start.variable, &path.segments[0].node.variable) {
                        (Some(a), Some(b)) => a != b,
                        _ => true,
                    }
                };

            if use_edge_count {
                let edge_type = query.match_clauses[0].pattern.paths[0].segments[0]
                    .edge
                    .types
                    .first()
                    .map(|t| t.as_str().to_string());
                let alias = aggregates[0].alias.clone();
                operator = Box::new(EdgeCountOperator::new(edge_type, alias));
                operator = Box::new(ProjectOperator::new(operator, post_projections));
            } else if use_edge_type_count {
                let type_alias = group_by[0].1.clone();
                let count_alias = aggregates[0].alias.clone();
                operator = Box::new(EdgeTypeCountOperator::new(type_alias, count_alias));
                operator = Box::new(ProjectOperator::new(operator, post_projections));

                // Sort after projection
                if let Some(order_by) = &query.order_by {
                    let sort_items: Vec<(Expression, bool)> = order_by.items.iter()
                        .map(|i| (
                            resolve_sort_key(&i.expression, &return_item_aliases, SortPosition::AfterProjection),
                            i.ascending,
                        )).collect();
                    operator = Box::new(SortOperator::new(operator, sort_items));
                }
            } else if use_label_count {
                let labels = query.match_clauses[0].pattern.paths[0]
                    .start
                    .labels
                    .clone();
                let alias = aggregates[0].alias.clone();
                operator = Box::new(LabelCountOperator::new(labels, alias));
                // Apply post-projection to map __agg_0 -> user alias
                operator = Box::new(ProjectOperator::new(operator, post_projections));
            } else if has_aggregation {
                operator = Box::new(AggregateOperator::new(operator, group_by, aggregates));
                // Post-aggregation projection: compute final expressions from aggregate aliases
                operator = Box::new(ProjectOperator::new(operator, post_projections));

                // Sort after aggregation + projection
                if let Some(order_by) = &query.order_by {
                    let mut sort_items = Vec::new();
                    for item in &order_by.items {
                        sort_items.push((
                            resolve_sort_key(&item.expression, &return_item_aliases, SortPosition::AfterProjection),
                            item.ascending,
                        ));
                    }
                    operator = Box::new(SortOperator::new(operator, sort_items));
                }
            } else {
                // Non-aggregation: Sort -> Project
                if let Some(order_by) = &query.order_by {
                    let mut sort_items = Vec::new();
                    for item in &order_by.items {
                        sort_items.push((
                            resolve_sort_key(&item.expression, &return_item_aliases, SortPosition::BeforeProjection),
                            item.ascending,
                        ));
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

        // DISTINCT, before SKIP and LIMIT.
        //
        // openCypher evaluates `RETURN DISTINCT` ahead of `ORDER BY`, `SKIP`
        // and `LIMIT`: the projection is deduplicated, and only then is the
        // result ordered and sliced. Deduplicating afterwards means `LIMIT n`
        // slices a list that still contains duplicates, so fewer than n rows
        // survive -- and `SKIP k` skips raw rows rather than distinct ones,
        // which can leave the row it was meant to skip in the output (#522).
        //
        // `DistinctOperator` is a streaming, first-occurrence-wins filter, so
        // placing it above the sort preserves the ordering the sort produced;
        // no second sort is needed. Both plan shapes reach here with the
        // projection already on top -- `Sort -> Project` when there is no
        // aggregation, `Aggregate -> Project -> Sort` when there is -- so one
        // insertion point covers both.
        if query.return_clause.as_ref().is_some_and(|r| r.distinct) {
            operator = Box::new(DistinctOperator::new(operator));
        }

        // Add SKIP if present
        if let Some(skip) = query.skip {
            operator = Box::new(SkipOperator::new(operator, skip));
        }

        // Add LIMIT if present
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));

            // QP-04: push the limit down through pass-through operators
            // (Project, Limit) so leaf NodeScans can stop iterating early.
            // Operators that change cardinality (Filter, Sort, Distinct,
            // Aggregate, Expand) implement the default `try_push_limit`
            // returning false, so the push stops there. SKIP changes the
            // hint — when SKIP=k and LIMIT=n, the scan must yield k+n rows
            // for SKIP to drop the first k.
            let push_n = match query.skip {
                Some(skip) => skip.saturating_add(limit),
                None => limit,
            };
            operator.try_push_limit(push_n);
        }

        // QP-01: Predicate pushdown is handled inline during plan_match() via AND-chain decomposition
        // QP-02: Cost-based plan selection uses GraphStatistics to pick indexes over scans
        // QP-04: Early LIMIT propagation — done when NodeScanOperator gets early_limit set

        // Return execution plan
        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
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

            // Any numeric list literal, not only one that parsed as a
            // `Vector`. List literals stay lists now (#628), so requiring the
            // `Vector` variant here would reject every query vector written
            // with a decimal point -- which is all of them.
            let query_vector = match &call_clause.arguments[2] {
                Expression::Literal(pv) => pv.to_vector().ok_or_else(|| {
                    ExecutionError::PlanningError(
                        "Third argument (vector) must be a list of numbers".to_string(),
                    )
                })?,
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
        } else if call_clause.procedure_name.starts_with("algo.")
            || AlgorithmOperator::is_algorithm(&call_clause.procedure_name)
        {
            // Namespace optional and case-insensitive: `pagerank`, `algo.pagerank`,
            // `algo.pageRank` and `samyama.pageRank` all route here. Routing on the
            // `algo.` prefix alone meant `CALL pagerank()` never reached the operator and
            // came back as "Unknown procedure" (#198).
            //
            // The prefix check is kept alongside so an unrecognised `algo.*` name still
            // reaches the operator and gets the specific "Unknown algorithm" error rather
            // than the generic "Unknown procedure".
            Ok(Box::new(AlgorithmOperator::new(
                call_clause.procedure_name.clone(),
                call_clause.arguments.clone(),
            )))
        } else {
            // Its own code: the procedure surface is fine and this name is not
            // on it, which is a different recovery from "the planner could not
            // build a plan".
            Err(ExecutionError::unknown_procedure(format!(
                "Unknown procedure: {}", call_clause.procedure_name)))
        }
    }

    /// Dispatch to graph-native or legacy planner based on configuration.
    /// Falls back to legacy planner if graph-native fails (e.g., label-free patterns).
    /// Every conjunct any `Filter` in this subtree is already applying.
    ///
    /// Read off the built plan rather than tracked during construction: the
    /// planner has several paths into a match subplan and the question here is
    /// what the chosen one *did*, not what it was asked to do.
    fn collect_applied_predicates(op: &mut OperatorBox, out: &mut Vec<Expression>) {
        if let Some(predicate) = op.filter_predicate() {
            out.extend(flatten_and_predicates(predicate));
        }
        for child in op.children_mut() {
            Self::collect_applied_predicates(child, out);
        }
    }

    fn dispatch_plan_match(&self, match_clause: &MatchClause, where_clause: Option<&WhereClause>, store: &GraphStore) -> ExecutionResult<OperatorBox> {
        if self.config.graph_native {
            match self.plan_match_native(match_clause, where_clause, store) {
                Ok(plan) => Ok(plan),
                Err(_) => {
                    // Fallback to legacy planner for patterns the graph-native
                    // planner can't handle (e.g., no labels, variable-length paths)
                    self.plan_match(match_clause, where_clause, store)
                }
            }
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

        let candidates = enumerate_plans(&pg, where_clause, catalog, &store.property_index, &config);
        if candidates.is_empty() {
            return Err(ExecutionError::PlanningError("No valid plans enumerated".to_string()));
        }

        // Collect candidate diagnostics before consuming
        let num_candidates = candidates.len();
        let candidate_summaries: Vec<(String, f64)> = candidates.iter().map(|(plan, cost)| {
            let desc = plan.display_plan(0);
            (desc, *cost)
        }).collect();
        let best_cost = candidates[0].1;

        // Pick the cheapest plan (first one — already sorted)
        let (best_plan, _) = candidates.into_iter().next().unwrap();
        let physical = logical_to_physical(&best_plan);

        // Store diagnostics in thread-local for EXPLAIN to pick up
        PLAN_DIAGNOSTICS.with(|diag| {
            *diag.borrow_mut() = Some(PlanDiagnostics {
                candidates_evaluated: num_candidates,
                chosen_plan_cost: best_cost,
                candidate_costs: candidate_summaries,
            });
        });

        Ok(physical)
    }

    fn plan_match(&self, match_clause: &MatchClause, where_clause: Option<&WhereClause>, store: &GraphStore) -> ExecutionResult<OperatorBox> {
        let pattern = &match_clause.pattern;

        if pattern.paths.is_empty() {
            return Err(ExecutionError::PlanningError("Match pattern has no paths".to_string()));
        }

        // QP-02/QP-03: Cost-based optimization — reorder paths by estimated cardinality (smallest first)
        let stats = store.statistics();
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

            // Build the ordered list of pattern nodes (start + each segment's node),
            // assigning variable names in written order (including anonymous nodes).
            // This is computed up front so anchor selection below can consider any
            // node in the pattern, not just the first one written.
            let mut path_nodes: Vec<PathNodeRef> = Vec::with_capacity(path.segments.len() + 1);
            path_nodes.push(PathNodeRef {
                var: path.start.variable.clone().unwrap_or_else(|| {
                    let name = format!("_anon_{}", anon_counter);
                    anon_counter += 1;
                    name
                }),
                labels: path.start.labels.clone(),
                properties: path.start.properties.clone(),
            });
            for segment in &path.segments {
                path_nodes.push(PathNodeRef {
                    var: segment.node.variable.clone().unwrap_or_else(|| {
                        let name = format!("_anon_{}", anon_counter);
                        anon_counter += 1;
                        name
                    }),
                    labels: segment.node.labels.clone(),
                    properties: segment.node.properties.clone(),
                });
            }
            let start_var = path_nodes[0].var.clone();

            // QP-05: Anchor selection — pick the node the *whole path* is
            // cheapest from, not the node with the cheapest scan. Traversal
            // toward earlier-written nodes uses the reversed edge direction.
            //
            // shortestPath and named path variables stay excluded: both
            // materialise a path and assume forward traversal order from the
            // start. Variable-length hops used to be excluded too, which meant
            // any pattern containing a `*` always started at its first written
            // node — LDBC IC6 among them. `build_path_from_anchor` now
            // reverses those segments, so the restriction is no longer needed.
            let anchor_eligible = path.path_variable.is_none()
                && !matches!(path.path_type, PathType::Shortest | PathType::AllShortest);
            let anchor_idx = if anchor_eligible {
                choose_anchor_index(path, &path_nodes, &per_path_preds[path_idx], store)
            } else {
                0
            };

            let (mut path_operator, deferred_predicates): (OperatorBox, Vec<Expression>) =
                if anchor_eligible && anchor_idx > 0 {
                    self.build_path_from_anchor(path, &path_nodes, anchor_idx, &per_path_preds[path_idx], store)
                } else {
            // Merge inline start node properties into predicates for index selection.
            // Without this, {prop: val} in MATCH patterns falls back to NodeScan + Filter
            // instead of IndexScan. See ADR-015 for context.
            if let Some(ref props) = path.start.properties {
                for (prop_name, prop_value) in props {
                    per_path_preds[path_idx].push(Expression::Binary {
                        left: Box::new(Expression::Property {
                            variable: start_var.clone(),
                            property: prop_name.clone(),
                        }),
                        op: BinaryOp::Eq,
                        right: Box::new(Expression::Literal(prop_value.clone())),
                    });
                }
            }

            // Optimization: Check for index usage (using this path's assigned predicates).
            // Recognizes both `n.prop OP literal` and `literal OP n.prop` operand orders.
            let mut remaining_predicates: Vec<Expression> = per_path_preds[path_idx].clone();
            let mut path_operator: OperatorBox = if let Some((idx, ids)) =
                find_id_predicate(&start_var, &remaining_predicates)
            {
                // `id()` before any index: it is unique by construction, so
                // there is nothing for a cost model to weigh. Without this,
                // `MATCH (n) WHERE id(n) = 5` scanned the whole label and
                // filtered (#538).
                //
                // The label, if the pattern named one, still has to be
                // checked -- `MATCH (n:Person) WHERE id(n) = 5` must not
                // match a node of another label that happens to hold that id.
                remaining_predicates.remove(idx);
                Box::new(
                    NodeByIdOperator::new(ids, start_var.clone())
                        .with_labels(path.start.labels.clone()),
                )
            } else if let Some((idx, label, property, op, val)) =
                find_index_predicate(&start_var, &path.start.labels, &remaining_predicates, store)
            {
                remaining_predicates.remove(idx);
                Box::new(IndexScanOperator::new(start_var.clone(), label, property, op, val))
            } else {
                Box::new(NodeScanOperator::new(
                    start_var.clone(),
                    path.start.labels.clone(),
                ))
            };

            // Note: start node inline properties are already merged into per_path_preds
            // above, so they're handled via IndexScan or remaining_predicates Filter.
            // No separate FilterOperator needed here.

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

                // We need the target node to be scanned too — create a CartesianProduct with target scan.
                //
                // The target gets the same `id()` anchoring the start already
                // got (#538), because without it only *one* endpoint was
                // pinned: `WHERE id(a) = 1 AND id(b) = 6` planned as
                // `NodeById(a) x NodeScan(b)`, so the BFS ran once per node in
                // the label and a filter above discarded all but one result.
                // Measured against the same query written with inline
                // properties, that cost 329x (#584).
                let target_id_pred = find_id_predicate(&target_var, &deferred_predicates);
                let target_scan: OperatorBox = match &target_id_pred {
                    Some((_, ids)) => {
                        let mut op = NodeByIdOperator::new(ids.clone(), target_var.clone());
                        if !last_segment.node.labels.is_empty() {
                            // A scan by id bypasses the label index, so the
                            // pattern's labels still have to be checked.
                            op = op.with_labels(last_segment.node.labels.clone());
                        }
                        Box::new(op) as OperatorBox
                    }
                    // Then an indexed property, for the same reason and by the
                    // same argument. `WHERE a.seq = 0 AND b.seq = 5` planned as
                    // `IndexScan(a) x NodeScan(b)` -- the WHERE form of what the
                    // inline form `(b:N {seq: 5})` already got right, which is
                    // why this went unnoticed (#584).
                    None => match find_index_predicate(
                        &target_var,
                        &last_segment.node.labels,
                        &deferred_predicates,
                        store,
                    ) {
                        Some((_, label, property, op, val)) => Box::new(IndexScanOperator::new(
                            target_var.clone(),
                            label,
                            property,
                            op,
                            val,
                        )),
                        // Then an indexed **inline** property. `#584` taught
                        // the `WHERE` form to use the index and left this one
                        // behind, so `shortestPath((a {id: 1})-[*]-(b {id: 2}))`
                        // -- which is how the pattern is actually written --
                        // planned `IndexScan(a) x NodeScan(b)`: the source
                        // seeked and the target scanned the whole label.
                        //
                        // On FinBench SF10 that scan was the entire cost of
                        // CR-3. Making the BFS bidirectional first (#1050)
                        // moved it 541 ms -> 547 ms, because the search was
                        // never what the query was spending its time on.
                        None => match inline_index_scan(
                            &target_var,
                            &last_segment.node.labels,
                            last_segment.node.properties.as_ref(),
                            store,
                        ) {
                            Some(op) => op,
                            None => Box::new(NodeScanOperator::new(
                                target_var.clone(),
                                last_segment.node.labels.clone(),
                            )),
                        },
                    },
                };
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
                // Variables bound so far, so a deferred predicate can be
                // applied the moment its last variable arrives rather than
                // after the whole path (#328).
                let mut bound: HashSet<String> = HashSet::new();
                bound.insert(start_var.clone());
                // Relationship isomorphism (#684): a pattern with more than one
                // segment must not walk the same edge twice. The first expand
                // built is the first to execute, so it clears history from an
                // earlier clause — the rule is per-clause.
                let track_edges = path.segments.len() > 1;
                let mut first_expand = true;
                for (seg_idx, segment) in path.segments.iter().enumerate() {
                    let target_var = path_nodes[seg_idx + 1].var.clone();

                    // An inline relationship property constraint has to be
                    // applied; it used to be dropped, so
                    // `MATCH ()-[r:R {num: 2}]->()` returned every `:R` (#649).
                    // An anonymous relationship gets a name to filter on.
                    let edge_filter = self.edge_property_filter(&segment.edge, seg_idx);
                    let edge_var = match &edge_filter {
                        Some((var, _)) => Some(var.clone()),
                        None => segment.edge.variable.clone(),
                    };
                    let edge_types: Vec<String> = segment.edge.types.iter()
                        .map(|t| t.as_str().to_string())
                        .collect();

                    if let Some(ref length) = segment.edge.length {
                        // Variable-length traversal: BFS expand over [min, max] hops.
                        let min_hops = length.min.unwrap_or(1);
                        let max_hops = length.max.unwrap_or(usize::MAX);
                        let mut expand = VarLengthExpandOperator::new(
                            path_operator,
                            current_var.clone(),
                            target_var.clone(),
                            edge_types,
                            segment.edge.direction.clone(),
                            min_hops,
                            max_hops,
                        );
                // Inline relationship properties, e.g. `-[:R* {year: 1988}]->`.
                // The operator had nowhere to put these and the planner
                // dropped them, so the pattern matched every path and the
                // filter failed *open* (#934).
                if let Some(props) = &segment.edge.properties {
                    if !props.is_empty() {
                        expand = expand.with_edge_properties(props.clone());
                    }
                }
                if self
                    .trail_enumeration
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    expand = expand.with_trail_enumeration();
                }
                        // Relationship isomorphism applies to a var-length segment too: an
                        // edge an earlier segment of this clause walked is not available to
                        // it. `ExpandOperator` has done this since #684; this path did not
                        // inherit it, so `(a)-[:R]-(y)-[:R*1..1]-(z)` over one edge answered
                        // a row where openCypher answers none (#710).
                        if track_edges {
                            expand = expand.with_edge_isolation(first_expand);
                            first_expand = false;
                        }
                        if let Some(ref pv) = path.path_variable {
                            expand = expand.with_path_variable(pv.clone());
                        }
                        // `MATCH (a)-[r:T*]->(b)` binds `r` to the list of
                        // relationships traversed. Dropping it made the query
                        // fail with "Variable not found: r" (#652).
                        if let Some(ref rv) = segment.edge.variable {
                            expand = expand.with_rel_variable(rv.clone());
                        }
                        // If the destination resolves to exactly one node, the
                        // question is "can each source reach *this* node",
                        // which one reversed BFS answers for every row. LDBC
                        // IC6 reaches this operator with thousands of candidate
                        // friends and one pinned person; without the pin each
                        // candidate expands its own two-hop neighbourhood.
                        //
                        // `min_hops <= 1` because a set keyed on shortest
                        // distance cannot answer `*2..n` correctly, and no path
                        // variable because a membership test yields no path.
                        if path.path_variable.is_none() && min_hops <= 1 {
                            if let Some(pinned) =
                                pinned_node_for(&target_var, &deferred_predicates, store)
                            {
                                expand = expand.with_pinned_target(pinned);
                            }
                        }
                        // Prune non-matching endpoints before a record is
                        // built. The filter below stays: this is pruning, not a
                        // replacement (#1063).
                        expand = self.push_varlen_target_props(
                            expand,
                            &target_var,
                            &segment.node.labels,
                            segment.node.properties.as_ref(),
                            store,
                        );
                        path_operator = if !segment.node.labels.is_empty() {
                            Box::new(expand.with_target_labels(segment.node.labels.clone()))
                        } else {
                            Box::new(expand)
                        };
                        if let Some(ref props) = segment.node.properties {
                            if !props.is_empty() {
                                let filter_expr = self.build_property_filter(&target_var, props);
                                path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
                            }
                        }
                        bound.insert(target_var.clone());
                        if let Some(ev) = &segment.edge.variable {
                            bound.insert(ev.clone());
                        }
                        path_operator = Self::apply_ready_predicates(
                            path_operator,
                            &mut deferred_predicates,
                            &bound,
                        );
                        current_var = target_var;
                        continue;
                    }

                    // A variable already bound has to be *matched*, not
                    // rebound. `ExpandOperator` binds its target
                    // unconditionally, so `MATCH (b)-->(b)` bound the far end
                    // of each edge over the near one and every edge matched --
                    // a graph containing no self-relationships at all returned
                    // one row per edge (#639). Expanding into a synthetic name
                    // and requiring the two to be equal is what a repeated
                    // variable means; the walk then continues from the
                    // original, which the filter has just proved is the same
                    // node.
                    let self_ref = bound.contains(&target_var);
                    let expand_var = if self_ref {
                        format!("__self_{target_var}_{seg_idx}")
                    } else {
                        target_var.clone()
                    };
                    let mut expand = ExpandOperator::new(
                        path_operator,
                        current_var.clone(),
                        expand_var.clone(),
                        edge_var,
                        edge_types,
                        segment.edge.direction.clone(),
                    );
                    if track_edges {
                        expand = expand.with_edge_isolation(first_expand);
                        first_expand = false;
                    }
                    // A closing hop back onto an already-bound variable can
                    // only land on that node. Without this the expand walks
                    // every neighbour and the `__self_x = x` filter below
                    // discards all but one — BI-17's triangle close is ~41
                    // neighbours per person over ~17.8M paths (#195).
                    if self_ref {
                        expand = expand.with_target_bound_var(target_var.clone());
                    }
                    // If the *next* segment closes the pattern back onto a
                    // variable already bound, this expand's target must be a
                    // neighbour of that variable — so test it here, during the
                    // walk, instead of building a row for every candidate and
                    // discarding it two operators later.
                    //
                    // LDBC BI-17 is the case:
                    // `(a)-[:KNOWS]-(b)-[:KNOWS]-(c)-[:KNOWS]-(a)`. This
                    // expand binds `c`, and at SF1 it builds ~3.2M rows for
                    // the closing hop to reduce to 387,573 triangles. Pruning
                    // during the walk is the row-count half of #1082, whose
                    // full form (a sorted-merge intersection) measured a 170x
                    // ceiling (#1086).
                    //
                    // Deliberately narrow. Both segments must be undirected
                    // over the same edge types, so that "is a neighbour of the
                    // closing variable" has one unambiguous meaning and the
                    // sorted list the test binary-searches is the right one.
                    // A directed close would need the matching half of the
                    // index and is left to the operator #1082 asks for.
                    //
                    // This prunes and never widens: a row it rejects has no
                    // edge to close on, and a row it keeps still faces the
                    // closing hop, which also enforces relationship
                    // isomorphism.
                    if !self_ref {
                        if let Some(next) = path.segments.get(seg_idx + 1) {
                            let next_target = &path_nodes[seg_idx + 2].var;
                            let same_types = !segment.edge.types.is_empty()
                                && next.edge.types.len() == segment.edge.types.len()
                                && next.edge.types.iter().zip(segment.edge.types.iter())
                                    .all(|(a, b)| a.as_str() == b.as_str());
                            let closes_onto_bound = bound.contains(next_target)
                                && *next_target != target_var;
                            if closes_onto_bound
                                && same_types
                                && next.edge.length.is_none()
                                && segment.edge.length.is_none()
                                && matches!(segment.edge.direction, Direction::Both)
                                && matches!(next.edge.direction, Direction::Both)
                            {
                                expand = expand.with_co_neighbour(next_target.clone());
                            }
                        }
                    }
                    // A selective equality on the far side of the expansion —
                    // LDBC IC11's `org.name = "..."` — applied during the walk
                    // rather than to the rows it produces (#656).
                    let pushed = Self::target_equality_props(&deferred_predicates, &target_var);
                    if !pushed.is_empty() {
                        // Resolved to ids where possible: the property check
                        // costs a node fetch per candidate edge and this costs
                        // a hash lookup (#665).
                        if let Some(ids) =
                            self.resolve_target_ids(&segment.node.labels, &pushed, store)
                        {
                            expand = expand.with_target_ids(ids);
                        }
                        expand = expand.with_target_props(pushed);
                    }

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
                    if self_ref {
                        path_operator = Box::new(FilterOperator::new(
                            path_operator,
                            Expression::Binary {
                                left: Box::new(Expression::Variable(expand_var)),
                                op: BinaryOp::Eq,
                                right: Box::new(Expression::Variable(target_var.clone())),
                            },
                        ));
                    }

                    // Add property filter for target node if properties specified
                    if let Some(ref props) = segment.node.properties {
                        if !props.is_empty() {
                            let filter_expr = self.build_property_filter(&target_var, props);
                            path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
                        }
                    }

                    // Single-hop only; see the note at the other two sites.
                    // A variable-length segment's inline properties are
                    // enforced inside the expand (#934).
                    if let (Some((_, predicate)), None) =
                        (edge_filter, segment.edge.length.as_ref())
                    {
                        path_operator = Box::new(FilterOperator::new(path_operator, predicate));
                    }

                    bound.insert(target_var.clone());
                    if let Some(ev) = &segment.edge.variable {
                        bound.insert(ev.clone());
                    }
                    path_operator = Self::apply_ready_predicates(
                        path_operator,
                        &mut deferred_predicates,
                        &bound,
                    );

                    current_var = target_var;
                }
            }
                    (path_operator, deferred_predicates)
                };

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
                result = Box::new(JoinOperator::new(result, op, shared.clone()));
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

    /// The exact node set a target's equality predicates admit, when that can
    /// be computed without scanning the whole store.
    ///
    /// The profile of LDBC IC11 at SF10 is what motivates this. Pushing
    /// `org.name = "..."` into the expand (#656) correctly cut its output to
    /// 190 rows, and the expand still took **74% of the query** — because the
    /// check costs a `get_node` and a property compare per candidate edge, and
    /// there are ~29,000 of them. Moving a filter earlier made it more
    /// expensive per candidate.
    ///
    /// Resolving the predicate to node ids once turns that into a hash lookup.
    /// The property index answers it directly when one exists; otherwise a
    /// label scan is used, which is worth it only because it happens once at
    /// plan time against a small label — 7,955 Organisations against 29,000
    /// edge candidates. The scan is capped for that reason: above the cap the
    /// per-candidate check is the cheaper of the two (#665).
    /// Push a var-length target's inline properties into the operator so a
    /// non-matching endpoint never becomes a record.
    ///
    /// The `Filter` above is deliberately **left in place**. This is pruning,
    /// not a replacement: if the operator's test is ever narrower than the
    /// filter's the filter still decides, so the worst case is wasted work
    /// rather than a dropped row. Every other pushdown in this file that
    /// removed its filter had to be reasoned about for null and type
    /// coercion; this one does not.
    fn push_varlen_target_props(
        &self,
        expand: VarLengthExpandOperator,
        target_var: &str,
        labels: &[Label],
        properties: Option<&HashMap<String, PropertyValue>>,
        store: &GraphStore,
    ) -> VarLengthExpandOperator {
        let _ = target_var;
        // An off switch, so the two arms can be measured in one binary on one
        // host. Comparing a build from before this change against one after it
        // means comparing two runs, and on this machine the host calibration
        // moved from 29 ms to 75 ms between them -- a 2.6x difference in the
        // ruler, which is larger than anything being measured.
        if std::env::var("SAMYAMA_VARLEN_TARGET_PRUNING").as_deref() == Ok("0") {
            return expand;
        }
        let Some(props) = properties else { return expand };
        if props.is_empty() {
            return expand;
        }
        // Deterministic: `HashMap` iteration order is not, and the resolved
        // set depends on which property is consulted first.
        let mut pairs: Vec<(String, PropertyValue)> =
            props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));

        // An id set answers without touching a node; the property compare is
        // the fallback when nothing resolves it.
        if let Some(ids) = self.resolve_target_ids(labels, &pairs, store) {
            return expand.with_target_ids(ids);
        }
        expand.with_target_props(pairs)
    }

    fn resolve_target_ids(
        &self,
        labels: &[Label],
        props: &[(String, PropertyValue)],
        store: &GraphStore,
    ) -> Option<HashSet<crate::graph::NodeId>> {
        let (key, value) = props.first()?;

        // An unlabelled target cannot be resolved: a property index covers one
        // label, and a node of any label may carry the property. Answering
        // from one label's index would return the wrong set, not a wider one.
        let label = labels.first()?;

        // Preferred: an index answers without touching any node. Only this
        // label's index — indexes are keyed by `(label, property)`, so
        // consulting another label's answers a different question. Reading
        // whichever index happened to hold the value first returned, say,
        // `Person` ids for an `:Organisation` target: not a superset of the
        // right answer but a disjoint set, so every correct row was dropped.
        //
        // A multi-label target resolves from the first label alone, which *is*
        // a superset — the expand still applies `with_target_labels` and
        // `target_props`, so a wider set only costs work, never rows.
        if let Some(index) = store.property_index.get_index(label, key) {
            let ids = index.read().unwrap().get(value);
            if !ids.is_empty() {
                return Some(ids.into_iter().collect());
            }
        }

        // Fallback: scan one label, once. Only worth doing when the label is
        // small — otherwise this trades a per-candidate cost for a per-query
        // one that is larger.
        const MAX_SCAN: usize = 50_000;
        let nodes = store.get_nodes_by_label(label);
        if nodes.len() > MAX_SCAN {
            return None;
        }
        Some(
            nodes
                .iter()
                .filter(|n| n.get_property(key).is_some_and(|p| p == value))
                .map(|n| n.id)
                .collect(),
        )
    }

    /// Equality predicates of the form `<var>.<prop> = <literal>` for one
    /// variable, read out of the deferred set **without removing them**.
    ///
    /// Additive on purpose. The filter the planner would have built stays
    /// where it is, so pushing these into an expand cannot change what the
    /// query returns — only how much is materialised on the way. Getting a
    /// pushdown subtly wrong is a wrong answer; getting this one wrong is a
    /// slow query.
    fn target_equality_props(
        deferred: &[Expression],
        var: &str,
    ) -> Vec<(String, PropertyValue)> {
        let mut out = Vec::new();
        for pred in deferred {
            for part in flatten_and_predicates(pred) {
                if let Expression::Binary { left, op: BinaryOp::Eq, right } = &part {
                    if let (
                        Expression::Property { variable, property },
                        Expression::Literal(value),
                    ) = (left.as_ref(), right.as_ref())
                    {
                        if variable == var {
                            out.push((property.clone(), value.clone()));
                        }
                    }
                }
            }
        }
        out
    }

    /// Attach any deferred predicate whose variables are all bound by now.
    ///
    /// The path builders used to hold every predicate that mentions a
    /// non-anchor variable until the **whole path** was expanded. On LDBC IC3
    /// that meant `m.creationDate >= … AND m.creationDate < …` -- which
    /// references only `m`, bound by the *first* expand -- was evaluated after
    /// the second expand had already produced 409,960 rows, of which 622
    /// survived. The predicate could have cut the second expand's input by
    /// ~80% (#328).
    ///
    /// Applying a conjunct as soon as its variables are bound cannot change
    /// the answer of a conjunctive pattern: the rows it removes are rows the
    /// later filter would have removed. `OPTIONAL MATCH` is not affected --
    /// this runs inside a single path of a single MATCH, and the outer join is
    /// built above it.
    fn apply_ready_predicates(
        operator: OperatorBox,
        deferred: &mut Vec<Expression>,
        bound: &HashSet<String>,
    ) -> OperatorBox {
        if deferred.is_empty() {
            return operator;
        }
        let mut ready: Vec<Expression> = Vec::new();
        deferred.retain(|pred| {
            let mut vars = HashSet::new();
            Self::collect_expression_variables(pred, &mut vars);
            // A predicate with no variables at all is a constant; leave it to
            // the existing early-predicate path rather than moving it here.
            if !vars.is_empty() && vars.iter().all(|v| bound.contains(v)) {
                ready.push(pred.clone());
                false
            } else {
                true
            }
        });
        if ready.is_empty() {
            return operator;
        }
        let filter_expr = ready
            .into_iter()
            .reduce(|acc, pred| Expression::Binary {
                left: Box::new(acc),
                op: BinaryOp::And,
                right: Box::new(pred),
            })
            .unwrap();
        Box::new(FilterOperator::new(operator, filter_expr))
    }

    /// Build a path plan anchored at `nodes[anchor_idx]` instead of the pattern's first
    /// node. Traverses backward (reversed edge direction) toward earlier-written nodes
    /// and forward (written direction) toward later-written nodes. Only invoked when
    /// `choose_anchor_index` found a cheaper starting point than the first node.
    fn build_path_from_anchor(
        &self,
        path: &PathPattern,
        nodes: &[PathNodeRef],
        anchor_idx: usize,
        path_preds: &[Expression],
        store: &GraphStore,
    ) -> (OperatorBox, Vec<Expression>) {
        let anchor = &nodes[anchor_idx];
        let anchor_var = anchor.var.clone();

        // Relationship isomorphism (#684). A single-hop pattern cannot reuse an
        // edge, so it does not pay for the bookkeeping; anything longer must.
        //
        // The first expand built here is also the first to execute — each one
        // wraps the previous — so it is the one that drops history inherited
        // from an earlier clause. That matters because the rule is scoped to a
        // clause: `MATCH (a)-[:R]-(b) MATCH (b)-[:R]-(c)` may legitimately walk
        // the same edge twice, and Neo4j agrees.
        let track_edges = path.segments.len() > 1;
        let mut first_expand = true;

        // Predicates referencing only the anchor variable can be evaluated at the
        // anchor scan; everything else is deferred until the whole path is built,
        // mirroring the conservative deferral used by the start-anchored builder.
        let mut anchor_only_preds: Vec<Expression> = Vec::new();
        let mut deferred_predicates: Vec<Expression> = Vec::new();
        for pred in path_preds {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(pred, &mut pred_vars);
            if pred_vars.iter().all(|v| v == &anchor_var) {
                anchor_only_preds.push(pred.clone());
            } else {
                deferred_predicates.push(pred.clone());
            }
        }

        let mut candidates: Vec<Expression> = Vec::new();
        if let Some(props) = &anchor.properties {
            for (prop_name, prop_value) in props {
                candidates.push(Expression::Binary {
                    left: Box::new(Expression::Property { variable: anchor_var.clone(), property: prop_name.clone() }),
                    op: BinaryOp::Eq,
                    right: Box::new(Expression::Literal(prop_value.clone())),
                });
            }
        }
        candidates.extend(anchor_only_preds);

        // `id()` first: it is unique by construction, so it beats any index
        // and needs no statistics to know that (#538).
        let mut path_operator: OperatorBox = if let Some((idx, ids)) =
            find_id_predicate(&anchor_var, &candidates)
        {
            candidates.remove(idx);
            Box::new(
                NodeByIdOperator::new(ids, anchor_var.clone())
                    .with_labels(anchor.labels.clone()),
            )
        } else if let Some((idx, label, property, op, val)) =
            find_index_predicate(&anchor_var, &anchor.labels, &candidates, store)
        {
            candidates.remove(idx);
            Box::new(IndexScanOperator::new(anchor_var.clone(), label, property, op, val))
        } else {
            Box::new(NodeScanOperator::new(anchor_var.clone(), anchor.labels.clone()))
        };
        if !candidates.is_empty() {
            let filter_expr = candidates.into_iter().reduce(|acc, pred| {
                Expression::Binary { left: Box::new(acc), op: BinaryOp::And, right: Box::new(pred) }
            }).unwrap();
            path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
        }

        // Variables bound so far, so a deferred predicate can be applied the
        // moment its last variable arrives rather than after the whole path
        // (#328). This builder is the one that runs whenever the cheapest
        // anchor is not the pattern's first node -- which on LDBC IC3 and IC6
        // is most of the time.
        let mut bound: HashSet<String> = HashSet::new();
        bound.insert(anchor_var.clone());

        // Walk backward toward earlier-written nodes using reversed edge direction:
        // the anchor is now the traversal source, so an originally-outgoing edge from
        // the earlier node must be read as incoming from the anchor's perspective.
        let mut current_var = anchor_var.clone();
        for seg_idx in (0..anchor_idx).rev() {
            let segment = &path.segments[seg_idx];
            let target = &nodes[seg_idx];
            // Same rule as the main path builder: an inline relationship
            // property constraint is applied rather than dropped (#649). Which
            // of these builders runs depends on where the cheapest anchor is,
            // so a fix in only one of them is a fix that depends on the
            // optimiser's mood.
            let edge_filter = self.edge_property_filter(&segment.edge, seg_idx);
            let edge_var = match &edge_filter {
                Some((var, _)) => Some(var.clone()),
                None => segment.edge.variable.clone(),
            };
            let edge_types: Vec<String> = segment.edge.types.iter().map(|t| t.as_str().to_string()).collect();
            let reversed_dir = match segment.edge.direction {
                Direction::Outgoing => Direction::Incoming,
                Direction::Incoming => Direction::Outgoing,
                Direction::Both => Direction::Both,
            };
            path_operator = if let Some(length) = &segment.edge.length {
                // A variable-length segment traversed against the written
                // direction is the same relation read the other way:
                // `(a)-[:R*1..2]->(b)` from `b` is `(b)<-[:R*1..2]-(a)`. The
                // pairs are identical, and the BFS deduplicates by node at
                // whichever end it starts from.
                //
                // Before this, any path containing a `*` was excluded from
                // anchor selection outright, which is why LDBC IC6 always
                // started at the person and expanded to 400,257 rows rather
                // than starting at the tag that selects seven.
                let mut expand = VarLengthExpandOperator::new(
                    path_operator,
                    current_var.clone(),
                    target.var.clone(),
                    edge_types,
                    reversed_dir,
                    length.min.unwrap_or(1),
                    length.max.unwrap_or(usize::MAX),
                );
                // Walked against the written direction, so anything this
                // segment binds -- the relationship list, a named path -- has
                // to come back in the pattern's order, not the walk's (#933).
                expand = expand.with_reversed_walk();
                // Inline relationship properties, e.g. `-[:R* {year: 1988}]->`.
                // The operator had nowhere to put these and the planner
                // dropped them, so the pattern matched every path and the
                // filter failed *open* (#934).
                if let Some(props) = &segment.edge.properties {
                    if !props.is_empty() {
                        expand = expand.with_edge_properties(props.clone());
                    }
                }
                if self
                    .trail_enumeration
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    expand = expand.with_trail_enumeration();
                }
                // Relationship isomorphism applies to a var-length segment too: an
                // edge an earlier segment of this clause walked is not available to
                // it. `ExpandOperator` has done this since #684; this path did not
                // inherit it, so `(a)-[:R]-(y)-[:R*1..1]-(z)` over one edge answered
                // a row where openCypher answers none (#710).
                if track_edges {
                    expand = expand.with_edge_isolation(first_expand);
                    first_expand = false;
                }
                // `MATCH (a)-[r:T*]->(b)` binds `r` to the list of
                // relationships traversed. Dropping it made the query fail
                // with "Variable not found: r" (#652).
                if let Some(ref rv) = segment.edge.variable {
                    expand = expand.with_rel_variable(rv.clone());
                }
                // When the far end resolves to a single node, one reversed BFS
                // from it answers every input row — see `with_pinned_target`.
                // This is the shape anchoring produces on LDBC IC6: the walk
                // ends at the pinned person, and without the pin each of
                // thousands of candidates expands its own neighbourhood to
                // discover whether that one person is in it.
                if path.path_variable.is_none() && length.min.unwrap_or(1) <= 1 {
                    if let Some(pinned) = pinned_node_for(&target.var, path_preds, store)
                        .or_else(|| target.properties.as_ref().and_then(|props| {
                            let inline: Vec<Expression> = props.iter().map(|(k, v)| Expression::Binary {
                                left: Box::new(Expression::Property {
                                    variable: target.var.clone(),
                                    property: k.clone(),
                                }),
                                op: BinaryOp::Eq,
                                right: Box::new(Expression::Literal(v.clone())),
                            }).collect();
                            pinned_node_for(&target.var, &inline, store)
                        }))
                    {
                        expand = expand.with_pinned_target(pinned);
                    }
                }
                // Prune non-matching endpoints before a record is built; the
                // filter above stays (#1063). Applied at all three sites that
                // build a var-length expand, because `Query` has two AST
                // shapes and a rule added to one silently no-ops the other.
                let expand = self.push_varlen_target_props(
                    expand,
                    &target.var,
                    &target.labels,
                    target.properties.as_ref(),
                    store,
                );
                if !target.labels.is_empty() {
                    Box::new(expand.with_target_labels(target.labels.clone())) as OperatorBox
                } else {
                    Box::new(expand) as OperatorBox
                }
            } else {
                // A variable already bound has to be *matched*, not rebound.
                // `ExpandOperator` binds its target unconditionally, so
                // `MATCH (b)-->(b)` bound the far end of each edge over the
                // near one and every edge matched -- a graph containing no
                // self-relationships at all returned one row per edge (#639).
                // Expanding into a synthetic name and requiring the two to be
                // equal is what a repeated variable means; the walk continues
                // from the original, which the filter has just proved is the
                // same node.
                let self_ref = bound.contains(&target.var);
                let expand_var = if self_ref {
                    format!("__self_{}_{}", target.var, seg_idx)
                } else {
                    target.var.clone()
                };
                let mut expand = ExpandOperator::new(path_operator, current_var.clone(), expand_var.clone(), edge_var, edge_types, reversed_dir);
                if track_edges {
                    expand = expand.with_edge_isolation(first_expand);
                    first_expand = false;
                }
                let expanded: OperatorBox = if !target.labels.is_empty() {
                    Box::new(expand.with_target_labels(target.labels.clone()))
                } else {
                    Box::new(expand)
                };
                if self_ref {
                    Box::new(FilterOperator::new(
                        expanded,
                        Expression::Binary {
                            left: Box::new(Expression::Variable(expand_var)),
                            op: BinaryOp::Eq,
                            right: Box::new(Expression::Variable(target.var.clone())),
                        },
                    )) as OperatorBox
                } else {
                    expanded
                }
            };
            if let Some(ref props) = target.properties {
                if !props.is_empty() {
                    let filter_expr = self.build_property_filter(&target.var, props);
                    path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
                }
            }
            // Only for a single-hop segment. On a variable-length one the
            // constraint belongs to every relationship on the path, which is
            // the expand operator's job (#934) -- and this filter cannot do it
            // anyway: it names either the *list* variable, comparing a list to
            // a scalar, or an invented name nothing binds, which raised
            // VariableNotFound.
            if let (Some((_, predicate)), None) = (edge_filter, segment.edge.length.as_ref()) {
                path_operator = Box::new(FilterOperator::new(path_operator, predicate));
            }
            bound.insert(target.var.clone());
            if let Some(ev) = &segment.edge.variable {
                bound.insert(ev.clone());
            }
            path_operator =
                Self::apply_ready_predicates(path_operator, &mut deferred_predicates, &bound);
            current_var = target.var.clone();
        }

        // Walk forward toward later-written nodes using the written edge direction.
        let mut current_var = anchor_var;
        for seg_idx in anchor_idx..path.segments.len() {
            let segment = &path.segments[seg_idx];
            let target = &nodes[seg_idx + 1];
            // Same rule as the main path builder: an inline relationship
            // property constraint is applied rather than dropped (#649). Which
            // of these builders runs depends on where the cheapest anchor is,
            // so a fix in only one of them is a fix that depends on the
            // optimiser's mood.
            let edge_filter = self.edge_property_filter(&segment.edge, seg_idx);
            let edge_var = match &edge_filter {
                Some((var, _)) => Some(var.clone()),
                None => segment.edge.variable.clone(),
            };
            let edge_types: Vec<String> = segment.edge.types.iter().map(|t| t.as_str().to_string()).collect();
            path_operator = if let Some(length) = &segment.edge.length {
                let mut expand = VarLengthExpandOperator::new(
                    path_operator,
                    current_var.clone(),
                    target.var.clone(),
                    edge_types,
                    segment.edge.direction.clone(),
                    length.min.unwrap_or(1),
                    length.max.unwrap_or(usize::MAX),
                );
                // Inline relationship properties, e.g. `-[:R* {year: 1988}]->`.
                // The operator had nowhere to put these and the planner
                // dropped them, so the pattern matched every path and the
                // filter failed *open* (#934).
                if let Some(props) = &segment.edge.properties {
                    if !props.is_empty() {
                        expand = expand.with_edge_properties(props.clone());
                    }
                }
                if self
                    .trail_enumeration
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    expand = expand.with_trail_enumeration();
                }
                // Relationship isomorphism applies to a var-length segment too: an
                // edge an earlier segment of this clause walked is not available to
                // it. `ExpandOperator` has done this since #684; this path did not
                // inherit it, so `(a)-[:R]-(y)-[:R*1..1]-(z)` over one edge answered
                // a row where openCypher answers none (#710).
                if track_edges {
                    expand = expand.with_edge_isolation(first_expand);
                    first_expand = false;
                }
                // `MATCH (a)-[r:T*]->(b)` binds `r` to the list of
                // relationships traversed. Dropping it made the query fail
                // with "Variable not found: r" (#652).
                if let Some(ref rv) = segment.edge.variable {
                    expand = expand.with_rel_variable(rv.clone());
                }
                // Prune non-matching endpoints before a record is built; the
                // filter above stays (#1063). Applied at all three sites that
                // build a var-length expand, because `Query` has two AST
                // shapes and a rule added to one silently no-ops the other.
                let expand = self.push_varlen_target_props(
                    expand,
                    &target.var,
                    &target.labels,
                    target.properties.as_ref(),
                    store,
                );
                if !target.labels.is_empty() {
                    Box::new(expand.with_target_labels(target.labels.clone())) as OperatorBox
                } else {
                    Box::new(expand) as OperatorBox
                }
            } else {
                // A variable already bound has to be *matched*, not rebound.
                // `ExpandOperator` binds its target unconditionally, so
                // `MATCH (b)-->(b)` bound the far end of each edge over the
                // near one and every edge matched -- a graph containing no
                // self-relationships at all returned one row per edge (#639).
                // Expanding into a synthetic name and requiring the two to be
                // equal is what a repeated variable means; the walk continues
                // from the original, which the filter has just proved is the
                // same node.
                let self_ref = bound.contains(&target.var);
                let expand_var = if self_ref {
                    format!("__self_{}_{}", target.var, seg_idx)
                } else {
                    target.var.clone()
                };
                let mut expand = ExpandOperator::new(path_operator, current_var.clone(), expand_var.clone(), edge_var, edge_types, segment.edge.direction.clone());
                if track_edges {
                    expand = expand.with_edge_isolation(first_expand);
                    first_expand = false;
                }
                let expanded: OperatorBox = if !target.labels.is_empty() {
                    Box::new(expand.with_target_labels(target.labels.clone()))
                } else {
                    Box::new(expand)
                };
                if self_ref {
                    Box::new(FilterOperator::new(
                        expanded,
                        Expression::Binary {
                            left: Box::new(Expression::Variable(expand_var)),
                            op: BinaryOp::Eq,
                            right: Box::new(Expression::Variable(target.var.clone())),
                        },
                    )) as OperatorBox
                } else {
                    expanded
                }
            };
            if let Some(ref props) = target.properties {
                if !props.is_empty() {
                    let filter_expr = self.build_property_filter(&target.var, props);
                    path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
                }
            }
            // Only for a single-hop segment. On a variable-length one the
            // constraint belongs to every relationship on the path, which is
            // the expand operator's job (#934) -- and this filter cannot do it
            // anyway: it names either the *list* variable, comparing a list to
            // a scalar, or an invented name nothing binds, which raised
            // VariableNotFound.
            if let (Some((_, predicate)), None) = (edge_filter, segment.edge.length.as_ref()) {
                path_operator = Box::new(FilterOperator::new(path_operator, predicate));
            }
            bound.insert(target.var.clone());
            if let Some(ev) = &segment.edge.variable {
                bound.insert(ev.clone());
            }
            path_operator =
                Self::apply_ready_predicates(path_operator, &mut deferred_predicates, &bound);
            current_var = target.var.clone();
        }

        (path_operator, deferred_predicates)
    }

    /// Build a filter expression from node properties.
    /// Converts {name: "Alice", age: 30} into (n.name = "Alice" AND n.age = 30)
    /// A filter for an inline relationship property constraint, if it has one.
    ///
    /// `MATCH ()-[r:R {num: 2}]->()` was planned with the type filter only and
    /// the properties dropped, so it returned **every** `:R` -- a silent
    /// over-match, and the same query written `WHERE r.num = 2` answered
    /// correctly. The node side of the pattern has always been filtered; this
    /// is the relationship side of the same rule (#649).
    ///
    /// An anonymous relationship needs a name to filter on, so one is invented
    /// when the pattern did not give it one. Nothing reads it: `RETURN *`
    /// expands from the query's own variables, not from the record's keys.
    fn edge_property_filter(
        &self,
        edge: &crate::query::ast::EdgePattern,
        seg_idx: usize,
    ) -> Option<(String, Expression)> {
        let props = edge.properties.as_ref().filter(|p| !p.is_empty())?;
        let var = edge
            .variable
            .clone()
            .unwrap_or_else(|| format!("__edge_props_{seg_idx}"));
        Some((var.clone(), self.build_property_filter(&var, props)))
    }

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
            Expression::ExistsSubquery { pattern, where_clause, .. } => {
                // An EXISTS subquery's pattern routinely references variables bound
                // by the enclosing query — `NOT EXISTS { MATCH (a)-[:KNOWS]-(b) }`
                // where both `a` and `b` come from the outer MATCH. Those are real
                // dependencies: the predicate cannot be evaluated until they are
                // bound. Reporting no variables (the previous behaviour) made this
                // predicate look constant, so it was applied at the initial scan —
                // before the expansion that binds `b`. With `b` free the subquery
                // degenerates to "does `a` have any such edge at all", which is
                // true for almost every row, so `NOT EXISTS` silently eliminated
                // the entire result set.
                //
                // Collecting every variable named in the subquery over-approximates:
                // a variable local to the subquery is counted as a dependency too.
                // That only ever defers the filter to a later, still-correct point
                // (deferred predicates and cross-path predicates are both applied
                // after the joins), never evaluates it too early.
                for path in &pattern.paths {
                    if let Some(v) = &path.start.variable { vars.insert(v.clone()); }
                    for seg in &path.segments {
                        if let Some(v) = &seg.node.variable { vars.insert(v.clone()); }
                        if let Some(v) = &seg.edge.variable { vars.insert(v.clone()); }
                    }
                }
                if let Some(wc) = where_clause {
                    Self::collect_expression_variables(&wc.predicate, vars);
                }
            }
            // Everything else that *contains* expressions.
            //
            // These used to fall into `_ => {}`, so a predicate whose only
            // variables sat inside a comprehension looked **constant** and was
            // pushed down to the initial scan, where those variables are not
            // bound yet:
            //
            //   MATCH (n)-->(b) WHERE n.name IN [x IN labels(b) | toLower(x)]
            //   -> VariableNotFound("b")
            //
            // Same failure the `ExistsSubquery` arm above was added for, in
            // every other compound expression (#948). The reasoning there
            // applies unchanged: over-approximating only ever defers a filter
            // to a later, still-correct point, while under-approximating
            // evaluates it too early.
            Expression::ListExpr(items) => {
                for e in items {
                    Self::collect_expression_variables(e, vars);
                }
            }
            Expression::MapExpr(entries) => {
                for (_, e) in entries {
                    Self::collect_expression_variables(e, vars);
                }
            }
            Expression::Index { expr, index } => {
                Self::collect_expression_variables(expr, vars);
                Self::collect_expression_variables(index, vars);
            }
            Expression::ListSlice { expr, start, end } => {
                Self::collect_expression_variables(expr, vars);
                for e in start.iter().chain(end.iter()) {
                    Self::collect_expression_variables(e, vars);
                }
            }
            Expression::Case { operand, when_clauses, else_result } => {
                for e in operand.iter() {
                    Self::collect_expression_variables(e, vars);
                }
                for (w, t) in when_clauses {
                    Self::collect_expression_variables(w, vars);
                    Self::collect_expression_variables(t, vars);
                }
                for e in else_result.iter() {
                    Self::collect_expression_variables(e, vars);
                }
            }
            // The binder cases. Each introduces a variable of its own, which
            // is *not* an outer dependency -- deferring a predicate on a name
            // nothing outside ever binds would defer it past every point that
            // could apply it.
            Expression::ListComprehension { variable, list_expr, filter, map_expr } => {
                let mut inner = HashSet::new();
                Self::collect_expression_variables(list_expr, &mut inner);
                for e in filter.iter() {
                    Self::collect_expression_variables(e, &mut inner);
                }
                Self::collect_expression_variables(map_expr, &mut inner);
                inner.remove(variable);
                vars.extend(inner);
            }
            Expression::PredicateFunction { variable, list_expr, predicate, .. } => {
                let mut inner = HashSet::new();
                Self::collect_expression_variables(list_expr, &mut inner);
                Self::collect_expression_variables(predicate, &mut inner);
                inner.remove(variable);
                vars.extend(inner);
            }
            Expression::Reduce { accumulator, init, variable, list_expr, expression } => {
                let mut inner = HashSet::new();
                Self::collect_expression_variables(init, &mut inner);
                Self::collect_expression_variables(list_expr, &mut inner);
                Self::collect_expression_variables(expression, &mut inner);
                inner.remove(accumulator);
                inner.remove(variable);
                vars.extend(inner);
            }
            Expression::PatternComprehension { pattern, filter, projection } => {
                // The pattern's own variables are over-approximated as
                // dependencies, exactly as in the `ExistsSubquery` arm.
                for path in &pattern.paths {
                    if let Some(v) = &path.start.variable { vars.insert(v.clone()); }
                    for seg in &path.segments {
                        if let Some(v) = &seg.node.variable { vars.insert(v.clone()); }
                        if let Some(v) = &seg.edge.variable { vars.insert(v.clone()); }
                    }
                }
                for e in filter.iter() {
                    Self::collect_expression_variables(e, vars);
                }
                Self::collect_expression_variables(projection, vars);
            }
            Expression::PathVariable(v) => {
                vars.insert(v.clone());
            }
            _ => {}
        }
    }

    /// Plan a CREATE-only query (no MATCH clause)
    /// Build the specialized plan for the adjacency-count-aggregate pattern
    /// (ADR-017). Called only after `adjacency_agg_detector::detect` returns
    /// `Some`, which guarantees the constraints below hold.
    fn plan_adjacency_count_aggregate(
        &self,
        query: &Query,
        pat: super::adjacency_agg_detector::AdjacencyAggPattern,
    ) -> ExecutionResult<ExecutionPlan> {
        use super::operator::{
            AdjacencyCountAggregateOperator, LimitOperator, NodeScanOperator, ProjectOperator,
            SortOperator,
        };

        let physical_direction = match pat.direction {
            super::logical_plan::ExpandDirection::Forward => Direction::Outgoing,
            super::logical_plan::ExpandDirection::Reverse => Direction::Incoming,
        };

        // Scan + optional WHERE filter on grouped side + count.
        let scan: OperatorBox = Box::new(NodeScanOperator::new(
            pat.grouped_var.clone(),
            vec![pat.grouped_label.clone()],
        ));
        let scan: OperatorBox = if let Some(pred) = &pat.prefilter {
            use super::operator::FilterOperator;
            Box::new(FilterOperator::new(scan, pred.clone()))
        } else {
            scan
        };
        // Push GROUP BY into the operator: it accumulates per-(prop_values)
        // counts in an internal HashMap during the per-node walk, emitting
        // one row per group rather than per node. Replaces the earlier
        // post-aggregate hash-group (which on PubMed-scale workloads cost
        // ~76 minutes across the 500-query mega-bench, per ADR-017 bug doc).
        // Variable-only GROUP BY leaves `group_by_props` empty — per-node =
        // per-group for that case, no hashing needed.
        let group_by_props: Vec<String> = pat
            .group_by_items
            .iter()
            .filter_map(|(var, prop)| {
                if var == &pat.grouped_var {
                    prop.clone()
                } else {
                    None
                }
            })
            .collect();
        let mut adj_op = AdjacencyCountAggregateOperator::new(
            scan,
            pat.grouped_var.clone(),
            pat.count_alias.clone(),
            pat.edge_type.clone(),
            physical_direction,
        )
        // A degree counts every edge of the type whatever sits at the far end,
        // so a pattern that labels the neighbour needs the label applied while
        // counting (#601).
        .with_neighbor_label(pat.neighbor_label.clone());
        if !group_by_props.is_empty() {
            adj_op = adj_op.with_group_by_props(group_by_props);
        }
        if pat.count_distinct {
            adj_op = adj_op.with_count_distinct(true);
        }
        let mut operator: OperatorBox = Box::new(adj_op);

        // RETURN projections — the detector guarantees each item is either a
        // Property/Variable on the grouped side or the single count() aggregate,
        // so we can project directly against the enriched record.
        let return_clause = query
            .return_clause
            .as_ref()
            .expect("detector enforces RETURN presence");
        let mut output_columns = Vec::new();
        let projections: Vec<(Expression, String)> = return_clause
            .items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| item.column_name(i));
                output_columns.push(alias.clone());
                // For the count() item, project the already-bound alias
                // rather than re-evaluating the aggregate function.
                let expr = match &item.expression {
                    Expression::Function { name, .. }
                        if name.eq_ignore_ascii_case("count") =>
                    {
                        Expression::Variable(pat.count_alias.clone())
                    }
                    other => other.clone(),
                };
                (expr, alias)
            })
            .collect();
        // Pair each RETURN item's original expression with the alias this plan actually
        // emits, so an ORDER BY written either way resolves after the projection.
        let order_keys: Vec<(Expression, String)> = query
            .return_clause
            .as_ref()
            .map(|rc| {
                rc.items
                    .iter()
                    .zip(projections.iter())
                    .map(|(item, (_, alias))| (item.expression.clone(), alias.clone()))
                    .collect()
            })
            .unwrap_or_default();
        operator = Box::new(ProjectOperator::new(operator, projections));

        // (GROUP BY is now handled inside AdjacencyCountAggregateOperator
        // via `with_group_by_props` above — no post-aggregate step needed.)

        // ORDER BY — resolved against the projected aliases, since the sort runs after
        // the projection and the source variables are gone by then.
        if let Some(order_by) = &query.order_by {
            let sort_items: Vec<(Expression, bool)> = order_by
                .items
                .iter()
                .map(|i| {
                    (
                        resolve_sort_key(&i.expression, &order_keys, SortPosition::AfterProjection),
                        i.ascending,
                    )
                })
                .collect();
            operator = Box::new(SortOperator::new(operator, sort_items));
        }

        if let Some(skip) = query.skip {
            operator = Box::new(super::operator::SkipOperator::new(operator, skip));
        }
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write: false,
            candidates_evaluated: 1,
            chosen_plan_cost: 0.0,
            candidate_costs: Vec::new(),
        })
    }

    /// Phase 3a: specialized plan for the WITH-bound adjacency-count shape.
    /// The pre-WITH MATCH scans the grouped label; any WHERE filter is applied
    /// as a FilterOperator; the WITH's SKIP/LIMIT bound the scan; the
    /// AdjacencyCountAggregate operator counts neighbors per surviving row.
    fn plan_adjacency_count_aggregate_with_binding(
        &self,
        query: &Query,
        pat: super::adjacency_agg_detector::AdjacencyAggWithBindingPattern,
    ) -> ExecutionResult<ExecutionPlan> {
        use super::operator::{
            AdjacencyCountAggregateOperator, FilterOperator, LimitOperator, NodeScanOperator,
            ProjectOperator, SkipOperator, SortOperator,
        };

        let physical_direction = match pat.core.direction {
            super::logical_plan::ExpandDirection::Forward => Direction::Outgoing,
            super::logical_plan::ExpandDirection::Reverse => Direction::Incoming,
        };

        // Grouped-side scan. An explicit early_limit on NodeScan lets us stop
        // iterating after WITH's LIMIT rows — this is the pre-WITH-LIMIT
        // equivalent of PR #192's streaming idea, applied to the scan itself.
        let mut scan = NodeScanOperator::new(
            pat.core.grouped_var.clone(),
            vec![pat.core.grouped_label.clone()],
        );
        // Only push early_limit down when there's no pre-filter — otherwise
        // we'd stop at the wrong rows. With a filter, we apply LIMIT after.
        let filter_will_run = pat.prefilter.is_some();
        if !filter_will_run {
            if let Some(skip) = pat.grouped_scan_skip {
                if let Some(lim) = pat.grouped_scan_limit {
                    scan = scan.with_early_limit(skip + lim);
                }
            } else if let Some(lim) = pat.grouped_scan_limit {
                scan = scan.with_early_limit(lim);
            }
        }
        let mut operator: OperatorBox = Box::new(scan);

        // Pre-filter (WHERE on the grouped side).
        if let Some(pred) = pat.prefilter {
            operator = Box::new(FilterOperator::new(operator, pred));
        }

        // SKIP/LIMIT from the WITH clause. When a filter ran, these are
        // applied in user order: skip first, then limit of the filtered
        // stream. When no filter ran, early_limit on the scan already
        // handled both, so these wrappers are redundant — but harmless.
        if filter_will_run {
            if let Some(skip) = pat.grouped_scan_skip {
                operator = Box::new(SkipOperator::new(operator, skip));
            }
            if let Some(lim) = pat.grouped_scan_limit {
                operator = Box::new(LimitOperator::new(operator, lim));
            }
        }

        // Adjacency-count aggregate. Like Phase 1 (P8.5), push any
        // property-based GROUP BY into the operator so it accumulates
        // per-group counts in an internal HashMap during the per-node walk
        // — avoids a post-step that would re-traverse the bound rows on
        // PubMed-scale inputs (root cause of MB053/MB111/MB113 35s–407s
        // under the post-aggregate plan).
        let group_by_props: Vec<String> = pat
            .core
            .group_by_items
            .iter()
            .filter_map(|(var, prop)| {
                if var == &pat.core.grouped_var {
                    prop.clone()
                } else {
                    None
                }
            })
            .collect();
        let mut adj_op = AdjacencyCountAggregateOperator::new(
            operator,
            pat.core.grouped_var.clone(),
            pat.core.count_alias.clone(),
            pat.core.edge_type.clone(),
            physical_direction,
        )
        .with_neighbor_label(pat.core.neighbor_label.clone());
        if !group_by_props.is_empty() {
            adj_op = adj_op.with_group_by_props(group_by_props);
        }
        if pat.core.count_distinct {
            adj_op = adj_op.with_count_distinct(true);
        }
        operator = Box::new(adj_op);

        // RETURN projections — same logic as the Phase 1 helper.
        let return_clause = query
            .return_clause
            .as_ref()
            .expect("detector enforces RETURN presence");
        let mut output_columns = Vec::new();
        let projections: Vec<(Expression, String)> = return_clause
            .items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| item.column_name(i));
                output_columns.push(alias.clone());
                let expr = match &item.expression {
                    Expression::Function { name, .. }
                        if name.eq_ignore_ascii_case("count") =>
                    {
                        Expression::Variable(pat.core.count_alias.clone())
                    }
                    other => other.clone(),
                };
                (expr, alias)
            })
            .collect();
        // Pair each RETURN item's original expression with the alias this plan actually
        // emits, so an ORDER BY written either way resolves after the projection.
        let order_keys: Vec<(Expression, String)> = query
            .return_clause
            .as_ref()
            .map(|rc| {
                rc.items
                    .iter()
                    .zip(projections.iter())
                    .map(|(item, (_, alias))| (item.expression.clone(), alias.clone()))
                    .collect()
            })
            .unwrap_or_default();
        operator = Box::new(ProjectOperator::new(operator, projections));

        if let Some(order_by) = &query.order_by {
            let sort_items: Vec<(Expression, bool)> = order_by
                .items
                .iter()
                .map(|i| {
                    (
                        resolve_sort_key(&i.expression, &order_keys, SortPosition::AfterProjection),
                        i.ascending,
                    )
                })
                .collect();
            operator = Box::new(SortOperator::new(operator, sort_items));
        }
        if let Some(skip) = query.skip {
            operator = Box::new(SkipOperator::new(operator, skip));
        }
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write: false,
            candidates_evaluated: 1,
            chosen_plan_cost: 0.0,
            candidate_costs: Vec::new(),
        })
    }

    /// Phase 4 (PR-P2.8): aggregate-then-expand pattern. Targets B3 CT20.
    fn plan_aggregate_then_expand(
        &self,
        query: &Query,
        pat: super::adjacency_agg_detector::AggregateThenExpandPattern,
    ) -> ExecutionResult<ExecutionPlan> {
        use super::operator::{
            AdjacencyCountAggregateOperator, ExpandOperator, FilterOperator, LimitOperator,
            NodeScanOperator, ProjectOperator, SkipOperator, SortOperator,
        };

        let physical_direction = match pat.core.direction {
            super::logical_plan::ExpandDirection::Forward => Direction::Outgoing,
            super::logical_plan::ExpandDirection::Reverse => Direction::Incoming,
        };

        let scan: OperatorBox = Box::new(NodeScanOperator::new(
            pat.core.grouped_var.clone(),
            vec![pat.core.grouped_label.clone()],
        ));
        let scan: OperatorBox = if let Some(pred) = &pat.core.prefilter {
            Box::new(FilterOperator::new(scan, pred.clone()))
        } else {
            scan
        };
        let group_by_props: Vec<String> = pat
            .core
            .group_by_items
            .iter()
            .filter_map(|(var, prop)| {
                if var == &pat.core.grouped_var {
                    prop.clone()
                } else {
                    None
                }
            })
            .collect();
        let mut adj_op = AdjacencyCountAggregateOperator::new(
            scan,
            pat.core.grouped_var.clone(),
            pat.core.count_alias.clone(),
            pat.core.edge_type.clone(),
            physical_direction,
        )
        .with_neighbor_label(pat.core.neighbor_label.clone());
        if !group_by_props.is_empty() {
            adj_op = adj_op.with_group_by_props(group_by_props);
        }
        if pat.core.count_distinct {
            adj_op = adj_op.with_count_distinct(true);
        }
        let mut operator: OperatorBox = Box::new(adj_op);

        if let Some(pred) = &pat.post_aggregate_filter {
            operator = Box::new(FilterOperator::new(operator, pred.clone()));
        }
        if let Some(items) = &pat.post_aggregate_order_by {
            operator = Box::new(SortOperator::new(operator, items.clone()));
        }
        if let Some(skip) = pat.post_aggregate_skip {
            operator = Box::new(SkipOperator::new(operator, skip));
        }
        if let Some(lim) = pat.post_aggregate_limit {
            operator = Box::new(LimitOperator::new(operator, lim));
        }

        let mut expand = ExpandOperator::new(
            operator,
            pat.core.grouped_var.clone(),
            pat.expand_neighbor_var.clone(),
            None,
            vec![pat.expand_edge_type.as_str().to_string()],
            pat.expand_direction,
        );
        if let Some(label) = &pat.expand_neighbor_label {
            expand = expand.with_target_labels(vec![label.clone()]);
        }
        operator = Box::new(expand);

        let return_clause = query
            .return_clause
            .as_ref()
            .expect("detector enforces RETURN presence");
        let mut output_columns = Vec::new();
        let projections: Vec<(Expression, String)> = return_clause
            .items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| item.column_name(i));
                output_columns.push(alias.clone());
                (item.expression.clone(), alias)
            })
            .collect();
        // Pair each RETURN item's original expression with the alias this plan actually
        // emits, so an ORDER BY written either way resolves after the projection.
        let order_keys: Vec<(Expression, String)> = query
            .return_clause
            .as_ref()
            .map(|rc| {
                rc.items
                    .iter()
                    .zip(projections.iter())
                    .map(|(item, (_, alias))| (item.expression.clone(), alias.clone()))
                    .collect()
            })
            .unwrap_or_default();
        operator = Box::new(ProjectOperator::new(operator, projections));

        if let Some(order_by) = &query.order_by {
            let sort_items: Vec<(Expression, bool)> = order_by
                .items
                .iter()
                .map(|i| {
                    (
                        resolve_sort_key(&i.expression, &order_keys, SortPosition::AfterProjection),
                        i.ascending,
                    )
                })
                .collect();
            operator = Box::new(SortOperator::new(operator, sort_items));
        }
        if let Some(skip) = query.skip {
            operator = Box::new(SkipOperator::new(operator, skip));
        }
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write: false,
            candidates_evaluated: 1,
            chosen_plan_cost: 0.0,
            candidate_costs: Vec::new(),
        })
    }

    /// Supports:
    /// - CREATE (n:Person {name: "Alice", age: 30})
    /// - CREATE (a:Person)-[:KNOWS]->(b:Person)
    /// - CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
    fn plan_create_only(&self, create_clause: &CreateClause) -> ExecutionResult<ExecutionPlan> {
        let pattern = &create_clause.pattern;

        // Collect all nodes to create from the pattern
        // Each node has: (labels, properties, variable_name)
        let mut nodes_to_create: Vec<(
            Vec<Label>,
            HashMap<String, PropertyValue>,
            Option<String>,
            Option<HashMap<String, Expression>>,
        )> = Vec::new();
        let mut output_columns: Vec<String> = Vec::new();

        // Collect edges to create: (source_var, target_var, edge_type, properties, edge_var)
        let mut edges_to_create: Vec<crate::query::executor::operator::EdgeToCreate> = Vec::new();

        // Anonymous nodes still need a handle for edge wiring. Edges are wired by variable
        // name, so an endpoint written as `(:Label)` used to have nothing to wire to and
        // the edge was dropped -- silently, since the nodes were still created. Give every
        // anonymous node a synthetic name that cannot collide with a user variable, and
        // keep it out of `output_columns` so it stays invisible to RETURN.
        let mut declared: std::collections::HashSet<String> = std::collections::HashSet::new();
        for path in &pattern.paths {
            if let Some(v) = &path.start.variable {
                declared.insert(v.clone());
            }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable {
                    declared.insert(v.clone());
                }
            }
        }
        let mut anon_seq = 0usize;
        let mut next_anon = move |declared: &std::collections::HashSet<String>| -> String {
            loop {
                let name = format!("__anon_create_{anon_seq}");
                anon_seq += 1;
                if !declared.contains(&name) {
                    return name;
                }
            }
        };

        // A variable bound earlier in the *same* CREATE refers to the node
        // already being created, not to a new one:
        //
        //     CREATE (a), (b), (a)-[:R]->(b)
        //
        // creates two nodes and one edge. Re-registering `a` and `b` for
        // creation made it four nodes — the edge was correct, so the query
        // succeeded and quietly doubled the graph. This is the shape every
        // TCK fixture and most of our own loaders are written in.
        let mut created_vars: HashSet<String> = HashSet::new();

        // `CREATE p = (a)-[:R]->(b)` binds `p`. The parser has always captured
        // `path_variable`; nothing bound it, so `RETURN p` failed with
        // `VariableNotFound("p")` -- a query that parses and then cannot name
        // what it just made (#876).
        //
        // Handles are collected here, where the synthetic names for anonymous
        // positions are already being minted for edge wiring, and a
        // `BindPathOperator` assembles the path from them afterwards.
        let mut named_paths: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();

        for path in &pattern.paths {
            // Add start node
            let start = &path.start;
            let labels: Vec<Label> = start.labels.clone();
            let properties: HashMap<String, PropertyValue> = start.properties.clone().unwrap_or_default();
            let variable = start.variable.clone();

            let start_already_bound = variable
                .as_ref()
                .is_some_and(|v| created_vars.contains(v));

            // Track output column if variable exists — once per variable, since
            // a repeat mention is the same node.
            if let Some(ref var) = variable {
                if !start_already_bound {
                    output_columns.push(var.clone());
                }
            }

            // Only the *named* variable reaches output_columns above; the synthetic one is
            // purely for edge wiring.
            let start_handle = match &variable {
                Some(v) => v.clone(),
                None => next_anon(&declared),
            };
            if !start_already_bound {
                if let Some(v) = &variable {
                    created_vars.insert(v.clone());
                }
                nodes_to_create.push((
                    labels,
                    properties,
                    Some(start_handle.clone()),
                    start.property_exprs.clone(),
                ));
            }

            let mut path_nodes: Vec<String> = vec![start_handle.clone()];
            let mut path_edges: Vec<String> = Vec::new();

            // Track current source variable for edge creation
            let mut current_source_var = Some(start_handle);

            // Add nodes and edges from path segments (if any)
            // Example: CREATE (a:Person)-[:KNOWS]->(b:Person)
            for segment in &path.segments {
                let node = &segment.node;
                let node_labels: Vec<Label> = node.labels.clone();
                let node_properties: HashMap<String, PropertyValue> = node.properties.clone().unwrap_or_default();
                let node_variable = node.variable.clone();

                let node_already_bound = node_variable
                    .as_ref()
                    .is_some_and(|v| created_vars.contains(v));

                if let Some(ref var) = node_variable {
                    if !node_already_bound {
                        output_columns.push(var.clone());
                    }
                }

                let node_handle = match &node_variable {
                    Some(v) => v.clone(),
                    None => next_anon(&declared),
                };
                if !node_already_bound {
                    if let Some(v) = &node_variable {
                        created_vars.insert(v.clone());
                    }
                    nodes_to_create.push((
                        node_labels,
                        node_properties,
                        Some(node_handle.clone()),
                        node.property_exprs.clone(),
                    ));
                }

                // Extract edge information
                let edge = &segment.edge;
                let edge_type = edge.types.first()
                    .cloned()
                    .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                let edge_properties: HashMap<String, PropertyValue> = edge.properties.clone().unwrap_or_default();
                let edge_variable = edge.variable.clone();

                // Both endpoints now always have a handle (real or synthetic), so an edge
                // is created for every segment regardless of how the pattern was written.
                //
                // Direction is taken from the pattern rather than from write order: an
                // `<-` segment points at the *earlier* node. This was previously ignored
                // entirely, so `CREATE (a)<-[:R]-(b)` stored a->b -- an edge pointing the
                // opposite way to what was written, with nothing to indicate it.
                // An anonymous relationship inside a **named** path still needs
                // a handle, for the same reason an anonymous node does: the
                // path has to be able to reference it afterwards.
                let edge_variable = match (&edge_variable, &path.path_variable) {
                    (None, Some(_)) => Some(next_anon(&declared)),
                    (other, _) => other.clone(),
                };
                if let Some(v) = &edge_variable {
                    path_edges.push(v.clone());
                }
                path_nodes.push(node_handle.clone());

                if let Some(source_var) = &current_source_var {
                    let (from, to) = match segment.edge.direction {
                        Direction::Incoming => (node_handle.clone(), source_var.clone()),
                        // `-[:R]-` is undirected; CREATE has to pick one, and written
                        // order is the least surprising choice.
                        Direction::Outgoing | Direction::Both => {
                            (source_var.clone(), node_handle.clone())
                        }
                    };
                    edges_to_create.push((
                        from,
                        to,
                        edge_type,
                        edge_properties,
                        edge_variable,
                        edge.property_exprs.clone(),
                    ));
                }

                // Update source variable for next segment
                current_source_var = Some(node_handle);
            }

            if let Some(pv) = &path.path_variable {
                output_columns.push(pv.clone());
                named_paths.push((pv.clone(), path_nodes, path_edges));
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

        // Bind any named paths from the handles collected above (#876).
        let final_operator: OperatorBox = if named_paths.is_empty() {
            final_operator
        } else {
            Box::new(crate::query::executor::operator::BindPathOperator::new(
                final_operator,
                named_paths,
            ))
        };

        // Return execution plan with is_write: true (this mutates the graph)
        Ok(ExecutionPlan {
            root: final_operator,
            output_columns,
            is_write: true, candidates_evaluated: 0, chosen_plan_cost: 0.0, candidate_costs: Vec::new(),
        })
    }
}

/// The node and relationship handles a named path on a write pattern needs.
///
/// Returns `None` when any position is anonymous and therefore cannot be
/// referenced afterwards — the path is then left unbound rather than bound to a
/// shorter one that looks plausible (#876).
///
/// An anonymous *relationship* in a named path is given a synthetic handle by
/// the caller, the way `CREATE` already does for anonymous nodes; an anonymous
/// *node* cannot be, on the MERGE paths, so those return `None`.
fn named_path_handles(
    pattern: &crate::query::ast::Pattern,
) -> Vec<(String, Vec<String>, Vec<String>)> {
    let mut out = Vec::new();
    let mut anon = 0usize;
    for path in &pattern.paths {
        let Some(pv) = &path.path_variable else { continue };
        let Some(start) = &path.start.variable else { continue };
        let mut nodes = vec![start.clone()];
        let mut edges = Vec::new();
        let mut complete = true;
        for segment in &path.segments {
            let Some(node) = &segment.node.variable else {
                complete = false;
                break;
            };
            let edge = match &segment.edge.variable {
                Some(v) => v.clone(),
                None => {
                    anon += 1;
                    format!("__merge_path_edge_{anon}")
                }
            };
            nodes.push(node.clone());
            edges.push(edge);
        }
        if complete {
            out.push((pv.clone(), nodes, edges));
        }
    }
    out
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Propagate node labels and inline properties for variables shared across multiple
/// MATCH clauses (planned independently and later joined on the shared variable).
/// If a variable is typed or constrained in one clause but referenced bare (e.g.
/// `(m)`) in another, copy the known labels/properties over so the second clause's
/// scan can also be narrowed/indexed instead of falling back to an all-nodes scan.
/// Safe because the same variable name within a query always refers to the same
/// node, so any constraint declared for it anywhere already applies everywhere.
fn propagate_shared_variable_labels(clauses: &[MatchClause]) -> Vec<MatchClause> {
    fn record_node(
        var_labels: &mut HashMap<String, Vec<Label>>,
        var_properties: &mut HashMap<String, HashMap<String, PropertyValue>>,
        node: &NodePattern,
    ) {
        let var = match &node.variable {
            Some(v) => v,
            None => return,
        };
        if !node.labels.is_empty() {
            var_labels.entry(var.clone()).or_insert_with(|| node.labels.clone());
        }
        if let Some(props) = &node.properties {
            let entry = var_properties.entry(var.clone()).or_default();
            for (k, v) in props {
                entry.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }

    fn enrich_node(
        var_labels: &HashMap<String, Vec<Label>>,
        var_properties: &HashMap<String, HashMap<String, PropertyValue>>,
        node: &mut NodePattern,
    ) {
        let var = match &node.variable {
            Some(v) => v.clone(),
            None => return,
        };
        if node.labels.is_empty() {
            if let Some(labels) = var_labels.get(&var) {
                node.labels = labels.clone();
            }
        }
        if let Some(known_props) = var_properties.get(&var) {
            let existing = node.properties.get_or_insert_with(HashMap::new);
            for (k, v) in known_props {
                existing.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }

    let mut var_labels: HashMap<String, Vec<Label>> = HashMap::new();
    let mut var_properties: HashMap<String, HashMap<String, PropertyValue>> = HashMap::new();

    for clause in clauses {
        for path in &clause.pattern.paths {
            record_node(&mut var_labels, &mut var_properties, &path.start);
            for seg in &path.segments {
                record_node(&mut var_labels, &mut var_properties, &seg.node);
            }
        }
    }

    if var_labels.is_empty() && var_properties.is_empty() {
        return clauses.to_vec();
    }

    let mut result: Vec<MatchClause> = clauses.to_vec();
    for clause in &mut result {
        for path in &mut clause.pattern.paths {
            enrich_node(&var_labels, &var_properties, &mut path.start);
            for seg in &mut path.segments {
                enrich_node(&var_labels, &var_properties, &mut seg.node);
            }
        }
    }
    result
}

/// A single node within a path pattern, resolved to a concrete variable name
/// (anonymous nodes get an auto-generated `_anon_N` name). Used by anchor
/// selection to consider indexing/scanning any node in the pattern, not just
/// the first one written.
struct PathNodeRef {
    var: String,
    labels: Vec<Label>,
    properties: Option<HashMap<String, PropertyValue>>,
}

/// Flip a comparison operator to normalize a reversed-operand predicate.
/// E.g. `5 < n.age` is equivalent to `n.age > 5`, so `Lt` flips to `Gt`.
fn flip_comparison_op(op: &BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Le => BinaryOp::Ge,
        BinaryOp::Ge => BinaryOp::Le,
        other => other.clone(),
    }
}

/// Find a predicate in `preds` usable as an index lookup for `var`, matching
/// either operand order (`var.prop OP literal` or `literal OP var.prop`).
/// Returns the predicate's index within `preds` plus the matched label,
/// property, normalized operator, and literal value.
/// Find an `id(var) = <literal>` or `id(var) IN [...]` predicate.
///
/// Returns the predicate's position and the node ids it pins, so the caller
/// can drop it and scan those ids directly.
///
/// `id()` is unique by construction, so this needs no cost model and no
/// statistics: the predicate selects at most one node per literal. Without it
/// `MATCH (n) WHERE id(n) = 5` lowered to a full label scan plus a filter, and
/// `shortestPath((a)-[:KNOWS*]-(b)) WHERE id(a) = 1 AND id(b) = 3500` ran
/// ~1000x slower than the same query written with inline properties (#538).
fn find_id_predicate(var: &str, preds: &[Expression]) -> Option<(usize, Vec<crate::graph::NodeId>)> {
    /// `id(x)` applied to exactly this variable.
    fn is_id_of(expr: &Expression, var: &str) -> bool {
        matches!(
            expr,
            Expression::Function { name, args, .. }
                if name.eq_ignore_ascii_case("id")
                    && args.len() == 1
                    && matches!(&args[0], Expression::Variable(v) if v == var)
        )
    }

    fn as_node_id(value: &PropertyValue) -> Option<crate::graph::NodeId> {
        match value {
            // Negative ids cannot exist, and `as u64` would wrap one into a
            // very large positive id that matches nothing slowly.
            PropertyValue::Integer(i) if *i >= 0 => Some(crate::graph::NodeId::new(*i as u64)),
            _ => None,
        }
    }

    for (i, pred) in preds.iter().enumerate() {
        let Expression::Binary { left, op, right } = pred else {
            continue;
        };
        // Accept the literal on either side: `id(n) = 5` and `5 = id(n)`.
        let literal = match (left.as_ref(), right.as_ref()) {
            (l, Expression::Literal(v)) if is_id_of(l, var) => Some(v),
            (Expression::Literal(v), r) if is_id_of(r, var) => Some(v),
            _ => None,
        };
        let Some(literal) = literal else { continue };

        match op {
            BinaryOp::Eq => {
                if let Some(id) = as_node_id(literal) {
                    return Some((i, vec![id]));
                }
            }
            BinaryOp::In => {
                if let PropertyValue::Array(items) = literal {
                    let ids: Vec<crate::graph::NodeId> = items.iter().filter_map(as_node_id).collect();
                    // Only if every element was a usable id -- dropping one
                    // silently would lose rows.
                    if !ids.is_empty() && ids.len() == items.len() {
                        return Some((i, ids));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// An `IndexScan` for an inline property, when one of the node's labels has an
/// index on it.
///
/// The property filter stays above this regardless: an index answers one
/// equality, and a pattern may carry several inline properties. Narrowing the
/// scan is the win; the filter still decides correctness.
fn inline_index_scan(
    var: &str,
    labels: &[Label],
    properties: Option<&HashMap<String, PropertyValue>>,
    store: &GraphStore,
) -> Option<OperatorBox> {
    let props = properties?;
    if props.is_empty() || labels.is_empty() {
        return None;
    }
    // Deterministic: a pattern with two indexed properties must plan the same
    // way on every run, and `HashMap` iteration order does not.
    let mut keys: Vec<&String> = props.keys().collect();
    keys.sort();
    for label in labels {
        for key in &keys {
            if store.property_index.has_index(label, key) {
                return Some(Box::new(IndexScanOperator::new(
                    var.to_string(),
                    label.clone(),
                    (*key).clone(),
                    BinaryOp::Eq,
                    props[*key].clone(),
                )) as OperatorBox);
            }
        }
    }
    None
}

fn find_index_predicate(
    var: &str,
    labels: &[Label],
    preds: &[Expression],
    store: &GraphStore,
) -> Option<(usize, Label, String, BinaryOp, PropertyValue)> {
    for (i, pred) in preds.iter().enumerate() {
        if let Expression::Binary { left, op, right } = pred {
            let matched = match (left.as_ref(), right.as_ref()) {
                (Expression::Property { variable, property }, Expression::Literal(val)) if variable == var => {
                    Some((property.clone(), op.clone(), val.clone()))
                }
                (Expression::Literal(val), Expression::Property { variable, property }) if variable == var => {
                    Some((property.clone(), flip_comparison_op(op), val.clone()))
                }
                _ => None,
            };
            if let Some((property, norm_op, val)) = matched {
                if matches!(norm_op, BinaryOp::Eq | BinaryOp::Gt | BinaryOp::Ge | BinaryOp::Lt | BinaryOp::Le) {
                    for label in labels {
                        if store.property_index.has_index(label, &property) {
                            return Some((i, label.clone(), property, norm_op, val));
                        }
                    }
                }
            }
        }
    }
    None
}

/// Choose the cheapest node in a path pattern to anchor the scan at: prefer a
/// node with an indexable predicate (cost ~= label cardinality * selectivity),
/// falling back to plain label-scan cardinality, and finally an all-nodes scan
/// for label-free nodes. Ties favor the earliest (first-written) node so
/// behavior is unchanged when no node is strictly cheaper than the start.
/// What the anchor scan reads, and how many rows survive its own predicates.
///
/// These are different numbers and conflating them is what made anchor
/// selection pick the wrong end. A `Tag {name: "…"}` with no index on `name`
/// must **read** all 16,080 tags, but it **emits** one — and it is the emitted
/// row count that every subsequent expand multiplies.
fn anchor_cardinality(
    node: &PathNodeRef,
    path_preds: &[Expression],
    store: &GraphStore,
) -> (f64, f64) {
    let stats = store.statistics();

    let mut candidates: Vec<Expression> = Vec::new();
    if let Some(props) = &node.properties {
        for (prop_name, prop_value) in props {
            candidates.push(Expression::Binary {
                left: Box::new(Expression::Property {
                    variable: node.var.clone(),
                    property: prop_name.clone(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(Expression::Literal(prop_value.clone())),
            });
        }
    }
    candidates.extend(path_preds.iter().cloned());

    // `id()` pins exactly one node per literal, whatever the label holds.
    if let Some((_, ids)) = find_id_predicate(&node.var, &candidates) {
        let n = ids.len() as f64;
        return (n, n);
    }

    if let Some((_, label, property, op, val)) =
        find_index_predicate(&node.var, &node.labels, &candidates, store)
    {
        // An indexed equality: ask the index how many nodes match rather than
        // estimating. `estimate_equality_selectivity` falls back to 10% when a
        // property has no statistics, which on a large label costed an index
        // lookup at a tenth of the label -- more than a full scan of a smaller
        // one -- and inverted the choice (#303). One probe at plan time also
        // makes a missing anchor cost 0, so the plan short-circuits.
        let exact = if matches!(op, BinaryOp::Eq) {
            store.property_index.indexed_equality_count(&label, &property, &val)
        } else {
            None
        };
        let rows = match exact {
            Some(n) => n as f64,
            None => {
                let base = stats.estimate_label_scan(&label) as f64;
                let selectivity = if matches!(op, BinaryOp::Eq) {
                    stats.estimate_equality_selectivity_for_value(&label, &property, &val)
                } else {
                    0.3
                };
                (base * selectivity).max(1.0)
            }
        };
        // An index read costs what it returns.
        return (rows, rows);
    }

    let Some(label) = node.labels.first() else {
        return (f64::MAX, f64::MAX);
    };
    let scan = stats.estimate_label_scan(label) as f64;

    // No index, so the scan reads the whole label -- but an equality on a
    // property still cuts what comes *out* of it, and that is what the rest of
    // the path multiplies.
    //
    // Both sources of equality count. Reading only the inline form made
    // `MATCH (org:Organisation) WHERE org.name = '…'` look like all 7,955
    // organisations while the identical `MATCH (org:Organisation {name: '…'})`
    // looked like one — the same query costed two ways depending on where the
    // author put the predicate. On LDBC IC11 that is the difference between
    // anchoring on one organisation and anchoring on a person whose two-hop
    // neighbourhood then has to be enumerated.
    let mut emitted = scan;
    let mut apply = |prop_name: &str, prop_value: &PropertyValue| {
        emitted *= stats.estimate_equality_selectivity_for_value(label, prop_name, prop_value);
    };
    if let Some(props) = &node.properties {
        for (prop_name, prop_value) in props {
            apply(prop_name, prop_value);
        }
    }
    for pred in path_preds {
        let Expression::Binary { left, op: BinaryOp::Eq, right } = pred else { continue };
        let (var, prop, value) = match (left.as_ref(), right.as_ref()) {
            (Expression::Property { variable, property }, Expression::Literal(v)) => (variable, property, v),
            (Expression::Literal(v), Expression::Property { variable, property }) => (variable, property, v),
            _ => continue,
        };
        if var == &node.var {
            apply(prop, value);
        }
    }
    (scan, emitted.max(1.0))
}

/// Rows out per row in, for one traversal step.
///
/// `forward` is the direction the pattern was written; anchoring elsewhere in
/// the path means traversing some segments against it, and the fan-out of an
/// edge type is not symmetric -- `HAS_CREATOR` is ~1 outgoing from a Post and
/// ~337 incoming to a Person.
fn segment_fanout(
    segment: &crate::query::ast::PathSegment,
    from: &PathNodeRef,
    to: &PathNodeRef,
    forward: bool,
    store: &GraphStore,
) -> f64 {
    let catalog = store.catalog();
    let edge_types = &segment.edge.types;

    let step = |source: &PathNodeRef, target: &PathNodeRef, outgoing: bool| -> f64 {
        if edge_types.is_empty() {
            // No type named: fall back to the graph-wide average degree.
            return store.statistics().estimate_expand(None).max(1.0);
        }
        edge_types
            .iter()
            .map(|et| {
                if outgoing {
                    match source.labels.first() {
                        Some(l) => catalog.estimate_expand_out(l, et),
                        None => store.statistics().estimate_expand(Some(et)),
                    }
                } else {
                    match target.labels.first() {
                        Some(l) => catalog.estimate_expand_in(l, et),
                        None => store.statistics().estimate_expand(Some(et)),
                    }
                }
            })
            .sum::<f64>()
            .max(0.01)
    };

    let one_hop = match (&segment.edge.direction, forward) {
        (Direction::Outgoing, true) | (Direction::Incoming, false) => step(from, to, true),
        (Direction::Incoming, true) | (Direction::Outgoing, false) => step(to, from, false),
        // Undirected reads both adjacencies.
        (Direction::Both, _) => step(from, to, true) + step(to, from, false),
    };

    // A variable-length segment compounds: reaching depth k costs roughly
    // d + d^2 + … + d^k. Capped, because an unbounded `*` would otherwise
    // produce infinity and make every anchor look equally bad.
    match &segment.edge.length {
        None => one_hop,
        Some(length) => {
            let max_hops = length.max.unwrap_or(3).min(6) as i32;
            let mut total = 0.0;
            for hop in 1..=max_hops.max(1) {
                total += one_hop.powi(hop);
            }
            total.min(1e12)
        }
    }
}

/// Total intermediate rows for a plan anchored at `nodes[anchor]`.
///
/// The sum of every operator's output, not just the scan's. That distinction
/// is the whole point: anchoring LDBC IC6 on the person costs 1 row to start
/// and then 3,272 -> 409,960 -> 400,257, while anchoring on the tag costs
/// 16,080 rows to scan and almost nothing after it.
/// The single node a variable is pinned to, if the predicates pin it to one.
///
/// Resolved at plan time by asking the property index, so this only fires when
/// the pin is exact — an indexed equality matching exactly one node, or an
/// `id()` literal. A predicate that merely narrows the variable is not a pin,
/// and treating it as one would silently drop rows.
fn pinned_node_for(
    var: &str,
    preds: &[Expression],
    store: &GraphStore,
) -> Option<crate::graph::NodeId> {
    if let Some((_, ids)) = find_id_predicate(var, preds) {
        if ids.len() == 1 {
            return Some(ids[0]);
        }
        return None;
    }
    for pred in preds {
        let Expression::Binary { left, op: BinaryOp::Eq, right } = pred else { continue };
        let (prop_var, prop_name, value) = match (left.as_ref(), right.as_ref()) {
            (Expression::Property { variable, property }, Expression::Literal(v)) => (variable, property, v),
            (Expression::Literal(v), Expression::Property { variable, property }) => (variable, property, v),
            _ => continue,
        };
        if prop_var != var {
            continue;
        }
        // Ask the index for the actual matches rather than estimating: a pin
        // has to be exactly one node, and an estimate cannot establish that.
        for label in store.catalog().label_counts.keys() {
            let Some(index) = store.property_index.get_index(label, prop_name) else { continue };
            let nodes = index.read().unwrap().get(value);
            if nodes.len() == 1 {
                return nodes.first().copied();
            }
            if !nodes.is_empty() {
                return None;
            }
        }
    }
    None
}

fn estimate_path_cost(
    path: &PathPattern,
    nodes: &[PathNodeRef],
    anchor: usize,
    path_preds: &[Expression],
    store: &GraphStore,
) -> f64 {
    let (scan_rows, anchor_rows) = anchor_cardinality(&nodes[anchor], path_preds, store);
    if scan_rows == f64::MAX {
        return f64::MAX;
    }
    let mut total = scan_rows;

    // Backward toward earlier-written nodes, against the written direction.
    let mut rows = anchor_rows;
    for seg_idx in (0..anchor).rev() {
        rows = step_rows(
            rows,
            &path.segments[seg_idx],
            &nodes[seg_idx + 1],
            &nodes[seg_idx],
            false,
            path_preds,
            store,
        );
        total += rows;
        if !total.is_finite() {
            return f64::MAX;
        }
    }

    // Forward toward later-written nodes.
    let mut rows = anchor_rows;
    for seg_idx in anchor..path.segments.len() {
        rows = step_rows(
            rows,
            &path.segments[seg_idx],
            &nodes[seg_idx],
            &nodes[seg_idx + 1],
            true,
            path_preds,
            store,
        );
        total += rows;
        if !total.is_finite() {
            return f64::MAX;
        }
    }

    total
}

/// Rows after one traversal step, bounded by **both** ends.
///
/// A step cannot produce more rows than its destination can supply. Multiplying
/// the incoming rows by a fan-out ignores that, and on a pattern whose far end
/// is pinned it is wrong by orders of magnitude: LDBC IC6 anchors `p` with
/// `{id: …}`, so traversing `KNOWS*1..2` *toward* `p` is a check against one
/// person, not a 41² expansion. Costing it as an expansion made every
/// alternative anchor look expensive and the planner never left the first node.
///
/// The bound is the standard one for a join: the result is at most what either
/// side can produce, so take the smaller of "rows in × fan-out forward" and
/// "rows the destination has × fan-out back".
fn step_rows(
    rows_in: f64,
    segment: &crate::query::ast::PathSegment,
    from: &PathNodeRef,
    to: &PathNodeRef,
    forward: bool,
    path_preds: &[Expression],
    store: &GraphStore,
) -> f64 {
    let forward_estimate = rows_in * segment_fanout(segment, from, to, forward, store);

    // What the destination side can supply, if it is constrained at all.
    let (_, dest_rows) = anchor_cardinality(to, path_preds, store);
    if dest_rows == f64::MAX {
        return forward_estimate;
    }
    let reverse_estimate = dest_rows * segment_fanout(segment, to, from, !forward, store);

    forward_estimate.min(reverse_estimate).max(1.0)
}

/// Which node to start the scan from.
///
/// Costed by the **whole path**, not by the anchor's own scan. The previous
/// version compared only scan cardinalities and therefore always preferred an
/// indexed endpoint, however expensive the traversal away from it was. On
/// LDBC IC6:
///
/// ```text
///   anchored on p:Person {id: …}   scan 1     -> 3,272 -> 409,960 -> 400,257
///   anchored on tag:Tag {name: …}  scan 16,080 -> ~1 -> a handful
/// ```
///
/// Scanning 16,080 rows to avoid 800,000 is the better trade, and only a model
/// that sums the intermediates can see it.
fn choose_anchor_index(
    path: &PathPattern,
    nodes: &[PathNodeRef],
    path_preds: &[Expression],
    store: &GraphStore,
) -> usize {
    let mut best_idx = 0usize;
    let mut best_cost = f64::MAX;

    // `SAMYAMA_EXPLAIN_ANCHORS=1` prints the cost of every candidate anchor.
    // Anchor choice is the difference between a plan that finishes and one
    // that times out, and `EXPLAIN` shows only the winner — so when the winner
    // looks wrong there is otherwise nothing to inspect but the source.
    let trace = std::env::var("SAMYAMA_EXPLAIN_ANCHORS").is_ok_and(|v| v == "1");

    for i in 0..nodes.len() {
        let cost = estimate_path_cost(path, nodes, i, path_preds, store);
        if trace {
            let (scan, emitted) = anchor_cardinality(&nodes[i], path_preds, store);
            eprintln!(
                "[anchor] {:<12} labels={:?} scan={:.0} emitted={:.0} path_cost={:.0}",
                nodes[i].var, nodes[i].labels.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
                scan, emitted, cost
            );
        }
        // Strict, so node 0 wins ties: re-anchoring has a real cost the model
        // does not capture (a reversed traversal reads the other adjacency,
        // which may be colder), and the written order is the better default
        // when the estimate cannot tell them apart.
        if cost < best_cost {
            best_cost = cost;
            best_idx = i;
        }
    }

    best_idx
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

impl QueryPlanner {
    /// Plan a query held as an ordered clause sequence.
    ///
    /// Walks the clauses in written order, threading one operator through
    /// them, which is what the by-kind representation cannot do: it has one
    /// `Option<CreateClause>` and no way to say "this create happens after
    /// that projection".
    /// Wrap `input` with the node and edge creation a CREATE pattern asks for.
    ///
    /// Mirrors the construction the `MATCH … CREATE` path uses, so the clause
    /// pipeline creates through the same operator rather than a second
    /// implementation. `bound` names the variables already in scope: those are
    /// referenced, not re-created, which is the rule that stops
    /// `MATCH (a) CREATE (a)-[:R]->(b)` making a second `a`.
    fn build_create_on_input(
        &self,
        input: OperatorBox,
        pattern: &crate::query::ast::Pattern,
        bound: &HashSet<String>,
        anon_seq: &mut usize,
    ) -> OperatorBox {
        use crate::query::executor::operator::MatchCreateEdgeOperator;

        let mut nodes_to_create: Vec<(
            String,
            Vec<Label>,
            HashMap<String, PropertyValue>,
            Option<HashMap<String, Expression>>,
        )> = Vec::new();
        let mut edges_to_create: Vec<crate::query::executor::operator::EdgeToCreate> =
            Vec::new();

        let mut handle_for = |node: &crate::query::ast::NodePattern,
                              nodes: &mut Vec<(
            String,
            Vec<Label>,
            HashMap<String, PropertyValue>,
            Option<HashMap<String, Expression>>,
        )>,
                              seq: &mut usize|
         -> String {
            match &node.variable {
                // Already in scope, or already registered by an earlier path in
                // this same CREATE: reference it.
                Some(v) if bound.contains(v) || nodes.iter().any(|(h, ..)| h == v) => v.clone(),
                Some(v) => {
                    nodes.push((
                        v.clone(),
                        node.labels.clone(),
                        node.properties.clone().unwrap_or_default(),
                        node.property_exprs.clone(),
                    ));
                    v.clone()
                }
                None => {
                    let h = format!("__anon_pipe_{seq}");
                    *seq += 1;
                    nodes.push((
                        h.clone(),
                        node.labels.clone(),
                        node.properties.clone().unwrap_or_default(),
                        node.property_exprs.clone(),
                    ));
                    h
                }
            }
        };

        for path in &pattern.paths {
            let mut current = handle_for(&path.start, &mut nodes_to_create, anon_seq);
            for segment in &path.segments {
                let target = handle_for(&segment.node, &mut nodes_to_create, anon_seq);
                let edge_type = segment
                    .edge
                    .types
                    .first()
                    .cloned()
                    .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                let (from, to) = match segment.edge.direction {
                    Direction::Incoming => (target.clone(), current.clone()),
                    Direction::Outgoing | Direction::Both => (current.clone(), target.clone()),
                };
                edges_to_create.push((
                    from,
                    to,
                    edge_type,
                    segment.edge.properties.clone().unwrap_or_default(),
                    segment.edge.variable.clone(),
                    segment.edge.property_exprs.clone(),
                ));
                current = target;
            }
        }

        if edges_to_create.is_empty() && nodes_to_create.is_empty() {
            return input;
        }
        Box::new(MatchCreateEdgeOperator::with_nodes(
            input,
            nodes_to_create,
            edges_to_create,
        ))
    }

    fn plan_clause_pipeline(
        &self,
        query: &Query,
        store: &GraphStore,
    ) -> ExecutionResult<ExecutionPlan> {
        use crate::query::ast::Clause;
        use crate::query::executor::operator::SingleRowOperator;

        let clauses = &query.clauses;

        // The leading run of *reading* clauses is planned by the established
        // path: it is legacy-representable by construction, and rebuilding
        // pattern planning here would be a second implementation of the
        // hardest part of the planner.
        let split = clauses
            .iter()
            .position(|c| !matches!(c, Clause::Match(_) | Clause::Where(_) | Clause::Unwind(_)))
            .unwrap_or(clauses.len());

        let mut operator: OperatorBox = if split == 0 {
            // No reading prefix — a query that opens with `WITH` projects from
            // a single empty row.
            Box::new(SingleRowOperator::new())
        } else {
            let mut prefix = Query::new();
            for clause in &clauses[..split] {
                match clause {
                    Clause::Match(m) => prefix.match_clauses.push(m.clone()),
                    Clause::Where(w) => prefix.where_clause = Some(w.clone()),
                    Clause::Unwind(u) => {
                        if prefix.unwind_clause.is_none() {
                            prefix.unwind_clause = Some(u.clone());
                            prefix.unwind_leading = prefix.match_clauses.is_empty();
                        } else {
                            prefix.extra_unwind_clauses.push(u.clone());
                        }
                    }
                    _ => unreachable!("split stops at the first non-reading clause"),
                }
            }
            self.plan_inner(&prefix, store)?.root
        };

        let mut output_columns: Vec<String> = Vec::new();
        // Variables in scope. A CREATE references those and creates the rest;
        // a WITH replaces the set with what it projects.
        let mut bound: HashSet<String> = HashSet::new();
        for clause in &clauses[..split] {
            match clause {
                Clause::Match(m) => {
                    for path in &m.pattern.paths {
                        if let Some(v) = &path.path_variable { bound.insert(v.clone()); }
                        if let Some(v) = &path.start.variable { bound.insert(v.clone()); }
                        for seg in &path.segments {
                            if let Some(v) = &seg.edge.variable { bound.insert(v.clone()); }
                            if let Some(v) = &seg.node.variable { bound.insert(v.clone()); }
                        }
                    }
                }
                Clause::Unwind(u) => { bound.insert(u.variable.clone()); }
                _ => {}
            }
        }
        let mut anon_seq = 0usize;
        // Set when a RETURN has already placed the sort below its projection.
        let mut order_by_applied = false;

        for clause in &clauses[split..] {
            match clause {
                Clause::Create(cc) => {
                    // Scope as it stands *before* this CREATE decides what to
                    // create. Adding the pattern's own variables first would
                    // make every one of them look already-bound, and the
                    // clause would create nothing.
                    operator =
                        self.build_create_on_input(operator, &cc.pattern, &bound, &mut anon_seq);
                    for path in &cc.pattern.paths {
                        if let Some(v) = &path.start.variable {
                            bound.insert(v.clone());
                        }
                        for seg in &path.segments {
                            if let Some(v) = &seg.edge.variable {
                                bound.insert(v.clone());
                            }
                            if let Some(v) = &seg.node.variable {
                                bound.insert(v.clone());
                            }
                        }
                    }
                }
                Clause::Match(mc) => {
                    // Same join rule as the established multi-MATCH path: every
                    // shared variable is a join key, and only a pattern sharing
                    // nothing with what is already bound becomes a cartesian
                    // product. Taking a subset of the keys would silently widen
                    // the result instead of failing (#360), so the intersection
                    // is taken whole and sorted — it comes from a HashSet, and
                    // an unsorted key order varies between runs.
                    let clause_vars = Self::clause_variables(&mc.pattern);
                    let match_op = self.dispatch_plan_match(mc, None, store)?;
                    let mut shared: Vec<String> = bound.intersection(&clause_vars).cloned().collect();
                    shared.sort();
                    operator = if shared.is_empty() {
                        Box::new(CartesianProductOperator::new(operator, match_op)) as OperatorBox
                    } else if mc.optional {
                        let right_only: Vec<String> =
                            clause_vars.difference(&bound).cloned().collect();
                        Box::new(LeftOuterJoinOperator::new(operator, match_op, shared, right_only))
                            as OperatorBox
                    } else {
                        Box::new(JoinOperator::new(operator, match_op, shared)) as OperatorBox
                    };
                    bound.extend(clause_vars);
                }
                Clause::Merge(mc) => {
                    let on_create: Vec<(String, String, Expression)> = mc
                        .on_create_set
                        .iter()
                        .map(|i| (i.variable.clone(), i.property.clone(), i.value.clone()))
                        .collect();
                    let on_match: Vec<(String, String, Expression)> = mc
                        .on_match_set
                        .iter()
                        .map(|i| (i.variable.clone(), i.property.clone(), i.value.clone()))
                        .collect();
                    let on_create_labels: Vec<(String, Vec<Label>)> = mc
                        .on_create_labels
                        .iter()
                        .map(|l| (l.variable.clone(), l.labels.clone()))
                        .collect();
                    let on_match_labels: Vec<(String, Vec<Label>)> = mc
                        .on_match_labels
                        .iter()
                        .map(|l| (l.variable.clone(), l.labels.clone()))
                        .collect();
                    operator = Box::new(
                        MergeOperator::new(
                            mc.pattern.clone(),
                            on_create,
                            on_match,
                            on_create_labels,
                            on_match_labels,
                        )
                        .with_entity_sets(
                            mc.on_create_entity_set
                                .iter()
                                .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                                .collect(),
                            mc.on_match_entity_set
                                .iter()
                                .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                                .collect(),
                        )
                        .with_input(operator),
                    );
                    // `MERGE p = (...)` binds `p` (#876).
                    let merge_paths = named_path_handles(&mc.pattern);
                    if !merge_paths.is_empty() {
                        for (pv, _, _) in &merge_paths {
                            bound.insert(pv.clone());
                        }
                        operator =
                            Box::new(crate::query::executor::operator::BindPathOperator::new(
                                operator,
                                merge_paths,
                            ));
                    }
                    for path in &mc.pattern.paths {
                        if let Some(v) = &path.start.variable {
                            bound.insert(v.clone());
                        }
                        for seg in &path.segments {
                            if let Some(v) = &seg.edge.variable {
                                bound.insert(v.clone());
                            }
                            if let Some(v) = &seg.node.variable {
                                bound.insert(v.clone());
                            }
                        }
                    }
                }
                Clause::With(wc) => {
                    operator = self.build_with_barrier(operator, wc, store)?;
                    bound = wc
                        .items
                        .iter()
                        .filter_map(|i| i.alias.clone().or_else(|| match &i.expression {
                            Expression::Variable(v) => Some(v.clone()),
                            _ => None,
                        }))
                        .collect();
                    output_columns = wc
                        .items
                        .iter()
                        .map(|i| i.alias.clone().unwrap_or_else(|| match &i.expression {
                            Expression::Variable(v) => v.clone(),
                            Expression::Property { variable, property } => {
                                format!("{variable}.{property}")
                            }
                            _ => String::new(),
                        }))
                        .collect();
                }
                Clause::Unwind(u) => {
                    operator = Box::new(UnwindOperator::new(
                        operator,
                        u.expression.clone(),
                        u.variable.clone(),
                    ));
                    bound.insert(u.variable.clone());
                }
                Clause::Set(sc) => {
                    let items: Vec<(String, String, Expression)> = sc
                        .items
                        .iter()
                        .map(|i| (i.variable.clone(), i.property.clone(), i.value.clone()))
                        .collect();
                    let entity_items: Vec<(String, bool, Expression)> = sc
                        .entity_items
                        .iter()
                        .map(|i| (i.variable.clone(), i.merge, i.value.clone()))
                        .collect();
                    if !items.is_empty() || !entity_items.is_empty() {
                        operator = Box::new(SetPropertyOperator::with_entity_items(
                            operator, items, entity_items,
                        ));
                    }
                    let adds: Vec<(String, Label)> = sc
                        .label_items
                        .iter()
                        .flat_map(|i| i.labels.iter().map(|l| (i.variable.clone(), l.clone())))
                        .collect();
                    if !adds.is_empty() {
                        operator = Box::new(LabelMutationOperator::new(operator, adds, Vec::new()));
                    }
                }
                Clause::Remove(rc) => {
                    let mut props = Vec::new();
                    let mut labels = Vec::new();
                    for item in &rc.items {
                        match item {
                            crate::query::ast::RemoveItem::Property { variable, property } => {
                                props.push((variable.clone(), property.clone()));
                            }
                            crate::query::ast::RemoveItem::Label { variable, label } => {
                                labels.push((variable.clone(), label.clone()));
                            }
                        }
                    }
                    if !props.is_empty() {
                        operator = Box::new(RemovePropertyOperator::new(operator, props));
                    }
                    if !labels.is_empty() {
                        operator = Box::new(LabelMutationOperator::new(operator, Vec::new(), labels));
                    }
                }
                Clause::Delete(dc) => {
                    // Materialised first -- see the by-kind path (#899).
                    operator = Box::new(crate::query::executor::operator::EagerOperator::new(
                        operator, 0, None,
                    ));
                    operator = Box::new(DeleteOperator::new(operator, dc.expressions.clone(), dc.detach));
                    // The same drain in the clause-pipeline shape (#994). A
                    // rule applied to one AST shape and not its twin silently
                    // no-ops for every query written the other way.
                    operator = Box::new(crate::query::executor::operator::EagerOperator::new(
                        operator, 0, None,
                    ));
                }
                Clause::Where(w) => {
                    operator = Box::new(FilterOperator::new(operator, w.predicate.clone()));
                }
                Clause::Return(rc) => {
                    // ORDER BY goes **below** the projection, not after it.
                    //
                    // `WITH p, count(q) AS rng RETURN p ORDER BY rng` sorts on
                    // a column the RETURN does not carry. Sorting above the
                    // projection leaves the key unbound, the sort silently
                    // becomes a no-op, and the rows come back in whatever
                    // order the barrier produced — which is hash order, so the
                    // answer differs between processes. CH-DETERM caught
                    // exactly this scenario after the pipeline landed.
                    if let Some(order_by) = &query.order_by {
                        let order_keys: Vec<(Expression, String)> = rc
                            .items
                            .iter()
                            .map(|i| {
                                let alias = i.alias.clone().unwrap_or_else(|| match &i.expression {
                                    Expression::Variable(v) => v.clone(),
                                    Expression::Property { variable, property } => {
                                        format!("{variable}.{property}")
                                    }
                                    _ => String::new(),
                                });
                                (i.expression.clone(), alias)
                            })
                            .collect();
                        let sort_items: Vec<(Expression, bool)> = order_by
                            .items
                            .iter()
                            .map(|i| {
                                (
                                    resolve_sort_key(
                                        &i.expression,
                                        &order_keys,
                                        SortPosition::BeforeProjection,
                                    ),
                                    i.ascending,
                                )
                            })
                            .collect();
                        operator = Box::new(SortOperator::new(operator, sort_items));
                        order_by_applied = true;
                    }

                    // An aggregate in RETURN needs an Aggregate below the
                    // projection. Without this the pipeline projected
                    // `count(*)` as an ordinary expression, it reached the
                    // scalar evaluator, and the query died with "Unknown
                    // function: count" — for every shape that opens with WITH,
                    // which is what routes a query here in the first place.
                    //
                    // The `Clause::With` arm of this same function already did
                    // this; only RETURN was missed, so the two clauses in one
                    // pipeline disagreed about what an aggregate is.
                    let mut agg_counter = 0usize;
                    let mut aggregates: Vec<AggregateFunction> = Vec::new();
                    let mut group_by: Vec<(Expression, String)> = Vec::new();
                    let mut post_projections: Vec<(Expression, String)> = Vec::new();
                    let mut has_aggregation = false;
                    let mut rewritten_items: Vec<(Expression, Expression, String)> = Vec::new();

                    for (idx, i) in rc.items.iter().enumerate() {
                        let alias = i.column_name(idx);
                        let (rewritten, extracted) =
                            extract_nested_aggregates(&i.expression, &mut agg_counter);
                        if !extracted.is_empty() {
                            has_aggregation = true;
                        }
                        rewritten_items.push((i.expression.clone(), rewritten, alias));
                        aggregates.extend(extracted);
                    }

                    let projections: Vec<(Expression, String)> = if has_aggregation {
                        for (original, rewritten, alias) in &rewritten_items {
                            // An item with no aggregate inside it is a grouping
                            // key; one with an aggregate projects from the
                            // aggregate's alias after the Aggregate runs.
                            if rewritten == original {
                                group_by.push((original.clone(), alias.clone()));
                                post_projections
                                    .push((Expression::Variable(alias.clone()), alias.clone()));
                            } else {
                                post_projections.push((rewritten.clone(), alias.clone()));
                            }
                        }
                        operator = Box::new(AggregateOperator::new(
                            operator,
                            group_by,
                            std::mem::take(&mut aggregates),
                        ));
                        post_projections
                    } else {
                        rewritten_items
                            .into_iter()
                            .map(|(original, _, alias)| (original, alias))
                            .collect()
                    };
                    output_columns = projections.iter().map(|(_, a)| a.clone()).collect();
                    operator = Box::new(ProjectOperator::new(operator, projections));
                    if rc.distinct {
                        operator = Box::new(DistinctOperator::new(operator));
                    }
                }
                // Not yet threaded through the pipeline. Refusing is the point:
                // planning these as though the clause order were different is
                // how a parse error becomes a wrong answer.
                unsupported => {
                    let shape: Vec<&str> = clauses.iter().map(|c| c.kind()).collect();
                    return Err(ExecutionError::RuntimeError(format!(
                        "`{}` is not yet supported in this clause position (query shape: {}). \
                         The parser accepts this order; the planner threads MATCH, WHERE, \
                         UNWIND, WITH, CREATE, MERGE, SET, REMOVE, DELETE and RETURN through it \
                         so far, and FOREACH and CALL are still to come (samyama-graph#617).",
                        unsupported.kind(),
                        shape.join(" ")
                    )));
                }
            }
        }

        if let Some(order_by) = &query.order_by {
            if !order_by_applied {
                let sort_items: Vec<(Expression, bool)> = order_by
                    .items
                    .iter()
                    .map(|i| (i.expression.clone(), i.ascending))
                    .collect();
                operator = Box::new(SortOperator::new(operator, sort_items));
            }
        }
        if let Some(skip) = query.skip {
            operator = Box::new(SkipOperator::new(operator, skip));
        }
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        let is_write = clauses.iter().any(|c| c.is_write());
        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write,
            candidates_evaluated: 1,
            chosen_plan_cost: 0.0,
            candidate_costs: Vec::new(),
        })
    }

    /// Build a WithBarrier operator from a WithClause (extracted for multi-WITH reuse)
    fn build_with_barrier(&self, input: OperatorBox, with_clause: &WithClause, _store: &GraphStore) -> ExecutionResult<OperatorBox> {
        let mut items = Vec::new();
        let mut aggregates = Vec::new();
        let mut group_by = Vec::new();
        let mut has_aggregation = false;
        let mut agg_counter = 0usize;

        struct WithItemInfo {
            alias: String,
            original_expr: Expression,
            rewritten_expr: Expression,
            extracted_aggs: Vec<AggregateFunction>,
        }
        let mut item_infos = Vec::new();

        for (idx, item) in with_clause.items.iter().enumerate() {
            let alias = item.column_name(idx);

            let (rewritten, extracted) = extract_nested_aggregates(&item.expression, &mut agg_counter);
            if !extracted.is_empty() {
                has_aggregation = true;
            }
            item_infos.push(WithItemInfo {
                alias,
                original_expr: item.expression.clone(),
                rewritten_expr: rewritten,
                extracted_aggs: extracted,
            });
        }

        // Captured before `item_infos` is consumed: ORDER BY may restate any
        // projected expression instead of naming its alias.
        let projections: Vec<(Expression, String)> = item_infos
            .iter()
            .map(|i| (i.original_expr.clone(), i.alias.clone()))
            .collect();

        for info in item_infos {
            if has_aggregation {
                if !info.extracted_aggs.is_empty() {
                    aggregates.extend(info.extracted_aggs);
                    items.push((info.rewritten_expr, info.alias.clone()));
                } else {
                    group_by.push((info.original_expr, info.alias.clone()));
                    items.push((Expression::Variable(info.alias.clone()), info.alias.clone()));
                }
            } else {
                items.push((info.original_expr, info.alias.clone()));
            }
        }

        let sort_items: Vec<(Expression, bool)> = with_clause
            .order_by
            .as_ref()
            .map(|ob| {
                ob.items
                    .iter()
                    .map(|i| (rewrite_sort_key(&i.expression, &projections), i.ascending))
                    .collect()
            })
            .unwrap_or_default();

        // A `WITH ... WHERE ...` may filter on variables the projection does not
        // carry forward:
        //
        //     WITH types[i] AS lhs, types[j] AS rhs
        //     WHERE i <> j
        //
        // Applied inside the barrier, after the projection has dropped them,
        // `i` and `j` evaluate to null for every row and the query returns
        // **zero rows instead of ninety** -- silently, since a filter that
        // matches nothing is a legitimate outcome. Neo4j answers this scenario,
        // so those names are still reachable there (#840).
        //
        // Split by conjunct rather than moving the clause wholesale: a
        // predicate naming a projected alias still belongs after the barrier,
        // and `WITH n.name AS name WHERE name STARTS WITH 'a'` must keep
        // working. A conjunct moves ahead only when it names at least one
        // variable and none of them is a projected alias -- so an aggregate's
        // output can never be filtered before it is computed.
        //
        // This sits in `build_with_barrier` because **both** planner paths call
        // it. The by-kind stage loop and the clause pipeline each construct
        // their own WITH stages, and putting the split in one of them would
        // have fixed the queries that happen to take that path (#797).
        let aliases: HashSet<String> = with_clause
            .items
            .iter()
            .enumerate()
            .map(|(idx, item)| item.column_name(idx))
            .collect();
        let mut input = input;
        let mut where_predicate = with_clause.where_clause.as_ref().map(|wc| wc.predicate.clone());
        if let Some(pred) = where_predicate.take() {
            let (mut pre, mut post): (Vec<Expression>, Vec<Expression>) = (Vec::new(), Vec::new());
            for conjunct in flatten_and_predicates(&pred) {
                let mut vars = HashSet::new();
                Self::collect_expression_variables(&conjunct, &mut vars);
                let before = !vars.is_empty() && !vars.iter().any(|v| aliases.contains(v));
                if before { pre.push(conjunct) } else { post.push(conjunct) }
            }
            let join = |v: Vec<Expression>| {
                v.into_iter().reduce(|acc, p| Expression::Binary {
                    left: Box::new(acc),
                    op: BinaryOp::And,
                    right: Box::new(p),
                })
            };
            if let Some(expr) = join(pre) {
                input = Box::new(FilterOperator::new(input, expr));
            }
            where_predicate = join(post);
        }

        Ok(Box::new(WithBarrierOperator::new(
            input,
            items,
            aggregates,
            group_by,
            has_aggregation,
            with_clause.distinct,
            where_predicate,
            sort_items,
            with_clause.skip,
            with_clause.limit,
        )))
    }

    /// Extract variable names from a MATCH clause
    fn extract_match_vars(&self, mc: &MatchClause) -> HashSet<String> {
        Self::clause_variables(&mc.pattern)
    }

    /// Every variable a MATCH clause binds, **including the named path**.
    ///
    /// Leaving the path variable out is what made
    /// `OPTIONAL MATCH p = (a)-[:X]->(b) RETURN p` fail with "Variable not
    /// found: p" when nothing matched. The left outer join fills its
    /// right-hand-only variables with null, and that list is this set minus
    /// what was already bound -- so a variable missing here is a variable the
    /// join never nulls, and an unmatched OPTIONAL MATCH then looks like a
    /// query referring to something that does not exist. `b` was nulled
    /// correctly the whole time; only `p` was invisible.
    fn clause_variables(pattern: &crate::query::ast::Pattern) -> HashSet<String> {
        let mut vars = HashSet::new();
        for path in &pattern.paths {
            if let Some(v) = &path.path_variable { vars.insert(v.clone()); }
            if let Some(v) = &path.start.variable { vars.insert(v.clone()); }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable { vars.insert(v.clone()); }
                if let Some(v) = &seg.edge.variable { vars.insert(v.clone()); }
            }
        }
        vars
    }

    /// Plan a single path where the start variable is already bound from upstream (e.g., WITH output).
    /// Instead of creating a NodeScanOperator, chains ExpandOperators directly onto `upstream`.
    /// Returns the operator and the set of variables introduced by this path.
    fn plan_path_with_bound_start(
        &self,
        path: &PathPattern,
        start_var: &str,
        predicates: Vec<Expression>,
        upstream: OperatorBox,
        anon_counter: &mut usize,
    ) -> ExecutionResult<(OperatorBox, HashSet<String>)> {
        let mut path_operator = upstream;
        let mut current_var = start_var.to_string();
        let mut vars = HashSet::new();
        vars.insert(start_var.to_string());

        // Split predicates: early (only start_var) vs deferred (references expand targets)
        let mut deferred_predicates = Vec::new();
        for pred in predicates {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(&pred, &mut pred_vars);
            // Start-only predicates are redundant (already filtered before WITH),
            // but predicates on expanded vars must be deferred.
            if !pred_vars.is_empty() && !pred_vars.iter().all(|v| v == start_var) {
                deferred_predicates.push(pred);
            }
        }

        // Chain ExpandOperators for each segment
        for segment in &path.segments {
            let target_var = segment.node.variable.clone().unwrap_or_else(|| {
                let name = format!("_anon_{}", *anon_counter);
                *anon_counter += 1;
                name
            });

            let edge_var = segment.edge.variable.clone();
            let edge_types: Vec<String> = segment
                .edge
                .types
                .iter()
                .map(|t| t.as_str().to_string())
                .collect();

            let mut expand = ExpandOperator::new(
                path_operator,
                current_var.clone(),
                target_var.clone(),
                edge_var.clone(),
                edge_types,
                segment.edge.direction.clone(),
            );

            if let Some(ref pv) = path.path_variable {
                expand = expand.with_path_variable(pv.clone());
            }

            path_operator = if !segment.node.labels.is_empty() {
                Box::new(expand.with_target_labels(segment.node.labels.clone()))
            } else {
                Box::new(expand)
            };

            // Add property filter for target node inline properties
            if let Some(ref props) = segment.node.properties {
                if !props.is_empty() {
                    let filter_expr = self.build_property_filter(&target_var, props);
                    path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
                }
            }

            vars.insert(target_var.clone());
            if let Some(ref ev) = edge_var {
                vars.insert(ev.clone());
            }
            // Apply any deferred conjunct whose variables are now all bound,
            // rather than holding it until the whole path is expanded (#328).
            //
            // This is the builder LDBC IC3, IC6 and IC9 actually use -- their
            // second MATCH starts from a variable bound by a preceding WITH,
            // so neither of the other two path builders runs. IC3 carried
            // 409,960 rows through `(m)-[:IS_LOCATED_IN]->(place)` before
            // applying a date filter on `m` that only 622 rows survive, and
            // `m` is bound by the expand before it.
            path_operator =
                Self::apply_ready_predicates(path_operator, &mut deferred_predicates, &vars);
            current_var = target_var;
        }

        // Apply deferred WHERE predicates
        if !deferred_predicates.is_empty() {
            let filter_expr = deferred_predicates
                .into_iter()
                .reduce(|acc, pred| Expression::Binary {
                    left: Box::new(acc),
                    op: BinaryOp::And,
                    right: Box::new(pred),
                })
                .unwrap();
            path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
        }

        Ok((path_operator, vars))
    }

    /// Check if all paths in a match clause have bound start variables and no special path types.
    /// Returns true if push-down can be applied to the entire clause.
    /// Plan a MATCH whose paths all start from a variable the pipeline already
    /// bound, chaining expands onto `upstream` instead of planning the clause
    /// standalone and joining.
    ///
    /// The caller must have checked `can_pushdown_match`. Returns the operator
    /// and the variables the clause introduced.
    ///
    /// This was inline in the post-`WITH` loop and nowhere else, so a query
    /// whose clauses all precede the first `WITH` — which is most queries —
    /// planned every clause independently and joined them. `MATCH (m:Post
    /// {id: $id})-[:HAS_CREATOR]->(op) MATCH (op)-[:KNOWS]-(f) RETURN count(f)`
    /// therefore scanned all 10,620 `:Person` nodes and hash-joined back to the
    /// one node the first clause had already resolved: 99 ms to count one
    /// person's ~23 friends (#711).
    fn plan_pushed_down_match(
        &self,
        match_clause: &MatchClause,
        where_clause: Option<&WhereClause>,
        clause_vars: &HashSet<String>,
        upstream: OperatorBox,
        anon_counter: &mut usize,
    ) -> ExecutionResult<(OperatorBox, HashSet<String>)> {
        let preds = where_clause
            .map(|wc| flatten_and_predicates(&wc.predicate))
            .unwrap_or_default();

        let mut current_op = upstream;
        let mut new_vars = HashSet::new();

        for path in &match_clause.pattern.paths {
            let start_var = path
                .start
                .variable
                .as_ref()
                .expect("can_pushdown_match requires a bound start variable");
            let path_var_set: HashSet<String> = {
                let mut vs = HashSet::new();
                vs.insert(start_var.clone());
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable {
                        vs.insert(v.clone());
                    }
                    if let Some(v) = &seg.edge.variable {
                        vs.insert(v.clone());
                    }
                }
                vs
            };
            let path_preds: Vec<Expression> = preds
                .iter()
                .filter(|p| {
                    let mut pvars = HashSet::new();
                    Self::collect_expression_variables(p, &mut pvars);
                    pvars.is_empty() || pvars.iter().all(|v| path_var_set.contains(v))
                })
                .cloned()
                .collect();

            let (expanded_op, path_vars) = self.plan_path_with_bound_start(
                path,
                start_var,
                path_preds,
                current_op,
                anon_counter,
            )?;
            current_op = expanded_op;
            new_vars.extend(path_vars);
        }

        // Predicates the per-path pass did not take.
        let remaining_preds: Vec<Expression> = preds
            .into_iter()
            .filter(|p| {
                let mut pvars = HashSet::new();
                Self::collect_expression_variables(p, &mut pvars);
                !pvars.is_empty()
                    && !clause_vars.iter().any(|_| pvars.iter().all(|v| new_vars.contains(v)))
            })
            .collect();
        if !remaining_preds.is_empty() {
            let filter_expr = remaining_preds
                .into_iter()
                .reduce(|acc, pred| Expression::Binary {
                    left: Box::new(acc),
                    op: BinaryOp::And,
                    right: Box::new(pred),
                })
                .unwrap();
            current_op = Box::new(FilterOperator::new(current_op, filter_expr));
        }

        Ok((current_op, new_vars))
    }

    /// Build the optional expand for a clause `optional_pushdown_vars` accepted.
    ///
    /// One segment, so there is exactly one expand and no partial-match
    /// ambiguity. Everything that decides whether the pattern matches has to
    /// live *inside* the expand: a `FilterOperator` above it would delete the
    /// null row the clause owes a source that matched nothing, turning
    /// `OPTIONAL MATCH` back into `MATCH`. Inline properties therefore go
    /// through `with_target_props`, which filters during the walk, and the
    /// caller declines the pushdown when the clause carries a `WHERE`.
    fn plan_optional_expand(
        &self,
        path: &PathPattern,
        start_var: &str,
        null_vars: Vec<String>,
        known_vars: &HashSet<String>,
        upstream: OperatorBox,
        store: &GraphStore,
    ) -> OperatorBox {
        let segment = &path.segments[0];
        let target_var = segment
            .node
            .variable
            .clone()
            .expect("optional_pushdown_vars requires a named far end");
        let edge_types: Vec<String> =
            segment.edge.types.iter().map(|t| t.as_str().to_string()).collect();

        let mut expand = ExpandOperator::new(
            upstream,
            start_var.to_string(),
            target_var.clone(),
            segment.edge.variable.clone(),
            edge_types,
            segment.edge.direction.clone(),
        );
        // A far end that is already bound is a closing hop: it can only land on
        // that node, and the pin says so during the walk rather than after it.
        // This is the `OPTIONAL MATCH (op)-[k:KNOWS]-(author)` case.
        if known_vars.contains(&target_var) {
            expand = expand.with_target_bound_var(target_var.clone());
        }
        if !segment.node.labels.is_empty() {
            expand = expand.with_target_labels(segment.node.labels.clone());
        }
        if let Some(props) = &segment.node.properties {
            if !props.is_empty() {
                let mut pushed: Vec<(String, PropertyValue)> =
                    props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                pushed.sort_by(|a, b| a.0.cmp(&b.0));
                // Same resolution the non-optional pushdown does: a hash
                // lookup per candidate edge instead of a node fetch and a
                // property compare (#665). This path was written after that
                // one and did not inherit it.
                if let Some(ids) =
                    self.resolve_target_ids(&segment.node.labels, &pushed, store)
                {
                    expand = expand.with_target_ids(ids);
                }
                expand = expand.with_target_props(pushed);
            }
        }
        Box::new(expand.with_optional(null_vars))
    }

    /// Whether an `OPTIONAL MATCH` can be pushed onto the pipeline as an
    /// optional expand instead of planned standalone and left-outer-joined.
    ///
    /// Deliberately narrower than `can_pushdown_match`. An optional clause has
    /// to emit a null-filled row when it matches nothing, and only a
    /// **single-segment** path makes that unambiguous: with two segments a
    /// source row that matches the first hop and not the second still owes one
    /// null row, not one per partial match, and the chain cannot tell the
    /// difference. That case keeps the join, which already handles it.
    ///
    /// The shape this does cover is the common one — `OPTIONAL MATCH
    /// (a)-[r:T]->(b)` hanging off something already bound — and it is the one
    /// that costs 422 ms where the equivalent `EXISTS` costs 0.02 (#726).
    ///
    /// Returns the variables the clause introduces, which are the ones the
    /// operator nulls when nothing matches. `author` in
    /// `OPTIONAL MATCH (op)-[k:KNOWS]-(author)` with both ends bound is *not*
    /// among them: nulling it would erase a binding the row already had.
    fn optional_pushdown_vars(
        match_clause: &MatchClause,
        known_vars: &HashSet<String>,
    ) -> Option<Vec<String>> {
        if !match_clause.optional {
            return None;
        }
        let [path] = &match_clause.pattern.paths[..] else {
            return None;
        };
        if !matches!(path.path_type, PathType::Normal) || path.path_variable.is_some() {
            return None;
        }
        let [seg] = &path.segments[..] else {
            return None;
        };
        if seg.edge.length.is_some() {
            return None;
        }
        let start = path.start.variable.as_ref()?;
        if !known_vars.contains(start) {
            return None;
        }
        let mut introduced = Vec::new();
        // The far node may be bound -- that is the pinned case, and the expand
        // already knows how to close onto it -- or new, in which case it is
        // nulled on a miss.
        if let Some(v) = &seg.node.variable {
            if !known_vars.contains(v) {
                introduced.push(v.clone());
            }
        } else {
            // An anonymous far end cannot be observed, so there is nothing to
            // null and nothing to bind; the join is no worse and is simpler.
            return None;
        }
        // A bound edge variable would have to be matched rather than rebound,
        // which this path does not do.
        match &seg.edge.variable {
            Some(v) if known_vars.contains(v) => return None,
            Some(v) => introduced.push(v.clone()),
            None => {}
        }
        if introduced.is_empty() {
            return None;
        }
        Some(introduced)
    }

    fn can_pushdown_match(match_clause: &MatchClause, known_vars: &HashSet<String>) -> bool {
        if match_clause.optional {
            return false;
        }
        // Paths in one clause may not share a variable this clause introduces.
        //
        // The pushdown chains the paths, so a second path binding a variable
        // the first already bound *rebinds* it rather than matching it, and
        // the correlation between them is lost. TCK `Match3` [20]:
        //
        //   MATCH (a {name:'A'}), (b {name:'B'}), (c {name:'C'})
        //   MATCH (a)-->(x), (b)-->(x), (c)-->(x) RETURN x
        //
        // wants the two nodes all three point at; chained, it answers with
        // whatever the last path bound. A join across the paths gets this
        // right, so a clause of that shape declines the pushdown.
        let mut introduced: HashSet<&String> = HashSet::new();
        for path in &match_clause.pattern.paths {
            let mut this_path: Vec<&String> = Vec::new();
            if let Some(v) = &path.start.variable {
                this_path.push(v);
            }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable {
                    this_path.push(v);
                }
                if let Some(v) = &seg.edge.variable {
                    this_path.push(v);
                }
            }
            for v in this_path {
                if known_vars.contains(v) {
                    continue;
                }
                if !introduced.insert(v) {
                    return false;
                }
            }
        }

        for path in &match_clause.pattern.paths {
            // Must have a start variable that's bound
            let start_var = match &path.start.variable {
                Some(v) => v,
                None => return false,
            };
            if !known_vars.contains(start_var) {
                return false;
            }
            // ...and nothing *after* the start may be bound already. The
            // pushdown chains expands, which bind their target; a target that
            // is already bound has to be *matched* against, and chaining
            // rebinds it instead, losing the correlation the query asks for:
            //
            //   MATCH (b)<-[:ON]-(d1)-[:OF]->(v1)-[:VARIANT_OF]->(m) ...
            //   MATCH (b)<-[:ON]-(d2)-[:OF]->(v2)-[:VARIANT_OF]->(m) ...
            //
            // pairs each model's fp32 variant with its *own* int8 variant --
            // two rows. Chained, the second path rebinds `m` and answers four.
            // A join on every shared variable gets this right (#360), so a
            // clause that closes back onto a bound variable declines.
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable {
                    if known_vars.contains(v) {
                        return false;
                    }
                }
                if let Some(v) = &seg.edge.variable {
                    if known_vars.contains(v) {
                        return false;
                    }
                }
            }
            // Skip shortestPath and variable-length patterns
            if !matches!(path.path_type, PathType::Normal) {
                return false;
            }
            for seg in &path.segments {
                if seg.edge.length.is_some() {
                    return false;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse_query;

    /// `resolve_target_ids` must answer from a label scan when no index exists.
    ///
    /// LDBC IC11 pushes `org.name = "..."` into the expand over `:WORK_AT`.
    /// With the equality resolved to ids the per-candidate test is a hash
    /// lookup; without it, `target_props` fetches the node and compares a
    /// property for each of ~29,000 candidate edges, which is 74% of the query
    /// at SF10 (#665). Nothing asserted that the resolution actually happens.
    #[test]
    fn a_target_equality_resolves_to_ids_by_scanning_a_small_label() {
        use crate::graph::{Label, PropertyValue};

        let mut store = GraphStore::new();
        let mut wanted = Vec::new();
        for i in 0..20i64 {
            let id = store.create_node(Label::new("Organisation"));
            let name = if i % 5 == 0 { "Acme" } else { "Other" };
            store
                .set_node_property("default", id, "name", PropertyValue::String(name.into()))
                .unwrap();
            if name == "Acme" {
                wanted.push(id);
            }
        }

        let planner = QueryPlanner::new();
        let ids = planner
            .resolve_target_ids(
                &[Label::new("Organisation")],
                &[("name".to_string(), PropertyValue::String("Acme".into()))],
                &store,
            )
            .expect("a 20-node label is far under the scan cap");

        let mut got: Vec<_> = ids.into_iter().collect();
        got.sort();
        wanted.sort();
        assert_eq!(got, wanted);
    }

    /// A property index belongs to one label, so only the target's own index
    /// may answer for it.
    ///
    /// `resolve_target_ids` used to walk every label in the catalog and take
    /// the first index that held the value. With an index on `Person.name` and
    /// none on `Organisation.name`, an `:Organisation {name: "Acme"}` target
    /// resolved to the *Person* whose name is "Acme" — a disjoint set, not a
    /// wider one, so the expand dropped every correct row. Silent: the plan
    /// looked identical and the query returned nothing.
    #[test]
    fn a_target_equality_ignores_another_label_index_on_the_same_property() {
        use crate::graph::{Label, PropertyValue};

        let mut store = GraphStore::new();
        store.property_index.create_index(Label::new("Person"), "name".to_string());

        let person = store.create_node(Label::new("Person"));
        store
            .set_node_property("default", person, "name", PropertyValue::String("Acme".into()))
            .unwrap();

        let org = store.create_node(Label::new("Organisation"));
        store
            .set_node_property("default", org, "name", PropertyValue::String("Acme".into()))
            .unwrap();

        let planner = QueryPlanner::new();
        let ids = planner
            .resolve_target_ids(
                &[Label::new("Organisation")],
                &[("name".to_string(), PropertyValue::String("Acme".into()))],
                &store,
            )
            .expect("the Organisation label is far under the scan cap");

        assert_eq!(ids, std::iter::once(org).collect::<std::collections::HashSet<_>>());
        assert!(!ids.contains(&person), "a Person index must not answer for an Organisation target");
    }

    /// An unlabelled target declines rather than guessing a label.
    ///
    /// Any label may carry the property, so no single label's index or scan
    /// answers the question. Declining leaves `target_props` to do it.
    #[test]
    fn a_target_equality_without_a_label_declines() {
        use crate::graph::{Label, PropertyValue};

        let mut store = GraphStore::new();
        store.property_index.create_index(Label::new("Person"), "name".to_string());
        let person = store.create_node(Label::new("Person"));
        store
            .set_node_property("default", person, "name", PropertyValue::String("Acme".into()))
            .unwrap();

        let planner = QueryPlanner::new();
        let ids = planner.resolve_target_ids(
            &[],
            &[("name".to_string(), PropertyValue::String("Acme".into()))],
            &store,
        );
        assert_eq!(ids, None);
    }

    /// A label bigger than the cap declines rather than scanning: above it the
    /// per-candidate check is genuinely cheaper than one whole-label pass.
    #[test]
    fn a_target_equality_declines_a_label_over_the_scan_cap() {
        use crate::graph::{Label, PropertyValue};

        let store = GraphStore::new();
        let planner = QueryPlanner::new();
        // No such label, no index: the index loop finds nothing and the scan
        // finds an empty label, which is a resolvable answer (the empty set) —
        // not a decline. The decline is the *cap*, exercised in the engine at
        // scale; asserted here only as "an unknown label is not a wildcard".
        let ids = planner.resolve_target_ids(
            &[Label::new("NoSuchLabel")],
            &[("name".to_string(), PropertyValue::String("Acme".into()))],
            &store,
        );
        assert_eq!(ids, Some(std::collections::HashSet::new()));
    }

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
            post_with_unwind_clauses: vec![],
            create_clause: None,
            order_by: None,
            limit: None,
            extra_unwind_clauses: Vec::new(),
            clauses: Vec::new(),
            needs_clause_pipeline: false,
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
            create_hierarchy_index_clause: None,
            drop_hierarchy_index: None,
            rebuild_hierarchy_index: None,
            show_hierarchy_indexes: false,
            show_indexes: false,
            show_constraints: false,
            profile: false,
            params: std::collections::HashMap::new(),
            foreach_clause: None,
            unwind_clause: None,
            unwind_leading: false,
            star_expanded_to_nothing: false,
            merge_clause: None,
            union_queries: vec![],
            explain: false,
            with_split_index: None,
            post_with_where_clause: None,
            extra_with_stages: vec![],
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
    fn test_inline_properties_trigger_index_scan() {
        // Inline properties like {name: 'Alice'} should use IndexScan when an index exists,
        // not fall back to NodeScan + Filter (O(n)).
        let mut store = GraphStore::new();
        for i in 0..100 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "name",
                crate::graph::PropertyValue::String(format!("Person{}", i))).unwrap();
        }
        store.property_index.create_index(crate::graph::Label::new("Person"), "name".to_string());

        // Both forms should produce the same plan with IndexScan
        use crate::query::executor::record::Value;
        use crate::graph::PropertyValue;

        // WHERE form (already works)
        let q_where = parse_query("EXPLAIN MATCH (n:Person) WHERE n.name = 'Person50' RETURN n").unwrap();
        let executor_where = crate::query::executor::QueryExecutor::new(&store);
        let r_where = executor_where.execute(&q_where).unwrap();
        let plan_where = if let Some(Value::Property(PropertyValue::String(s))) = r_where.records[0].get("plan") {
            s.clone()
        } else { panic!("Expected plan text"); };

        // Inline form (was broken, should now use IndexScan)
        let q_inline = parse_query("EXPLAIN MATCH (n:Person {name: 'Person50'}) RETURN n").unwrap();
        let executor_inline = crate::query::executor::QueryExecutor::new(&store);
        let r_inline = executor_inline.execute(&q_inline).unwrap();
        let plan_inline = if let Some(Value::Property(PropertyValue::String(s))) = r_inline.records[0].get("plan") {
            s.clone()
        } else { panic!("Expected plan text"); };

        assert!(plan_where.contains("IndexScan"),
            "WHERE form should use IndexScan: {}", plan_where);
        assert!(plan_inline.contains("IndexScan"),
            "Inline properties should use IndexScan: {}", plan_inline);
        assert!(!plan_inline.contains("NodeScan"),
            "Inline properties should NOT use NodeScan when index exists: {}", plan_inline);
    }

    #[test]
    fn test_reversed_operand_eq_triggers_index_scan() {
        // `'Person50' = n.name` is semantically identical to `n.name = 'Person50'` and
        // should also use the index — operand order must not affect index selection.
        let mut store = GraphStore::new();
        for i in 0..100 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "name",
                crate::graph::PropertyValue::String(format!("Person{}", i))).unwrap();
        }
        store.property_index.create_index(crate::graph::Label::new("Person"), "name".to_string());

        use crate::query::executor::record::Value;
        use crate::graph::PropertyValue;

        let query = parse_query("EXPLAIN MATCH (n:Person) WHERE 'Person50' = n.name RETURN n").unwrap();
        let executor = crate::query::executor::QueryExecutor::new(&store);
        let result = executor.execute(&query).unwrap();
        let plan_text = if let Some(Value::Property(PropertyValue::String(s))) = result.records[0].get("plan") {
            s.clone()
        } else { panic!("Expected plan text"); };

        assert!(plan_text.contains("IndexScan"),
            "Reversed-operand equality should use IndexScan: {}", plan_text);
        assert!(!plan_text.contains("NodeScan"),
            "Reversed-operand equality should not fall back to NodeScan: {}", plan_text);
    }

    #[test]
    fn test_reversed_operand_comparison_triggers_index_scan() {
        // `25 < n.age` is equivalent to `n.age > 25` — the comparison operator must be
        // flipped (not just the operands) when normalizing to the indexed form.
        let mut store = GraphStore::new();
        store.property_index.create_index(crate::graph::Label::new("Person"), "age".to_string());
        for i in 0..50 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "age", crate::graph::PropertyValue::Integer(i as i64)).unwrap();
        }

        use crate::query::executor::record::Value;
        use crate::graph::PropertyValue;

        let query = parse_query("EXPLAIN MATCH (n:Person) WHERE 25 < n.age RETURN n").unwrap();
        let executor = crate::query::executor::QueryExecutor::new(&store);
        let result = executor.execute(&query).unwrap();
        let plan_text = if let Some(Value::Property(PropertyValue::String(s))) = result.records[0].get("plan") {
            s.clone()
        } else { panic!("Expected plan text"); };

        assert!(plan_text.contains("IndexScan"),
            "Reversed-operand comparison should use IndexScan: {}", plan_text);
        assert!(!plan_text.contains("NodeScan"),
            "Reversed-operand comparison should not fall back to NodeScan: {}", plan_text);

        // Correctness: flipping `25 < n.age` to `n.age > 25` must preserve results —
        // exercise actual execution, not just the plan shape.
        let exec_query = parse_query("MATCH (n:Person) WHERE 25 < n.age RETURN n.age AS age").unwrap();
        let exec = crate::query::executor::QueryExecutor::new(&store);
        let rows = exec.execute(&exec_query).unwrap();
        assert_eq!(rows.records.len(), 24, "Expected ages 26..49 to match 25 < n.age");
    }

    #[test]
    fn test_anchor_selection_uses_index_on_non_start_node() {
        // MATCH (a:Company)-[:WORKS_AT]->(b:Person) WHERE b.name = '...' — the predicate
        // lands on the *second* pattern node. The planner should anchor the scan at `b`
        // via its index and traverse the relationship in reverse, rather than always
        // full-scanning `a`'s (much larger) label first.
        let mut store = GraphStore::new();
        store.property_index.create_index(crate::graph::Label::new("Person"), "name".to_string());
        let mut company_ids = Vec::new();
        for i in 0..500 {
            let id = store.create_node("Company");
            store.set_node_property("default", id, "name",
                crate::graph::PropertyValue::String(format!("Company{}", i))).unwrap();
            company_ids.push(id);
        }
        for i in 0..500 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "name",
                crate::graph::PropertyValue::String(format!("Person{}", i))).unwrap();
            store.create_edge(company_ids[i], id, "WORKS_AT").unwrap();
        }

        use crate::query::executor::record::Value;
        use crate::graph::PropertyValue;

        let query = parse_query(
            "EXPLAIN MATCH (a:Company)-[:WORKS_AT]->(b:Person) WHERE b.name = 'Person250' RETURN a, b"
        ).unwrap();
        let executor = crate::query::executor::QueryExecutor::new(&store);
        let result = executor.execute(&query).unwrap();
        let plan_text = if let Some(Value::Property(PropertyValue::String(s))) = result.records[0].get("plan") {
            s.clone()
        } else { panic!("Expected plan text"); };

        assert!(plan_text.contains("IndexScan"),
            "Predicate on non-start node should still use IndexScan: {}", plan_text);

        // Correctness: executing the query must still return the single matching row,
        // with the relationship traversed to the correct company.
        let exec_query = parse_query(
            "MATCH (a:Company)-[:WORKS_AT]->(b:Person) WHERE b.name = 'Person250' RETURN a.name AS company, b.name AS person"
        ).unwrap();
        let exec = crate::query::executor::QueryExecutor::new(&store);
        let rows = exec.execute(&exec_query).unwrap();
        assert_eq!(rows.records.len(), 1);
        assert_eq!(rows.records[0].get("company"),
            Some(&Value::Property(PropertyValue::String("Company250".to_string()))));
        assert_eq!(rows.records[0].get("person"),
            Some(&Value::Property(PropertyValue::String("Person250".to_string()))));
    }

    #[test]
    fn test_anchor_selection_reversed_direction_pattern() {
        // Same as above but with the arrow written backward: (b:Person)<-[:WORKS_AT]-(a:Company).
        // The reversed anchor traversal must flip the already-reversed direction correctly
        // (Incoming written direction -> Outgoing effective direction from the anchor).
        let mut store = GraphStore::new();
        store.property_index.create_index(crate::graph::Label::new("Person"), "name".to_string());
        let mut company_ids = Vec::new();
        for i in 0..500 {
            let id = store.create_node("Company");
            company_ids.push(id);
        }
        for i in 0..500 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "name",
                crate::graph::PropertyValue::String(format!("Person{}", i))).unwrap();
            store.create_edge(company_ids[i], id, "WORKS_AT").unwrap();
        }

        let exec_query = parse_query(
            "MATCH (b:Person)<-[:WORKS_AT]-(a:Company) WHERE b.name = 'Person250' RETURN a, b"
        ).unwrap();
        let exec = crate::query::executor::QueryExecutor::new(&store);
        let rows = exec.execute(&exec_query).unwrap();
        assert_eq!(rows.records.len(), 1, "Expected exactly one company employing Person250");
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

    #[test]
    fn test_where_not_duplicated_after_with_barrier() {
        // Regression test: WHERE predicates referencing variables that are projected
        // away by WITH should not cause "Variable not found" errors. The WHERE must
        // only be applied before the WithBarrier, not after it.
        let mut store = GraphStore::new();
        let n1 = store.create_node("Team");
        store.get_node_mut(n1).unwrap().set_property("name", PropertyValue::String("India".into()));
        let n2 = store.create_node("Match");
        let n3 = store.create_node("Tournament");
        store.get_node_mut(n3).unwrap().set_property("name", PropertyValue::String("IPL".into()));
        store.create_edge(n1, n2, "COMPETED_IN").unwrap();
        store.create_edge(n2, n3, "PART_OF").unwrap();

        let query = parse_query(
            "MATCH (t:Team)-[:COMPETED_IN]->(m:Match)-[:PART_OF]->(trn:Tournament) \
             WHERE trn.name = 'IPL' \
             WITH t, count(m) AS played \
             RETURN t.name AS team, played"
        ).unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(result.is_ok(), "WHERE + WITH should not fail: {:?}", result.err());
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
    }

    #[test]
    fn test_node_identity_comparison() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Team");
        store.get_node_mut(n1).unwrap().set_property("name", PropertyValue::String("India".into()));
        let n2 = store.create_node("Team");
        store.get_node_mut(n2).unwrap().set_property("name", PropertyValue::String("Australia".into()));
        let m1 = store.create_node("Match");
        store.create_edge(n1, m1, "COMPETED_IN").unwrap();
        store.create_edge(n2, m1, "COMPETED_IN").unwrap();

        // Test: t1 <> t2 (node inequality comparison)
        let query = parse_query(
            "MATCH (t1:Team)-[:COMPETED_IN]->(m:Match)<-[:COMPETED_IN]-(t2:Team) \
             WHERE t1 <> t2 \
             RETURN t1.name AS team1, t2.name AS team2"
        ).unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(result.is_ok(), "Node identity comparison should work: {:?}", result.err());
        let batch = result.unwrap();
        // Should get 2 rows: (India, Australia) and (Australia, India)
        assert_eq!(batch.records.len(), 2);
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
        let candidates = enumerate_plans(&pg, query.where_clause.as_ref(), catalog, &store.property_index, &config);
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

    // ============================

    // Regression tests: graph-native planner fallback to legacy
    // Ensures dashboard/common queries work with SAMYAMA_GRAPH_NATIVE=true
    // ============================

    /// Helper: execute a query with graph-native planner and verify it doesn't error
    fn assert_native_query_ok(store: &GraphStore, cypher: &str) {
        let query = crate::query::parse_query(cypher).unwrap_or_else(|e| panic!("Parse error for '{}': {}", cypher, e));

        let mut executor = super::super::QueryExecutor::with_planner(store, QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        }));
        let result = executor.execute(&query);
        assert!(result.is_ok(), "Query failed for '{}': {:?}", cypher, result.err());
    }

    /// Helper: create a store with Horse Digital Twin-like data
    fn horse_twin_store() -> GraphStore {
        let mut store = GraphStore::new();
        // Stables
        let s1 = store.create_node("Stable");
        store.get_node_mut(s1).unwrap().set_property("name", PropertyValue::String("Flyinge".to_string()));
        let s2 = store.create_node("Stable");
        store.get_node_mut(s2).unwrap().set_property("name", PropertyValue::String("Täby".to_string()));
        // Horses
        let h1 = store.create_node("Horse");
        store.get_node_mut(h1).unwrap().set_property("name", PropertyValue::String("Storm Runner".to_string()));
        store.get_node_mut(h1).unwrap().set_property("breed", PropertyValue::String("Thoroughbred".to_string()));
        store.get_node_mut(h1).unwrap().set_property("sex", PropertyValue::String("male".to_string()));
        let h2 = store.create_node("Horse");
        store.get_node_mut(h2).unwrap().set_property("name", PropertyValue::String("Nordic Star".to_string()));
        store.get_node_mut(h2).unwrap().set_property("breed", PropertyValue::String("Swedish Warmblood".to_string()));
        store.get_node_mut(h2).unwrap().set_property("sex", PropertyValue::String("female".to_string()));
        let h3 = store.create_node("Horse");
        store.get_node_mut(h3).unwrap().set_property("name", PropertyValue::String("Autumn Gold".to_string()));
        // Pedigree
        store.create_edge(h3, h1, "SIRE").unwrap(); // Autumn Gold's sire = Storm Runner
        store.create_edge(h3, h2, "DAM").unwrap();  // Autumn Gold's dam = Nordic Star
        // Stabled
        store.create_edge(h1, s1, "STABLED_AT").unwrap();
        store.create_edge(h2, s2, "STABLED_AT").unwrap();
        store.create_edge(h3, s1, "STABLED_AT").unwrap();
        // Sensors
        let sn1 = store.create_node("Sensor");
        store.get_node_mut(sn1).unwrap().set_property("sensor_type", PropertyValue::String("heart_rate".to_string()));
        store.create_edge(sn1, h1, "WEARS").unwrap();
        // Alerts
        let a1 = store.create_node("Alert");
        store.get_node_mut(a1).unwrap().set_property("severity", PropertyValue::String("critical".to_string()));
        store.get_node_mut(a1).unwrap().set_property("alert_type", PropertyValue::String("hr_spike".to_string()));
        store.get_node_mut(a1).unwrap().set_property("resolved", PropertyValue::Boolean(false));
        store.create_edge(sn1, a1, "TRIGGERED").unwrap();
        // Trainer + TrainingSession
        let t1 = store.create_node("Trainer");
        store.get_node_mut(t1).unwrap().set_property("name", PropertyValue::String("Johan".to_string()));
        store.create_edge(h1, t1, "TRAINED_BY").unwrap();
        let ts1 = store.create_node("TrainingSession");
        store.get_node_mut(ts1).unwrap().set_property("session_type", PropertyValue::String("gallop".to_string()));
        store.get_node_mut(ts1).unwrap().set_property("distance_km", PropertyValue::Float(5.2));
        store.create_edge(h1, ts1, "COMPLETED").unwrap();
        store.create_edge(t1, ts1, "SUPERVISED_BY").unwrap();
        // Race + RaceResult
        let r1 = store.create_node("Race");
        store.get_node_mut(r1).unwrap().set_property("name", PropertyValue::String("Täby Cup".to_string()));
        let rr1 = store.create_node("RaceResult");
        store.get_node_mut(rr1).unwrap().set_property("position", PropertyValue::Integer(1));
        store.create_edge(h1, rr1, "ENTERED").unwrap();
        store.create_edge(rr1, r1, "RESULT_OF").unwrap();
        // HealthRecord + Vet
        let hr1 = store.create_node("HealthRecord");
        store.get_node_mut(hr1).unwrap().set_property("record_type", PropertyValue::String("vaccination".to_string()));
        store.get_node_mut(hr1).unwrap().set_property("diagnosis", PropertyValue::String("routine".to_string()));
        store.create_edge(h1, hr1, "HAS_RECORD").unwrap();
        let v1 = store.create_node("Veterinarian");
        store.get_node_mut(v1).unwrap().set_property("name", PropertyValue::String("Dr. Eva".to_string()));
        store.create_edge(v1, hr1, "EXAMINED_BY").unwrap();
        // Owner
        let o1 = store.create_node("Owner");
        store.get_node_mut(o1).unwrap().set_property("name", PropertyValue::String("Erik".to_string()));
        store.create_edge(h1, o1, "OWNED_BY").unwrap();

        store
    }

    #[test]
    fn test_native_fallback_label_free_edge_count() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH ()-[r]->() RETURN count(r) AS total_edges");
    }

    #[test]
    fn test_native_fallback_label_free_node_count() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (n) RETURN count(n) AS total_nodes");
    }

    #[test]
    fn test_native_label_count() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count ORDER BY count DESC");
    }

    #[test]
    fn test_native_edge_type_count() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH ()-[r]->() RETURN type(r) AS edge_type, count(r) AS count ORDER BY count DESC");
    }

    #[test]
    fn test_native_single_hop_with_label() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse)-[:STABLED_AT]->(s:Stable) RETURN h.name, s.name");
    }

    #[test]
    fn test_native_two_hop_chain() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse)-[:ENTERED]->(rr:RaceResult)-[:RESULT_OF]->(r:Race) RETURN h.name, r.name");
    }

    #[test]
    fn test_native_two_hop_sensor_chain() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (s:Sensor)-[:WEARS]->(h:Horse)-[:COMPLETED]->(ts:TrainingSession) RETURN s.sensor_type, h.name");
    }

    #[test]
    fn test_native_dual_match_join() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse)-[:SIRE]->(sire:Horse) MATCH (h)-[:DAM]->(dam:Horse) RETURN h.name, sire.name, dam.name");
    }

    #[test]
    fn test_native_with_where_filter() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (a:Alert) WHERE a.severity = 'critical' AND a.resolved = false RETURN a.alert_type");
    }

    #[test]
    fn test_native_with_aggregation() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse)-[:STABLED_AT]->(s:Stable) RETURN s.name, count(h) AS horses ORDER BY horses DESC");
    }

    #[test]
    fn test_native_with_optional_match() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse) OPTIONAL MATCH (h)-[:SIRE]->(sire:Horse) RETURN h.name, sire.name");
    }

    #[test]
    fn test_native_explain_shows_diagnostics() {
        let store = horse_twin_store();
        let mut executor = super::super::QueryExecutor::with_planner(&store, QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        }));
        let query = crate::query::parse_query(
            "EXPLAIN MATCH (s:Sensor)-[:WEARS]->(h:Horse)-[:COMPLETED]->(ts:TrainingSession) RETURN s.sensor_type"
        ).unwrap();
        let result = executor.execute(&query);
        assert!(result.is_ok(), "EXPLAIN should succeed: {:?}", result.err());
        let batch = result.unwrap();
        let plan_text = if let Some(record) = batch.records.first() {
            if let Some(super::super::Value::Property(PropertyValue::String(s))) = record.get("plan") {
                s.clone()
            } else { String::new() }
        } else { String::new() };

        assert!(plan_text.contains("Planner Diagnostics"), "EXPLAIN should include planner diagnostics section. Got: {}", &plan_text[..200.min(plan_text.len())]);
        assert!(plan_text.contains("Candidates evaluated"), "EXPLAIN should show candidate count");
    }

    #[test]
    fn test_native_reverse_direction() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (ts:TrainingSession)<-[:COMPLETED]-(h:Horse) RETURN h.name, ts.session_type");
    }

    #[test]
    fn test_native_variable_length_path_fallback() {
        let store = horse_twin_store();
        // Variable-length paths may not be supported by graph-native planner — should fallback
        assert_native_query_ok(&store, "MATCH (h:Horse)-[:SIRE*1..3]->(ancestor:Horse) RETURN h.name, ancestor.name");
    }

    #[test]
    fn test_native_three_hop_chain() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (s:Sensor)-[:WEARS]->(h:Horse)-[:ENTERED]->(rr:RaceResult)-[:RESULT_OF]->(r:Race) RETURN s.sensor_type, r.name");
    }

    #[test]
    fn test_native_vet_health_horse_chain() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (v:Veterinarian)-[:EXAMINED_BY]->(hr:HealthRecord)<-[:HAS_RECORD]-(h:Horse) RETURN v.name, hr.diagnosis, h.name");
    }

    #[test]
    fn test_native_limit() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse) RETURN h.name LIMIT 1");
    }

    #[test]
    fn test_native_order_by() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse) RETURN h.name ORDER BY h.name");
    }

    #[test]
    fn test_native_with_clause() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse)-[:STABLED_AT]->(s:Stable) WITH s, count(h) AS cnt RETURN s.name, cnt ORDER BY cnt DESC");
    }

    #[test]
    fn test_native_count_star() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse) RETURN count(*) AS total");
    }

    #[test]
    fn test_native_distinct() {
        let store = horse_twin_store();
        assert_native_query_ok(&store, "MATCH (h:Horse) RETURN DISTINCT h.breed");
    }

    // --- WITH Push-Down Tests ---

    fn with_pushdown_store() -> GraphStore {
        let mut store = GraphStore::new();

        // Create 100 countries
        let mut country_ids = Vec::new();
        for i in 0..100 {
            let id = store.create_node("Country");
            store
                .get_node_mut(id)
                .unwrap()
                .set_property("name", PropertyValue::String(format!("Country{}", i)));
            country_ids.push(id);
        }

        // Create indicators for countries 0-9
        for i in 0..10 {
            let ind = store.create_node("Indicator");
            store
                .get_node_mut(ind)
                .unwrap()
                .set_property("year", PropertyValue::Integer(2020));
            store
                .get_node_mut(ind)
                .unwrap()
                .set_property("value", PropertyValue::Float(i as f64 * 10.0));
            store
                .create_edge(country_ids[i], ind, "HAS_INDICATOR")
                .unwrap();
        }

        // Create demographics for only countries 0, 1, 2
        for i in 0..3 {
            let dem = store.create_node("Demographic");
            store
                .get_node_mut(dem)
                .unwrap()
                .set_property("population", PropertyValue::Integer((i as i64 + 1) * 1_000_000));
            store
                .create_edge(country_ids[i], dem, "DEMOGRAPHIC_OF")
                .unwrap();
        }

        store
    }

    #[test]
    fn test_with_pushdown_basic() {
        let store = with_pushdown_store();
        let query = parse_query(
            "MATCH (c:Country)-[:HAS_INDICATOR]->(i:Indicator) \
             WITH c \
             MATCH (c)-[:DEMOGRAPHIC_OF]->(d:Demographic) \
             RETURN c.name, d.population",
        )
        .unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(
            result.is_ok(),
            "WITH push-down basic should work: {:?}",
            result.err()
        );
        let batch = result.unwrap();
        assert_eq!(
            batch.records.len(),
            3,
            "Expected 3 results (countries 0,1,2), got {}",
            batch.records.len()
        );
    }

    #[test]
    fn test_variable_length_expand_reachability() {
        use crate::query::QueryExecutor;
        // Path graph: a -> b -> c -> d (KNOWS chain) plus a branch a -> e.
        let mut store = GraphStore::new();
        let mk = |s: &mut GraphStore, name: &str| {
            let n = s.create_node("Person");
            s.get_node_mut(n)
                .unwrap()
                .set_property("name", PropertyValue::String(name.to_string()));
            n
        };
        let a = mk(&mut store, "a");
        let b = mk(&mut store, "b");
        let c = mk(&mut store, "c");
        let d = mk(&mut store, "d");
        let e = mk(&mut store, "e");
        store.create_edge(a, b, "KNOWS").unwrap();
        store.create_edge(b, c, "KNOWS").unwrap();
        store.create_edge(c, d, "KNOWS").unwrap();
        store.create_edge(a, e, "KNOWS").unwrap();

        let count = |cypher: &str| {
            let q = parse_query(cypher).unwrap();
            QueryExecutor::new(&store).execute(&q).unwrap().records.len()
        };

        // Outgoing reachability from a within k hops (distinct endpoints):
        //   *1..1 -> {b,e}            (2)
        //   *1..2 -> {b,e,c}          (3)
        //   *1..3 -> {b,e,c,d}        (4)
        //   *2..2 -> {c}              (1)   (regression guard: NOT the 1-hop set)
        //   *2..3 -> {c,d}            (2)
        assert_eq!(count("MATCH (a:Person {name:'a'})-[:KNOWS*1..1]->(o:Person) RETURN o.name"), 2);
        assert_eq!(count("MATCH (a:Person {name:'a'})-[:KNOWS*1..2]->(o:Person) RETURN o.name"), 3);
        assert_eq!(count("MATCH (a:Person {name:'a'})-[:KNOWS*1..3]->(o:Person) RETURN o.name"), 4);
        assert_eq!(count("MATCH (a:Person {name:'a'})-[:KNOWS*2..2]->(o:Person) RETURN o.name"), 1);
        assert_eq!(count("MATCH (a:Person {name:'a'})-[:KNOWS*2..3]->(o:Person) RETURN o.name"), 2);
        // Undirected expansion reaches the whole component from any node.
        assert_eq!(count("MATCH (d:Person {name:'d'})-[:KNOWS*1..9]-(o:Person) RETURN o.name"), 4);
    }

    #[test]
    fn test_with_pushdown_multi_hop() {
        let mut store = GraphStore::new();
        let c = store.create_node("Country");
        store
            .get_node_mut(c)
            .unwrap()
            .set_property("name", PropertyValue::String("USA".into()));
        let r = store.create_node("Region");
        store
            .get_node_mut(r)
            .unwrap()
            .set_property("name", PropertyValue::String("West".into()));
        let city = store.create_node("City");
        store
            .get_node_mut(city)
            .unwrap()
            .set_property("name", PropertyValue::String("LA".into()));
        store.create_edge(c, r, "HAS_REGION").unwrap();
        store.create_edge(r, city, "HAS_CITY").unwrap();

        let c2 = store.create_node("Country");
        store
            .get_node_mut(c2)
            .unwrap()
            .set_property("name", PropertyValue::String("Decoy".into()));

        let query = parse_query(
            "MATCH (c:Country) WHERE c.name = 'USA' \
             WITH c \
             MATCH (c)-[:HAS_REGION]->(r:Region)-[:HAS_CITY]->(city:City) \
             RETURN c.name, r.name, city.name",
        )
        .unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(
            result.is_ok(),
            "WITH push-down multi-hop should work: {:?}",
            result.err()
        );
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1, "Expected 1 result (USA->West->LA)");
    }

    #[test]
    fn test_with_pushdown_unbound_start_no_regression() {
        let mut store = GraphStore::new();
        let c = store.create_node("Country");
        store
            .get_node_mut(c)
            .unwrap()
            .set_property("name", PropertyValue::String("USA".into()));
        let org = store.create_node("Org");
        store
            .get_node_mut(org)
            .unwrap()
            .set_property("name", PropertyValue::String("UN".into()));
        store.create_edge(org, c, "OPERATES_IN").unwrap();

        let query = parse_query(
            "MATCH (c:Country) WHERE c.name = 'USA' \
             WITH c \
             MATCH (o:Org)-[:OPERATES_IN]->(c) \
             RETURN o.name, c.name",
        )
        .unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(
            result.is_ok(),
            "Unbound start should still work: {:?}",
            result.err()
        );
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
    }

    #[test]
    fn test_with_pushdown_where_on_expanded_node() {
        let store = with_pushdown_store();
        let query = parse_query(
            "MATCH (c:Country)-[:HAS_INDICATOR]->(i:Indicator) \
             WITH c \
             MATCH (c)-[:DEMOGRAPHIC_OF]->(d:Demographic) \
             WHERE d.population > 1500000 \
             RETURN c.name, d.population",
        )
        .unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(
            result.is_ok(),
            "WHERE on expanded node should work: {:?}",
            result.err()
        );
        let batch = result.unwrap();
        assert_eq!(
            batch.records.len(),
            2,
            "Expected 2 results with pop > 1.5M, got {}",
            batch.records.len()
        );
    }

    #[test]
    fn test_with_pushdown_with_aggregation() {
        let store = with_pushdown_store();
        let query = parse_query(
            "MATCH (c:Country)-[:HAS_INDICATOR]->(i:Indicator) \
             WITH c, count(i) AS ind_count \
             MATCH (c)-[:DEMOGRAPHIC_OF]->(d:Demographic) \
             RETURN c.name, ind_count, d.population",
        )
        .unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(
            result.is_ok(),
            "Aggregation + push-down should work: {:?}",
            result.err()
        );
        let batch = result.unwrap();
        assert_eq!(
            batch.records.len(),
            3,
            "Expected 3 results, got {}",
            batch.records.len()
        );
    }

    #[test]
    fn test_label_count_cache_basic() {
        let mut store = GraphStore::new();
        for _ in 0..500 {
            store.create_node("Article");
        }
        for _ in 0..100 {
            store.create_node("Journal");
        }

        let query = parse_query("MATCH (n:Article) RETURN count(n) AS total").unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan).unwrap();
        assert_eq!(result.records.len(), 1);
        let val = result.records[0].get("total").unwrap();
        assert_eq!(
            val,
            &crate::query::executor::record::Value::Property(PropertyValue::Integer(500)),
            "Label count should be 500"
        );
    }

    #[test]
    fn test_label_count_cache_count_star() {
        let mut store = GraphStore::new();
        for _ in 0..200 {
            store.create_node("Person");
        }

        let query = parse_query("MATCH (n:Person) RETURN count(*) AS total").unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan).unwrap();
        assert_eq!(result.records.len(), 1);
        let val = result.records[0].get("total").unwrap();
        assert_eq!(
            val,
            &crate::query::executor::record::Value::Property(PropertyValue::Integer(200)),
        );
    }

    #[test]
    fn test_label_count_not_used_with_where() {
        // When there's a WHERE clause, should NOT use label count shortcut
        let mut store = GraphStore::new();
        for i in 0..100 {
            let id = store.create_node("Person");
            store
                .get_node_mut(id)
                .unwrap()
                .set_property("age", PropertyValue::Integer(i));
        }

        let query =
            parse_query("MATCH (n:Person) WHERE n.age > 50 RETURN count(n) AS total").unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan).unwrap();
        assert_eq!(result.records.len(), 1);
        let val = result.records[0].get("total").unwrap();
        // 51..99 = 49 nodes
        assert_eq!(
            val,
            &crate::query::executor::record::Value::Property(PropertyValue::Integer(49)),
        );
    }
}
