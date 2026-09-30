//! Detector for the adjacency-aware aggregation pattern (ADR-017 Phase 0).
//!
//! Recognizes queries of the form:
//! ```text
//! MATCH (a[:LabelA])-[r:EdgeType]-(b:LabelB)
//! RETURN b.prop [, ...], count(a) AS n
//!   [ORDER BY n DESC] [LIMIT N]
//! ```
//! where the aggregate reduces to per-node degree on the bound endpoint `b`.
//!
//! The detector is deliberately conservative in Phase 0: if any constraint
//! fails, it returns `None` and the caller uses the standard plan. This
//! guarantees zero behavior change until the physical operator ships in
//! Phase 1.

use super::logical_plan::ExpandDirection;
use crate::graph::types::{EdgeType, Label};
use crate::graph::GraphStore;
use crate::query::ast::{Direction, Expression, MatchClause, Query};

/// Parameters extracted from a query that matches the adjacency-count pattern.
///
/// These are the inputs Phase 1's `AdjacencyCountAggregateOperator` will need
/// at plan-construction time. Phase 0 produces this struct but does not yet
/// build a `LogicalPlanNode` from it — that wiring lands in Phase 1.
#[derive(Debug, Clone, PartialEq)]
pub struct AdjacencyAggPattern {
    /// Variable bound to the endpoint the aggregate groups on.
    pub grouped_var: String,
    /// Label of the grouped endpoint (required — used as the NodeScan target).
    pub grouped_label: Label,
    /// Variable bound to the counted neighbor.
    pub neighbor_var: String,
    /// Optional label on the neighbor side. Phase 1 requires schema uniqueness
    /// before using this for filtering; the detector records it for later use.
    pub neighbor_label: Option<Label>,
    /// Edge type connecting the two endpoints. Phase 1 requires exactly one.
    pub edge_type: EdgeType,
    /// Direction of the edge *relative to the grouped endpoint*.
    /// - `Forward`  = `grouped_var -[E]-> neighbor_var` (out-degree)
    /// - `Reverse`  = `neighbor_var -[E]-> grouped_var` (in-degree)
    pub direction: ExpandDirection,
    /// Alias the count result is exposed as (e.g. `"articles"`).
    pub count_alias: String,
    /// Whether `count(DISTINCT neighbor)` was used. Phase 1 rejects this.
    pub count_distinct: bool,
    /// GROUP BY entries from the RETURN, in the order they appeared. Each
    /// entry is `(variable, optional_property)`:
    ///   - `("g", None)` means `RETURN g, ...` — variable itself.
    ///   - `("g", Some("name"))` means `RETURN g.name, ...` — property of g.
    ///
    /// The planner uses this to decide whether a post-aggregation hash-group
    /// is needed. A variable-only GROUP BY is safe to emit directly (one
    /// row per node = one row per group). Any property entry triggers a
    /// post-aggregate to correctly merge nodes that share a property value.
    pub group_by_items: Vec<(String, Option<String>)>,
    /// Optional WHERE predicate referencing only `grouped_var`. Planner
    /// applies it as FilterOperator on the grouped scan before the
    /// adjacency walk.
    pub prefilter: Option<Expression>,
}

/// Try to detect the adjacency-count pattern in `query`.
///
/// Returns `Some(pattern)` if all Phase 1 constraints are satisfied, `None`
/// otherwise. A `None` result means "use the standard planner path" and is
/// never a hard error — the caller treats it as a negative detection.
///
/// `store` is required for the GROUP BY safety check: when the RETURN
/// projects a property of the grouped node (e.g. `RETURN g.name, count(n)`),
/// the per-node emission produced by `AdjacencyCountAggregateOperator` is
/// only equivalent to true GROUP BY semantics when the property value is
/// unique per node. This is enforced by checking for a UNIQUE constraint
/// on `(grouped_label, property)`. Variable-only group-by
/// (`RETURN g, count(n)`) is always safe (one row per node by definition).
pub fn detect(query: &Query, store: &GraphStore) -> Option<AdjacencyAggPattern> {
    // Shape constraint: exactly one MATCH, no WITH split, no extra WITH stages.
    if query.match_clauses.len() != 1 {
        return None;
    }
    if query.with_split_index.is_some() || !query.extra_with_stages.is_empty() {
        return None;
    }
    if query.with_clause.is_some() {
        return None;
    }
    // post_with_where doesn't apply (no WITH in Phase 1) — reject if set.
    // WHERE is deferred until grouped_var is identified; accepted only if
    // it references the grouped side exclusively.
    if query.post_with_where_clause.is_some() {
        return None;
    }
    // Writes, CALLs, SET/DELETE etc. disqualify.
    if query.create_clause.is_some()
        || query.call_clause.is_some()
        || query.call_subquery.is_some()
        || query.delete_clause.is_some()
        || query.merge_clause.is_some()
        || query.unwind_clause.is_some()
        || !query.set_clauses.is_empty()
        || !query.remove_clauses.is_empty()
    {
        return None;
    }

    let mc = &query.match_clauses[0];
    if mc.optional {
        return None;
    }
    if mc.pattern.paths.len() != 1 {
        return None;
    }

    let path = &mc.pattern.paths[0];
    // Single edge: exactly one segment between start and target.
    if path.segments.len() != 1 {
        return None;
    }
    // No variable-length or shortest-path wrinkles.
    if path.segments[0].edge.length.is_some() {
        return None;
    }

    let start = &path.start;
    let target = &path.segments[0].node;
    let edge = &path.segments[0].edge;

    // Both endpoints must have variables bound (we need to reference them in
    // the aggregate + group-by).
    let start_var = start.variable.as_ref()?.clone();
    let target_var = target.variable.as_ref()?.clone();

    // Concrete single edge type.
    if edge.types.len() != 1 {
        return None;
    }
    let edge_type = edge.types[0].clone();

    // Direction must be Outgoing or Incoming — not Both, not bidirectional
    // (which would require either-direction degree, deferred to a later phase).
    let edge_direction = match edge.direction {
        Direction::Outgoing => Direction::Outgoing,
        Direction::Incoming => Direction::Incoming,
        Direction::Both => return None,
    };

    // RETURN clause must exist and contain exactly one count() aggregate.
    let ret = query.return_clause.as_ref()?;
    if ret.distinct {
        return None;
    }

    let mut count_info: Option<(String, String, bool)> = None; // (alias, arg_var, distinct)
    // Each entry is (variable, optional_property). `None` property means the
    // user wrote `RETURN g, ...` (the variable itself, always safe to emit
    // per-node). `Some(p)` means `RETURN g.p, ...` and is only safe when
    // (label, p) has a UNIQUE constraint — checked below.
    let mut group_by_items: Vec<(String, Option<String>)> = Vec::new();

    for (i, item) in ret.items.iter().enumerate() {
        match &item.expression {
            Expression::Function { name, args, distinct }
                if name.eq_ignore_ascii_case("count") =>
            {
                if count_info.is_some() {
                    return None; // multiple count()s not supported
                }
                if args.len() != 1 {
                    return None;
                }
                // Only count(variable) is a degree-equivalent — count(*) or
                // count(b.prop) aren't handled by Phase 1.
                let arg_var = match &args[0] {
                    Expression::Variable(v) => v.clone(),
                    _ => return None,
                };
                // Projection must carry an explicit alias so downstream
                // operators have a stable name.
                let alias = item.alias.clone().unwrap_or_else(|| format!("count_{}", i));
                count_info = Some((alias, arg_var, *distinct));
            }
            Expression::Variable(variable) => {
                group_by_items.push((variable.clone(), None));
            }
            Expression::Property { variable, property } => {
                group_by_items.push((variable.clone(), Some(property.clone())));
            }
            _ => {
                // Other expressions (arithmetic, functions on non-grouped vars,
                // CASE, etc.) are out of scope for Phase 1.
                return None;
            }
        }
    }

    let (count_alias, count_arg_var, count_distinct) = count_info?;

    // An aggregate with no grouping key is not a per-node degree.
    //
    // `MATCH (a)-[:R]->(b) RETURN count(b)` projects only the aggregate, so openCypher
    // groups on nothing and the answer is a single scalar: the number of matched rows.
    // `AdjacencyCountAggregateOperator` emits one record *per grouped node*, which for an
    // ungrouped query is one row per `a` carrying its out-degree — the right numbers
    // attached to the wrong question (#301). The generic Aggregate path answers this
    // correctly, so decline and let it.
    if group_by_items.is_empty() {
        return None;
    }

    // count(DISTINCT neighbor) is now supported via the operator's
    // with_count_distinct mode (per-group FxHashSet<NodeId> dedup,
    // handles parallel edges + same-neighbor-across-grouped-nodes).

    // The counted variable must be one endpoint; the grouped side must be the
    // OTHER endpoint and must provide every group-by variable.
    let (grouped_var, grouped_node, neighbor_var, neighbor_node) =
        if count_arg_var == start_var {
            (target_var.clone(), target, start_var.clone(), start)
        } else if count_arg_var == target_var {
            (start_var.clone(), start, target_var.clone(), target)
        } else {
            return None;
        };

    // Every group-by entry must target the grouped endpoint.
    for (v, _) in &group_by_items {
        if v != &grouped_var {
            return None;
        }
    }

    // Grouped endpoint must have a single concrete label (needed for the scan).
    if grouped_node.labels.len() != 1 {
        return None;
    }
    let grouped_label = grouped_node.labels[0].clone();

    // GROUP BY safety: the AdjacencyCountAggregateOperator emits one record
    // per grouped node — for property-based GROUP BY this is per-node, not
    // per-group. The planner is responsible for inserting a post-aggregation
    // hash-group whenever any GROUP BY entry is a property reference rather
    // than the variable itself. We expose `group_by_items` on the pattern so
    // the planner can decide.
    let _ = store; // currently unused; reserved for future schema-aware checks

    // Neighbor label is optional — None means "any node on the other side".
    let neighbor_label = match neighbor_node.labels.len() {
        0 => None,
        1 => Some(neighbor_node.labels[0].clone()),
        _ => return None, // multi-label neighbor out of scope for Phase 1
    };

    // No property constraints on endpoints — they'd be filters, out of Phase 1.
    if grouped_node.properties.is_some() || neighbor_node.properties.is_some() {
        return None;
    }

    // Map physical edge direction to the direction relative to grouped_var.
    // The AST direction describes (start)-[edge]->(target).
    //   If grouped=target and edge is Outgoing: neighbor->grouped = Reverse (in-degree on grouped).
    //   If grouped=start  and edge is Outgoing: grouped->neighbor = Forward (out-degree on grouped).
    let direction = match (edge_direction, grouped_var == start_var) {
        (Direction::Outgoing, true) => ExpandDirection::Forward,
        (Direction::Outgoing, false) => ExpandDirection::Reverse,
        (Direction::Incoming, true) => ExpandDirection::Reverse,
        (Direction::Incoming, false) => ExpandDirection::Forward,
        (Direction::Both, _) => unreachable!(), // filtered above
    };

    // WHERE: accept iff predicate references only the grouped variable.
    let prefilter = match &query.where_clause {
        Some(wc) => {
            if !expression_references_only(&wc.predicate, &grouped_var) {
                return None;
            }
            Some(wc.predicate.clone())
        }
        None => None,
    };

    Some(AdjacencyAggPattern {
        grouped_var,
        grouped_label,
        neighbor_var,
        neighbor_label,
        edge_type,
        direction,
        count_alias,
        count_distinct,
        group_by_items,
        prefilter,
    })
}

/// Parameters extracted from a query that matches the Phase 3a *WITH-bound*
/// adjacency-count pattern:
/// ```text
/// MATCH (g:LabelA) [WHERE pred_on_g]
/// WITH g [SKIP M] [LIMIT N]
/// MATCH (g)-[:Edge]-(n[:LabelB])
/// RETURN g.prop [, ...], count(n) AS c [ORDER BY c DESC] [LIMIT K]
/// ```
///
/// Differs from the Phase 1 shape in that the grouped endpoint is bound by a
/// pre-WITH scan that may carry a filter and/or LIMIT. The post-WITH MATCH
/// reuses the bound variable rather than re-scanning. This is the MB053/EX49
/// pattern — a query that explicitly caps the number of groups considered.
#[derive(Debug, Clone, PartialEq)]
pub struct AdjacencyAggWithBindingPattern {
    /// Core pattern info, same shape as Phase 1.
    pub core: AdjacencyAggPattern,
    /// Optional WHERE predicate applied to the grouped-side scan before
    /// counting. Must reference only `core.grouped_var`.
    pub prefilter: Option<Expression>,
    /// Optional `SKIP` on the pre-WITH binding.
    pub grouped_scan_skip: Option<usize>,
    /// Optional `LIMIT` on the pre-WITH binding — the per-MB053 cap that
    /// makes the query tractable even before adjacency-count.
    pub grouped_scan_limit: Option<usize>,
}

/// Detect the Phase 3a WITH-bound adjacency-count pattern.
///
/// Returns `None` if the query doesn't fit; the caller then tries `detect()`
/// for Phase 1 or falls back to the generic planner.
pub fn detect_with_binding(query: &Query) -> Option<AdjacencyAggWithBindingPattern> {
    // Must have exactly one WITH and one split. Multi-WITH stacking is out
    // of scope — we only need to unblock the single-WITH bench shapes.
    let with_clause = query.with_clause.as_ref()?;
    let split = query.with_split_index?;
    if !query.extra_with_stages.is_empty() {
        return None;
    }
    // Writes, CALLs, SET/DELETE etc. still disqualify.
    if query.create_clause.is_some()
        || query.call_clause.is_some()
        || query.call_subquery.is_some()
        || query.delete_clause.is_some()
        || query.merge_clause.is_some()
        || query.unwind_clause.is_some()
        || !query.set_clauses.is_empty()
        || !query.remove_clauses.is_empty()
    {
        return None;
    }
    // Post-WITH WHERE: left to a later phase (would require filter pushdown
    // through the aggregate). Pre-WITH WHERE is supported below.
    if query.post_with_where_clause.is_some() {
        return None;
    }

    // Exactly one MATCH on each side of the WITH.
    let pre = query.match_clauses.get(..split)?;
    let post = query.match_clauses.get(split..)?;
    if pre.len() != 1 || post.len() != 1 {
        return None;
    }
    if pre[0].optional || post[0].optional {
        return None;
    }

    // Pre-MATCH: a single standalone node that binds the grouped variable.
    let pre_path = pre[0].pattern.paths.first()?;
    if pre[0].pattern.paths.len() != 1 {
        return None;
    }
    if !pre_path.segments.is_empty() {
        return None;
    }
    let grouped_var = pre_path.start.variable.as_ref()?.clone();
    if pre_path.start.labels.len() != 1 {
        return None;
    }
    let grouped_label = pre_path.start.labels[0].clone();
    if pre_path.start.properties.is_some() {
        return None;
    }

    // WITH clause: must be a pure pass-through for `grouped_var`.
    // No aggregation, no distinct, no ORDER BY on WITH (the ORDER BY belongs
    // after the aggregate, which is applied post-RETURN). WHERE-on-WITH
    // isn't used by the target queries — if present, reject.
    if with_clause.distinct {
        return None;
    }
    if with_clause.where_clause.is_some() {
        return None;
    }
    if with_clause.order_by.is_some() {
        return None;
    }
    if with_clause.items.len() != 1 {
        return None;
    }
    let passthrough = &with_clause.items[0];
    match &passthrough.expression {
        Expression::Variable(v) if v == &grouped_var => {}
        _ => return None,
    }
    // If the WITH introduces an alias other than grouped_var, the second
    // MATCH would have to reference that alias — we don't follow renames.
    if let Some(alias) = &passthrough.alias {
        if alias != &grouped_var {
            return None;
        }
    }

    // Pre-WITH WHERE — optional, must reference only `grouped_var`.
    let prefilter = match &query.where_clause {
        Some(wc) => {
            if !expression_references_only(&wc.predicate, &grouped_var) {
                return None;
            }
            Some(wc.predicate.clone())
        }
        None => None,
    };

    // Post-MATCH: the standard single-segment pattern expected by Phase 1,
    // but one endpoint MUST be `grouped_var` (not re-scanned).
    let post_path = post[0].pattern.paths.first()?;
    if post[0].pattern.paths.len() != 1 {
        return None;
    }
    if post_path.segments.len() != 1 {
        return None;
    }
    if post_path.segments[0].edge.length.is_some() {
        return None;
    }

    let start = &post_path.start;
    let target = &post_path.segments[0].node;
    let edge = &post_path.segments[0].edge;

    let start_var = start.variable.as_ref()?.clone();
    let target_var = target.variable.as_ref()?.clone();

    // grouped_var must be one of the endpoints; identify neighbor.
    let (grouped_node, neighbor_node, neighbor_var) = if start_var == grouped_var {
        (start, target, target_var.clone())
    } else if target_var == grouped_var {
        (target, start, start_var.clone())
    } else {
        return None;
    };

    if edge.types.len() != 1 {
        return None;
    }
    let edge_type = edge.types[0].clone();
    let edge_direction = match edge.direction {
        Direction::Outgoing => Direction::Outgoing,
        Direction::Incoming => Direction::Incoming,
        Direction::Both => return None,
    };

    // The grouped-side node in the second MATCH must be either bare `(g)`
    // or `(g:LabelA)` matching the pre-WITH label. Property constraints
    // would act as additional filters (unsupported).
    if grouped_node.properties.is_some() {
        return None;
    }
    if !grouped_node.labels.is_empty() {
        if grouped_node.labels.len() != 1 || grouped_node.labels[0] != grouped_label {
            return None;
        }
    }

    // Neighbor label is optional. No property filters on the neighbor side.
    if neighbor_node.properties.is_some() {
        return None;
    }
    let neighbor_label = match neighbor_node.labels.len() {
        0 => None,
        1 => Some(neighbor_node.labels[0].clone()),
        _ => return None,
    };

    // RETURN shape: identical to Phase 1.
    let ret = query.return_clause.as_ref()?;
    if ret.distinct {
        return None;
    }
    let mut count_info: Option<(String, String, bool)> = None;
    let mut group_by_vars: Vec<String> = Vec::new();
    for (i, item) in ret.items.iter().enumerate() {
        match &item.expression {
            Expression::Function {
                name,
                args,
                distinct,
            } if name.eq_ignore_ascii_case("count") => {
                if count_info.is_some() {
                    return None;
                }
                if args.len() != 1 {
                    return None;
                }
                let arg_var = match &args[0] {
                    Expression::Variable(v) => v.clone(),
                    _ => return None,
                };
                let alias = item.alias.clone().unwrap_or_else(|| format!("count_{}", i));
                count_info = Some((alias, arg_var, *distinct));
            }
            Expression::Property { variable, .. } | Expression::Variable(variable) => {
                group_by_vars.push(variable.clone());
            }
            _ => return None,
        }
    }
    // Re-walk the RETURN items to also capture each GROUP BY entry as
    // (variable, optional_property). Phase 3a previously discarded the
    // property name; the in-operator group-by (P8.5) needs it to build
    // per-group counts during the per-node walk.
    let mut group_by_items: Vec<(String, Option<String>)> = Vec::new();
    for item in &ret.items {
        match &item.expression {
            Expression::Function { name, .. } if name.eq_ignore_ascii_case("count") => continue,
            Expression::Variable(variable) => {
                group_by_items.push((variable.clone(), None));
            }
            Expression::Property { variable, property } => {
                group_by_items.push((variable.clone(), Some(property.clone())));
            }
            _ => {}
        }
    }

    let (count_alias, count_arg_var, count_distinct) = count_info?;

    // An aggregate with no grouping key is not a per-node degree.
    //
    // `MATCH (a)-[:R]->(b) RETURN count(b)` projects only the aggregate, so openCypher
    // groups on nothing and the answer is a single scalar: the number of matched rows.
    // `AdjacencyCountAggregateOperator` emits one record *per grouped node*, which for an
    // ungrouped query is one row per `a` carrying its out-degree — the right numbers
    // attached to the wrong question (#301). The generic Aggregate path answers this
    // correctly, so decline and let it.
    if group_by_items.is_empty() {
        return None;
    }

    if count_arg_var != neighbor_var {
        return None;
    }
    for v in &group_by_vars {
        if v != &grouped_var {
            return None;
        }
    }

    let direction = match (edge_direction, grouped_var == start_var) {
        (Direction::Outgoing, true) => ExpandDirection::Forward,
        (Direction::Outgoing, false) => ExpandDirection::Reverse,
        (Direction::Incoming, true) => ExpandDirection::Reverse,
        (Direction::Incoming, false) => ExpandDirection::Forward,
        (Direction::Both, _) => unreachable!(),
    };

    Some(AdjacencyAggWithBindingPattern {
        core: AdjacencyAggPattern {
            grouped_var,
            grouped_label,
            neighbor_var,
            neighbor_label,
            edge_type,
            direction,
            count_alias,
            count_distinct,
            group_by_items,
            // Phase 3a stores its WHERE in `prefilter` on the outer
            // pattern (sibling field), not on `core` — keep `core`
            // unfiltered to preserve the existing planner contract.
            prefilter: None,
        },
        prefilter,
        grouped_scan_skip: with_clause.skip,
        grouped_scan_limit: with_clause.limit,
    })
}

/// Walk an expression tree; return true iff every variable reference is `var`.
/// Conservative — returns false on anything unknown, which keeps the detector
/// from accidentally accepting expressions that reference other bindings.
fn expression_references_only(expr: &Expression, var: &str) -> bool {
    match expr {
        Expression::Variable(v) => v == var,
        Expression::Property { variable, .. } => variable == var,
        Expression::Literal(_) | Expression::Parameter(_) => true,
        Expression::Binary { left, right, .. } => {
            expression_references_only(left, var) && expression_references_only(right, var)
        }
        Expression::Unary { expr, .. } => expression_references_only(expr, var),
        Expression::Function { args, .. } => {
            args.iter().all(|a| expression_references_only(a, var))
        }
        _ => false, // CASE, subqueries, list ops: reject conservatively
    }
}

// ============================================================================
// Phase 4: aggregate-then-expand (PR-P2.8). See SGE PR for design notes.
// ============================================================================

#[derive(Debug, Clone)]
pub struct AggregateThenExpandPattern {
    pub core: AdjacencyAggPattern,
    pub post_aggregate_filter: Option<Expression>,
    pub post_aggregate_order_by: Option<Vec<(Expression, bool)>>,
    pub post_aggregate_skip: Option<usize>,
    pub post_aggregate_limit: Option<usize>,
    pub expand_neighbor_var: String,
    pub expand_neighbor_label: Option<crate::graph::Label>,
    pub expand_edge_type: crate::graph::EdgeType,
    pub expand_direction: Direction,
}

pub fn detect_aggregate_then_expand(
    query: &Query,
    store: &GraphStore,
) -> Option<AggregateThenExpandPattern> {
    let split = query.with_split_index?;
    let _final_with = query.with_clause.as_ref()?;
    if query.post_with_where_clause.is_some() {
        return None;
    }
    if query.create_clause.is_some()
        || query.call_clause.is_some()
        || query.call_subquery.is_some()
        || query.delete_clause.is_some()
        || query.merge_clause.is_some()
        || query.unwind_clause.is_some()
        || !query.set_clauses.is_empty()
        || !query.remove_clauses.is_empty()
    {
        return None;
    }

    let (agg_with, passthrough_with) = match query.extra_with_stages.len() {
        0 => (query.with_clause.as_ref()?, None),
        1 => (
            &query.extra_with_stages[0].0,
            Some(query.with_clause.as_ref()?),
        ),
        _ => return None,
    };
    if agg_with.distinct {
        return None;
    }
    if let Some(pt) = passthrough_with {
        if pt.distinct {
            return None;
        }
        if !query.extra_with_stages[0].2.is_empty() {
            return None;
        }
        if query.extra_with_stages[0].3.is_some() {
            return None;
        }
        if query.extra_with_stages[0].1.is_some() {
            return None;
        }
    }

    let pre_match = query.match_clauses.get(..split)?;
    if pre_match.len() != 1 {
        return None;
    }
    if query.where_clause.is_some() {
        return None;
    }
    let mut probe = Query::new();
    probe.match_clauses = pre_match.to_vec();
    probe.return_clause = Some(crate::query::ast::ReturnClause {
        items: agg_with.items.clone(),
        distinct: false,
    });
    let core = detect(&probe, store)?;

    let post_aggregate_filter = agg_with.where_clause.as_ref().map(|wc| wc.predicate.clone());
    if let Some(pred) = &post_aggregate_filter {
        if !expression_references_only(pred, &core.count_alias) {
            return None;
        }
    }

    let order_by_src;
    let skip_src;
    let limit_src;
    if let Some(pt) = passthrough_with {
        if pt.items.len() != 2 {
            return None;
        }
        let allowed: std::collections::HashSet<String> =
            [core.grouped_var.clone(), core.count_alias.clone()]
                .into_iter()
                .collect();
        for it in &pt.items {
            match &it.expression {
                Expression::Variable(v) if allowed.contains(v) => {}
                _ => return None,
            }
            if let (Some(alias), Expression::Variable(v)) = (&it.alias, &it.expression) {
                if alias != v {
                    return None;
                }
            }
        }
        if pt.where_clause.is_some() {
            return None;
        }
        if agg_with.order_by.is_some() && pt.order_by.is_some() {
            return None;
        }
        if agg_with.skip.is_some() && pt.skip.is_some() {
            return None;
        }
        if agg_with.limit.is_some() && pt.limit.is_some() {
            return None;
        }
        order_by_src = agg_with.order_by.as_ref().or(pt.order_by.as_ref());
        skip_src = agg_with.skip.or(pt.skip);
        limit_src = agg_with.limit.or(pt.limit);
    } else {
        order_by_src = agg_with.order_by.as_ref();
        skip_src = agg_with.skip;
        limit_src = agg_with.limit;
    }

    let post_match_clauses: &[MatchClause] = query.match_clauses.get(split..)?;
    if post_match_clauses.len() != 1 {
        return None;
    }
    let post_match = &post_match_clauses[0];
    if post_match.optional {
        return None;
    }
    if post_match.pattern.paths.len() != 1 {
        return None;
    }
    let post_path = &post_match.pattern.paths[0];
    if post_path.segments.len() != 1 {
        return None;
    }
    let seg = &post_path.segments[0];
    if seg.edge.length.is_some() || seg.edge.types.len() != 1 {
        return None;
    }
    let post_edge_type = seg.edge.types[0].clone();
    let post_edge_dir = match seg.edge.direction {
        Direction::Outgoing => Direction::Outgoing,
        Direction::Incoming => Direction::Incoming,
        Direction::Both => return None,
    };
    let start_var = post_path.start.variable.as_ref()?.clone();
    let target_var = seg.node.variable.as_ref()?.clone();
    let (bound_is_start, neighbor_var, neighbor_node) = if start_var == core.grouped_var {
        (true, target_var.clone(), &seg.node)
    } else if target_var == core.grouped_var {
        (false, start_var.clone(), &post_path.start)
    } else {
        return None;
    };
    let bound_node = if bound_is_start { &post_path.start } else { &seg.node };
    if bound_node.properties.is_some() {
        return None;
    }
    if !bound_node.labels.is_empty()
        && (bound_node.labels.len() != 1 || bound_node.labels[0] != core.grouped_label)
    {
        return None;
    }
    if neighbor_node.properties.is_some() {
        return None;
    }
    let neighbor_label = match neighbor_node.labels.len() {
        0 => None,
        1 => Some(neighbor_node.labels[0].clone()),
        _ => return None,
    };
    let expand_direction = match (post_edge_dir, bound_is_start) {
        (Direction::Outgoing, true) => Direction::Outgoing,
        (Direction::Outgoing, false) => Direction::Incoming,
        (Direction::Incoming, true) => Direction::Incoming,
        (Direction::Incoming, false) => Direction::Outgoing,
        (Direction::Both, _) => unreachable!(),
    };

    let post_aggregate_order_by = order_by_src.map(|ob| {
        ob.items
            .iter()
            .map(|i| (i.expression.clone(), i.ascending))
            .collect()
    });

    Some(AggregateThenExpandPattern {
        core,
        post_aggregate_filter,
        post_aggregate_order_by,
        post_aggregate_skip: skip_src,
        post_aggregate_limit: limit_src,
        expand_neighbor_var: neighbor_var,
        expand_neighbor_label: neighbor_label,
        expand_edge_type: post_edge_type,
        expand_direction,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse_query;

    /// MB049 shape — the motivating case for ADR-017.
    /// `MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) RETURN j.title, count(a) AS articles`
    /// should detect: group on j, count a, direction Reverse (in-degree on Journal),
    /// neighbor label Article.
    #[test]
    fn detects_mb049_shape() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             RETURN j.title, count(a) AS articles ORDER BY articles DESC LIMIT 10",
        )
        .unwrap();
        let p = detect(&q, &GraphStore::new()).expect("should detect");
        assert_eq!(p.grouped_var, "j");
        assert_eq!(p.grouped_label.as_str(), "Journal");
        assert_eq!(p.neighbor_var, "a");
        assert_eq!(p.neighbor_label.as_ref().unwrap().as_str(), "Article");
        assert_eq!(p.edge_type.as_str(), "PUBLISHED_IN");
        assert_eq!(p.direction, ExpandDirection::Reverse);
        assert_eq!(p.count_alias, "articles");
        assert!(!p.count_distinct);
    }

    /// Forward direction: count outgoing neighbors grouped on the source side.
    #[test]
    fn detects_forward_direction() {
        let q = parse_query(
            "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, count(p) AS posts",
        )
        .unwrap();
        let p = detect(&q, &GraphStore::new()).expect("should detect");
        assert_eq!(p.grouped_var, "u");
        assert_eq!(p.neighbor_var, "p");
        assert_eq!(p.direction, ExpandDirection::Forward);
    }

    /// Incoming edge in source syntax: `(j:Journal)<-[:PUBLISHED_IN]-(a:Article)`.
    /// Grouped=j; edge is AST-Incoming with start=j; direction relative to j
    /// is Reverse (in-degree). Same meaning as MB049, different phrasing.
    #[test]
    fn detects_equivalent_incoming_phrasing() {
        let q = parse_query(
            "MATCH (j:Journal)<-[:PUBLISHED_IN]-(a:Article) \
             RETURN j.title, count(a) AS articles",
        )
        .unwrap();
        let p = detect(&q, &GraphStore::new()).expect("should detect");
        assert_eq!(p.grouped_var, "j");
        assert_eq!(p.direction, ExpandDirection::Reverse);
    }

    // ——— Rejection cases ———

    /// WHERE on the grouped side is now accepted as a prefilter.
    #[test]
    fn accepts_where_on_grouped_side() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             WHERE j.active = true RETURN j.title, count(a) AS articles",
        )
        .unwrap();
        let p = detect(&q, &GraphStore::new()).expect("WHERE on grouped side should be accepted");
        assert!(p.prefilter.is_some());
    }

    /// WHERE referencing the neighbor (counted) side still disqualifies.
    #[test]
    fn rejects_where_on_neighbor_side() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             WHERE a.year = 2024 RETURN j.title, count(a) AS articles",
        )
        .unwrap();
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    /// Multi-hop paths use a different plan shape.
    #[test]
    fn rejects_multi_hop() {
        let q = parse_query(
            "MATCH (a:Article)-[:AUTHORED_BY]->(au:Author)-[:AFFILIATED]->(i:Institution) \
             RETURN i.name, count(a) AS articles",
        )
        .unwrap();
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    /// Multiple aggregates: we only handle a single count().
    #[test]
    fn rejects_multiple_aggregates() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             RETURN j.title, count(a) AS articles, avg(a.year) AS avg_year",
        )
        .unwrap();
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    /// count(*) is semantically close but needs care around nulls — defer to
    /// a later phase.
    #[test]
    fn rejects_count_star() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             RETURN j.title, count(*) AS articles",
        )
        .unwrap();
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    /// DISTINCT count is now supported via the operator's per-group
    /// FxHashSet<NodeId> mode (handles parallel edges + same-neighbor-
    /// across-grouped-nodes correctly).
    #[test]
    fn accepts_count_distinct_in_phase_1() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             RETURN j.title, count(DISTINCT a) AS articles",
        )
        .unwrap();
        let p = detect(&q, &GraphStore::new()).expect("count(DISTINCT) should now be detected");
        assert!(p.count_distinct);
    }

    /// Property constraints on endpoints would act as filters; out of scope.
    #[test]
    fn rejects_property_filter_on_endpoint() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal {active: true}) \
             RETURN j.title, count(a) AS articles",
        )
        .unwrap();
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    /// Group-by variable must be the grouped endpoint — not the neighbor.
    #[test]
    fn rejects_groupby_on_neighbor() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             RETURN a.year, count(a) AS articles",
        )
        .unwrap();
        // `count(a)` grouped by `a.year` — but `a` is the counted side, so
        // the group-by doesn't match either endpoint cleanly. Reject.
        // (Note: the real "articles per year" query would group by a.year
        // without count(a), or count something else.)
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    /// Wildcard or multiple edge types need multi-degree lookups; defer.
    #[test]
    fn rejects_multi_edge_types() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN|REFERENCED_IN]->(j:Journal) \
             RETURN j.title, count(a) AS articles",
        )
        .unwrap();
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    /// No aggregate at all — not our shape.
    #[test]
    fn rejects_plain_match_return() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) RETURN j.title, a.pmid",
        )
        .unwrap();
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    // ——— Phase 3a: WITH-bound detector ———

    /// MB053 shape: MATCH-bind + WITH LIMIT + second MATCH + count.
    /// The pre-WITH cap is what makes the real MB053 tractable on PubMed;
    /// the detector must preserve the 500-row limit on the grouped scan.
    #[test]
    fn with_binding_detects_mb053_shape() {
        let q = parse_query(
            "MATCH (m:MeSHTerm) WITH m LIMIT 500 \
             MATCH (a:Article)-[:ANNOTATED_WITH]->(m) \
             RETURN m.name, count(a) AS articles ORDER BY articles DESC LIMIT 10",
        )
        .unwrap();
        let p = detect_with_binding(&q).expect("should detect");
        assert_eq!(p.core.grouped_var, "m");
        assert_eq!(p.core.grouped_label.as_str(), "MeSHTerm");
        assert_eq!(p.core.neighbor_var, "a");
        assert_eq!(p.core.neighbor_label.as_ref().unwrap().as_str(), "Article");
        assert_eq!(p.core.edge_type.as_str(), "ANNOTATED_WITH");
        assert_eq!(p.core.direction, ExpandDirection::Reverse);
        assert_eq!(p.core.count_alias, "articles");
        assert_eq!(p.grouped_scan_limit, Some(500));
        assert!(p.prefilter.is_none());
        // Single-MATCH `detect` should not also claim this query.
        assert!(detect(&q, &GraphStore::new()).is_none());
    }

    /// EX49 shape: pre-WITH WHERE filter + LIMIT.
    /// Detector must carry the WHERE predicate as a prefilter.
    #[test]
    fn with_binding_detects_ex49_shape() {
        let q = parse_query(
            "MATCH (au:Author) WHERE au.name STARTS WITH 'Smith' \
             WITH au LIMIT 100 \
             MATCH (a:Article)-[:AUTHORED_BY]->(au) \
             RETURN au.name, count(a) AS articles ORDER BY articles DESC LIMIT 10",
        )
        .unwrap();
        let p = detect_with_binding(&q).expect("should detect");
        assert_eq!(p.core.grouped_var, "au");
        assert_eq!(p.core.grouped_label.as_str(), "Author");
        assert_eq!(p.core.direction, ExpandDirection::Reverse);
        assert_eq!(p.grouped_scan_limit, Some(100));
        assert!(p.prefilter.is_some(), "WHERE on grouped side must be kept");
    }

    /// Post-WITH WHERE clause needs filter pushdown to be safe; reject.
    #[test]
    fn with_binding_rejects_post_with_where() {
        let q = parse_query(
            "MATCH (m:MeSHTerm) WITH m LIMIT 500 \
             MATCH (a:Article)-[:ANNOTATED_WITH]->(m) \
             WHERE a.year > 2020 \
             RETURN m.name, count(a) AS articles",
        )
        .unwrap();
        assert!(detect_with_binding(&q).is_none());
    }

    /// Pre-WITH WHERE that references the neighbor (which isn't bound yet)
    /// must be rejected — the predicate is effectively malformed, but
    /// conservative rejection is the right call.
    #[test]
    fn with_binding_rejects_filter_on_unbound_var() {
        let q = parse_query(
            "MATCH (m:MeSHTerm) WITH m LIMIT 500 \
             MATCH (a:Article)-[:ANNOTATED_WITH]->(m) \
             RETURN m.name, count(a) AS articles",
        )
        .unwrap();
        // Baseline: no prefilter → detects.
        assert!(detect_with_binding(&q).is_some());
    }

    /// Pre-MATCH with an edge (not just a bare node) doesn't fit the shape.
    #[test]
    fn with_binding_rejects_edge_in_pre_match() {
        let q = parse_query(
            "MATCH (m:MeSHTerm)-[:PARENT]->(p:MeSHTerm) WITH m LIMIT 500 \
             MATCH (a:Article)-[:ANNOTATED_WITH]->(m) \
             RETURN m.name, count(a) AS articles",
        )
        .unwrap();
        assert!(detect_with_binding(&q).is_none());
    }

    /// WITH renaming the variable — second MATCH can't reference the old
    /// name. We reject rather than rewrite.
    #[test]
    fn with_binding_rejects_with_renaming() {
        let q = parse_query(
            "MATCH (m:MeSHTerm) WITH m AS term LIMIT 500 \
             MATCH (a:Article)-[:ANNOTATED_WITH]->(term) \
             RETURN term.name, count(a) AS articles",
        )
        .unwrap();
        assert!(detect_with_binding(&q).is_none());
    }

    /// Multi-WITH query — out of scope for Phase 3a.
    #[test]
    fn with_binding_rejects_multi_with() {
        let q = parse_query(
            "MATCH (m:MeSHTerm) WITH m LIMIT 500 \
             MATCH (a:Article)-[:ANNOTATED_WITH]->(m) \
             WITH m, count(a) AS cnt \
             RETURN m.name, cnt",
        )
        .unwrap();
        assert!(detect_with_binding(&q).is_none());
    }

    fn q(s: &str) -> Query {
        parse_query(s).unwrap_or_else(|e| panic!("parse {}: {:?}", s, e))
    }

    fn d(s: &str) -> Option<AdjacencyAggPattern> {
        detect(&q(s), &GraphStore::new())
    }

    fn dw(s: &str) -> Option<AdjacencyAggWithBindingPattern> {
        detect_with_binding(&q(s))
    }

    fn dae(s: &str) -> Option<AggregateThenExpandPattern> {
        detect_aggregate_then_expand(&q(s), &GraphStore::new())
    }

    fn truth() -> WhereClauseAlias {
        crate::query::ast::WhereClause {
            predicate: Expression::Literal(crate::graph::PropertyValue::Boolean(true)),
        }
    }
    type WhereClauseAlias = crate::query::ast::WhereClause;

    const P1: &str = "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) RETURN j.title, count(a) AS n";

    // ——— Phase 1 detector: remaining rejection paths ———

    #[test]
    fn phase1_detects_variable_group_by_and_generated_alias() {
        let p = d("MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) RETURN j, count(a)").unwrap();
        assert_eq!(p.group_by_items, vec![("j".to_string(), None)]);
        assert_eq!(p.count_alias, "count_1");
        assert!(p.prefilter.is_none());
    }

    #[test]
    fn phase1_counting_target_groups_on_start() {
        // (j)<-(a) with count(a): grouped = j (start), Incoming => Reverse.
        let p = d("MATCH (j:Journal)<-[:PUBLISHED_IN]-(a) RETURN j.title, count(a) AS n").unwrap();
        assert_eq!(p.neighbor_label, None);
        assert_eq!(p.direction, ExpandDirection::Reverse);
        // (a)<-(j) with count(a): grouped = j (target), Incoming => Forward.
        let p = d("MATCH (a:Article)<-[:CITES]-(j:Journal) RETURN j.title, count(a) AS n").unwrap();
        assert_eq!(p.grouped_var, "j");
        assert_eq!(p.direction, ExpandDirection::Forward);
    }

    #[test]
    fn phase1_rejects_query_shapes() {
        for s in [
            // two MATCH clauses
            "MATCH (a:Article)-[:P]->(j:Journal) MATCH (x) RETURN j.title, count(a) AS n",
            // a WITH split
            "MATCH (a:Article)-[:P]->(j:Journal) WITH a, j RETURN j.title, count(a) AS n",
            // OPTIONAL MATCH
            "OPTIONAL MATCH (a:Article)-[:P]->(j:Journal) RETURN j.title, count(a) AS n",
            // two paths
            "MATCH (a:Article)-[:P]->(j:Journal), (x) RETURN j.title, count(a) AS n",
            // variable length
            "MATCH (a:Article)-[:P*1..2]->(j:Journal) RETURN j.title, count(a) AS n",
            // anonymous endpoint
            "MATCH (a:Article)-[:P]->(:Journal) RETURN count(a) AS n",
            // undirected
            "MATCH (a:Article)-[:P]-(j:Journal) RETURN j.title, count(a) AS n",
            // RETURN DISTINCT
            "MATCH (a:Article)-[:P]->(j:Journal) RETURN DISTINCT j.title, count(a) AS n",
            // two counts
            "MATCH (a:Article)-[:P]->(j:Journal) RETURN j.title, count(a) AS n, count(a) AS m",
            // count of a property
            "MATCH (a:Article)-[:P]->(j:Journal) RETURN j.title, count(a.x) AS n",
            // counting something that is not an endpoint
            "MATCH (a:Article)-[r:P]->(j:Journal) RETURN j.title, count(r) AS n",
            // no grouping key
            "MATCH (a:Article)-[:P]->(j:Journal) RETURN count(a) AS n",
            // grouped endpoint unlabelled / multi-labelled
            "MATCH (a:Article)-[:P]->(j) RETURN j.title, count(a) AS n",
            "MATCH (a:Article)-[:P]->(j:Journal:Venue) RETURN j.title, count(a) AS n",
            // neighbour multi-labelled
            "MATCH (a:Article:Paper)-[:P]->(j:Journal) RETURN j.title, count(a) AS n",
            // property on the counted side
            "MATCH (a:Article {x: 1})-[:P]->(j:Journal) RETURN j.title, count(a) AS n",
        ] {
            assert!(d(s).is_none(), "should reject: {}", s);
        }
    }

    #[test]
    fn phase1_rejects_clause_fields_set_programmatically() {
        let mut query = q(P1);
        query.with_clause = q("MATCH (m:M) WITH m LIMIT 1 MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c").with_clause;
        assert!(detect(&query, &GraphStore::new()).is_none());

        let mut query = q(P1);
        query.post_with_where_clause = Some(truth());
        assert!(detect(&query, &GraphStore::new()).is_none());

        let mut query = q(P1);
        query.unwind_clause = q("UNWIND [1] AS x RETURN x").unwind_clause;
        assert!(detect(&query, &GraphStore::new()).is_none());

        let mut query = q(P1);
        query.return_clause = None;
        assert!(detect(&query, &GraphStore::new()).is_none());

        let mut query = q(P1);
        if let Expression::Function { args, .. } = &mut query.return_clause.as_mut().unwrap().items[1].expression {
            args.push(Expression::Variable("a".into()));
        }
        assert!(detect(&query, &GraphStore::new()).is_none());
    }

    #[test]
    fn phase1_where_must_reference_only_grouped_side() {
        let p = d("MATCH (a:Article)-[:P]->(j:Journal) WHERE NOT j.hidden AND toUpper(j.title) <> 'X' AND j = j \
                   RETURN j.title, count(a) AS n")
            .expect("grouped-only WHERE accepted");
        assert!(p.prefilter.is_some());
        assert!(d("MATCH (a:Article)-[:P]->(j:Journal) WHERE CASE WHEN j.x THEN true END \
                   RETURN j.title, count(a) AS n")
            .is_none());
        assert!(d("MATCH (a:Article)-[:P]->(j:Journal) WHERE j.x = $p RETURN j.title, count(a) AS n").is_some());
    }

    // ——— Phase 3a WITH-bound detector ———

    const W: &str = "MATCH (m:MeSHTerm) WITH m LIMIT 5 MATCH (a:Article)-[:ANNOTATED_WITH]->(m) RETURN m.name, count(a) AS c";

    #[test]
    fn with_binding_grouped_as_start_and_directions() {
        let p = dw("MATCH (m:MeSHTerm) WITH m MATCH (m)-[:TAGS]->(a) RETURN m, count(a)").unwrap();
        assert_eq!(p.core.direction, ExpandDirection::Forward);
        assert_eq!(p.core.neighbor_label, None);
        assert_eq!(p.core.group_by_items, vec![("m".to_string(), None)]);
        assert_eq!(p.core.count_alias, "count_1");
        assert_eq!(p.grouped_scan_limit, None);

        let p = dw("MATCH (m:MeSHTerm) WITH m MATCH (m:MeSHTerm)<-[:TAGS]-(a) RETURN m.name, count(a) AS c").unwrap();
        assert_eq!(p.core.direction, ExpandDirection::Reverse);

        let p = dw("MATCH (m:MeSHTerm) WITH m SKIP 2 MATCH (a)<-[:TAGS]-(m) RETURN m.name, count(a) AS c").unwrap();
        assert_eq!(p.core.direction, ExpandDirection::Forward);
        assert_eq!(p.grouped_scan_skip, Some(2));

        let p = dw("MATCH (m:MeSHTerm) WITH m AS m MATCH (a)-[:TAGS]->(m) RETURN m.name, count(DISTINCT a) AS c").unwrap();
        assert!(p.core.count_distinct);
        assert_eq!(p.core.direction, ExpandDirection::Reverse);
    }

    #[test]
    fn with_binding_rejects_shapes() {
        for s in [
            // no WITH
            P1,
            // two MATCH clauses before the WITH
            "MATCH (m:MeSHTerm) MATCH (z:Z) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            // WITH carrying a second item
            "MATCH (m:MeSHTerm) WITH m, 1 AS k MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            // pre-MATCH optional
            "OPTIONAL MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            // post-MATCH optional
            "MATCH (m:MeSHTerm) WITH m OPTIONAL MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            // two pre-MATCH paths
            "MATCH (m:MeSHTerm), (z) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            // pre node without label / two labels / properties
            "MATCH (m) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            "MATCH (m:A:B) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm {k: 1}) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            // WITH DISTINCT / WHERE / ORDER BY / two items / non-variable item
            "MATCH (m:MeSHTerm) WITH DISTINCT m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m WHERE m.k = 1 MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m ORDER BY m.k MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm), (z:Z) WITH m, z MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m.k AS m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            // pre-WITH WHERE the detector cannot vet
            "MATCH (m:MeSHTerm) WHERE CASE WHEN m.k THEN true END WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c",
            // post path: two paths / two segments / var-length
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m), (q) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m)-[:Y]->(q) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X*1..3]->(m) RETURN m.name, count(a) AS c",
            // anonymous endpoint in post path
            "MATCH (m:MeSHTerm) WITH m MATCH ()-[:X]->(m) RETURN m.name, count(*) AS c",
            // grouped var not in post path
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(b) RETURN m.name, count(a) AS c",
            // edge type count / undirected
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-->(m) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]-(m) RETURN m.name, count(a) AS c",
            // grouped node properties / wrong label
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m {k: 1}) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m:Other) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m:MeSHTerm:Other) RETURN m.name, count(a) AS c",
            // neighbour properties / two labels
            "MATCH (m:MeSHTerm) WITH m MATCH (a {k: 1})-[:X]->(m) RETURN m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a:A:B)-[:X]->(m) RETURN m.name, count(a) AS c",
            // RETURN DISTINCT / two counts / count(*) / count(prop) / other expression
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN DISTINCT m.name, count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a) AS c, count(a) AS d",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(*) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(a.k) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN m.k + 1, count(a) AS c",
            // no grouping key / no count
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN count(a) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN m.name",
            // counting the grouped side / grouping on the neighbour
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN m.name, count(m) AS c",
            "MATCH (m:MeSHTerm) WITH m MATCH (a)-[:X]->(m) RETURN a.name, count(a) AS c",
        ] {
            assert!(dw(s).is_none(), "should reject: {}", s);
        }
    }

    #[test]
    fn with_binding_rejects_clause_fields_set_programmatically() {
        let mut query = q(W);
        query.with_split_index = None;
        assert!(detect_with_binding(&query).is_none());

        let mut query = q(W);
        let stage = query.with_clause.clone().unwrap();
        query.extra_with_stages.push((stage, None, vec![], None));
        assert!(detect_with_binding(&query).is_none());

        let mut query = q(W);
        query.set_clauses = q("MATCH (n) SET n.x = 1").set_clauses;
        assert!(detect_with_binding(&query).is_none());

        let mut query = q(W);
        query.with_split_index = Some(9);
        assert!(detect_with_binding(&query).is_none());

        let mut query = q(W);
        query.match_clauses[0].pattern.paths.clear();
        assert!(detect_with_binding(&query).is_none());

        let mut query = q(W);
        query.match_clauses[1].pattern.paths.clear();
        assert!(detect_with_binding(&query).is_none());

        let mut query = q(W);
        query.return_clause = None;
        assert!(detect_with_binding(&query).is_none());

        let mut query = q(W);
        if let Expression::Function { args, .. } = &mut query.return_clause.as_mut().unwrap().items[1].expression {
            args.push(Expression::Variable("a".into()));
        }
        assert!(detect_with_binding(&query).is_none());
    }

    // ——— Phase 4: aggregate-then-expand ———

    const AE: &str = "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
                      WITH j, count(a) AS c ORDER BY c DESC SKIP 1 LIMIT 3 \
                      MATCH (j)-[:EDITED_BY]->(e:Editor) RETURN j.title, c, e.name";

    #[test]
    fn aggregate_then_expand_detects_single_with() {
        let p = dae(AE).expect("should detect");
        assert_eq!(p.core.grouped_var, "j");
        assert_eq!(p.core.count_alias, "c");
        assert_eq!(p.expand_neighbor_var, "e");
        assert_eq!(p.expand_neighbor_label.as_ref().map(|l| l.as_str()), Some("Editor"));
        assert_eq!(p.expand_edge_type.as_str(), "EDITED_BY");
        assert_eq!(p.expand_direction, Direction::Outgoing);
        assert_eq!(p.post_aggregate_skip, Some(1));
        assert_eq!(p.post_aggregate_limit, Some(3));
        assert_eq!(p.post_aggregate_order_by, Some(vec![(Expression::Variable("c".into()), false)]));
        assert!(p.post_aggregate_filter.is_none());
    }

    #[test]
    fn aggregate_then_expand_directions_and_post_filter() {
        let p = dae("MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c WHERE c > 2 \
                     MATCH (e)-[:EDITS]->(j:Journal) RETURN j, c, e")
            .expect("should detect");
        assert_eq!(p.expand_direction, Direction::Incoming);
        assert_eq!(p.expand_neighbor_label, None);
        assert!(p.post_aggregate_filter.is_some());
        assert!(p.post_aggregate_order_by.is_none());

        let p = dae("MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c \
                     MATCH (j)<-[:EDITS]-(e) RETURN j, c, e")
            .unwrap();
        assert_eq!(p.expand_direction, Direction::Incoming);
        let p = dae("MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c \
                     MATCH (e)<-[:EDITS]-(j) RETURN j, c, e")
            .unwrap();
        assert_eq!(p.expand_direction, Direction::Outgoing);
    }

    #[test]
    fn aggregate_then_expand_rejects_shapes() {
        for s in [
            // no WITH
            P1,
            // post-aggregate filter on the grouped node
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c WHERE j.k = 1 MATCH (j)-[:E]->(e) RETURN j, c, e",
            // DISTINCT aggregate WITH
            "MATCH (a:Article)-[:P]->(j:Journal) WITH DISTINCT j, count(a) AS c MATCH (j)-[:E]->(e) RETURN j, c, e",
            // two MATCH clauses before / after the WITH
            "MATCH (a:Article)-[:P]->(j:Journal) MATCH (z:Z) WITH j, count(a) AS c MATCH (j)-[:E]->(e) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E]->(e) MATCH (z:Z) RETURN j, c, e, z",
            // pre-WITH WHERE
            "MATCH (a:Article)-[:P]->(j:Journal) WHERE j.k = 1 WITH j, count(a) AS c MATCH (j)-[:E]->(e) RETURN j, c, e",
            // pre-WITH not an adjacency aggregate
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, sum(a.x) AS c MATCH (j)-[:E]->(e) RETURN j, c, e",
            // post-MATCH optional / multiple paths / multi-hop / var-length / no type / undirected
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c OPTIONAL MATCH (j)-[:E]->(e) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E]->(e), (z) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E]->(e)-[:F]->(z) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E*1..2]->(e) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-->(e) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E]-(e) RETURN j, c, e",
            // anonymous neighbour / grouped var absent
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E]->() RETURN j, c",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (x)-[:E]->(e) RETURN j, c, e",
            // bound node properties / wrong label / two labels
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j {k: 1})-[:E]->(e) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j:Other)-[:E]->(e) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j:Journal:Other)-[:E]->(e) RETURN j, c, e",
            // neighbour properties / two labels
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E]->(e {k: 1}) RETURN j, c, e",
            "MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E]->(e:A:B) RETURN j, c, e",
        ] {
            assert!(dae(s).is_none(), "should reject: {}", s);
        }
    }

    #[test]
    fn aggregate_then_expand_rejects_clause_fields_set_programmatically() {
        let mut query = q(AE);
        query.post_with_where_clause = Some(truth());
        assert!(detect_aggregate_then_expand(&query, &GraphStore::new()).is_none());

        let mut query = q(AE);
        query.unwind_clause = q("UNWIND [1] AS x RETURN x").unwind_clause;
        assert!(detect_aggregate_then_expand(&query, &GraphStore::new()).is_none());

        let mut query = q(AE);
        let stage = query.with_clause.clone().unwrap();
        query.extra_with_stages.push((stage.clone(), None, vec![], None));
        query.extra_with_stages.push((stage, None, vec![], None));
        assert!(detect_aggregate_then_expand(&query, &GraphStore::new()).is_none());

        let mut query = q(AE);
        query.with_split_index = Some(9);
        assert!(detect_aggregate_then_expand(&query, &GraphStore::new()).is_none());
    }

    /// Builds the two-WITH form directly: `extra_with_stages[0]` holds the
    /// aggregating WITH, `with_clause` the pass-through that feeds the MATCH.
    fn two_stage(agg: &str, passthrough: &str) -> Query {
        let agg_q = q(&format!("MATCH (a:Article)-[:P]->(j:Journal) {} RETURN j, c", agg));
        let pt_q = q(&format!("MATCH (j), (c) {} RETURN j", passthrough));
        let mut query = q("MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c MATCH (j)-[:E]->(e) RETURN j, c, e");
        query.extra_with_stages = vec![(agg_q.with_clause.unwrap(), None, vec![], None)];
        query.with_clause = pt_q.with_clause;
        query
    }

    fn dae_q(query: &Query) -> Option<AggregateThenExpandPattern> {
        detect_aggregate_then_expand(query, &GraphStore::new())
    }

    #[test]
    fn aggregate_then_expand_two_stage_takes_limits_from_either_with() {
        let p = dae_q(&two_stage("WITH j, count(a) AS c ORDER BY c DESC", "WITH j, c SKIP 1 LIMIT 4")).expect("detect");
        assert_eq!(p.post_aggregate_order_by, Some(vec![(Expression::Variable("c".into()), false)]));
        assert_eq!(p.post_aggregate_skip, Some(1));
        assert_eq!(p.post_aggregate_limit, Some(4));

        let p = dae_q(&two_stage("WITH j, count(a) AS c SKIP 2 LIMIT 5", "WITH j AS j, c ORDER BY c")).expect("detect");
        assert_eq!(p.post_aggregate_order_by, Some(vec![(Expression::Variable("c".into()), true)]));
        assert_eq!(p.post_aggregate_skip, Some(2));
        assert_eq!(p.post_aggregate_limit, Some(5));
    }

    #[test]
    fn aggregate_then_expand_detects_parsed_two_with_query() {
        let p = dae("MATCH (a:Article)-[:P]->(j:Journal) WITH j, count(a) AS c \
                     WITH j, c ORDER BY c DESC LIMIT 4 \
                     MATCH (j)-[:E]->(e) RETURN j, c, e")
            .expect("two-WITH form should detect");
        assert_eq!(p.post_aggregate_limit, Some(4));
        assert_eq!(p.post_aggregate_order_by, Some(vec![(Expression::Variable("c".into()), false)]));
    }

    #[test]
    fn aggregate_then_expand_two_stage_rejections() {
        for (agg, pt) in [
            ("WITH j, count(a) AS c", "WITH DISTINCT j, c"),
            ("WITH j, count(a) AS c", "WITH j"),
            ("WITH j, count(a) AS c", "WITH j, c.x AS c"),
            ("WITH j, count(a) AS c", "WITH j AS k, c"),
            ("WITH j, count(a) AS c", "WITH j, c WHERE c > 1"),
            ("WITH j, count(a) AS c ORDER BY c", "WITH j, c ORDER BY c"),
            ("WITH j, count(a) AS c SKIP 1", "WITH j, c SKIP 1"),
            ("WITH j, count(a) AS c LIMIT 1", "WITH j, c LIMIT 1"),
        ] {
            assert!(dae_q(&two_stage(agg, pt)).is_none(), "should reject: {} / {}", agg, pt);
        }
    }

    #[test]
    fn aggregate_then_expand_two_stage_rejects_stage_extras() {
        let base = two_stage("WITH j, count(a) AS c", "WITH j, c");
        assert!(dae_q(&base).is_some());

        let mut query = base.clone();
        query.extra_with_stages[0].2 = q("MATCH (x) RETURN x").match_clauses;
        assert!(dae_q(&query).is_none());

        let mut query = base.clone();
        query.extra_with_stages[0].3 = Some(truth());
        assert!(dae_q(&query).is_none());

        let mut query = base.clone();
        query.extra_with_stages[0].1 = q("UNWIND [1] AS x RETURN x").unwind_clause;
        assert!(query.extra_with_stages[0].1.is_some());
        assert!(dae_q(&query).is_none());

        let mut query = base;
        query.with_clause = None;
        assert!(dae_q(&query).is_none());
    }

    #[test]
    fn expression_references_only_covers_each_kind() {
        let parse_where = |s: &str| q(&format!("MATCH (x), (y) WHERE {} RETURN x", s)).where_clause.unwrap().predicate;
        assert!(expression_references_only(&Expression::Variable("x".into()), "x"));
        assert!(!expression_references_only(&Expression::Variable("y".into()), "x"));
        assert!(expression_references_only(&parse_where("NOT x.a"), "x"));
        assert!(!expression_references_only(&parse_where("NOT y.a"), "x"));
        assert!(expression_references_only(&parse_where("toUpper(x.a) = $p"), "x"));
        assert!(!expression_references_only(&parse_where("toUpper(y.a) = 'A'"), "x"));
        assert!(!expression_references_only(&parse_where("x.l[0] = 1"), "x"));
    }

    /// Phase 1 shape must not be accidentally matched by the Phase 3
    /// detector — the two are mutually exclusive by design.
    #[test]
    fn with_binding_ignores_phase_1_shape() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             RETURN j.title, count(a) AS articles ORDER BY articles DESC LIMIT 10",
        )
        .unwrap();
        assert!(detect_with_binding(&q).is_none());
        assert!(detect(&q, &GraphStore::new()).is_some());
    }
}
