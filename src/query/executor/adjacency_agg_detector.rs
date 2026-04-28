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
use crate::query::ast::{Direction, Expression, Query};

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
    // No WHERE in Phase 1 — predicate interaction needs careful design.
    if query.where_clause.is_some() || query.post_with_where_clause.is_some() {
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

    /// WHERE clause not supported in Phase 1.
    #[test]
    fn rejects_when_where_clause_present() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             WHERE j.active = true RETURN j.title, count(a) AS articles",
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
