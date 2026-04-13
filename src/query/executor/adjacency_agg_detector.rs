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
}

/// Try to detect the adjacency-count pattern in `query`.
///
/// Returns `Some(pattern)` if all Phase 1 constraints are satisfied, `None`
/// otherwise. A `None` result means "use the standard planner path" and is
/// never a hard error — the caller treats it as a negative detection.
pub fn detect(query: &Query) -> Option<AdjacencyAggPattern> {
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
    let mut group_by_vars: Vec<String> = Vec::new();

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
            Expression::Property { variable, .. } | Expression::Variable(variable) => {
                group_by_vars.push(variable.clone());
            }
            _ => {
                // Other expressions (arithmetic, functions on non-grouped vars,
                // CASE, etc.) are out of scope for Phase 1.
                return None;
            }
        }
    }

    let (count_alias, count_arg_var, count_distinct) = count_info?;

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

    // Every group-by variable reference must target the grouped endpoint.
    for v in &group_by_vars {
        if v != &grouped_var {
            return None;
        }
    }

    // Grouped endpoint must have a single concrete label (needed for the scan).
    if grouped_node.labels.len() != 1 {
        return None;
    }
    let grouped_label = grouped_node.labels[0].clone();

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
        let p = detect(&q).expect("should detect");
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
        let p = detect(&q).expect("should detect");
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
        let p = detect(&q).expect("should detect");
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
        assert!(detect(&q).is_none());
    }

    /// Multi-hop paths use a different plan shape.
    #[test]
    fn rejects_multi_hop() {
        let q = parse_query(
            "MATCH (a:Article)-[:AUTHORED_BY]->(au:Author)-[:AFFILIATED]->(i:Institution) \
             RETURN i.name, count(a) AS articles",
        )
        .unwrap();
        assert!(detect(&q).is_none());
    }

    /// Multiple aggregates: we only handle a single count().
    #[test]
    fn rejects_multiple_aggregates() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             RETURN j.title, count(a) AS articles, avg(a.year) AS avg_year",
        )
        .unwrap();
        assert!(detect(&q).is_none());
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
        assert!(detect(&q).is_none());
    }

    /// DISTINCT count — set-cardinality semantics diverge from raw degree.
    #[test]
    fn rejects_count_distinct_in_phase_1() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             RETURN j.title, count(DISTINCT a) AS articles",
        )
        .unwrap();
        let p = detect(&q);
        // Detector still extracts it — Phase 1's lowering will reject on the
        // `count_distinct` flag. Make sure we faithfully record the modifier.
        let p = p.expect("detector preserves DISTINCT info");
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
        assert!(detect(&q).is_none());
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
        assert!(detect(&q).is_none());
    }

    /// Wildcard or multiple edge types need multi-degree lookups; defer.
    #[test]
    fn rejects_multi_edge_types() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN|REFERENCED_IN]->(j:Journal) \
             RETURN j.title, count(a) AS articles",
        )
        .unwrap();
        assert!(detect(&q).is_none());
    }

    /// No aggregate at all — not our shape.
    #[test]
    fn rejects_plain_match_return() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) RETURN j.title, a.pmid",
        )
        .unwrap();
        assert!(detect(&q).is_none());
    }
}
