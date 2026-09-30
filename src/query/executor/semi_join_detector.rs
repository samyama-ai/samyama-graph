//! Detector for the semi-join + aggregate pattern (ADR-017 Phase 3b).
//!
//! Recognizes queries of the shape:
//! ```text
//! MATCH (x:LX)-[:E1]-(y[:LY]) WHERE pred1
//! MATCH (x)-[:E2]-(z[:LZ]) WHERE pred2
//! WITH z.prop AS group_alias, count(DISTINCT x) AS count_alias
//!   [ORDER BY count_alias DESC] [LIMIT K]
//! RETURN group_alias, count_alias
//! ```
//! where `y` appears only as a filter — not in the WITH projections, ORDER BY,
//! or RETURN. Motivating case: OM27 (Depression comorbidities).
//!
//! The bottleneck in the generic planner: both MATCHes produce their full
//! tuple stream and the aggregate builds a HashSet<NodeId> per group for the
//! `count(DISTINCT x)` — on OMOP 51M Persons × their conditions this times
//! out. The semi-join rewrite computes the qualifying set of `x` once, then
//! counts how many qualify per `z.prop`.

use crate::graph::types::{EdgeType, Label};
use crate::query::ast::{BinaryOp, Direction, Expression, Query, WhereClause};
use std::collections::HashSet;

/// A predicate that survives the detector's pre-analysis, tagged with the
/// side it applies to. Lets the planner attach predicates to the correct
/// expand operator without re-inspecting every AND node.
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateSide {
    /// Predicate references only `shared_var` and/or the first MATCH's non-shared endpoint.
    First,
    /// Predicate references only `shared_var` and/or the second MATCH's non-shared endpoint.
    Second,
}

/// Parameters extracted from a Phase 3b query.
#[derive(Debug, Clone)]
pub struct SemiJoinPattern {
    /// Variable bound to the probe side (e.g. `p` in OM27 — the Person).
    pub shared_var: String,
    /// Label of the probe variable (required — used as the driving scan).
    pub shared_label: Label,

    /// First MATCH: edge type + direction relative to `shared_var`.
    pub first_edge_type: EdgeType,
    pub first_direction: Direction,
    /// Non-shared endpoint of the first MATCH (e.g. `c1`). Variable name is
    /// recorded so the filter predicates can reference it correctly.
    pub first_other_var: String,
    pub first_other_label: Option<Label>,
    /// Predicates applying to the first MATCH (shared_var and/or first_other_var).
    pub first_predicate: Option<Expression>,

    /// Second MATCH parameters — same layout.
    pub second_edge_type: EdgeType,
    pub second_direction: Direction,
    pub second_other_var: String,
    pub second_other_label: Option<Label>,
    pub second_predicate: Option<Expression>,

    /// Expression emitted as the grouping key (e.g. `c2.condition_concept`).
    pub group_by_expr: Expression,
    pub group_by_alias: String,

    /// Count alias from the WITH.
    pub count_alias: String,

    /// `ORDER BY` items (copied from the WITH; typically `[(count_alias_var, DESC)]`).
    pub order_by: Option<Vec<(Expression, bool)>>,
    /// `LIMIT` on the WITH.
    pub limit: Option<usize>,
}

/// Try to detect the Phase 3b pattern. Returns `None` on any mismatch.
pub fn detect(query: &Query) -> Option<SemiJoinPattern> {
    // Shape: exactly two MATCHes, a WITH that owns the aggregate, a final
    // RETURN. No create/set/delete/call/unwind/merge.
    if query.match_clauses.len() != 2 {
        return None;
    }
    let split = query.with_split_index?;
    if split != 2 {
        return None; // both MATCHes must be pre-WITH
    }
    if !query.extra_with_stages.is_empty() {
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
    if query.post_with_where_clause.is_some() {
        return None;
    }

    let mc1 = &query.match_clauses[0];
    let mc2 = &query.match_clauses[1];
    if mc1.optional || mc2.optional {
        return None;
    }

    // Each MATCH must have a single-path, single-segment pattern.
    if mc1.pattern.paths.len() != 1 || mc2.pattern.paths.len() != 1 {
        return None;
    }
    let p1 = &mc1.pattern.paths[0];
    let p2 = &mc2.pattern.paths[0];
    if p1.segments.len() != 1 || p2.segments.len() != 1 {
        return None;
    }
    if p1.segments[0].edge.length.is_some() || p2.segments[0].edge.length.is_some() {
        return None;
    }

    // Each path exposes two variables; we need to identify which one is
    // shared between the two matches.
    let p1_start = &p1.start;
    let p1_target = &p1.segments[0].node;
    let p2_start = &p2.start;
    let p2_target = &p2.segments[0].node;
    let p1_start_var = p1_start.variable.as_ref()?.clone();
    let p1_target_var = p1_target.variable.as_ref()?.clone();
    let p2_start_var = p2_start.variable.as_ref()?.clone();
    let p2_target_var = p2_target.variable.as_ref()?.clone();

    let m1_vars: HashSet<&str> = [p1_start_var.as_str(), p1_target_var.as_str()]
        .iter()
        .copied()
        .collect();
    let m2_vars: HashSet<&str> = [p2_start_var.as_str(), p2_target_var.as_str()]
        .iter()
        .copied()
        .collect();
    let shared: Vec<&str> = m1_vars.intersection(&m2_vars).copied().collect();
    if shared.len() != 1 {
        // Zero shared vars = disjoint matches (cartesian product).
        // Two shared = both endpoints same = effectively an ExpandInto on
        // parallel edges — different plan shape.
        return None;
    }
    let shared_var = shared[0].to_string();

    // Resolve, for each match, the "other" endpoint (not the shared one).
    let (first_other_node, first_other_var, first_shared_is_start) =
        if p1_start_var == shared_var {
            (p1_target, p1_target_var.clone(), true)
        } else {
            (p1_start, p1_start_var.clone(), false)
        };
    let (second_other_node, second_other_var, second_shared_is_start) =
        if p2_start_var == shared_var {
            (p2_target, p2_target_var.clone(), true)
        } else {
            (p2_start, p2_start_var.clone(), false)
        };

    // `y` (first_other_var) must not be referenced downstream. This is the
    // critical "filter-only" condition that makes the semi-join sound.
    if variable_appears_in_with_or_return(query, &first_other_var) {
        return None;
    }

    // Locate the shared label — must be on the shared side of at least one
    // MATCH (the driving scan needs a label).
    let shared_label = resolve_shared_label(&shared_var, p1_start, p1_target, p2_start, p2_target)?;

    // Endpoint labels on the "other" sides.
    let first_other_label = match first_other_node.labels.len() {
        0 => None,
        1 => Some(first_other_node.labels[0].clone()),
        _ => return None,
    };
    let second_other_label = match second_other_node.labels.len() {
        0 => None,
        1 => Some(second_other_node.labels[0].clone()),
        _ => return None,
    };

    // No inline property constraints — predicates must come via WHERE.
    if first_other_node.properties.is_some() || second_other_node.properties.is_some() {
        return None;
    }
    if p1_start.properties.is_some() || p1_target.properties.is_some() {
        return None;
    }
    if p2_start.properties.is_some() || p2_target.properties.is_some() {
        return None;
    }

    // Edge types: concrete single type each.
    let first_edge = &p1.segments[0].edge;
    let second_edge = &p2.segments[0].edge;
    if first_edge.types.len() != 1 || second_edge.types.len() != 1 {
        return None;
    }
    let first_edge_type = first_edge.types[0].clone();
    let second_edge_type = second_edge.types[0].clone();

    // Map AST direction + "shared is start?" to a direction relative to
    // the shared variable (the driving side of the scan).
    let first_direction = match (first_edge.direction.clone(), first_shared_is_start) {
        (Direction::Outgoing, true) => Direction::Outgoing,
        (Direction::Outgoing, false) => Direction::Incoming,
        (Direction::Incoming, true) => Direction::Incoming,
        (Direction::Incoming, false) => Direction::Outgoing,
        (Direction::Both, _) => return None,
    };
    let second_direction = match (second_edge.direction.clone(), second_shared_is_start) {
        (Direction::Outgoing, true) => Direction::Outgoing,
        (Direction::Outgoing, false) => Direction::Incoming,
        (Direction::Incoming, true) => Direction::Incoming,
        (Direction::Incoming, false) => Direction::Outgoing,
        (Direction::Both, _) => return None,
    };

    // Split the top-level WHERE on AND into per-side predicates. A predicate
    // is eligible for MATCH-N if it only references shared_var and/or
    // MATCH-N's other_var. A predicate that touches both matches' other_vars
    // is "cross-match" and we don't support it here.
    let (first_predicate, second_predicate) = split_where(
        &query.where_clause,
        &shared_var,
        &first_other_var,
        &second_other_var,
    )?;

    // WITH clause must own the aggregate: exactly one group-by projection on
    // second_other_var (or a property of it) and exactly one count aggregate
    // over shared_var. No other items, no distinct, no where.
    let with_clause = query.with_clause.as_ref()?;
    if with_clause.distinct
        || with_clause.where_clause.is_some()
        || with_clause.items.len() != 2
    {
        return None;
    }

    let mut group_by: Option<(Expression, String)> = None;
    let mut count_info: Option<(String, bool)> = None;
    for (i, item) in with_clause.items.iter().enumerate() {
        match &item.expression {
            Expression::Function {
                name,
                args,
                distinct,
            } if name.eq_ignore_ascii_case("count") => {
                if count_info.is_some() || args.len() != 1 {
                    return None;
                }
                match &args[0] {
                    Expression::Variable(v) if v == &shared_var => {}
                    _ => return None, // must count the shared variable
                }
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("count_{}", i));
                count_info = Some((alias, *distinct));
            }
            expr @ (Expression::Property { .. } | Expression::Variable(_)) => {
                // Group-by side. Must reference only `second_other_var`.
                if !expression_references_only(expr, &second_other_var) {
                    return None;
                }
                let alias = item.alias.clone().unwrap_or_else(|| format!("g_{}", i));
                if group_by.is_some() {
                    return None;
                }
                group_by = Some((expr.clone(), alias));
            }
            _ => return None,
        }
    }
    let (group_by_expr, group_by_alias) = group_by?;
    let (count_alias, _count_distinct) = count_info?;
    // Whether DISTINCT was written or not doesn't matter for our rewrite:
    // our plan inserts an explicit DISTINCT on shared_var before the second
    // MATCH, so the downstream count is guaranteed to be per-unique-x.

    // RETURN: must just project the WITH aliases. No further transformation.
    let ret = query.return_clause.as_ref()?;
    if ret.distinct {
        return None;
    }
    for item in &ret.items {
        match &item.expression {
            Expression::Variable(v) if v == &group_by_alias || v == &count_alias => {}
            _ => return None,
        }
    }

    let order_by = with_clause.order_by.as_ref().map(|ob| {
        ob.items
            .iter()
            .map(|i| (i.expression.clone(), i.ascending))
            .collect::<Vec<_>>()
    });

    Some(SemiJoinPattern {
        shared_var,
        shared_label,
        first_edge_type,
        first_direction,
        first_other_var,
        first_other_label,
        first_predicate,
        second_edge_type,
        second_direction,
        second_other_var,
        second_other_label,
        second_predicate,
        group_by_expr,
        group_by_alias,
        count_alias,
        order_by,
        limit: with_clause.limit,
    })
}

/// Find the shared variable's label by scanning both MATCHes for a node
/// pattern that names the shared variable AND carries a single label.
/// Both MATCHes should agree; we accept if any of the four references
/// has a label (and they're all consistent).
fn resolve_shared_label(
    shared_var: &str,
    p1_start: &crate::query::ast::NodePattern,
    p1_target: &crate::query::ast::NodePattern,
    p2_start: &crate::query::ast::NodePattern,
    p2_target: &crate::query::ast::NodePattern,
) -> Option<Label> {
    let mut found: Option<Label> = None;
    for node in [p1_start, p1_target, p2_start, p2_target] {
        if node.variable.as_deref() == Some(shared_var) && !node.labels.is_empty() {
            if node.labels.len() > 1 {
                return None;
            }
            match &found {
                None => found = Some(node.labels[0].clone()),
                Some(existing) => {
                    if existing != &node.labels[0] {
                        return None;
                    }
                }
            }
        }
    }
    found
}

/// Return true if `var` appears anywhere downstream of the two MATCHes:
/// the WITH clause's items / order_by, or the RETURN clause.
fn variable_appears_in_with_or_return(query: &Query, var: &str) -> bool {
    if let Some(wc) = &query.with_clause {
        for item in &wc.items {
            if expression_references(&item.expression, var) {
                return true;
            }
        }
        if let Some(ob) = &wc.order_by {
            for item in &ob.items {
                if expression_references(&item.expression, var) {
                    return true;
                }
            }
        }
    }
    if let Some(rc) = &query.return_clause {
        for item in &rc.items {
            if expression_references(&item.expression, var) {
                return true;
            }
        }
    }
    false
}

/// Walk an expression tree looking for any reference to `var`.
fn expression_references(expr: &Expression, var: &str) -> bool {
    match expr {
        Expression::Variable(v) => v == var,
        Expression::Property { variable, .. } => variable == var,
        Expression::PathVariable(v) => v == var,
        Expression::Literal(_) | Expression::Parameter(_) => false,
        Expression::Binary { left, right, .. } => {
            expression_references(left, var) || expression_references(right, var)
        }
        Expression::Unary { expr, .. } => expression_references(expr, var),
        Expression::Function { args, .. } => {
            args.iter().any(|a| expression_references(a, var))
        }
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand.as_deref().map_or(false, |e| expression_references(e, var))
                || when_clauses.iter().any(|(c, r)| {
                    expression_references(c, var) || expression_references(r, var)
                })
                || else_result
                    .as_deref()
                    .map_or(false, |e| expression_references(e, var))
        }
        Expression::Index { expr, index } => {
            expression_references(expr, var) || expression_references(index, var)
        }
        Expression::ListSlice { expr, start, end } => {
            expression_references(expr, var)
                || start.as_deref().map_or(false, |e| expression_references(e, var))
                || end.as_deref().map_or(false, |e| expression_references(e, var))
        }
        // Subquery / comprehension / predicate expressions: be conservative
        // — say "yes" so we reject rather than misclassify.
        _ => true,
    }
}

/// Return true if every variable reference in `expr` is one of the
/// allowed set. Uses a whitelist that matches what the planner will
/// later evaluate correctly.
fn expression_references_only(expr: &Expression, allowed_var: &str) -> bool {
    references_only_allowed(expr, &[allowed_var])
}

fn references_only_allowed(expr: &Expression, allowed: &[&str]) -> bool {
    match expr {
        Expression::Variable(v) => allowed.contains(&v.as_str()),
        Expression::Property { variable, .. } => allowed.contains(&variable.as_str()),
        Expression::Literal(_) | Expression::Parameter(_) => true,
        Expression::Binary { left, right, .. } => {
            references_only_allowed(left, allowed) && references_only_allowed(right, allowed)
        }
        Expression::Unary { expr, .. } => references_only_allowed(expr, allowed),
        Expression::Function { args, .. } => {
            args.iter().all(|a| references_only_allowed(a, allowed))
        }
        _ => false,
    }
}

/// Split a flat AND-chain WHERE into predicates tagged by which MATCH they
/// apply to. Returns `(first_predicate, second_predicate)`.
/// A predicate that references only `shared_var` is assigned to the first
/// MATCH (where it can short-circuit earliest). A predicate that touches
/// both other-vars is unsupportable in this rewrite — return `None`.
fn split_where(
    where_clause: &Option<WhereClause>,
    shared_var: &str,
    first_other: &str,
    second_other: &str,
) -> Option<(Option<Expression>, Option<Expression>)> {
    let wc = match where_clause {
        Some(w) => w,
        None => return Some((None, None)),
    };
    let flat = flatten_and(&wc.predicate);
    let mut first: Option<Expression> = None;
    let mut second: Option<Expression> = None;
    let first_allow = [shared_var, first_other];
    let second_allow = [shared_var, second_other];
    for pred in flat {
        let allowed_first = references_only_allowed(&pred, &first_allow);
        let allowed_second = references_only_allowed(&pred, &second_allow);
        let tag = match (allowed_first, allowed_second) {
            (true, _) => PredicateSide::First,
            (_, true) => PredicateSide::Second,
            _ => return None, // predicate spans both matches — unsupported
        };
        match tag {
            PredicateSide::First => {
                first = Some(match first.take() {
                    Some(prev) => and_(prev, pred),
                    None => pred,
                });
            }
            PredicateSide::Second => {
                second = Some(match second.take() {
                    Some(prev) => and_(prev, pred),
                    None => pred,
                });
            }
        }
    }
    Some((first, second))
}

fn flatten_and(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut v = flatten_and(left);
            v.extend(flatten_and(right));
            v
        }
        other => vec![other.clone()],
    }
}

fn and_(l: Expression, r: Expression) -> Expression {
    Expression::Binary {
        left: Box::new(l),
        op: BinaryOp::And,
        right: Box::new(r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse_query;

    /// OM27 shape (the motivating case).
    #[test]
    fn detects_om27_depression_comorbidities() {
        let q = parse_query(
            "MATCH (p:Person)-[:HAS_CONDITION]->(c1:ConditionOccurrence) \
             WHERE c1.condition_concept IS NOT NULL AND c1.condition_concept CONTAINS 'Depression' \
             MATCH (p)-[:HAS_CONDITION]->(c2:ConditionOccurrence) \
             WHERE c2.condition_concept IS NOT NULL AND NOT (c2.condition_concept CONTAINS 'Depression') \
             WITH c2.condition_concept AS condition, count(DISTINCT p) AS patients \
             ORDER BY patients DESC LIMIT 10 RETURN condition, patients",
        )
        .unwrap();
        let p = detect(&q).expect("OM27 should be detected");
        assert_eq!(p.shared_var, "p");
        assert_eq!(p.shared_label.as_str(), "Person");
        assert_eq!(p.first_other_var, "c1");
        assert_eq!(p.second_other_var, "c2");
        assert_eq!(p.first_edge_type.as_str(), "HAS_CONDITION");
        assert_eq!(p.second_edge_type.as_str(), "HAS_CONDITION");
        assert_eq!(p.first_direction, Direction::Outgoing);
        assert_eq!(p.second_direction, Direction::Outgoing);
        assert_eq!(p.group_by_alias, "condition");
        assert_eq!(p.count_alias, "patients");
        assert_eq!(p.limit, Some(10));
        assert!(p.first_predicate.is_some());
        assert!(p.second_predicate.is_some());
    }

    /// When the first MATCH's non-shared variable IS referenced downstream,
    /// the semi-join rewrite would lose information — reject.
    #[test]
    fn rejects_when_first_match_other_var_appears_downstream() {
        let q = parse_query(
            "MATCH (p:Person)-[:HAS_CONDITION]->(c1:ConditionOccurrence) \
             WHERE c1.condition_concept CONTAINS 'Depression' \
             MATCH (p)-[:HAS_CONDITION]->(c2:ConditionOccurrence) \
             WHERE NOT (c2.condition_concept CONTAINS 'Depression') \
             WITH c1.condition_concept AS d_concept, c2.condition_concept AS condition, count(DISTINCT p) AS patients \
             RETURN d_concept, condition, patients",
        )
        .unwrap();
        // c1 appears in the WITH projections → rewrite would drop it wrongly.
        assert!(detect(&q).is_none());
    }

    /// MATCHes with no shared variable aren't this pattern — they'd be a
    /// cartesian product, not a semi-join.
    #[test]
    fn rejects_disjoint_matches() {
        let q = parse_query(
            "MATCH (a:Article) WHERE a.year = 2024 \
             MATCH (b:Book) WHERE b.year = 2024 \
             WITH a.year AS year, count(DISTINCT b) AS books \
             RETURN year, books",
        )
        .unwrap();
        assert!(detect(&q).is_none());
    }

    /// Single MATCH — handled by Phase 1, not us.
    #[test]
    fn rejects_single_match() {
        let q = parse_query(
            "MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) \
             WITH j.title AS t, count(DISTINCT a) AS c RETURN t, c",
        )
        .unwrap();
        assert!(detect(&q).is_none());
    }

    /// Three MATCHes — out of scope for this phase.
    #[test]
    fn rejects_three_matches() {
        let q = parse_query(
            "MATCH (p:Person)-[:HAS]->(c1:Condition) \
             MATCH (p)-[:HAS]->(c2:Condition) \
             MATCH (p)-[:HAS]->(c3:Condition) \
             WITH c3.name AS n, count(DISTINCT p) AS c RETURN n, c",
        )
        .unwrap();
        assert!(detect(&q).is_none());
    }

    /// If WHERE contains a predicate that touches BOTH non-shared variables,
    /// it can't be pushed to either MATCH — we conservatively reject.
    #[test]
    fn rejects_cross_match_predicate() {
        let q = parse_query(
            "MATCH (p:Person)-[:HAS]->(c1:Condition) \
             MATCH (p)-[:HAS]->(c2:Condition) \
             WHERE c1.year = c2.year \
             WITH c2.name AS n, count(DISTINCT p) AS c RETURN n, c",
        )
        .unwrap();
        assert!(detect(&q).is_none());
    }

    fn q(s: &str) -> Query {
        parse_query(s).unwrap_or_else(|e| panic!("parse {}: {:?}", s, e))
    }

    /// A minimal query the detector accepts; each rejection test perturbs it.
    const BASE: &str = "MATCH (p:Person)-[:HAS]->(c1:Condition) \
                        MATCH (p)-[:HAS]->(c2:Condition) \
                        WITH c2.name AS n, count(DISTINCT p) AS c RETURN n, c";

    fn var(s: &str) -> Expression {
        Expression::Variable(s.to_string())
    }

    fn prop(v: &str, p: &str) -> Expression {
        Expression::Property { variable: v.to_string(), property: p.to_string() }
    }

    #[test]
    fn base_query_is_detected_without_predicates() {
        let p = detect(&q(BASE)).expect("base shape");
        assert_eq!(p.shared_var, "p");
        assert!(p.first_predicate.is_none());
        assert!(p.second_predicate.is_none());
        assert!(p.order_by.is_none());
        assert_eq!(p.limit, None);
        assert_eq!(p.first_other_label.as_ref().map(|l| l.as_str()), Some("Condition"));
        assert_eq!(p.group_by_expr, prop("c2", "name"));
    }

    #[test]
    fn detects_reversed_endpoints_and_directions() {
        // Shared var is the *target* of both paths, written with both arrow styles.
        let p = detect(&q(
            "MATCH (c1)-[:HAS]->(p:Person) \
             MATCH (c2:Condition)<-[:OWNS]-(p) \
             WITH c2 AS cond, count(p) AS c ORDER BY c DESC RETURN cond, c",
        ))
        .expect("should detect");
        assert_eq!(p.first_other_var, "c1");
        assert_eq!(p.first_other_label, None);
        assert_eq!(p.first_direction, Direction::Incoming);
        assert_eq!(p.second_other_var, "c2");
        assert_eq!(p.second_direction, Direction::Outgoing);
        assert_eq!(p.group_by_expr, var("c2"));
        assert_eq!(p.order_by, Some(vec![(var("c"), false)]));
    }

    #[test]
    fn detects_incoming_arrow_on_shared_start_and_outgoing_on_shared_end() {
        let p = detect(&q(
            "MATCH (p:Person)<-[:A]-(c1) \
             MATCH (c2)-[:B]->(p) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .expect("should detect");
        assert_eq!(p.first_direction, Direction::Incoming);
        assert_eq!(p.second_direction, Direction::Incoming);
    }

    #[test]
    fn detects_remaining_direction_combinations() {
        let p = detect(&q(
            "MATCH (c1)<-[:A]-(p:Person) \
             MATCH (p)<-[:B]-(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .expect("should detect");
        assert_eq!(p.first_direction, Direction::Outgoing);
        assert_eq!(p.second_direction, Direction::Incoming);
    }

    #[test]
    fn rejects_count_of_the_second_endpoint() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(c2) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn predicates_are_split_by_side_and_anded() {
        let p = detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1:Condition) \
             MATCH (p)-[:HAS]->(c2:Condition) \
             WHERE p.age > 18 AND c1.x = 1 AND c2.y = 2 AND c2.z = 3 \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .expect("should detect");
        // Shared-only and first-side predicates go to the first MATCH.
        let first = p.first_predicate.expect("first");
        assert!(matches!(first, Expression::Binary { op: BinaryOp::And, .. }));
        let second = p.second_predicate.expect("second");
        assert!(matches!(second, Expression::Binary { op: BinaryOp::And, .. }));
    }

    #[test]
    fn unaliased_projections_get_generated_aliases() {
        let mut query = q(BASE);
        for item in &mut query.with_clause.as_mut().unwrap().items {
            item.alias = None;
        }
        query.return_clause.as_mut().unwrap().items[0].expression = var("g_0");
        query.return_clause.as_mut().unwrap().items[1].expression = var("count_1");
        let p = detect(&query).expect("should detect");
        assert_eq!(p.group_by_alias, "g_0");
        assert_eq!(p.count_alias, "count_1");
    }

    #[test]
    fn rejects_when_split_is_not_after_both_matches() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) WITH p \
             MATCH (p)-[:HAS]->(c2) RETURN c2.name, count(p)",
        ))
        .is_none());
        let mut query = q(BASE);
        query.with_split_index = None;
        assert!(detect(&query).is_none());
        query.with_split_index = Some(1);
        assert!(detect(&query).is_none());
    }

    #[test]
    fn rejects_extra_with_stages() {
        let mut query = q(BASE);
        let stage = query.with_clause.clone().unwrap();
        query.extra_with_stages.push((stage, None, vec![], None));
        assert!(detect(&query).is_none());
    }

    #[test]
    fn rejects_write_or_call_clauses() {
        let mut query = q(BASE);
        query.set_clauses = q("MATCH (n) SET n.x = 1").set_clauses;
        assert!(!query.set_clauses.is_empty());
        assert!(detect(&query).is_none());

        let mut query = q(BASE);
        query.unwind_clause = q("UNWIND [1] AS x RETURN x").unwind_clause;
        assert!(query.unwind_clause.is_some());
        assert!(detect(&query).is_none());
    }

    #[test]
    fn rejects_post_with_where() {
        let mut query = q(BASE);
        query.post_with_where_clause = Some(WhereClause { predicate: Expression::Literal(crate::graph::PropertyValue::Boolean(true)) });
        assert!(detect(&query).is_none());
    }

    #[test]
    fn rejects_optional_match() {
        let mut query = q(BASE);
        query.match_clauses[1].optional = true;
        assert!(detect(&query).is_none());
    }

    #[test]
    fn rejects_multiple_paths_in_a_match() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1), (x) \
             MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn rejects_multi_hop_and_var_length() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1)-[:X]->(d) \
             MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS*1..2]->(c1) \
             MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn rejects_anonymous_endpoint() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->() \
             MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn rejects_two_shared_variables() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c) \
             MATCH (p)-[:LIKES]->(c) \
             WITH c.name AS n, count(p) AS k RETURN n, k",
        ))
        .is_none());
    }

    #[test]
    fn rejects_unlabelled_or_multi_labelled_shared_variable() {
        assert!(detect(&q(
            "MATCH (p)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
        assert!(detect(&q(
            "MATCH (p:Person:Patient)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p:Patient)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn accepts_consistent_shared_label_repeated() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p:Person)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_some());
    }

    #[test]
    fn rejects_multi_labelled_other_endpoints() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1:A:B) MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2:A:B) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn second_other_label_is_recorded() {
        let p = detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2:Drug) \
             WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .unwrap();
        assert_eq!(p.second_other_label.as_ref().map(|l| l.as_str()), Some("Drug"));
    }

    #[test]
    fn rejects_inline_properties_anywhere() {
        for s in [
            "MATCH (p:Person)-[:HAS]->(c1 {x: 1}) MATCH (p)-[:HAS]->(c2) WITH c2.name AS n, count(p) AS c RETURN n, c",
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2 {x: 1}) WITH c2.name AS n, count(p) AS c RETURN n, c",
            "MATCH (p:Person {x: 1})-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) WITH c2.name AS n, count(p) AS c RETURN n, c",
            "MATCH (c1)-[:HAS]->(p:Person {x: 1}) MATCH (p)-[:HAS]->(c2) WITH c2.name AS n, count(p) AS c RETURN n, c",
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p {x: 1})-[:HAS]->(c2) WITH c2.name AS n, count(p) AS c RETURN n, c",
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (c2)-[:HAS]->(p {x: 1}) WITH c2.name AS n, count(p) AS c RETURN n, c",
        ] {
            assert!(detect(&q(s)).is_none(), "{}", s);
        }
    }

    #[test]
    fn rejects_untyped_or_multi_typed_edges() {
        assert!(detect(&q(
            "MATCH (p:Person)-->(c1) MATCH (p)-[:HAS]->(c2) WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:A|B]->(c2) WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn rejects_undirected_edges() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]-(c1) MATCH (p)-[:HAS]->(c2) WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]-(c2) WITH c2.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn rejects_with_distinct_where_or_extra_items() {
        let mut query = q(BASE);
        query.with_clause.as_mut().unwrap().distinct = true;
        assert!(detect(&query).is_none());

        let mut query = q(BASE);
        query.with_clause.as_mut().unwrap().where_clause =
            Some(WhereClause { predicate: Expression::Literal(crate::graph::PropertyValue::Boolean(true)) });
        assert!(detect(&query).is_none());

        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, c2.code AS k, count(p) AS c RETURN n, k, c",
        ))
        .is_none());
    }

    #[test]
    fn rejects_bad_count_shapes() {
        // Two counts.
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH count(p) AS a, count(p) AS b RETURN a, b",
        ))
        .is_none());
        // count(*) — not a variable.
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(*) AS c RETURN n, c",
        ))
        .is_none());
        // count with two args is not produced by the parser; build it.
        let mut query = q(BASE);
        if let Expression::Function { args, .. } = &mut query.with_clause.as_mut().unwrap().items[1].expression {
            args.push(var("p"));
        }
        assert!(detect(&query).is_none());
    }

    #[test]
    fn rejects_group_by_that_is_not_the_second_endpoint() {
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH p.name AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
        // Two group-by items on c2.
        let mut query = q(BASE);
        query.with_clause.as_mut().unwrap().items[1].expression = prop("c2", "other");
        assert!(detect(&query).is_none());
        // A non-projection expression.
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH c2.a + 1 AS n, count(p) AS c RETURN n, c",
        ))
        .is_none());
    }

    #[test]
    fn rejects_with_missing_group_by_or_count() {
        let mut query = q(BASE);
        query.with_clause.as_mut().unwrap().items[1].expression = var("x");
        // Now two group-by candidates, `x` not on c2 -> rejected earlier.
        assert!(detect(&query).is_none());

        // Only a count and a literal: the literal is not a projection.
        let mut query = q(BASE);
        query.with_clause.as_mut().unwrap().items[0].expression = Expression::Literal(crate::graph::PropertyValue::Integer(1));
        assert!(detect(&query).is_none());
    }

    #[test]
    fn rejects_return_shapes_other_than_aliases() {
        let mut query = q(BASE);
        query.return_clause.as_mut().unwrap().distinct = true;
        assert!(detect(&query).is_none());
        assert!(detect(&q(
            "MATCH (p:Person)-[:HAS]->(c1) MATCH (p)-[:HAS]->(c2) \
             WITH c2.name AS n, count(p) AS c RETURN n, c + 1",
        ))
        .is_none());
        let mut query = q(BASE);
        query.return_clause = None;
        assert!(detect(&query).is_none());
        let mut query = q(BASE);
        query.with_clause = None;
        assert!(detect(&query).is_none());
    }

    #[test]
    fn rejects_first_other_var_in_with_order_by_or_return() {
        let mut query = q(BASE);
        query.with_clause.as_mut().unwrap().order_by = Some(crate::query::ast::OrderByClause {
            items: vec![crate::query::ast::OrderByItem { expression: prop("c1", "x"), ascending: true }],
        });
        assert!(detect(&query).is_none());

        let mut query = q(BASE);
        query.return_clause.as_mut().unwrap().items[0].expression = prop("c1", "x");
        assert!(detect(&query).is_none());

        // An ORDER BY that does not mention c1 is fine.
        let mut query = q(BASE);
        query.with_clause.as_mut().unwrap().order_by = Some(crate::query::ast::OrderByClause {
            items: vec![crate::query::ast::OrderByItem { expression: var("c"), ascending: true }],
        });
        assert_eq!(detect(&query).unwrap().order_by, Some(vec![(var("c"), true)]));
    }

    #[test]
    fn expression_references_walks_every_supported_node() {
        use crate::graph::PropertyValue;
        let lit = Expression::Literal(PropertyValue::Integer(1));
        assert!(expression_references(&Expression::PathVariable("x".into()), "x"));
        assert!(!expression_references(&Expression::PathVariable("y".into()), "x"));
        assert!(!expression_references(&lit, "x"));
        assert!(!expression_references(&Expression::Parameter("x".into()), "x"));

        let parse_where = |s: &str| q(&format!("MATCH (x), (y) WHERE {} RETURN x", s)).where_clause.unwrap().predicate;
        assert!(expression_references(&parse_where("NOT x.a"), "x"));
        assert!(!expression_references(&parse_where("NOT y.a"), "x"));
        assert!(expression_references(&parse_where("toUpper(x.a) = 'A'"), "x"));
        assert!(expression_references(&parse_where("CASE x.a WHEN 1 THEN true END"), "x"));
        assert!(expression_references(&parse_where("CASE WHEN y.a = 1 THEN x.b END"), "x"));
        assert!(expression_references(&parse_where("CASE WHEN y.a = 1 THEN true ELSE x.b END"), "x"));
        assert!(!expression_references(&parse_where("CASE WHEN y.a = 1 THEN true ELSE false END"), "x"));
        assert!(expression_references(&parse_where("y.l[x.i] = 1"), "x"));
        assert!(!expression_references(&parse_where("y.l[0] = 1"), "x"));
        assert!(expression_references(&parse_where("size(y.l[x.i..]) = 1"), "x"));
        assert!(expression_references(&parse_where("size(y.l[..x.i]) = 1"), "x"));
        assert!(!expression_references(&parse_where("size(y.l[1..2]) = 1"), "x"));
        let open_slice = Expression::ListSlice { expr: Box::new(var("y")), start: None, end: None };
        assert!(!expression_references(&open_slice, "x"));
        // Anything else is conservatively a reference.
        assert!(expression_references(&parse_where("any(z IN y.l WHERE z = 1)"), "x"));
    }

    #[test]
    fn references_only_allowed_rejects_unknown_expression_kinds() {
        use crate::graph::PropertyValue;
        let parse_where = |s: &str| q(&format!("MATCH (x), (y) WHERE {} RETURN x", s)).where_clause.unwrap().predicate;
        assert!(references_only_allowed(&Expression::Parameter("p".into()), &["x"]));
        assert!(references_only_allowed(&Expression::Literal(PropertyValue::Null), &[]));
        assert!(references_only_allowed(&parse_where("NOT x.a"), &["x"]));
        assert!(!references_only_allowed(&parse_where("NOT y.a"), &["x"]));
        assert!(references_only_allowed(&parse_where("toUpper(x.a) = 'A'"), &["x"]));
        assert!(!references_only_allowed(&parse_where("CASE WHEN x.a THEN 1 END = 1"), &["x"]));
        assert!(!references_only_allowed(&var("z"), &["x", "y"]));
        assert!(expression_references_only(&prop("x", "a"), "x"));
    }

    #[test]
    fn split_where_without_clause_is_empty() {
        assert_eq!(split_where(&None, "p", "a", "b"), Some((None, None)));
    }

    #[test]
    fn predicate_side_variants_compare() {
        assert_eq!(PredicateSide::First, PredicateSide::First);
        assert_ne!(PredicateSide::First, PredicateSide::Second);
    }

    /// The aggregate must be over the shared variable — not the group-by side.
    #[test]
    fn rejects_count_on_wrong_variable() {
        let q = parse_query(
            "MATCH (p:Person)-[:HAS]->(c1:Condition) \
             MATCH (p)-[:HAS]->(c2:Condition) \
             WITH c2.name AS n, count(c1) AS c RETURN n, c",
        )
        .unwrap();
        assert!(detect(&q).is_none());
    }
}
