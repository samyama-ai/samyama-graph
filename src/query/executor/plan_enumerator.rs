//! Plan enumerator: generates candidate logical plans for a MATCH pattern (ADR-015)
//!
//! For each pattern node as a starting point, builds a plan by BFS through the
//! pattern graph. At each expansion step, chooses direction based on catalog
//! statistics. Both-endpoints-bound expansions become ExpandInto.
//! Applicable WHERE predicates are pushed after each expansion.

use std::collections::HashSet;
use crate::graph::catalog::GraphCatalog;
use crate::graph::types::{Label, EdgeType};
use crate::index::manager::IndexManager;
use crate::query::ast::{WhereClause, Expression, BinaryOp};
use super::cost_model::estimate_plan_cost;
use super::logical_plan::{LogicalPlanNode, PatternGraph, PatternEdge, ExpandDirection};
use super::logical_optimizer::optimize;

/// Configuration for plan enumeration
#[derive(Debug, Clone)]
pub struct EnumerationConfig {
    /// Maximum number of candidate plans to evaluate
    pub max_candidate_plans: usize,
}

impl Default for EnumerationConfig {
    fn default() -> Self {
        Self { max_candidate_plans: 64 }
    }
}

/// Enumerate candidate plans, score them, and return sorted by cost (cheapest first).
///
/// Each candidate starts from a different pattern node as the entry point.
/// The planner uses BFS through the pattern graph, choosing Expand direction
/// based on catalog statistics (avg_out_degree vs avg_in_degree).
pub fn enumerate_plans(
    pattern: &PatternGraph,
    where_clause: Option<&WhereClause>,
    catalog: &GraphCatalog,
    index: &IndexManager,
    config: &EnumerationConfig,
) -> Vec<(LogicalPlanNode, f64)> {
    let mut candidates: Vec<(LogicalPlanNode, f64)> = Vec::new();

    // Flatten WHERE into individual AND predicates
    let mut predicates = where_clause
        .map(|wc| flatten_and_predicates(&wc.predicate))
        .unwrap_or_default();

    // Inline node properties, e.g. `(n:Person {name: 'Alice'})`, are treated as
    // additional equality predicates so they participate in index selection the
    // same way an equivalent WHERE clause would.
    let mut node_names: Vec<&String> = pattern.nodes.keys().collect();
    node_names.sort();
    for var_name in &node_names {
        let node = &pattern.nodes[*var_name];
        for (prop, val) in &node.properties {
            predicates.push(Expression::Binary {
                left: Box::new(Expression::Property {
                    variable: (*var_name).clone(),
                    property: prop.clone(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(Expression::Literal(val.clone())),
            });
        }
    }

    // For each node as a potential starting point
    for var_name in node_names {
        if candidates.len() >= config.max_candidate_plans {
            break;
        }

        let plan = build_plan_from_start(var_name, pattern, &predicates, catalog, index);
        let optimized = optimize(plan);
        let cost = estimate_plan_cost(&optimized, catalog);
        candidates.push((optimized, cost));
    }

    // Sort by cost (cheapest first)
    candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    // Trim to max
    candidates.truncate(config.max_candidate_plans);
    candidates
}

/// Build a logical plan starting from a specific pattern node.
///
/// Uses BFS through the pattern graph. At each step:
/// 1. If both endpoints are already bound → ExpandInto
/// 2. Otherwise → Expand with direction chosen by catalog stats
/// 3. Push applicable filter predicates after each expansion
fn build_plan_from_start(
    start_var: &str,
    pattern: &PatternGraph,
    predicates: &[Expression],
    catalog: &GraphCatalog,
    index: &IndexManager,
) -> LogicalPlanNode {
    let start_node = &pattern.nodes[start_var];
    let label = start_node.labels.first().cloned();

    let mut used_predicates: HashSet<usize> = HashSet::new();

    // Try to satisfy the start node via an index lookup before falling back to a
    // full LabelScan. Matches both `n.prop OP literal` and `literal OP n.prop`.
    let mut plan = None;
    if let Some(l) = &label {
        for (i, pred) in predicates.iter().enumerate() {
            if let Some((property, op, value)) = normalize_index_predicate(pred, start_var) {
                if matches!(op, BinaryOp::Eq | BinaryOp::Gt | BinaryOp::Ge | BinaryOp::Lt | BinaryOp::Le)
                    && index.has_index(l, &property)
                {
                    plan = Some(LogicalPlanNode::IndexLookup {
                        variable: start_var.to_string(),
                        label: l.clone(),
                        property,
                        op: op.clone(),
                        value,
                    });
                    used_predicates.insert(i);
                    break;
                }
            }
        }
    }
    let mut plan = plan.unwrap_or_else(|| LogicalPlanNode::LabelScan {
        variable: start_var.to_string(),
        label,
    });

    // Push any predicates that only reference the start variable
    plan = push_applicable_predicates(plan, predicates, &mut used_predicates);

    // BFS through the pattern — process edges, checking visited at each step
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(start_var.to_string());
    let mut processed_edges: HashSet<usize> = HashSet::new();

    let mut frontier: Vec<String> = vec![start_var.to_string()];

    while !frontier.is_empty() {
        let mut next_frontier = Vec::new();

        for current_var in &frontier {
            // Get ALL neighbor edges (including to already-visited nodes for ExpandInto)
            let neighbors = pattern.neighbors(current_var);

            for (edge_idx, edge) in neighbors {
                if processed_edges.contains(&edge_idx) {
                    continue;
                }

                let other_var = if edge.source_var == *current_var {
                    &edge.target_var
                } else {
                    &edge.source_var
                };

                if visited.contains(other_var) {
                    // Both endpoints bound → ExpandInto
                    processed_edges.insert(edge_idx);
                    plan = LogicalPlanNode::ExpandInto {
                        input: Box::new(plan),
                        source_var: edge.source_var.clone(),
                        target_var: edge.target_var.clone(),
                        edge_types: edge.edge_types.clone(),
                        edge_var: edge.edge_var.clone(),
                    };
                } else {
                    // One endpoint bound → Expand with direction choice
                    processed_edges.insert(edge_idx);
                    let direction = choose_direction(current_var, edge);

                    // source_var is always the BOUND variable (current_var),
                    // target_var is always the NEW variable (other_var).
                    // Direction tells the physical operator how to traverse.
                    plan = LogicalPlanNode::Expand {
                        input: Box::new(plan),
                        source_var: current_var.clone(),
                        target_var: other_var.clone(),
                        edge_var: edge.edge_var.clone(),
                        edge_types: edge.edge_types.clone(),
                        direction,
                    };

                    visited.insert(other_var.clone());
                    next_frontier.push(other_var.clone());
                }

                // Push applicable predicates
                plan = push_applicable_predicates(plan, predicates, &mut used_predicates);
            }
        }

        frontier = next_frontier;
    }

    // Handle disconnected pattern components — any unvisited nodes get CartesianProduct
    for (var_name, node) in &pattern.nodes {
        if !visited.contains(var_name) {
            let label = node.labels.first().cloned();
            let scan = LogicalPlanNode::LabelScan {
                variable: var_name.clone(),
                label,
            };
            plan = LogicalPlanNode::CartesianProduct {
                left: Box::new(plan),
                right: Box::new(scan),
            };
            visited.insert(var_name.clone());
        }
    }

    // Apply any remaining predicates not yet pushed
    for (i, pred) in predicates.iter().enumerate() {
        if !used_predicates.contains(&i) {
            plan = LogicalPlanNode::Filter {
                input: Box::new(plan),
                predicate: pred.clone(),
            };
        }
    }

    plan
}

/// Determine which adjacency list to walk to discover `other_var` from `bound_var`
/// along `edge`.
///
/// This is *not* a cost tradeoff: the edge is stored as `source_var -> target_var`,
/// so reaching the unbound endpoint from `bound_var` has exactly one correct
/// direction — outgoing (Forward) when `bound_var` is the edge's source, incoming
/// (Reverse) when `bound_var` is the edge's target. Picking the other direction
/// would traverse a semantically different adjacency list and return wrong
/// neighbors, not merely a slower plan. The catalog-driven cost comparison that
/// actually matters for direction — whether it's cheaper to approach this edge
/// from its source side or its target side — happens one level up, in
/// `enumerate_plans`, by generating a separate start-from-every-node candidate
/// for each pattern node and letting `estimate_plan_cost` pick the cheapest.
fn choose_direction(bound_var: &str, edge: &PatternEdge) -> ExpandDirection {
    if edge.source_var == bound_var {
        ExpandDirection::Forward
    } else {
        ExpandDirection::Reverse
    }
}

/// If `pred` is an equality/range comparison between `var`'s property and a
/// literal, return `(property, op, literal_expr)` normalized so `op` always
/// reads left-to-right as `var.property OP literal` — regardless of which side
/// the property appeared on in the source query. `5 < n.age` and `n.age > 5`
/// both normalize to `("age", Gt, 5)`.
pub(crate) fn normalize_index_predicate(pred: &Expression, var: &str) -> Option<(String, BinaryOp, Expression)> {
    let Expression::Binary { left, op, right } = pred else { return None };

    if let (Expression::Property { variable, property }, Expression::Literal(_)) = (left.as_ref(), right.as_ref()) {
        if variable == var {
            return Some((property.clone(), op.clone(), (**right).clone()));
        }
    }
    if let (Expression::Literal(_), Expression::Property { variable, property }) = (left.as_ref(), right.as_ref()) {
        if variable == var {
            let flipped = match op {
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::Ge => BinaryOp::Le,
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::Le => BinaryOp::Ge,
                other => other.clone(),
            };
            return Some((property.clone(), flipped, (**left).clone()));
        }
    }
    None
}

/// Push filter predicates whose variables are all bound by the current plan
fn push_applicable_predicates(
    mut plan: LogicalPlanNode,
    predicates: &[Expression],
    used: &mut HashSet<usize>,
) -> LogicalPlanNode {
    let bound_vars = plan.bound_variables();
    for (i, pred) in predicates.iter().enumerate() {
        if used.contains(&i) {
            continue;
        }
        let pred_vars = collect_expression_vars(pred);
        if !pred_vars.is_empty() && pred_vars.iter().all(|v| bound_vars.contains(v)) {
            plan = LogicalPlanNode::Filter {
                input: Box::new(plan),
                predicate: pred.clone(),
            };
            used.insert(i);
        }
    }
    plan
}

/// Collect variable names from an expression
fn collect_expression_vars(expr: &Expression) -> HashSet<String> {
    let mut vars = HashSet::new();
    collect_vars_inner(expr, &mut vars);
    vars
}

fn collect_vars_inner(expr: &Expression, vars: &mut HashSet<String>) {
    match expr {
        Expression::Variable(v) => { vars.insert(v.clone()); }
        Expression::Property { variable, .. } => { vars.insert(variable.clone()); }
        Expression::Binary { left, right, .. } => {
            collect_vars_inner(left, vars);
            collect_vars_inner(right, vars);
        }
        Expression::Unary { expr: inner, .. } => {
            collect_vars_inner(inner, vars);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_vars_inner(arg, vars);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_vars_inner(op, vars);
            }
            for (cond, result) in when_clauses {
                collect_vars_inner(cond, vars);
                collect_vars_inner(result, vars);
            }
            if let Some(el) = else_result {
                collect_vars_inner(el, vars);
            }
        }
        Expression::Index { expr: inner, index } => {
            collect_vars_inner(inner, vars);
            collect_vars_inner(index, vars);
        }
        _ => {} // Literal, Parameter, PathVariable, subqueries, etc.
    }
}

/// Flatten AND-connected expressions into a list
fn flatten_and_predicates(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::Binary { left, op: BinaryOp::And, right } => {
            let mut result = flatten_and_predicates(left);
            result.extend(flatten_and_predicates(right));
            result
        }
        other => vec![other.clone()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::catalog::GraphCatalog;
    use crate::graph::types::{Label, EdgeType, NodeId};
    use crate::query::ast::*;
    use crate::graph::PropertyValue;

    fn make_match_clause(paths: Vec<PathPattern>) -> MatchClause {
        MatchClause {
            pattern: Pattern { paths },
            optional: false,
        }
    }

    fn make_path(start_var: &str, start_labels: Vec<Label>, segments: Vec<PathSegment>) -> PathPattern {
        PathPattern {
            restrictor: Default::default(),
            restrictor_explicit: false,
            selector: Default::default(),
            path_variable: None,
            path_type: PathType::Normal,
            start: NodePattern {
                variable: Some(start_var.to_string()),
                labels: start_labels,
                properties: None,
                property_exprs: None,
            },
            segments,
        }
    }

    fn make_segment(edge_var: Option<&str>, edge_types: Vec<EdgeType>, dir: Direction, node_var: &str, node_labels: Vec<Label>) -> PathSegment {
        PathSegment {
            edge: EdgePattern {
                variable: edge_var.map(|s| s.to_string()),
                types: edge_types,
                direction: dir,
                length: None,
                properties: None,
                property_exprs: None,
            },
            node: NodePattern {
                variable: Some(node_var.to_string()),
                labels: node_labels,
                properties: None,
                property_exprs: None,
            },
        }
    }

    #[test]
    fn test_enumerate_single_node() {
        let catalog = GraphCatalog::new();
        let clause = make_match_clause(vec![make_path(
            "n", vec![Label::new("Person")], vec![],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let config = EnumerationConfig::default();

        let plans = enumerate_plans(&pg, None, &catalog, &crate::index::manager::IndexManager::new(), &config);
        assert_eq!(plans.len(), 1);
        match &plans[0].0 {
            LogicalPlanNode::LabelScan { variable, label } => {
                assert_eq!(variable, "n");
                assert_eq!(label.as_ref().unwrap().as_str(), "Person");
            }
            other => panic!("Expected LabelScan, got {:?}", other),
        }
    }

    #[test]
    fn test_enumerate_two_nodes_produces_two_plans() {
        // MATCH (a:Person)-[:KNOWS]->(b:Person)
        let mut catalog = GraphCatalog::new();
        for _ in 0..10 {
            catalog.on_label_added(&Label::new("Person"));
        }

        let clause = make_match_clause(vec![make_path(
            "a", vec![Label::new("Person")],
            vec![make_segment(None, vec![EdgeType::new("KNOWS")], Direction::Outgoing, "b", vec![Label::new("Person")])],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let config = EnumerationConfig::default();

        let plans = enumerate_plans(&pg, None, &catalog, &crate::index::manager::IndexManager::new(), &config);
        // Two starting points: a and b
        assert_eq!(plans.len(), 2);
        // Both should have a cost > 0
        assert!(plans[0].1 > 0.0);
        assert!(plans[1].1 > 0.0);
        // First plan should be cheapest (sorted by cost)
        assert!(plans[0].1 <= plans[1].1);
    }

    #[test]
    fn test_enumerate_asymmetric_picks_cheaper_start() {
        // 1000 Persons, 10 Companies
        // MATCH (p:Person)-[:WORKS_AT]->(c:Company)
        // Starting from Company (10 nodes) should be cheaper than starting from Person (1000 nodes)
        let mut catalog = GraphCatalog::new();
        for _ in 0..1000 {
            catalog.on_label_added(&Label::new("Person"));
        }
        for _ in 0..10 {
            catalog.on_label_added(&Label::new("Company"));
        }
        // Each person works at 1 company
        for i in 0..1000 {
            catalog.on_edge_created(
                NodeId::new(i),
                &[Label::new("Person")],
                &EdgeType::new("WORKS_AT"),
                NodeId::new(1000 + (i % 10)),
                &[Label::new("Company")],
            );
        }

        let clause = make_match_clause(vec![make_path(
            "p", vec![Label::new("Person")],
            vec![make_segment(None, vec![EdgeType::new("WORKS_AT")], Direction::Outgoing, "c", vec![Label::new("Company")])],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let config = EnumerationConfig::default();

        let plans = enumerate_plans(&pg, None, &catalog, &crate::index::manager::IndexManager::new(), &config);
        assert_eq!(plans.len(), 2);
        // Cheapest plan should have lower cost
        let cheapest_cost = plans[0].1;
        let other_cost = plans[1].1;
        assert!(cheapest_cost <= other_cost);
    }

    #[test]
    fn test_enumerate_with_where_clause() {
        let catalog = GraphCatalog::new();
        let clause = make_match_clause(vec![make_path(
            "n", vec![Label::new("Person")], vec![],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let where_clause = WhereClause {
            predicate: Expression::Binary {
                left: Box::new(Expression::Property { variable: "n".to_string(), property: "name".to_string() }),
                op: BinaryOp::Eq,
                right: Box::new(Expression::Literal(PropertyValue::String("Alice".to_string()))),
            },
        };
        let config = EnumerationConfig::default();

        let plans = enumerate_plans(&pg, Some(&where_clause), &catalog, &crate::index::manager::IndexManager::new(), &config);
        assert_eq!(plans.len(), 1);
        // Plan should have a Filter node
        match &plans[0].0 {
            LogicalPlanNode::Filter { predicate, input } => {
                match input.as_ref() {
                    LogicalPlanNode::LabelScan { variable, .. } => {
                        assert_eq!(variable, "n");
                    }
                    other => panic!("Expected LabelScan under filter, got {:?}", other),
                }
            }
            other => panic!("Expected Filter at top, got {:?}", other),
        }
    }

    #[test]
    fn test_enumerate_three_node_chain() {
        // MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)
        let catalog = GraphCatalog::new();
        let clause = make_match_clause(vec![make_path(
            "a", vec![Label::new("Person")],
            vec![
                make_segment(None, vec![EdgeType::new("KNOWS")], Direction::Outgoing, "b", vec![Label::new("Person")]),
                make_segment(None, vec![EdgeType::new("WORKS_AT")], Direction::Outgoing, "c", vec![Label::new("Company")]),
            ],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let config = EnumerationConfig::default();

        let plans = enumerate_plans(&pg, None, &catalog, &crate::index::manager::IndexManager::new(), &config);
        // Three starting points: a, b, c
        assert_eq!(plans.len(), 3);
    }

    #[test]
    fn test_max_candidate_plans_limit() {
        let catalog = GraphCatalog::new();
        let clause = make_match_clause(vec![make_path(
            "a", vec![], vec![
                make_segment(None, vec![], Direction::Outgoing, "b", vec![]),
                make_segment(None, vec![], Direction::Outgoing, "c", vec![]),
                make_segment(None, vec![], Direction::Outgoing, "d", vec![]),
            ],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let config = EnumerationConfig { max_candidate_plans: 2 };

        let plans = enumerate_plans(&pg, None, &catalog, &crate::index::manager::IndexManager::new(), &config);
        assert!(plans.len() <= 2);
    }

    #[test]
    fn test_flatten_and_predicates() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Binary {
                left: Box::new(Expression::Literal(PropertyValue::Integer(1))),
                op: BinaryOp::And,
                right: Box::new(Expression::Literal(PropertyValue::Integer(2))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expression::Literal(PropertyValue::Integer(3))),
        };
        let preds = flatten_and_predicates(&expr);
        assert_eq!(preds.len(), 3);
    }

    #[test]
    fn test_plan_has_expand_into_for_triangle() {
        // Triangle pattern: a-b, b-c, a-c
        // When starting from a, after visiting b and c via expand,
        // the a-c edge should become ExpandInto since both are bound
        let mut catalog = GraphCatalog::new();
        for _ in 0..10 {
            catalog.on_label_added(&Label::new("Person"));
        }

        // Build a triangle pattern manually
        // We need: (a:Person)-[:KNOWS]->(b:Person), (b)-[:KNOWS]->(c:Person), (a)-[:KNOWS]->(c)
        // This requires multiple paths or a complex single path
        // For simplicity, use a PatternGraph directly
        let mut pg = PatternGraph {
            nodes: std::collections::HashMap::new(),
            edges: Vec::new(),
        };
        pg.nodes.insert("a".to_string(), super::super::logical_plan::PatternNode {
            variable: "a".to_string(),
            labels: vec![Label::new("Person")],
            properties: Vec::new(),
        });
        pg.nodes.insert("b".to_string(), super::super::logical_plan::PatternNode {
            variable: "b".to_string(),
            labels: vec![Label::new("Person")],
            properties: Vec::new(),
        });
        pg.nodes.insert("c".to_string(), super::super::logical_plan::PatternNode {
            variable: "c".to_string(),
            labels: vec![Label::new("Person")],
            properties: Vec::new(),
        });
        pg.edges.push(PatternEdge {
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("KNOWS")],
            ast_direction: super::super::logical_plan::AstDirection::Outgoing,
        });
        pg.edges.push(PatternEdge {
            source_var: "b".to_string(),
            target_var: "c".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("KNOWS")],
            ast_direction: super::super::logical_plan::AstDirection::Outgoing,
        });
        pg.edges.push(PatternEdge {
            source_var: "a".to_string(),
            target_var: "c".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("KNOWS")],
            ast_direction: super::super::logical_plan::AstDirection::Outgoing,
        });

        let config = EnumerationConfig::default();
        let plans = enumerate_plans(&pg, None, &catalog, &crate::index::manager::IndexManager::new(), &config);

        // After WCO optimization, ExpandInto+Expand chains become TrieJoin.
        // At least one plan should contain a TrieJoin (upgraded from ExpandInto).
        let has_wco = plans.iter().any(|(plan, _)| contains_trie_join(plan));
        let has_expand_into = plans.iter().any(|(plan, _)| contains_expand_into(plan));
        assert!(has_wco || has_expand_into,
            "Triangle pattern should produce at least one plan with TrieJoin or ExpandInto");
    }

    fn prop(var: &str, p: &str) -> Expression {
        Expression::Property { variable: var.to_string(), property: p.to_string() }
    }

    fn lit(n: i64) -> Expression {
        Expression::Literal(PropertyValue::Integer(n))
    }

    fn bin(l: Expression, op: BinaryOp, r: Expression) -> Expression {
        Expression::Binary { left: Box::new(l), op, right: Box::new(r) }
    }

    fn single_node(var: &str, label: &str) -> PatternGraph {
        PatternGraph::from_match_clause(&make_match_clause(vec![make_path(var, vec![Label::new(label)], vec![])]))
    }

    fn indexed(label: &str, property: &str) -> IndexManager {
        let idx = IndexManager::new();
        idx.create_index(Label::new(label), property.to_string());
        idx
    }

    fn where_(pred: Expression) -> WhereClause {
        WhereClause { predicate: pred }
    }

    #[test]
    fn test_normalize_index_predicate_property_on_left() {
        let got = normalize_index_predicate(&bin(prop("n", "age"), BinaryOp::Gt, lit(5)), "n");
        assert_eq!(got, Some(("age".to_string(), BinaryOp::Gt, lit(5))));
        // Other variable: not ours.
        assert_eq!(normalize_index_predicate(&bin(prop("m", "age"), BinaryOp::Gt, lit(5)), "n"), None);
    }

    #[test]
    fn test_normalize_index_predicate_flips_literal_on_left() {
        let cases = [
            (BinaryOp::Gt, BinaryOp::Lt),
            (BinaryOp::Ge, BinaryOp::Le),
            (BinaryOp::Lt, BinaryOp::Gt),
            (BinaryOp::Le, BinaryOp::Ge),
            (BinaryOp::Eq, BinaryOp::Eq),
        ];
        for (written, expected) in cases {
            let got = normalize_index_predicate(&bin(lit(5), written.clone(), prop("n", "age")), "n");
            assert_eq!(got, Some(("age".to_string(), expected, lit(5))), "for {:?}", written);
        }
        assert_eq!(normalize_index_predicate(&bin(lit(5), BinaryOp::Lt, prop("m", "age")), "n"), None);
    }

    #[test]
    fn test_normalize_index_predicate_rejects_non_comparisons() {
        assert_eq!(normalize_index_predicate(&prop("n", "age"), "n"), None);
        assert_eq!(normalize_index_predicate(&bin(prop("n", "a"), BinaryOp::Eq, prop("n", "b")), "n"), None);
        assert_eq!(normalize_index_predicate(&bin(lit(1), BinaryOp::Eq, lit(1)), "n"), None);
    }

    #[test]
    fn test_indexed_equality_becomes_index_lookup() {
        let pg = single_node("n", "Person");
        let wc = where_(bin(prop("n", "age"), BinaryOp::Eq, lit(30)));
        let plans = enumerate_plans(&pg, Some(&wc), &GraphCatalog::new(), &indexed("Person", "age"), &EnumerationConfig::default());
        match &plans[0].0 {
            LogicalPlanNode::IndexLookup { variable, property, op, value, .. } => {
                assert_eq!(variable, "n");
                assert_eq!(property, "age");
                assert_eq!(*op, BinaryOp::Eq);
                assert_eq!(*value, lit(30));
            }
            other => panic!("expected IndexLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_flipped_range_predicate_uses_index_with_normalized_op() {
        let pg = single_node("n", "Person");
        let wc = where_(bin(lit(18), BinaryOp::Lt, prop("n", "age")));
        let plans = enumerate_plans(&pg, Some(&wc), &GraphCatalog::new(), &indexed("Person", "age"), &EnumerationConfig::default());
        match &plans[0].0 {
            LogicalPlanNode::IndexLookup { op, .. } => assert_eq!(*op, BinaryOp::Gt),
            other => panic!("expected IndexLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_non_indexable_operator_falls_back_to_scan_and_filter() {
        let pg = single_node("n", "Person");
        let wc = where_(bin(prop("n", "age"), BinaryOp::Ne, lit(30)));
        let plans = enumerate_plans(&pg, Some(&wc), &GraphCatalog::new(), &indexed("Person", "age"), &EnumerationConfig::default());
        match &plans[0].0 {
            LogicalPlanNode::Filter { input, .. } => assert!(matches!(**input, LogicalPlanNode::LabelScan { .. })),
            other => panic!("expected Filter(LabelScan), got {:?}", other),
        }
    }

    #[test]
    fn test_unindexed_property_uses_scan() {
        let pg = single_node("n", "Person");
        let wc = where_(bin(prop("n", "name"), BinaryOp::Eq, lit(1)));
        let plans = enumerate_plans(&pg, Some(&wc), &GraphCatalog::new(), &indexed("Person", "age"), &EnumerationConfig::default());
        assert!(matches!(&plans[0].0, LogicalPlanNode::Filter { .. }));
    }

    #[test]
    fn test_inline_property_is_treated_as_equality_predicate() {
        let mut path = make_path("n", vec![Label::new("Person")], vec![]);
        let mut props = std::collections::HashMap::new();
        props.insert("age".to_string(), PropertyValue::Integer(42));
        path.start.properties = Some(props);
        let pg = PatternGraph::from_match_clause(&make_match_clause(vec![path]));

        // With an index, the inline property drives an IndexLookup.
        let plans = enumerate_plans(&pg, None, &GraphCatalog::new(), &indexed("Person", "age"), &EnumerationConfig::default());
        assert!(matches!(&plans[0].0, LogicalPlanNode::IndexLookup { value, .. } if *value == lit(42)));

        // Without one, it becomes a Filter.
        let plans = enumerate_plans(&pg, None, &GraphCatalog::new(), &IndexManager::new(), &EnumerationConfig::default());
        match &plans[0].0 {
            LogicalPlanNode::Filter { predicate, .. } => {
                assert_eq!(*predicate, bin(prop("n", "age"), BinaryOp::Eq, lit(42)));
            }
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[test]
    fn test_unlabelled_start_never_uses_index() {
        let clause = make_match_clause(vec![make_path("n", vec![], vec![])]);
        let pg = PatternGraph::from_match_clause(&clause);
        let wc = where_(bin(prop("n", "age"), BinaryOp::Eq, lit(30)));
        let plans = enumerate_plans(&pg, Some(&wc), &GraphCatalog::new(), &indexed("Person", "age"), &EnumerationConfig::default());
        assert!(matches!(&plans[0].0, LogicalPlanNode::Filter { .. }));
    }

    #[test]
    fn test_disconnected_components_are_joined_by_cartesian_product() {
        // MATCH (a:Person), (b:Company)
        let clause = make_match_clause(vec![
            make_path("a", vec![Label::new("Person")], vec![]),
            make_path("b", vec![Label::new("Company")], vec![]),
        ]);
        let pg = PatternGraph::from_match_clause(&clause);
        let plans = enumerate_plans(&pg, None, &GraphCatalog::new(), &IndexManager::new(), &EnumerationConfig::default());
        assert_eq!(plans.len(), 2);
        for (plan, _) in &plans {
            match plan {
                LogicalPlanNode::CartesianProduct { left, right } => {
                    let mut vars: Vec<String> = left.bound_variables().into_iter().chain(right.bound_variables()).collect();
                    vars.sort();
                    assert_eq!(vars, vec!["a", "b"]);
                }
                other => panic!("expected CartesianProduct, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_predicate_on_unknown_variable_is_applied_last() {
        let pg = single_node("n", "Person");
        let wc = where_(bin(prop("ghost", "x"), BinaryOp::Eq, lit(1)));
        let plans = enumerate_plans(&pg, Some(&wc), &GraphCatalog::new(), &IndexManager::new(), &EnumerationConfig::default());
        match &plans[0].0 {
            LogicalPlanNode::Filter { predicate, input } => {
                assert_eq!(*predicate, bin(prop("ghost", "x"), BinaryOp::Eq, lit(1)));
                assert!(matches!(**input, LogicalPlanNode::LabelScan { .. }));
            }
            other => panic!("expected trailing Filter, got {:?}", other),
        }
    }

    #[test]
    fn test_constant_predicate_is_applied_last() {
        let pg = single_node("n", "Person");
        let wc = where_(Expression::Literal(PropertyValue::Boolean(true)));
        let plans = enumerate_plans(&pg, Some(&wc), &GraphCatalog::new(), &IndexManager::new(), &EnumerationConfig::default());
        assert!(matches!(&plans[0].0, LogicalPlanNode::Filter { .. }));
    }

    #[test]
    fn test_max_candidate_plans_zero_yields_nothing() {
        let pg = single_node("n", "Person");
        let plans = enumerate_plans(&pg, None, &GraphCatalog::new(), &IndexManager::new(), &EnumerationConfig { max_candidate_plans: 0 });
        assert!(plans.is_empty());
    }

    #[test]
    fn test_max_candidate_plans_stops_enumeration_early() {
        let clause = make_match_clause(vec![make_path(
            "a", vec![], vec![
                make_segment(None, vec![], Direction::Outgoing, "b", vec![]),
                make_segment(None, vec![], Direction::Outgoing, "c", vec![]),
            ],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let plans = enumerate_plans(&pg, None, &GraphCatalog::new(), &IndexManager::new(), &EnumerationConfig { max_candidate_plans: 1 });
        assert_eq!(plans.len(), 1);
    }

    #[test]
    fn test_incoming_edge_from_target_expands_in_reverse() {
        // MATCH (a)<-[:E]-(b): stored edge is b->a. Starting from `a` walks
        // the incoming list.
        let clause = make_match_clause(vec![make_path(
            "a", vec![Label::new("A")],
            vec![make_segment(None, vec![EdgeType::new("E")], Direction::Incoming, "b", vec![Label::new("B")])],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let plans = enumerate_plans(&pg, None, &GraphCatalog::new(), &IndexManager::new(), &EnumerationConfig::default());
        let from_a = plans.iter().find(|(p, _)| matches!(p, LogicalPlanNode::Expand { source_var, .. } if source_var == "a")).expect("plan from a");
        match &from_a.0 {
            LogicalPlanNode::Expand { direction, target_var, .. } => {
                assert_eq!(*direction, ExpandDirection::Reverse);
                assert_eq!(target_var, "b");
            }
            _ => unreachable!(),
        }
        let from_b = plans.iter().find(|(p, _)| matches!(p, LogicalPlanNode::Expand { source_var, .. } if source_var == "b")).expect("plan from b");
        assert!(matches!(&from_b.0, LogicalPlanNode::Expand { direction: ExpandDirection::Forward, .. }));
    }

    #[test]
    fn test_predicate_on_both_endpoints_is_pushed_after_expand() {
        let clause = make_match_clause(vec![make_path(
            "a", vec![],
            vec![make_segment(None, vec![], Direction::Outgoing, "b", vec![])],
        )]);
        let pg = PatternGraph::from_match_clause(&clause);
        let wc = where_(bin(prop("a", "x"), BinaryOp::Eq, prop("b", "x")));
        let plans = enumerate_plans(&pg, Some(&wc), &GraphCatalog::new(), &IndexManager::new(), &EnumerationConfig::default());
        for (plan, _) in &plans {
            match plan {
                LogicalPlanNode::Filter { input, .. } => assert!(matches!(**input, LogicalPlanNode::Expand { .. })),
                other => panic!("expected Filter(Expand), got {:?}", other),
            }
        }
    }

    #[test]
    fn test_collect_vars_inner_walks_nested_expressions() {
        let q = crate::query::parser::parse_query(
            "MATCH (a), (b) WHERE NOT (toUpper(a.name) = CASE b.k WHEN 1 THEN a.x ELSE b.y END) AND a.list[b.i] = 1 RETURN a",
        )
        .unwrap();
        let mut vars: Vec<String> = collect_expression_vars(&q.where_clause.unwrap().predicate).into_iter().collect();
        vars.sort();
        assert_eq!(vars, vec!["a", "b"]);

        let q = crate::query::parser::parse_query("MATCH (a) WHERE CASE WHEN a.x = 1 THEN true END RETURN a").unwrap();
        let vars = collect_expression_vars(&q.where_clause.unwrap().predicate);
        assert!(vars.contains("a"));

        assert!(collect_expression_vars(&Expression::Parameter("p".into())).is_empty());
        assert_eq!(
            collect_expression_vars(&Expression::Variable("v".into())).into_iter().collect::<Vec<_>>(),
            vec!["v".to_string()]
        );
    }

    #[test]
    fn test_flatten_and_leaves_or_intact() {
        let or = bin(lit(1), BinaryOp::Or, lit(2));
        assert_eq!(flatten_and_predicates(&or), vec![or.clone()]);
    }

    #[test]
    fn test_contains_helpers_walk_every_node_kind() {
        let into = LogicalPlanNode::ExpandInto {
            input: Box::new(LogicalPlanNode::LabelScan { variable: "a".into(), label: None }),
            source_var: "a".into(), target_var: "b".into(), edge_types: vec![], edge_var: None,
        };
        let tj = LogicalPlanNode::TrieJoin { input: Box::new(LogicalPlanNode::LabelScan { variable: "a".into(), label: None }), target_var: "c".into(), constraints: vec![] };
        let scan = LogicalPlanNode::LabelScan { variable: "z".into(), label: None };
        let wrap = |inner: LogicalPlanNode| vec![
            LogicalPlanNode::Expand { input: Box::new(inner.clone()), source_var: "a".into(), target_var: "q".into(), edge_var: None, edge_types: vec![], direction: ExpandDirection::Forward },
            LogicalPlanNode::Filter { input: Box::new(inner.clone()), predicate: lit(1) },
            LogicalPlanNode::Join { left: Box::new(scan.clone()), right: Box::new(inner.clone()), join_keys: vec![] },
            LogicalPlanNode::CartesianProduct { left: Box::new(scan.clone()), right: Box::new(inner.clone()) },
            LogicalPlanNode::TrieJoin { input: Box::new(inner.clone()), target_var: "t".into(), constraints: vec![] },
            LogicalPlanNode::ExpandInto { input: Box::new(inner.clone()), source_var: "a".into(), target_var: "q".into(), edge_types: vec![], edge_var: None },
        ];
        for p in wrap(into.clone()) {
            assert!(contains_expand_into(&p), "{:?}", p);
        }
        for p in wrap(tj.clone()) {
            assert!(contains_trie_join(&p), "{:?}", p);
        }
        assert!(!contains_expand_into(&scan));
        assert!(!contains_trie_join(&scan));
    }

    /// Helper to check if a plan contains an ExpandInto node
    fn contains_expand_into(plan: &LogicalPlanNode) -> bool {
        match plan {
            LogicalPlanNode::ExpandInto { .. } => true,
            LogicalPlanNode::Expand { input, .. } => contains_expand_into(input),
            LogicalPlanNode::ExpandInto { input, .. } => contains_expand_into(input),
            LogicalPlanNode::TrieJoin { input, .. } => contains_expand_into(input),
            LogicalPlanNode::Filter { input, .. } => contains_expand_into(input),
            LogicalPlanNode::Join { left, right, .. } => contains_expand_into(left) || contains_expand_into(right),
            LogicalPlanNode::CartesianProduct { left, right } => contains_expand_into(left) || contains_expand_into(right),
            _ => false,
        }
    }

    /// Helper to check if a plan contains a TrieJoin node (WCO)
    fn contains_trie_join(plan: &LogicalPlanNode) -> bool {
        match plan {
            LogicalPlanNode::TrieJoin { .. } => true,
            LogicalPlanNode::Expand { input, .. } => contains_trie_join(input),
            LogicalPlanNode::ExpandInto { input, .. } => contains_trie_join(input),
            LogicalPlanNode::TrieJoin { input, .. } => contains_trie_join(input),
            LogicalPlanNode::Filter { input, .. } => contains_trie_join(input),
            LogicalPlanNode::Join { left, right, .. } => contains_trie_join(left) || contains_trie_join(right),
            LogicalPlanNode::CartesianProduct { left, right } => contains_trie_join(left) || contains_trie_join(right),
            _ => false,
        }
    }
}
