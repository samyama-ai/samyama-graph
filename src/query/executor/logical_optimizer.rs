//! Logical optimizer: rule-based transformations on logical plans (ADR-015)
//!
//! Rules:
//! - ExpandInto insertion: when both endpoints of an edge are already bound,
//!   convert Expand to ExpandInto for efficient edge-existence checks
//! - Predicate pushdown: push filters as close to their data source as possible

use std::collections::HashSet;
use super::logical_plan::{LogicalPlanNode, ExpandDirection, TrieJoinConstraint};

/// Apply all logical optimization rules to a plan
pub fn optimize(plan: LogicalPlanNode) -> LogicalPlanNode {
    let plan = push_filters_down(plan);
    let plan = insert_expand_into(plan);
    let plan = merge_cyclic_to_trie_join(plan);
    plan
}

/// Push Filter nodes closer to their data source when possible.
///
/// If a Filter sits on top of an Expand and the filter's predicate only references
/// variables bound by the Expand's input (not the Expand's target), push the
/// filter below the Expand.
fn push_filters_down(plan: LogicalPlanNode) -> LogicalPlanNode {
    match plan {
        LogicalPlanNode::Filter { input, predicate } => {
            let optimized_input = push_filters_down(*input);

            // Check if predicate can be pushed below the input
            let pred_vars = collect_expression_vars(&predicate);

            match optimized_input {
                LogicalPlanNode::Expand { input: expand_input, source_var, target_var, edge_var, edge_types, direction } => {
                    // Can push down if predicate doesn't reference target_var or edge_var
                    let expand_new_vars: HashSet<String> = {
                        let mut s = HashSet::new();
                        s.insert(target_var.clone());
                        if let Some(ref ev) = edge_var {
                            s.insert(ev.clone());
                        }
                        s
                    };
                    if !pred_vars.is_empty() && pred_vars.iter().all(|v| !expand_new_vars.contains(v)) {
                        // Push filter below expand
                        LogicalPlanNode::Expand {
                            input: Box::new(LogicalPlanNode::Filter {
                                input: expand_input,
                                predicate,
                            }),
                            source_var,
                            target_var,
                            edge_var,
                            edge_types,
                            direction,
                        }
                    } else {
                        // Can't push down — keep filter on top
                        LogicalPlanNode::Filter {
                            input: Box::new(LogicalPlanNode::Expand {
                                input: expand_input,
                                source_var,
                                target_var,
                                edge_var,
                                edge_types,
                                direction,
                            }),
                            predicate,
                        }
                    }
                }
                other => LogicalPlanNode::Filter {
                    input: Box::new(other),
                    predicate,
                },
            }
        }
        // Recursively optimize children
        LogicalPlanNode::Expand { input, source_var, target_var, edge_var, edge_types, direction } => {
            LogicalPlanNode::Expand {
                input: Box::new(push_filters_down(*input)),
                source_var,
                target_var,
                edge_var,
                edge_types,
                direction,
            }
        }
        LogicalPlanNode::ExpandInto { input, source_var, target_var, edge_types, edge_var } => {
            LogicalPlanNode::ExpandInto {
                input: Box::new(push_filters_down(*input)),
                source_var,
                target_var,
                edge_types,
                edge_var,
            }
        }
        LogicalPlanNode::Join { left, right, join_keys } => {
            LogicalPlanNode::Join {
                left: Box::new(push_filters_down(*left)),
                right: Box::new(push_filters_down(*right)),
                join_keys,
            }
        }
        LogicalPlanNode::CartesianProduct { left, right } => {
            LogicalPlanNode::CartesianProduct {
                left: Box::new(push_filters_down(*left)),
                right: Box::new(push_filters_down(*right)),
            }
        }
        // Leaf nodes
        other => other,
    }
}

/// If both endpoints of an edge expansion are already bound in the input,
/// convert Expand to ExpandInto.
fn insert_expand_into(plan: LogicalPlanNode) -> LogicalPlanNode {
    match plan {
        LogicalPlanNode::Expand { input, source_var, target_var, edge_var, edge_types, direction } => {
            let optimized_input = insert_expand_into(*input);
            let input_vars = optimized_input.bound_variables();

            if input_vars.contains(&source_var) && input_vars.contains(&target_var) {
                // Both endpoints bound → use ExpandInto (direction not needed)
                LogicalPlanNode::ExpandInto {
                    input: Box::new(optimized_input),
                    source_var,
                    target_var,
                    edge_types,
                    edge_var,
                }
            } else {
                // Preserve original direction
                LogicalPlanNode::Expand {
                    input: Box::new(optimized_input),
                    source_var,
                    target_var,
                    edge_var,
                    edge_types,
                    direction,
                }
            }
        }
        LogicalPlanNode::ExpandInto { input, source_var, target_var, edge_types, edge_var } => {
            LogicalPlanNode::ExpandInto {
                input: Box::new(insert_expand_into(*input)),
                source_var,
                target_var,
                edge_types,
                edge_var,
            }
        }
        LogicalPlanNode::Filter { input, predicate } => {
            LogicalPlanNode::Filter {
                input: Box::new(insert_expand_into(*input)),
                predicate,
            }
        }
        LogicalPlanNode::Join { left, right, join_keys } => {
            LogicalPlanNode::Join {
                left: Box::new(insert_expand_into(*left)),
                right: Box::new(insert_expand_into(*right)),
                join_keys,
            }
        }
        LogicalPlanNode::CartesianProduct { left, right } => {
            LogicalPlanNode::CartesianProduct {
                left: Box::new(insert_expand_into(*left)),
                right: Box::new(insert_expand_into(*right)),
            }
        }
        other => other,
    }
}

/// Convert Expand+ExpandInto chains into TrieJoin for cyclic patterns (WCO).
///
/// Detects the pattern:
///   ExpandInto(target_var → other_bound, ...)
///     └── Expand(bound_var → target_var, direction, ...)
///           └── <input where other_bound is already bound>
///
/// And merges it into:
///   TrieJoin(target_var, constraints=[from_expand, from_expand_into])
///     └── <input>
///
/// This enables worst-case optimal intersection via LeapFrog instead of
/// sequential expand-then-filter.
fn merge_cyclic_to_trie_join(plan: LogicalPlanNode) -> LogicalPlanNode {
    match plan {
        LogicalPlanNode::ExpandInto { input, source_var, target_var, edge_types, edge_var } => {
            let optimized_input = merge_cyclic_to_trie_join(*input);

            // Check if input is an Expand whose target is one of our endpoints
            match optimized_input {
                LogicalPlanNode::Expand { input: expand_input, source_var: exp_src, target_var: exp_tgt, edge_var: exp_ev, edge_types: exp_et, direction: exp_dir }
                    if exp_tgt == source_var || exp_tgt == target_var =>
                {
                    // The new variable introduced by the Expand
                    let new_var = exp_tgt.clone();

                    // Constraint from Expand: new_var ∈ neighbors of exp_src
                    let expand_constraint = TrieJoinConstraint {
                        bound_var: exp_src.clone(),
                        direction: exp_dir,
                        edge_types: exp_et,
                        edge_var: exp_ev,
                    };

                    // Constraint from ExpandInto: determine which bound var the new_var connects to
                    // ExpandInto checks edge source_var → target_var
                    let (into_bound, into_dir) = if new_var == source_var {
                        // ExpandInto(new_var → target_var): edge from new_var to target_var
                        // new_var ∈ N_in(target_var)
                        (target_var.clone(), ExpandDirection::Reverse)
                    } else {
                        // ExpandInto(source_var → new_var): edge from source_var to new_var
                        // new_var ∈ N_out(source_var)
                        (source_var.clone(), ExpandDirection::Forward)
                    };

                    let into_constraint = TrieJoinConstraint {
                        bound_var: into_bound,
                        direction: into_dir,
                        edge_types,
                        edge_var,
                    };

                    LogicalPlanNode::TrieJoin {
                        input: expand_input,
                        target_var: new_var,
                        constraints: vec![expand_constraint, into_constraint],
                    }
                }
                // Also merge ExpandInto on top of an existing TrieJoin (for 4-cliques etc.)
                LogicalPlanNode::TrieJoin { input: tj_input, target_var: tj_target, mut constraints }
                    if tj_target == source_var || tj_target == target_var =>
                {
                    let new_var = tj_target;
                    let (into_bound, into_dir) = if new_var == source_var {
                        (target_var.clone(), ExpandDirection::Reverse)
                    } else {
                        (source_var.clone(), ExpandDirection::Forward)
                    };
                    constraints.push(TrieJoinConstraint {
                        bound_var: into_bound,
                        direction: into_dir,
                        edge_types,
                        edge_var,
                    });
                    LogicalPlanNode::TrieJoin {
                        input: tj_input,
                        target_var: new_var,
                        constraints,
                    }
                }
                other => {
                    // Can't merge — keep as ExpandInto
                    LogicalPlanNode::ExpandInto {
                        input: Box::new(other),
                        source_var,
                        target_var,
                        edge_types,
                        edge_var,
                    }
                }
            }
        }
        // Recurse into children
        LogicalPlanNode::Expand { input, source_var, target_var, edge_var, edge_types, direction } => {
            LogicalPlanNode::Expand {
                input: Box::new(merge_cyclic_to_trie_join(*input)),
                source_var, target_var, edge_var, edge_types, direction,
            }
        }
        LogicalPlanNode::TrieJoin { input, target_var, constraints } => {
            LogicalPlanNode::TrieJoin {
                input: Box::new(merge_cyclic_to_trie_join(*input)),
                target_var, constraints,
            }
        }
        LogicalPlanNode::Filter { input, predicate } => {
            LogicalPlanNode::Filter {
                input: Box::new(merge_cyclic_to_trie_join(*input)),
                predicate,
            }
        }
        LogicalPlanNode::Join { left, right, join_keys } => {
            LogicalPlanNode::Join {
                left: Box::new(merge_cyclic_to_trie_join(*left)),
                right: Box::new(merge_cyclic_to_trie_join(*right)),
                join_keys,
            }
        }
        LogicalPlanNode::CartesianProduct { left, right } => {
            LogicalPlanNode::CartesianProduct {
                left: Box::new(merge_cyclic_to_trie_join(*left)),
                right: Box::new(merge_cyclic_to_trie_join(*right)),
            }
        }
        other => other,
    }
}

/// Collect variable names referenced in an expression (simplified)
fn collect_expression_vars(expr: &crate::query::ast::Expression) -> HashSet<String> {
    use crate::query::ast::Expression;
    let mut vars = HashSet::new();
    collect_vars_recursive(expr, &mut vars);
    vars
}

fn collect_vars_recursive(expr: &crate::query::ast::Expression, vars: &mut HashSet<String>) {
    use crate::query::ast::Expression;
    match expr {
        Expression::Variable(v) => { vars.insert(v.clone()); }
        Expression::Property { variable, .. } => { vars.insert(variable.clone()); }
        Expression::Binary { left, right, .. } => {
            collect_vars_recursive(left, vars);
            collect_vars_recursive(right, vars);
        }
        Expression::Unary { expr: inner, .. } => {
            collect_vars_recursive(inner, vars);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_vars_recursive(arg, vars);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_vars_recursive(op, vars);
            }
            for (cond, result) in when_clauses {
                collect_vars_recursive(cond, vars);
                collect_vars_recursive(result, vars);
            }
            if let Some(el) = else_result {
                collect_vars_recursive(el, vars);
            }
        }
        Expression::Index { expr: inner, index } => {
            collect_vars_recursive(inner, vars);
            collect_vars_recursive(index, vars);
        }
        Expression::ExistsSubquery { pattern, where_clause, .. } => {
            // Variables the subquery shares with the outer query are genuine
            // dependencies of this predicate — the filter must not be pushed
            // below the operator that binds them, or the subquery is evaluated
            // against an unbound variable and matches far too eagerly. Collecting
            // every variable named in the subquery over-approximates (a
            // subquery-local variable counts too), which can only block a
            // pushdown, never enable an unsafe one.
            for path in &pattern.paths {
                if let Some(v) = &path.start.variable { vars.insert(v.clone()); }
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable { vars.insert(v.clone()); }
                    if let Some(v) = &seg.edge.variable { vars.insert(v.clone()); }
                }
            }
            if let Some(wc) = where_clause {
                collect_vars_recursive(&wc.predicate, vars);
            }
        }
        Expression::ListComprehension { list_expr, filter, map_expr, .. } => {
            collect_vars_recursive(list_expr, vars);
            if let Some(f) = filter {
                collect_vars_recursive(f, vars);
            }
            collect_vars_recursive(map_expr, vars);
        }
        Expression::PredicateFunction { list_expr, predicate, .. } => {
            collect_vars_recursive(list_expr, vars);
            collect_vars_recursive(predicate, vars);
        }
        Expression::Reduce { init, list_expr, expression, .. } => {
            collect_vars_recursive(init, vars);
            collect_vars_recursive(list_expr, vars);
            collect_vars_recursive(expression, vars);
        }
        _ => {} // Literal, Parameter, PathVariable, PatternComprehension, ListSlice
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::{Label, EdgeType};
    use crate::query::ast::Expression;
    use crate::graph::PropertyValue;

    #[test]
    fn test_expand_into_insertion() {
        // Plan: CartesianProduct(Scan(a), Scan(b)) -> Expand(a -> b)
        // Since both a and b are bound, Expand should become ExpandInto
        let plan = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::CartesianProduct {
                left: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Person")) }),
                right: Box::new(LogicalPlanNode::LabelScan { variable: "b".to_string(), label: Some(Label::new("Person")) }),
            }),
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("KNOWS")],
            direction: ExpandDirection::Forward,
        };

        let optimized = optimize(plan);
        match optimized {
            LogicalPlanNode::ExpandInto { source_var, target_var, .. } => {
                assert_eq!(source_var, "a");
                assert_eq!(target_var, "b");
            }
            other => panic!("Expected ExpandInto, got {:?}", other),
        }
    }

    #[test]
    fn test_expand_not_converted_when_target_unbound() {
        // Plan: Scan(a) -> Expand(a -> b)
        // b is NOT bound → should remain as Expand
        let plan = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Person")) }),
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("KNOWS")],
            direction: ExpandDirection::Forward,
        };

        let optimized = optimize(plan);
        match optimized {
            LogicalPlanNode::Expand { .. } => { /* correct */ }
            other => panic!("Expected Expand, got {:?}", other),
        }
    }

    #[test]
    fn test_predicate_pushdown_below_expand() {
        // Plan: Expand(a -> b) -> Filter(a.name = "Alice")
        // The filter only references 'a' which is bound before expand,
        // so it should be pushed below the expand
        let plan = LogicalPlanNode::Filter {
            input: Box::new(LogicalPlanNode::Expand {
                input: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Person")) }),
                source_var: "a".to_string(),
                target_var: "b".to_string(),
                edge_var: None,
                edge_types: vec![EdgeType::new("KNOWS")],
                direction: ExpandDirection::Forward,
            }),
            predicate: Expression::Binary {
                left: Box::new(Expression::Property { variable: "a".to_string(), property: "name".to_string() }),
                op: crate::query::ast::BinaryOp::Eq,
                right: Box::new(Expression::Literal(PropertyValue::String("Alice".to_string()))),
            },
        };

        let optimized = optimize(plan);
        // Should be: Expand(Filter(Scan(a)), a -> b)
        match optimized {
            LogicalPlanNode::Expand { input, .. } => {
                match *input {
                    LogicalPlanNode::Filter { input: inner, .. } => {
                        match *inner {
                            LogicalPlanNode::LabelScan { variable, .. } => {
                                assert_eq!(variable, "a");
                            }
                            other => panic!("Expected LabelScan inside filter, got {:?}", other),
                        }
                    }
                    other => panic!("Expected Filter below expand, got {:?}", other),
                }
            }
            other => panic!("Expected Expand at top, got {:?}", other),
        }
    }

    #[test]
    fn test_predicate_not_pushed_when_references_target() {
        // Plan: Expand(a -> b) -> Filter(b.name = "Bob")
        // The filter references 'b' which is introduced by expand,
        // so it should NOT be pushed below
        let plan = LogicalPlanNode::Filter {
            input: Box::new(LogicalPlanNode::Expand {
                input: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Person")) }),
                source_var: "a".to_string(),
                target_var: "b".to_string(),
                edge_var: None,
                edge_types: vec![EdgeType::new("KNOWS")],
                direction: ExpandDirection::Forward,
            }),
            predicate: Expression::Binary {
                left: Box::new(Expression::Property { variable: "b".to_string(), property: "name".to_string() }),
                op: crate::query::ast::BinaryOp::Eq,
                right: Box::new(Expression::Literal(PropertyValue::String("Bob".to_string()))),
            },
        };

        let optimized = optimize(plan);
        // Should remain: Filter(Expand(Scan(a)))
        match optimized {
            LogicalPlanNode::Filter { input, .. } => {
                match *input {
                    LogicalPlanNode::Expand { .. } => { /* correct */ }
                    other => panic!("Expected Expand inside filter, got {:?}", other),
                }
            }
            other => panic!("Expected Filter at top, got {:?}", other),
        }
    }

    #[test]
    fn test_optimize_preserves_leaf_nodes() {
        let plan = LogicalPlanNode::LabelScan { variable: "n".to_string(), label: Some(Label::new("Person")) };
        let optimized = optimize(plan);
        match optimized {
            LogicalPlanNode::LabelScan { variable, .. } => assert_eq!(variable, "n"),
            other => panic!("Expected LabelScan, got {:?}", other),
        }
    }

    #[test]
    fn test_combined_pushdown_and_expand_into() {
        // Chain: CartesianProduct(Scan(a), Scan(b)) -> Expand(a -> b, :KNOWS) -> Filter(a.age > 30)
        // Optimizer should:
        // 1. Push filter below expand (since a.age doesn't reference b)
        // 2. Convert Expand to ExpandInto (since both a and b are bound)
        let plan = LogicalPlanNode::Filter {
            input: Box::new(LogicalPlanNode::Expand {
                input: Box::new(LogicalPlanNode::CartesianProduct {
                    left: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Person")) }),
                    right: Box::new(LogicalPlanNode::LabelScan { variable: "b".to_string(), label: Some(Label::new("Person")) }),
                }),
                source_var: "a".to_string(),
                target_var: "b".to_string(),
                edge_var: None,
                edge_types: vec![EdgeType::new("KNOWS")],
                direction: ExpandDirection::Forward,
            }),
            predicate: Expression::Binary {
                left: Box::new(Expression::Property { variable: "a".to_string(), property: "age".to_string() }),
                op: crate::query::ast::BinaryOp::Gt,
                right: Box::new(Expression::Literal(PropertyValue::Integer(30))),
            },
        };

        let optimized = optimize(plan);
        // After pushdown: Expand(Filter(CartesianProduct(Scan(a), Scan(b))), a -> b)
        // After expand_into: ExpandInto(Filter(CartesianProduct(Scan(a), Scan(b))), a -> b)
        match &optimized {
            LogicalPlanNode::ExpandInto { input, source_var, target_var, .. } => {
                assert_eq!(source_var, "a");
                assert_eq!(target_var, "b");
                match input.as_ref() {
                    LogicalPlanNode::Filter { input: inner, .. } => {
                        match inner.as_ref() {
                            LogicalPlanNode::CartesianProduct { .. } => { /* correct */ }
                            other => panic!("Expected CartesianProduct, got {:?}", other),
                        }
                    }
                    other => panic!("Expected Filter, got {:?}", other),
                }
            }
            other => panic!("Expected ExpandInto at top, got {:?}", other),
        }
    }

    #[test]
    fn test_triangle_pattern_becomes_trie_join() {
        // Simulate a triangle plan after insert_expand_into:
        // ExpandInto(c→a, input=Expand(b→c, input=Expand(a→b, input=Scan(a))))
        // The optimizer should merge Expand(b→c)+ExpandInto(c→a) into TrieJoin
        let plan = LogicalPlanNode::ExpandInto {
            input: Box::new(LogicalPlanNode::Expand {
                input: Box::new(LogicalPlanNode::Expand {
                    input: Box::new(LogicalPlanNode::LabelScan {
                        variable: "a".to_string(),
                        label: Some(Label::new("Node")),
                    }),
                    source_var: "a".to_string(),
                    target_var: "b".to_string(),
                    edge_var: None,
                    edge_types: vec![EdgeType::new("EDGE")],
                    direction: ExpandDirection::Forward,
                }),
                source_var: "b".to_string(),
                target_var: "c".to_string(),
                edge_var: None,
                edge_types: vec![EdgeType::new("EDGE")],
                direction: ExpandDirection::Forward,
            }),
            source_var: "c".to_string(),
            target_var: "a".to_string(),
            edge_types: vec![EdgeType::new("EDGE")],
            edge_var: None,
        };

        let optimized = merge_cyclic_to_trie_join(plan);
        match &optimized {
            LogicalPlanNode::TrieJoin { target_var, constraints, input } => {
                assert_eq!(target_var, "c", "TrieJoin should solve for c");
                assert_eq!(constraints.len(), 2, "Should have 2 constraints");
                // Constraint 1: from Expand(b→c) → c ∈ N_out(b)
                assert_eq!(constraints[0].bound_var, "b");
                assert_eq!(constraints[0].direction, ExpandDirection::Forward);
                // Constraint 2: from ExpandInto(c→a) → c ∈ N_in(a)
                assert_eq!(constraints[1].bound_var, "a");
                assert_eq!(constraints[1].direction, ExpandDirection::Reverse);
                // Input should be the Expand(a→b)
                match input.as_ref() {
                    LogicalPlanNode::Expand { source_var, target_var, .. } => {
                        assert_eq!(source_var, "a");
                        assert_eq!(target_var, "b");
                    }
                    other => panic!("Expected Expand(a→b) as input, got {:?}", other),
                }
            }
            other => panic!("Expected TrieJoin, got {:?}", other),
        }
    }

    #[test]
    fn test_non_cyclic_not_converted_to_trie_join() {
        // Chain: Scan(a) → Expand(a→b) → Expand(b→c)
        // No ExpandInto → no TrieJoin
        let plan = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::Expand {
                input: Box::new(LogicalPlanNode::LabelScan {
                    variable: "a".to_string(),
                    label: Some(Label::new("Node")),
                }),
                source_var: "a".to_string(),
                target_var: "b".to_string(),
                edge_var: None,
                edge_types: vec![],
                direction: ExpandDirection::Forward,
            }),
            source_var: "b".to_string(),
            target_var: "c".to_string(),
            edge_var: None,
            edge_types: vec![],
            direction: ExpandDirection::Forward,
        };

        let optimized = merge_cyclic_to_trie_join(plan);
        match optimized {
            LogicalPlanNode::Expand { .. } => { /* correct — no TrieJoin for acyclic */ }
            other => panic!("Expected Expand (no conversion), got {:?}", other),
        }
    }

    fn scan(var: &str) -> LogicalPlanNode {
        LogicalPlanNode::LabelScan { variable: var.to_string(), label: None }
    }

    fn expand(input: LogicalPlanNode, src: &str, tgt: &str, edge_var: Option<&str>) -> LogicalPlanNode {
        LogicalPlanNode::Expand {
            input: Box::new(input),
            source_var: src.to_string(),
            target_var: tgt.to_string(),
            edge_var: edge_var.map(|s| s.to_string()),
            edge_types: vec![EdgeType::new("E")],
            direction: ExpandDirection::Forward,
        }
    }

    fn expand_into(input: LogicalPlanNode, src: &str, tgt: &str) -> LogicalPlanNode {
        LogicalPlanNode::ExpandInto {
            input: Box::new(input),
            source_var: src.to_string(),
            target_var: tgt.to_string(),
            edge_types: vec![EdgeType::new("E")],
            edge_var: None,
        }
    }

    fn filter(input: LogicalPlanNode, predicate: Expression) -> LogicalPlanNode {
        LogicalPlanNode::Filter { input: Box::new(input), predicate }
    }

    fn prop_eq(var: &str) -> Expression {
        Expression::Binary {
            left: Box::new(Expression::Property { variable: var.to_string(), property: "x".to_string() }),
            op: crate::query::ast::BinaryOp::Eq,
            right: Box::new(Expression::Literal(PropertyValue::Integer(1))),
        }
    }

    /// Parse `MATCH (a), (b) WHERE <pred> RETURN a` and return the predicate.
    fn where_expr(pred: &str) -> Expression {
        let q = crate::query::parser::parse_query(&format!("MATCH (a), (b) WHERE {} RETURN a", pred))
            .unwrap_or_else(|e| panic!("parse {}: {:?}", pred, e));
        q.where_clause.expect("where clause").predicate
    }

    fn sorted_vars(expr: &Expression) -> Vec<String> {
        let mut v: Vec<String> = collect_expression_vars(expr).into_iter().collect();
        v.sort();
        v
    }

    #[test]
    fn test_filter_over_scan_stays_on_top() {
        let plan = filter(scan("a"), prop_eq("a"));
        match push_filters_down(plan) {
            LogicalPlanNode::Filter { input, .. } => assert!(matches!(*input, LogicalPlanNode::LabelScan { .. })),
            other => panic!("expected Filter, got {:?}", other),
        }
    }

    #[test]
    fn test_constant_filter_is_not_pushed_below_expand() {
        // A predicate with no variables must stay where it is.
        let plan = filter(expand(scan("a"), "a", "b", None), Expression::Literal(PropertyValue::Boolean(true)));
        match push_filters_down(plan) {
            LogicalPlanNode::Filter { input, .. } => assert!(matches!(*input, LogicalPlanNode::Expand { .. })),
            other => panic!("expected Filter on top, got {:?}", other),
        }
    }

    #[test]
    fn test_filter_on_edge_variable_is_not_pushed_below_expand() {
        let plan = filter(expand(scan("a"), "a", "b", Some("r")), prop_eq("r"));
        match push_filters_down(plan) {
            LogicalPlanNode::Filter { input, .. } => match *input {
                LogicalPlanNode::Expand { edge_var, .. } => assert_eq!(edge_var.as_deref(), Some("r")),
                other => panic!("expected Expand, got {:?}", other),
            },
            other => panic!("expected Filter on top, got {:?}", other),
        }
    }

    #[test]
    fn test_filter_on_source_is_pushed_below_expand_with_edge_var() {
        let plan = filter(expand(scan("a"), "a", "b", Some("r")), prop_eq("a"));
        match push_filters_down(plan) {
            LogicalPlanNode::Expand { input, edge_var, .. } => {
                assert_eq!(edge_var.as_deref(), Some("r"));
                assert!(matches!(*input, LogicalPlanNode::Filter { .. }));
            }
            other => panic!("expected Expand on top, got {:?}", other),
        }
    }

    #[test]
    fn test_pushdown_recurses_through_expand_into_join_and_cartesian() {
        // Each child subtree holds Filter(Expand(Scan)) with a pushable predicate.
        let pushable = || filter(expand(scan("a"), "a", "b", None), prop_eq("a"));
        let is_pushed = |p: &LogicalPlanNode| matches!(p, LogicalPlanNode::Expand { input, .. } if matches!(**input, LogicalPlanNode::Filter { .. }));

        match push_filters_down(expand_into(pushable(), "a", "b")) {
            LogicalPlanNode::ExpandInto { input, .. } => assert!(is_pushed(&input)),
            other => panic!("{:?}", other),
        }
        match push_filters_down(LogicalPlanNode::Join { left: Box::new(pushable()), right: Box::new(pushable()), join_keys: vec!["a".into()] }) {
            LogicalPlanNode::Join { left, right, join_keys } => {
                assert!(is_pushed(&left) && is_pushed(&right));
                assert_eq!(join_keys, vec!["a".to_string()]);
            }
            other => panic!("{:?}", other),
        }
        match push_filters_down(LogicalPlanNode::CartesianProduct { left: Box::new(pushable()), right: Box::new(pushable()) }) {
            LogicalPlanNode::CartesianProduct { left, right } => assert!(is_pushed(&left) && is_pushed(&right)),
            other => panic!("{:?}", other),
        }
        // Expand over Filter(Expand): the inner expand is optimised too.
        match push_filters_down(expand(pushable(), "b", "c", None)) {
            LogicalPlanNode::Expand { input, .. } => assert!(is_pushed(&input)),
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn test_insert_expand_into_recurses_through_wrappers() {
        let convertible = || expand(LogicalPlanNode::CartesianProduct { left: Box::new(scan("a")), right: Box::new(scan("b")) }, "a", "b", None);

        match insert_expand_into(filter(convertible(), prop_eq("a"))) {
            LogicalPlanNode::Filter { input, .. } => assert!(matches!(*input, LogicalPlanNode::ExpandInto { .. })),
            other => panic!("{:?}", other),
        }
        match insert_expand_into(expand_into(convertible(), "a", "b")) {
            LogicalPlanNode::ExpandInto { input, .. } => assert!(matches!(*input, LogicalPlanNode::ExpandInto { .. })),
            other => panic!("{:?}", other),
        }
        match insert_expand_into(LogicalPlanNode::Join { left: Box::new(convertible()), right: Box::new(scan("c")), join_keys: vec![] }) {
            LogicalPlanNode::Join { left, right, .. } => {
                assert!(matches!(*left, LogicalPlanNode::ExpandInto { .. }));
                assert!(matches!(*right, LogicalPlanNode::LabelScan { .. }));
            }
            other => panic!("{:?}", other),
        }
        match insert_expand_into(LogicalPlanNode::CartesianProduct { left: Box::new(scan("c")), right: Box::new(convertible()) }) {
            LogicalPlanNode::CartesianProduct { right, .. } => assert!(matches!(*right, LogicalPlanNode::ExpandInto { .. })),
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn test_trie_join_when_expand_introduces_the_into_target() {
        // ExpandInto(a -> c) over Expand(b -> c): c is the ExpandInto's target,
        // so the second constraint is c ∈ N_out(a).
        let plan = expand_into(expand(expand(scan("a"), "a", "b", None), "b", "c", Some("r")), "a", "c");
        match merge_cyclic_to_trie_join(plan) {
            LogicalPlanNode::TrieJoin { target_var, constraints, .. } => {
                assert_eq!(target_var, "c");
                assert_eq!(constraints[0].bound_var, "b");
                assert_eq!(constraints[0].edge_var.as_deref(), Some("r"));
                assert_eq!(constraints[1].bound_var, "a");
                assert_eq!(constraints[1].direction, ExpandDirection::Forward);
            }
            other => panic!("expected TrieJoin, got {:?}", other),
        }
    }

    #[test]
    fn test_expand_into_on_top_of_trie_join_adds_constraint() {
        // 4-clique style: a second ExpandInto touching the TrieJoin's target
        // becomes a third constraint on the same TrieJoin.
        let triangle = expand_into(expand(expand(scan("a"), "a", "b", None), "b", "c", None), "c", "a");
        let as_source = expand_into(triangle.clone(), "c", "d");
        match merge_cyclic_to_trie_join(as_source) {
            LogicalPlanNode::TrieJoin { target_var, constraints, .. } => {
                assert_eq!(target_var, "c");
                assert_eq!(constraints.len(), 3);
                assert_eq!(constraints[2].bound_var, "d");
                assert_eq!(constraints[2].direction, ExpandDirection::Reverse);
            }
            other => panic!("expected TrieJoin, got {:?}", other),
        }
        let as_target = expand_into(triangle, "d", "c");
        match merge_cyclic_to_trie_join(as_target) {
            LogicalPlanNode::TrieJoin { constraints, .. } => {
                assert_eq!(constraints.len(), 3);
                assert_eq!(constraints[2].bound_var, "d");
                assert_eq!(constraints[2].direction, ExpandDirection::Forward);
            }
            other => panic!("expected TrieJoin, got {:?}", other),
        }
    }

    #[test]
    fn test_expand_into_not_touching_new_variable_is_kept() {
        // ExpandInto(x -> y) over Expand(a -> b): b is neither endpoint.
        let plan = expand_into(expand(scan("a"), "a", "b", None), "x", "y");
        match merge_cyclic_to_trie_join(plan) {
            LogicalPlanNode::ExpandInto { source_var, input, .. } => {
                assert_eq!(source_var, "x");
                assert!(matches!(*input, LogicalPlanNode::Expand { .. }));
            }
            other => panic!("expected ExpandInto, got {:?}", other),
        }
        // Same for a TrieJoin whose target is not an endpoint.
        let tj = LogicalPlanNode::TrieJoin { input: Box::new(scan("a")), target_var: "t".into(), constraints: vec![] };
        match merge_cyclic_to_trie_join(expand_into(tj, "x", "y")) {
            LogicalPlanNode::ExpandInto { input, .. } => assert!(matches!(*input, LogicalPlanNode::TrieJoin { .. })),
            other => panic!("expected ExpandInto, got {:?}", other),
        }
    }

    #[test]
    fn test_trie_join_merge_recurses_through_wrappers() {
        let triangle = || expand_into(expand(expand(scan("a"), "a", "b", None), "b", "c", None), "c", "a");
        let is_tj = |p: &LogicalPlanNode| matches!(p, LogicalPlanNode::TrieJoin { .. });

        match merge_cyclic_to_trie_join(filter(triangle(), prop_eq("a"))) {
            LogicalPlanNode::Filter { input, .. } => assert!(is_tj(&input)),
            other => panic!("{:?}", other),
        }
        match merge_cyclic_to_trie_join(LogicalPlanNode::Join { left: Box::new(triangle()), right: Box::new(triangle()), join_keys: vec![] }) {
            LogicalPlanNode::Join { left, right, .. } => assert!(is_tj(&left) && is_tj(&right)),
            other => panic!("{:?}", other),
        }
        match merge_cyclic_to_trie_join(LogicalPlanNode::CartesianProduct { left: Box::new(triangle()), right: Box::new(scan("z")) }) {
            LogicalPlanNode::CartesianProduct { left, .. } => assert!(is_tj(&left)),
            other => panic!("{:?}", other),
        }
        let outer = LogicalPlanNode::TrieJoin { input: Box::new(triangle()), target_var: "q".into(), constraints: vec![] };
        match merge_cyclic_to_trie_join(outer) {
            LogicalPlanNode::TrieJoin { input, target_var, .. } => {
                assert_eq!(target_var, "q");
                assert!(is_tj(&input));
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn test_optimize_full_triangle_pipeline() {
        // Scan(a) -> Expand(a->b) -> Expand(b->c) -> Expand(c->a) where a is bound:
        // insert_expand_into turns the last hop into ExpandInto, then the
        // cyclic merge produces a TrieJoin solving for c.
        let plan = expand(expand(expand(scan("a"), "a", "b", None), "b", "c", None), "c", "a", None);
        match optimize(plan) {
            LogicalPlanNode::TrieJoin { target_var, constraints, .. } => {
                assert_eq!(target_var, "c");
                assert_eq!(constraints.len(), 2);
            }
            other => panic!("expected TrieJoin, got {:?}", other),
        }
    }

    #[test]
    fn test_collect_vars_unary_function_case_index() {
        assert_eq!(sorted_vars(&where_expr("NOT a.x")), vec!["a"]);
        assert_eq!(sorted_vars(&where_expr("toUpper(a.name) = b.name")), vec!["a", "b"]);
        assert_eq!(
            sorted_vars(&where_expr("CASE a.x WHEN b.y THEN a ELSE b END = 1")),
            vec!["a", "b"]
        );
        assert_eq!(sorted_vars(&where_expr("CASE WHEN a.x > 1 THEN 1 END = 1")), vec!["a"]);
        assert_eq!(sorted_vars(&where_expr("a.list[b.i] = 1")), vec!["a", "b"]);
        assert!(sorted_vars(&where_expr("1 = 1")).is_empty());
    }

    #[test]
    fn test_collect_vars_exists_subquery_includes_pattern_and_where() {
        let vars = sorted_vars(&where_expr("EXISTS { MATCH (a)-[r:KNOWS]->(c) WHERE c.age > b.age }"));
        assert_eq!(vars, vec!["a", "b", "c", "r"]);
    }

    #[test]
    fn test_collect_vars_list_comprehension_predicate_function_reduce() {
        let lc = sorted_vars(&where_expr("size([x IN a.list WHERE x > b.min | x * 2]) > 0"));
        assert!(lc.contains(&"a".to_string()) && lc.contains(&"b".to_string()), "{:?}", lc);
        let lc_no_filter = sorted_vars(&where_expr("size([x IN a.list | x]) > 0"));
        assert!(lc_no_filter.contains(&"a".to_string()), "{:?}", lc_no_filter);
        let pf = sorted_vars(&where_expr("any(x IN a.list WHERE x = b.v)"));
        assert!(pf.contains(&"a".to_string()) && pf.contains(&"b".to_string()), "{:?}", pf);
        let rd = sorted_vars(&where_expr("reduce(acc = b.start, x IN a.list | acc + x) > 0"));
        assert!(rd.contains(&"a".to_string()) && rd.contains(&"b".to_string()), "{:?}", rd);
    }

    #[test]
    fn test_exists_filter_is_not_pushed_below_the_expand_that_binds_it() {
        // The subquery references `b`, which the Expand introduces.
        let plan = filter(expand(scan("a"), "a", "b", None), where_expr("EXISTS { MATCH (b)-->(c) }"));
        assert!(matches!(push_filters_down(plan), LogicalPlanNode::Filter { .. }));
    }

    #[test]
    fn test_trie_join_bound_variables() {
        use super::super::logical_plan::TrieJoinConstraint;
        let plan = LogicalPlanNode::TrieJoin {
            input: Box::new(LogicalPlanNode::Expand {
                input: Box::new(LogicalPlanNode::LabelScan {
                    variable: "a".to_string(),
                    label: None,
                }),
                source_var: "a".to_string(),
                target_var: "b".to_string(),
                edge_var: None,
                edge_types: vec![],
                direction: ExpandDirection::Forward,
            }),
            target_var: "c".to_string(),
            constraints: vec![
                TrieJoinConstraint { bound_var: "b".to_string(), direction: ExpandDirection::Forward, edge_types: vec![], edge_var: Some("r2".to_string()) },
                TrieJoinConstraint { bound_var: "a".to_string(), direction: ExpandDirection::Reverse, edge_types: vec![], edge_var: None },
            ],
        };

        let vars = plan.bound_variables();
        assert!(vars.contains("a"));
        assert!(vars.contains("b"));
        assert!(vars.contains("c"));
        assert!(vars.contains("r2"));
        assert_eq!(vars.len(), 4);
    }
}
