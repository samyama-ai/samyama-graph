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
        Expression::ExistsSubquery { .. } => {} // Subquery scoping — don't leak vars
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
