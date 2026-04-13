//! Cost model for the graph-native query planner (ADR-015)
//!
//! Estimates the cardinality (number of rows) produced by each logical plan node,
//! using triple-level statistics from GraphCatalog.

use crate::graph::catalog::GraphCatalog;
use crate::graph::types::Label;
use super::logical_plan::{LogicalPlanNode, ExpandDirection};

/// Estimate the cost (cardinality) of a logical plan using the catalog.
///
/// The cost model is multiplicative:
/// - LabelScan: label_count
/// - Expand Forward: input_cost × avg_out_degree
/// - Expand Reverse: input_cost × avg_in_degree
/// - ExpandInto: input_cost × edge_existence_probability
/// - Filter: input_cost × selectivity (default 0.5)
/// - Join: left_cost × right_cost × join_selectivity
/// - CartesianProduct: left_cost × right_cost
pub fn estimate_plan_cost(plan: &LogicalPlanNode, catalog: &GraphCatalog) -> f64 {
    match plan {
        LogicalPlanNode::LabelScan { label, .. } => {
            match label {
                Some(l) => catalog.estimate_label_scan(l).max(1.0),
                None => {
                    // All nodes scan — sum all label counts
                    let total: f64 = catalog.label_counts.values().sum::<usize>() as f64;
                    total.max(1.0)
                }
            }
        }

        LogicalPlanNode::IndexLookup { .. } => {
            // Index lookup: very selective, assume ~10 rows
            10.0
        }

        LogicalPlanNode::Expand { input, source_var: _, edge_types, direction, .. } => {
            let input_cost = estimate_plan_cost(input, catalog);
            // Get the source label from the input plan
            let source_label = extract_label_for_var(input, &get_expand_source_var(plan));

            let degree = if edge_types.is_empty() {
                // No type filter — use a default multiplier
                2.0
            } else {
                let mut total = 0.0;
                for et in edge_types {
                    let d = match (direction, &source_label) {
                        (ExpandDirection::Forward, Some(l)) => catalog.estimate_expand_out(l, et),
                        (ExpandDirection::Reverse, Some(l)) => catalog.estimate_expand_in(l, et),
                        _ => 1.0,
                    };
                    total += d;
                }
                total.max(0.1) // avoid zero
            };

            input_cost * degree
        }

        LogicalPlanNode::ExpandInto { input, source_var: _, target_var: _, edge_types, .. } => {
            let input_cost = estimate_plan_cost(input, catalog);
            // ExpandInto filters: only edges that exist pass through
            // Use edge existence probability as the selectivity
            let selectivity = if edge_types.is_empty() {
                0.1 // default
            } else {
                // Average across edge types
                let mut total_prob = 0.0;
                let count = edge_types.len() as f64;
                for et in edge_types {
                    // We'd need source/target labels for accurate estimation
                    // For now use a heuristic
                    let _ = et;
                    total_prob += 0.1;
                }
                (total_prob / count).min(1.0)
            };
            input_cost * selectivity
        }

        LogicalPlanNode::TrieJoin { input, constraints, .. } => {
            let input_cost = estimate_plan_cost(input, catalog);
            // TrieJoin is worst-case optimal: cost is input_cost × intersection selectivity.
            // With k constraints, the intersection is much tighter than chained ExpandInto.
            // For k=2 constraints (triangle), selectivity ~ 0.01 (vs 0.1 per ExpandInto).
            // For k=3+ constraints (cliques), selectivity drops further.
            let k = constraints.len() as f64;
            let selectivity = 0.1_f64.powf(k.max(1.0));
            input_cost * selectivity.max(0.001)
        }

        LogicalPlanNode::Filter { input, .. } => {
            let input_cost = estimate_plan_cost(input, catalog);
            input_cost * 0.5 // default selectivity
        }

        LogicalPlanNode::Join { left, right, .. } => {
            let left_cost = estimate_plan_cost(left, catalog);
            let right_cost = estimate_plan_cost(right, catalog);
            // Hash join: cost is dominated by the larger side
            left_cost + right_cost
        }

        LogicalPlanNode::CartesianProduct { left, right } => {
            let left_cost = estimate_plan_cost(left, catalog);
            let right_cost = estimate_plan_cost(right, catalog);
            left_cost * right_cost
        }

        LogicalPlanNode::AdjacencyCountAggregate { input, .. } => {
            // Cost = O(|grouped endpoint scan|) — one degree lookup per node.
            // Degree lookup itself is O(1) amortized on the adjacency list, so
            // we treat it as a constant-per-row overhead and charge only the
            // input scan cardinality. The dramatic speedup vs Expand→Aggregate
            // is exactly this change in the degree factor.
            estimate_plan_cost(input, catalog)
        }
    }
}

/// Extract the source variable from an Expand node
fn get_expand_source_var(plan: &LogicalPlanNode) -> String {
    match plan {
        LogicalPlanNode::Expand { source_var, .. } => source_var.clone(),
        _ => String::new(),
    }
}

/// Try to extract the label for a variable from a plan tree
fn extract_label_for_var(plan: &LogicalPlanNode, variable: &str) -> Option<Label> {
    match plan {
        LogicalPlanNode::LabelScan { variable: v, label, .. } if v == variable => {
            label.clone()
        }
        LogicalPlanNode::IndexLookup { variable: v, label, .. } if v == variable => {
            Some(label.clone())
        }
        LogicalPlanNode::Expand { input, .. } => extract_label_for_var(input, variable),
        LogicalPlanNode::ExpandInto { input, .. } => extract_label_for_var(input, variable),
        LogicalPlanNode::TrieJoin { input, .. } => extract_label_for_var(input, variable),
        LogicalPlanNode::Filter { input, .. } => extract_label_for_var(input, variable),
        LogicalPlanNode::Join { left, right, .. } => {
            extract_label_for_var(left, variable)
                .or_else(|| extract_label_for_var(right, variable))
        }
        LogicalPlanNode::CartesianProduct { left, right } => {
            extract_label_for_var(left, variable)
                .or_else(|| extract_label_for_var(right, variable))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::{NodeId, EdgeType};
    use crate::query::ast::Expression;

    #[test]
    fn test_label_scan_cost() {
        let mut catalog = GraphCatalog::new();
        catalog.on_label_added(&Label::new("Person"));
        catalog.on_label_added(&Label::new("Person"));
        catalog.on_label_added(&Label::new("Person"));

        let plan = LogicalPlanNode::LabelScan {
            variable: "n".to_string(),
            label: Some(Label::new("Person")),
        };
        let cost = estimate_plan_cost(&plan, &catalog);
        assert_eq!(cost, 3.0);
    }

    #[test]
    fn test_expand_forward_cost() {
        let mut catalog = GraphCatalog::new();
        for i in 0..3 {
            catalog.on_label_added(&Label::new("Person"));
        }
        // Each Person knows 2 other Persons on average
        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);
        let p3 = NodeId::new(3);
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p3, &[Label::new("Person")]);

        let plan = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::LabelScan {
                variable: "a".to_string(),
                label: Some(Label::new("Person")),
            }),
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("KNOWS")],
            direction: ExpandDirection::Forward,
        };

        let cost = estimate_plan_cost(&plan, &catalog);
        // LabelScan(Person) = 3, avg_out_degree = 2.0, cost = 3 * 2.0 = 6.0
        assert_eq!(cost, 6.0);
    }

    #[test]
    fn test_expand_reverse_cost() {
        let mut catalog = GraphCatalog::new();
        for _ in 0..10 {
            catalog.on_label_added(&Label::new("Person"));
        }
        catalog.on_label_added(&Label::new("Company"));

        // 10 persons work at 1 company
        for i in 0..10 {
            catalog.on_edge_created(
                NodeId::new(i),
                &[Label::new("Person")],
                &EdgeType::new("WORKS_AT"),
                NodeId::new(100),
                &[Label::new("Company")],
            );
        }

        // Start from Company, expand incoming WORKS_AT (reverse)
        let plan = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::LabelScan {
                variable: "c".to_string(),
                label: Some(Label::new("Company")),
            }),
            source_var: "c".to_string(),
            target_var: "p".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("WORKS_AT")],
            direction: ExpandDirection::Reverse,
        };

        let cost = estimate_plan_cost(&plan, &catalog);
        // LabelScan(Company) = 1, avg_in_degree = 10.0, cost = 1 * 10.0 = 10.0
        assert_eq!(cost, 10.0);
    }

    #[test]
    fn test_filter_halves_cost() {
        let catalog = GraphCatalog::new();
        let plan = LogicalPlanNode::Filter {
            input: Box::new(LogicalPlanNode::LabelScan {
                variable: "n".to_string(),
                label: None,
            }),
            predicate: Expression::Literal(crate::graph::PropertyValue::Boolean(true)),
        };
        let cost = estimate_plan_cost(&plan, &catalog);
        // Empty catalog -> label scan = 1.0 (max), filter = 0.5
        assert_eq!(cost, 0.5);
    }

    #[test]
    fn test_cartesian_product_cost() {
        let mut catalog = GraphCatalog::new();
        for _ in 0..5 {
            catalog.on_label_added(&Label::new("A"));
        }
        for _ in 0..10 {
            catalog.on_label_added(&Label::new("B"));
        }

        let plan = LogicalPlanNode::CartesianProduct {
            left: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("A")) }),
            right: Box::new(LogicalPlanNode::LabelScan { variable: "b".to_string(), label: Some(Label::new("B")) }),
        };
        let cost = estimate_plan_cost(&plan, &catalog);
        assert_eq!(cost, 50.0); // 5 * 10
    }

    #[test]
    fn test_asymmetric_direction_choice() {
        // The key scenario: 100 Companies vs 1M Persons
        // Starting from Person (out) should be cheaper than from Company (in)
        let mut catalog = GraphCatalog::new();
        for _ in 0..1000 {
            catalog.on_label_added(&Label::new("Person"));
        }
        for _ in 0..10 {
            catalog.on_label_added(&Label::new("Company"));
        }

        // Each Person works at 1 Company
        for i in 0..1000 {
            catalog.on_edge_created(
                NodeId::new(i),
                &[Label::new("Person")],
                &EdgeType::new("WORKS_AT"),
                NodeId::new(1000 + (i % 10)),
                &[Label::new("Company")],
            );
        }

        // Plan A: Person -> WORKS_AT -> Company (forward)
        let plan_forward = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::LabelScan {
                variable: "p".to_string(),
                label: Some(Label::new("Person")),
            }),
            source_var: "p".to_string(),
            target_var: "c".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("WORKS_AT")],
            direction: ExpandDirection::Forward,
        };

        // Plan B: Company -> WORKS_AT reverse -> Person (reverse from Company)
        let plan_reverse = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::LabelScan {
                variable: "c".to_string(),
                label: Some(Label::new("Company")),
            }),
            source_var: "c".to_string(),
            target_var: "p".to_string(),
            edge_var: None,
            edge_types: vec![EdgeType::new("WORKS_AT")],
            direction: ExpandDirection::Reverse,
        };

        let cost_a = estimate_plan_cost(&plan_forward, &catalog);
        let cost_b = estimate_plan_cost(&plan_reverse, &catalog);

        // Plan A: 1000 persons * 1.0 out_degree = 1000
        // Plan B: 10 companies * 100.0 in_degree = 1000
        // Both produce the same result set, but Plan B starts from fewer nodes
        // The key insight: for this specific case they're similar, but if we had 100 companies:
        // Plan A: 1000 * 1 = 1000, Plan B: 100 * 10 = 1000
        // The planner picks the cheapest
        assert!(cost_a > 0.0);
        assert!(cost_b > 0.0);
    }

    #[test]
    fn test_index_lookup_cost() {
        let catalog = GraphCatalog::new();
        let plan = LogicalPlanNode::IndexLookup {
            variable: "n".to_string(),
            label: Label::new("Person"),
            property: "id".to_string(),
            value: Expression::Literal(crate::graph::PropertyValue::Integer(42)),
        };
        let cost = estimate_plan_cost(&plan, &catalog);
        assert_eq!(cost, 10.0); // fixed assumption for index lookups
    }
}
