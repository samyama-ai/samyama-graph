//! Physical planner: converts logical plan nodes to physical operators (ADR-015)
//!
//! Maps each LogicalPlanNode to a concrete physical operator implementation:
//! - LabelScan → NodeScanOperator
//! - Expand { Forward } → ExpandOperator(Direction::Outgoing)
//! - Expand { Reverse } → ExpandOperator(Direction::Incoming) (QP-14: direction reversal)
//! - ExpandInto → ExpandIntoOperator
//! - Filter → FilterOperator

use crate::query::ast::Direction;
use super::logical_plan::{LogicalPlanNode, ExpandDirection};
use super::operator::*;
use super::leapfrog::{TrieJoinOperator, PhysicalTrieConstraint};

/// Convert a logical plan tree into a physical operator tree
pub fn logical_to_physical(plan: &LogicalPlanNode) -> OperatorBox {
    match plan {
        LogicalPlanNode::LabelScan { variable, label } => {
            let labels = match label {
                Some(l) => vec![l.clone()],
                None => vec![],
            };
            Box::new(NodeScanOperator::new(variable.clone(), labels))
        }

        LogicalPlanNode::IndexLookup { variable, label, .. } => {
            // Fall back to label scan for now — index integration in Phase 3
            Box::new(NodeScanOperator::new(variable.clone(), vec![label.clone()]))
        }

        LogicalPlanNode::Expand { input, source_var, target_var, edge_var, edge_types, direction } => {
            let physical_input = logical_to_physical(input);

            // QP-14: direction reversal — map logical direction to physical
            let physical_direction = match direction {
                ExpandDirection::Forward => Direction::Outgoing,
                ExpandDirection::Reverse => Direction::Incoming,
            };

            let et_strings: Vec<String> = edge_types.iter().map(|et| et.as_str().to_string()).collect();

            Box::new(ExpandOperator::new(
                physical_input,
                source_var.clone(),
                target_var.clone(),
                edge_var.clone(),
                et_strings,
                physical_direction,
            ))
        }

        LogicalPlanNode::ExpandInto { input, source_var, target_var, edge_types, edge_var } => {
            let physical_input = logical_to_physical(input);

            let et = if edge_types.len() == 1 {
                Some(edge_types[0].as_str().to_string())
            } else {
                None // any type
            };

            Box::new(ExpandIntoOperator::new(
                physical_input,
                source_var.clone(),
                target_var.clone(),
                et,
                edge_var.clone(),
            ))
        }

        LogicalPlanNode::TrieJoin { input, target_var, constraints } => {
            let physical_input = logical_to_physical(input);

            let physical_constraints: Vec<PhysicalTrieConstraint> = constraints.iter().map(|c| {
                let direction = match c.direction {
                    ExpandDirection::Forward => Direction::Outgoing,
                    ExpandDirection::Reverse => Direction::Incoming,
                };
                let et_strings: Vec<String> = c.edge_types.iter().map(|et| et.as_str().to_string()).collect();
                PhysicalTrieConstraint {
                    bound_var: c.bound_var.clone(),
                    direction,
                    edge_types: et_strings,
                    edge_var: c.edge_var.clone(),
                }
            }).collect();

            Box::new(TrieJoinOperator::new(
                physical_input,
                target_var.clone(),
                physical_constraints,
            ))
        }

        LogicalPlanNode::Filter { input, predicate } => {
            let physical_input = logical_to_physical(input);
            Box::new(FilterOperator::new(physical_input, predicate.clone()))
        }

        LogicalPlanNode::Join { left, right, join_keys } => {
            let physical_left = logical_to_physical(left);
            let physical_right = logical_to_physical(right);
            // JoinOperator takes a single join variable
            let join_var = join_keys.first().cloned().unwrap_or_default();
            Box::new(JoinOperator::new(physical_left, physical_right, join_var))
        }

        LogicalPlanNode::CartesianProduct { left, right } => {
            let physical_left = logical_to_physical(left);
            let physical_right = logical_to_physical(right);
            Box::new(CartesianProductOperator::new(physical_left, physical_right))
        }

        LogicalPlanNode::AdjacencyCountAggregate {
            input,
            grouped_var,
            edge_type,
            direction,
            count_alias,
            ..
        } => {
            // ADR-017 Phase 1: lower to the physical operator. The `neighbor_var`,
            // `neighbor_label`, and `distinct` fields on the logical node are
            // recorded for diagnostics/EXPLAIN but not needed by the physical
            // operator — counting uses edge-type alone. The detector already
            // rejects shapes where neighbor_label filtering would change the count.
            let physical_input = logical_to_physical(input);
            let physical_direction = match direction {
                ExpandDirection::Forward => Direction::Outgoing,
                ExpandDirection::Reverse => Direction::Incoming,
            };
            Box::new(AdjacencyCountAggregateOperator::new(
                physical_input,
                grouped_var.clone(),
                count_alias.clone(),
                edge_type.clone(),
                physical_direction,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::{Label, EdgeType};
    use crate::graph::GraphStore;
    use super::super::record::Value;

    #[test]
    fn test_label_scan_conversion() {
        let plan = LogicalPlanNode::LabelScan {
            variable: "n".to_string(),
            label: Some(Label::new("Person")),
        };
        let op = logical_to_physical(&plan);
        let desc = op.describe();
        assert_eq!(desc.name, "NodeScan");
        assert!(desc.details.contains("Person"));
    }

    #[test]
    fn test_expand_forward_conversion() {
        let plan = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::LabelScan {
                variable: "a".to_string(),
                label: Some(Label::new("Person")),
            }),
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_var: Some("r".to_string()),
            edge_types: vec![EdgeType::new("KNOWS")],
            direction: ExpandDirection::Forward,
        };
        let op = logical_to_physical(&plan);
        let desc = op.describe();
        assert_eq!(desc.name, "Expand");
        assert!(desc.details.contains("KNOWS"));
    }

    #[test]
    fn test_expand_reverse_conversion() {
        // QP-14: verify direction reversal produces an Incoming operator
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
        let op = logical_to_physical(&plan);
        let desc = op.describe();
        assert_eq!(desc.name, "Expand");
        // Direction should be Incoming (arrow notation: <-)
        assert!(desc.details.contains("<-"), "Reverse expand should produce Incoming direction (arrow <-), got: {}", desc.details);
    }

    #[test]
    fn test_expand_into_conversion() {
        let plan = LogicalPlanNode::ExpandInto {
            input: Box::new(LogicalPlanNode::CartesianProduct {
                left: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Person")) }),
                right: Box::new(LogicalPlanNode::LabelScan { variable: "b".to_string(), label: Some(Label::new("Person")) }),
            }),
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_types: vec![EdgeType::new("KNOWS")],
            edge_var: None,
        };
        let op = logical_to_physical(&plan);
        let desc = op.describe();
        assert_eq!(desc.name, "ExpandInto");
    }

    #[test]
    fn test_direction_reversal_correctness() {
        // Build a graph: Person(p1) -[:WORKS_AT]-> Company(c1)
        let mut store = GraphStore::new();
        let p1 = store.create_node("Person");
        let c1 = store.create_node("Company");
        store.create_edge(p1, c1, "WORKS_AT").unwrap();

        // Forward plan: start from Person, expand WORKS_AT forward
        let forward_plan = LogicalPlanNode::Expand {
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

        // Reverse plan: start from Company, expand WORKS_AT reverse (incoming)
        let reverse_plan = LogicalPlanNode::Expand {
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

        // Execute forward plan
        let mut fwd_op = logical_to_physical(&forward_plan);
        let mut forward_results = Vec::new();
        while let Some(record) = fwd_op.next(&store).unwrap() {
            let p_id = record.get("p").unwrap().node_id().unwrap();
            let c_id = record.get("c").unwrap().node_id().unwrap();
            forward_results.push((p_id, c_id));
        }

        // Execute reverse plan
        let mut rev_op = logical_to_physical(&reverse_plan);
        let mut reverse_results = Vec::new();
        while let Some(record) = rev_op.next(&store).unwrap() {
            let c_id = record.get("c").unwrap().node_id().unwrap();
            let p_id = record.get("p").unwrap().node_id().unwrap();
            reverse_results.push((p_id, c_id));
        }

        // Both should find the same pair
        assert_eq!(forward_results.len(), 1);
        assert_eq!(reverse_results.len(), 1);
        assert_eq!(forward_results[0], (p1, c1));
        assert_eq!(reverse_results[0], (p1, c1));
    }
}
