//! Physical planner: converts logical plan nodes to physical operators (ADR-015)
//!
//! Maps each LogicalPlanNode to a concrete physical operator implementation:
//! - LabelScan → NodeScanOperator
//! - Expand { Forward } → ExpandOperator(Direction::Outgoing)
//! - Expand { Reverse } → ExpandOperator(Direction::Incoming) (QP-14: direction reversal)
//! - ExpandInto → ExpandIntoOperator
//! - Filter → FilterOperator

use crate::query::ast::{Direction, Expression};
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

        LogicalPlanNode::IndexLookup { variable, label, property, op, value } => {
            // The plan enumerator only ever emits IndexLookup after confirming an
            // index exists for (label, property) and normalizing `value` to a
            // literal (see plan_enumerator::normalize_index_predicate); by the time
            // we reach physical planning, query parameters have already been
            // substituted into literals as well (see executor::mod::substitute_params).
            match value {
                Expression::Literal(val) => Box::new(IndexScanOperator::new(
                    variable.clone(),
                    label.clone(),
                    property.clone(),
                    op.clone(),
                    val.clone(),
                )),
                _ => Box::new(NodeScanOperator::new(variable.clone(), vec![label.clone()])),
            }
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
            Box::new(JoinOperator::new(physical_left, physical_right, vec![join_var]))
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

    fn drain(op: &mut OperatorBox, store: &GraphStore) -> Vec<crate::query::executor::record::Record> {
        let mut out = Vec::new();
        while let Some(r) = op.next(store).unwrap() {
            out.push(r);
        }
        out
    }

    fn person_ages(store: &mut GraphStore, ages: &[i64]) -> Vec<crate::graph::NodeId> {
        use crate::graph::PropertyValue;
        ages.iter()
            .map(|&age| {
                let id = store.create_node("Person");
                store.set_node_property("default", id, "age", PropertyValue::Integer(age)).unwrap();
                id
            })
            .collect()
    }

    #[test]
    fn test_label_scan_without_label_scans_all_nodes() {
        let mut store = GraphStore::new();
        store.create_node("Person");
        store.create_node("Company");
        let plan = LogicalPlanNode::LabelScan { variable: "n".to_string(), label: None };
        let mut op = logical_to_physical(&plan);
        assert_eq!(op.describe().name, "NodeScan");
        assert_eq!(drain(&mut op, &store).len(), 2);
    }

    #[test]
    fn test_index_lookup_with_literal_becomes_index_scan() {
        use crate::graph::PropertyValue;
        use crate::query::ast::BinaryOp;
        let mut store = GraphStore::new();
        store.property_index.create_index(Label::new("Person"), "age".to_string());
        let ids = person_ages(&mut store, &[20, 30, 40]);

        let plan = LogicalPlanNode::IndexLookup {
            variable: "n".to_string(),
            label: Label::new("Person"),
            property: "age".to_string(),
            op: BinaryOp::Eq,
            value: Expression::Literal(PropertyValue::Integer(30)),
        };
        let mut op = logical_to_physical(&plan);
        assert_eq!(op.describe().name, "IndexScan");
        let rows = drain(&mut op, &store);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("n").unwrap().node_id(), Some(ids[1]));
    }

    #[test]
    fn test_index_lookup_with_non_literal_falls_back_to_label_scan() {
        use crate::query::ast::BinaryOp;
        let mut store = GraphStore::new();
        person_ages(&mut store, &[1, 2]);
        store.create_node("Company");

        let plan = LogicalPlanNode::IndexLookup {
            variable: "n".to_string(),
            label: Label::new("Person"),
            property: "age".to_string(),
            op: BinaryOp::Eq,
            value: Expression::Parameter("p".to_string()),
        };
        let mut op = logical_to_physical(&plan);
        let desc = op.describe();
        assert_eq!(desc.name, "NodeScan");
        assert!(desc.details.contains("Person"));
        // Falls back to scanning every Person, not only the matching one.
        assert_eq!(drain(&mut op, &store).len(), 2);
    }

    #[test]
    fn test_filter_conversion_applies_predicate() {
        use crate::graph::PropertyValue;
        use crate::query::ast::BinaryOp;
        let mut store = GraphStore::new();
        let ids = person_ages(&mut store, &[10, 50, 70]);

        let plan = LogicalPlanNode::Filter {
            input: Box::new(LogicalPlanNode::LabelScan { variable: "n".to_string(), label: Some(Label::new("Person")) }),
            predicate: Expression::Binary {
                left: Box::new(Expression::Property { variable: "n".to_string(), property: "age".to_string() }),
                op: BinaryOp::Gt,
                right: Box::new(Expression::Literal(PropertyValue::Integer(40))),
            },
        };
        let mut op = logical_to_physical(&plan);
        assert_eq!(op.describe().name, "Filter");
        let mut got: Vec<_> = drain(&mut op, &store).iter().map(|r| r.get("n").unwrap().node_id().unwrap()).collect();
        got.sort();
        assert_eq!(got, vec![ids[1], ids[2]]);
    }

    #[test]
    fn test_cartesian_product_conversion_multiplies_rows() {
        let mut store = GraphStore::new();
        store.create_node("Person");
        store.create_node("Person");
        store.create_node("Company");
        store.create_node("Company");
        store.create_node("Company");
        let plan = LogicalPlanNode::CartesianProduct {
            left: Box::new(LogicalPlanNode::LabelScan { variable: "p".to_string(), label: Some(Label::new("Person")) }),
            right: Box::new(LogicalPlanNode::LabelScan { variable: "c".to_string(), label: Some(Label::new("Company")) }),
        };
        let mut op = logical_to_physical(&plan);
        assert_eq!(op.describe().name, "CartesianProduct");
        let rows = drain(&mut op, &store);
        assert_eq!(rows.len(), 6);
        assert!(rows.iter().all(|r| r.has("p") && r.has("c")));
    }

    #[test]
    fn test_join_conversion_joins_on_first_key() {
        let mut store = GraphStore::new();
        let both = store.create_node_with_labels(vec![Label::new("Person"), Label::new("Employee")]);
        store.create_node("Person");
        store.create_node("Employee");

        let plan = LogicalPlanNode::Join {
            left: Box::new(LogicalPlanNode::LabelScan { variable: "n".to_string(), label: Some(Label::new("Person")) }),
            right: Box::new(LogicalPlanNode::LabelScan { variable: "n".to_string(), label: Some(Label::new("Employee")) }),
            join_keys: vec!["n".to_string(), "ignored".to_string()],
        };
        let mut op = logical_to_physical(&plan);
        let rows = drain(&mut op, &store);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("n").unwrap().node_id(), Some(both));
    }

    #[test]
    fn test_join_conversion_with_no_keys_uses_empty_join_var() {
        let plan = LogicalPlanNode::Join {
            left: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: None }),
            right: Box::new(LogicalPlanNode::LabelScan { variable: "b".to_string(), label: None }),
            join_keys: vec![],
        };
        let op = logical_to_physical(&plan);
        let desc = op.describe();
        assert_eq!(desc.children.len(), 2);
    }

    #[test]
    fn test_expand_into_with_multiple_types_matches_any_type() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("Person");
        store.create_edge(a, b, "LIKES").unwrap();

        let make = |types: Vec<EdgeType>| LogicalPlanNode::ExpandInto {
            input: Box::new(LogicalPlanNode::CartesianProduct {
                left: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Person")) }),
                right: Box::new(LogicalPlanNode::LabelScan { variable: "b".to_string(), label: Some(Label::new("Person")) }),
            }),
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_types: types,
            edge_var: None,
        };
        // Two types: the physical operator receives "any type" and finds the LIKES edge.
        let mut op = logical_to_physical(&make(vec![EdgeType::new("KNOWS"), EdgeType::new("HATES")]));
        let rows = drain(&mut op, &store);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("a").unwrap().node_id(), Some(a));
        assert_eq!(rows[0].get("b").unwrap().node_id(), Some(b));

        // One non-matching type filters it out.
        let mut op = logical_to_physical(&make(vec![EdgeType::new("KNOWS")]));
        assert!(drain(&mut op, &store).is_empty());
    }

    #[test]
    fn test_trie_join_conversion_finds_triangle() {
        use super::super::logical_plan::TrieJoinConstraint;
        let mut store = GraphStore::new();
        let a = store.create_node("Node");
        let b = store.create_node("Node");
        let c = store.create_node("Node");
        store.create_edge(a, b, "EDGE").unwrap();
        store.create_edge(b, c, "EDGE").unwrap();
        store.create_edge(c, a, "EDGE").unwrap();
        store.compact_adjacency();

        let plan = LogicalPlanNode::TrieJoin {
            input: Box::new(LogicalPlanNode::Expand {
                input: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Node")) }),
                source_var: "a".to_string(),
                target_var: "b".to_string(),
                edge_var: None,
                edge_types: vec![EdgeType::new("EDGE")],
                direction: ExpandDirection::Forward,
            }),
            target_var: "c".to_string(),
            constraints: vec![
                TrieJoinConstraint { bound_var: "b".to_string(), direction: ExpandDirection::Forward, edge_types: vec![EdgeType::new("EDGE")], edge_var: None },
                TrieJoinConstraint { bound_var: "a".to_string(), direction: ExpandDirection::Reverse, edge_types: vec![], edge_var: None },
            ],
        };
        let mut op = logical_to_physical(&plan);
        let desc = op.describe();
        assert_eq!(desc.name, "TrieJoin");
        assert!(desc.details.contains("N_out(b)[:EDGE]"), "{}", desc.details);
        assert!(desc.details.contains("N_in(a)"), "{}", desc.details);
        let rows = drain(&mut op, &store);
        assert_eq!(rows.len(), 3, "one triangle, three rotations");
    }

    fn degree_graph() -> (GraphStore, crate::graph::NodeId, crate::graph::NodeId) {
        let mut store = GraphStore::new();
        let hub = store.create_node("Person");
        let leaf = store.create_node("Person");
        let x = store.create_node("Person");
        store.create_edge(hub, leaf, "KNOWS").unwrap();
        store.create_edge(hub, x, "KNOWS").unwrap();
        store.create_edge(leaf, x, "KNOWS").unwrap();
        (store, hub, leaf)
    }

    fn adjacency_plan(direction: ExpandDirection) -> LogicalPlanNode {
        LogicalPlanNode::AdjacencyCountAggregate {
            input: Box::new(LogicalPlanNode::LabelScan { variable: "p".to_string(), label: Some(Label::new("Person")) }),
            grouped_var: "p".to_string(),
            neighbor_var: "f".to_string(),
            edge_type: EdgeType::new("KNOWS"),
            direction,
            neighbor_label: None,
            distinct: false,
            count_alias: "friends".to_string(),
        }
    }

    fn counts_by_node(rows: &[crate::query::executor::record::Record]) -> std::collections::HashMap<crate::graph::NodeId, i64> {
        use crate::graph::PropertyValue;
        rows.iter()
            .map(|r| {
                let id = r.get("p").unwrap().node_id().unwrap();
                let n = match r.get("friends").unwrap() {
                    Value::Property(PropertyValue::Integer(n)) => *n,
                    other => panic!("count should be an integer, got {:?}", other),
                };
                (id, n)
            })
            .collect()
    }

    #[test]
    fn test_adjacency_count_aggregate_forward_counts_out_degree() {
        let (store, hub, leaf) = degree_graph();
        let mut op = logical_to_physical(&adjacency_plan(ExpandDirection::Forward));
        assert_eq!(op.describe().name, "AdjacencyCountAggregate");
        let counts = counts_by_node(&drain(&mut op, &store));
        assert_eq!(counts.get(&hub), Some(&2));
        assert_eq!(counts.get(&leaf), Some(&1));
    }

    #[test]
    fn test_adjacency_count_aggregate_reverse_counts_in_degree() {
        let (store, hub, leaf) = degree_graph();
        let mut op = logical_to_physical(&adjacency_plan(ExpandDirection::Reverse));
        let counts = counts_by_node(&drain(&mut op, &store));
        assert_eq!(counts.get(&hub).copied().unwrap_or(0), 0);
        assert_eq!(counts.get(&leaf), Some(&1));
    }
}
