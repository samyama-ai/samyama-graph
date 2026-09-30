//! Additional unit tests for the logical-plan cost model.

use super::*;
use crate::graph::types::{EdgeType, NodeId};
use crate::query::ast::Expression;
use crate::query::executor::logical_plan::TrieJoinConstraint;

fn scan(var: &str, label: Option<&str>) -> LogicalPlanNode {
    LogicalPlanNode::LabelScan {
        variable: var.to_string(),
        label: label.map(Label::new),
    }
}

fn index_lookup(var: &str, label: &str) -> LogicalPlanNode {
    LogicalPlanNode::IndexLookup {
        variable: var.to_string(),
        label: Label::new(label),
        property: "id".to_string(),
        op: crate::query::ast::BinaryOp::Eq,
        value: Expression::Literal(crate::graph::PropertyValue::Integer(1)),
    }
}

fn expand(
    input: LogicalPlanNode,
    src: &str,
    types: &[&str],
    dir: ExpandDirection,
) -> LogicalPlanNode {
    LogicalPlanNode::Expand {
        input: Box::new(input),
        source_var: src.to_string(),
        target_var: "t".to_string(),
        edge_var: None,
        edge_types: types.iter().map(|t| EdgeType::new(*t)).collect(),
        direction: dir,
    }
}

fn expand_into(input: LogicalPlanNode, types: &[&str]) -> LogicalPlanNode {
    LogicalPlanNode::ExpandInto {
        input: Box::new(input),
        source_var: "a".to_string(),
        target_var: "b".to_string(),
        edge_types: types.iter().map(|t| EdgeType::new(*t)).collect(),
        edge_var: None,
    }
}

fn filter(input: LogicalPlanNode) -> LogicalPlanNode {
    LogicalPlanNode::Filter {
        input: Box::new(input),
        predicate: Expression::Literal(crate::graph::PropertyValue::Boolean(true)),
    }
}

fn constraint() -> TrieJoinConstraint {
    TrieJoinConstraint {
        bound_var: "a".to_string(),
        direction: ExpandDirection::Forward,
        edge_types: vec![],
        edge_var: None,
    }
}

fn trie_join(input: LogicalPlanNode, k: usize) -> LogicalPlanNode {
    LogicalPlanNode::TrieJoin {
        input: Box::new(input),
        target_var: "c".to_string(),
        constraints: (0..k).map(|_| constraint()).collect(),
    }
}

/// 4 `Person`s, 2 `City`s; each Person LIVES_IN one City (out-degree 1,
/// in-degree 2), and one Person KNOWS two others.
fn catalog() -> GraphCatalog {
    let mut c = GraphCatalog::new();
    for _ in 0..4 {
        c.on_label_added(&Label::new("Person"));
    }
    for _ in 0..2 {
        c.on_label_added(&Label::new("City"));
    }
    for i in 0..4u64 {
        c.on_edge_created(
            NodeId::new(i),
            &[Label::new("Person")],
            &EdgeType::new("LIVES_IN"),
            NodeId::new(100 + i % 2),
            &[Label::new("City")],
        );
    }
    c
}

fn approx(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn an_all_nodes_scan_sums_every_label() {
    assert_eq!(estimate_plan_cost(&scan("n", None), &catalog()), 6.0);
}

#[test]
fn an_untyped_expand_uses_the_default_multiplier() {
    let plan = expand(
        scan("a", Some("Person")),
        "a",
        &[],
        ExpandDirection::Forward,
    );
    assert_eq!(estimate_plan_cost(&plan, &catalog()), 8.0);
}

#[test]
fn an_expand_from_an_unlabelled_source_uses_unit_degree_per_type() {
    let plan = expand(
        scan("a", None),
        "a",
        &["LIVES_IN", "KNOWS"],
        ExpandDirection::Forward,
    );
    // 6 nodes x (1.0 + 1.0).
    assert_eq!(estimate_plan_cost(&plan, &catalog()), 12.0);
}

#[test]
fn an_expand_over_an_unseen_edge_type_assumes_one_edge_per_node() {
    let plan = expand(
        scan("a", Some("Person")),
        "a",
        &["NEVER_SEEN"],
        ExpandDirection::Forward,
    );
    assert_eq!(estimate_plan_cost(&plan, &catalog()), 4.0);
}

#[test]
fn expand_source_label_is_found_through_filters_and_index_lookups() {
    let through_filter = expand(
        filter(scan("a", Some("City"))),
        "a",
        &["LIVES_IN"],
        ExpandDirection::Reverse,
    );
    // Filter halves the 2 cities, then in-degree 2.
    assert_eq!(estimate_plan_cost(&through_filter, &catalog()), 2.0);

    let through_index = expand(
        index_lookup("a", "Person"),
        "a",
        &["LIVES_IN"],
        ExpandDirection::Forward,
    );
    assert_eq!(estimate_plan_cost(&through_index, &catalog()), 10.0);
}

#[test]
fn expand_source_label_is_found_through_nested_operators() {
    // The source label comes from a scan buried under another Expand,
    // an ExpandInto and a TrieJoin.
    let inner = expand(
        scan("a", Some("Person")),
        "x",
        &[],
        ExpandDirection::Forward,
    );
    let nested = trie_join(expand_into(inner, &[]), 1);
    let plan = expand(nested, "a", &["LIVES_IN"], ExpandDirection::Forward);
    let got = estimate_plan_cost(&plan, &catalog());
    // Person scan 4 x default 2.0 x ExpandInto 0.1 x TrieJoin 0.1 x out-degree 1.0.
    assert!(approx(got, 4.0 * 2.0 * 0.1 * 0.1), "{got}");
}

#[test]
fn expand_source_label_is_found_on_either_side_of_a_join_or_product() {
    let join_right = LogicalPlanNode::Join {
        left: Box::new(scan("z", Some("City"))),
        right: Box::new(scan("a", Some("Person"))),
        join_keys: vec![],
    };
    let plan = expand(join_right, "a", &["LIVES_IN"], ExpandDirection::Forward);
    // Join cost is additive: 2 + 4, times Person out-degree 1.
    assert_eq!(estimate_plan_cost(&plan, &catalog()), 6.0);

    let product_left = LogicalPlanNode::CartesianProduct {
        left: Box::new(scan("a", Some("City"))),
        right: Box::new(scan("z", Some("Person"))),
    };
    let plan = expand(product_left, "a", &["LIVES_IN"], ExpandDirection::Reverse);
    // Product 2 x 4, times City in-degree 2.
    assert_eq!(estimate_plan_cost(&plan, &catalog()), 16.0);
}

#[test]
fn an_unknown_variable_has_no_label() {
    let plan = expand(
        scan("other", Some("Person")),
        "a",
        &["LIVES_IN"],
        ExpandDirection::Forward,
    );
    // No label for `a`, so the per-type degree falls back to 1.0.
    assert_eq!(estimate_plan_cost(&plan, &catalog()), 4.0);
    assert_eq!(extract_label_for_var(&scan("x", Some("P")), "y"), None);
    assert_eq!(extract_label_for_var(&index_lookup("x", "P"), "y"), None);
    assert_eq!(get_expand_source_var(&scan("x", None)), "");
}

#[test]
fn expand_into_costs_a_tenth_of_its_input() {
    let untyped = expand_into(scan("a", Some("Person")), &[]);
    assert!(approx(estimate_plan_cost(&untyped, &catalog()), 0.4));
    let typed = expand_into(scan("a", Some("Person")), &["A", "B"]);
    assert!(approx(estimate_plan_cost(&typed, &catalog()), 0.4));
}

#[test]
fn trie_join_selectivity_tightens_with_more_constraints_and_is_floored() {
    let c = catalog();
    let one = estimate_plan_cost(&trie_join(scan("a", Some("Person")), 1), &c);
    let two = estimate_plan_cost(&trie_join(scan("a", Some("Person")), 2), &c);
    let none = estimate_plan_cost(&trie_join(scan("a", Some("Person")), 0), &c);
    let many = estimate_plan_cost(&trie_join(scan("a", Some("Person")), 6), &c);
    assert!(approx(one, 0.4), "{one}");
    assert!(approx(two, 0.04), "{two}");
    // Zero constraints is treated as one.
    assert!(approx(none, 0.4), "{none}");
    // 0.1^6 is floored at 0.001.
    assert!(approx(many, 4.0 * 0.001), "{many}");
}

#[test]
fn a_join_costs_the_sum_of_its_sides() {
    let plan = LogicalPlanNode::Join {
        left: Box::new(scan("a", Some("Person"))),
        right: Box::new(scan("b", Some("City"))),
        join_keys: vec!["a".to_string()],
    };
    assert_eq!(estimate_plan_cost(&plan, &catalog()), 6.0);
}

#[test]
fn an_adjacency_count_costs_its_input_scan() {
    let plan = LogicalPlanNode::AdjacencyCountAggregate {
        input: Box::new(scan("a", Some("Person"))),
        grouped_var: "a".to_string(),
        neighbor_var: "b".to_string(),
        edge_type: EdgeType::new("LIVES_IN"),
        direction: ExpandDirection::Forward,
        neighbor_label: None,
        distinct: false,
        count_alias: "c".to_string(),
    };
    assert_eq!(estimate_plan_cost(&plan, &catalog()), 4.0);
    // It is not a pass-through for label discovery.
    assert_eq!(extract_label_for_var(&plan, "a"), None);
}
