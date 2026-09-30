//! Unit tests for the `ANALYZE` operator.

use super::*;

fn int(r: &Record, col: &str) -> i64 {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("expected integer in {col}, got {other:?}"),
    }
}

fn boolean(r: &Record, col: &str) -> bool {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Boolean(b))) => *b,
        other => panic!("expected boolean in {col}, got {other:?}"),
    }
}

fn small_store() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    let b = store.create_node("Person");
    let c = store.create_node("City");
    store.create_edge(a, b, "KNOWS").unwrap();
    store.create_edge(a, c, "LIVES_IN").unwrap();
    store.create_edge(b, c, "LIVES_IN").unwrap();
    store
}

#[test]
fn analyze_columns_are_the_five_reported_fields() {
    assert_eq!(
        analyze_columns(),
        vec!["nodes", "edges", "labels", "edge_types", "cache_was_stale"]
    );
}

#[test]
fn analyze_reports_counts_over_the_current_graph() {
    let store = small_store();
    let mut op = AnalyzeOperator::new();
    let row = op.next(&store).unwrap().unwrap();
    assert_eq!(int(&row, "nodes"), 3);
    assert_eq!(int(&row, "edges"), 3);
    assert_eq!(int(&row, "labels"), 2);
    assert_eq!(int(&row, "edge_types"), 2);
    // One row only.
    assert!(op.next(&store).unwrap().is_none());
}

#[test]
fn analyze_says_whether_it_replaced_a_cached_set() {
    let store = small_store();
    store.invalidate_statistics_cache();
    let mut op = AnalyzeOperator::default();
    let first = op.next(&store).unwrap().unwrap();
    assert!(!boolean(&first, "cache_was_stale"));
    // The first run left statistics cached, so the next run replaces them.
    assert!(store.has_cached_statistics());
    op.reset();
    let second = op.next(&store).unwrap().unwrap();
    assert!(boolean(&second, "cache_was_stale"));
}

#[test]
fn analyze_on_an_empty_graph_reports_zeroes() {
    let store = GraphStore::new();
    let row = AnalyzeOperator::new().next(&store).unwrap().unwrap();
    assert_eq!(int(&row, "nodes"), 0);
    assert_eq!(int(&row, "edges"), 0);
    assert_eq!(int(&row, "labels"), 0);
    assert_eq!(int(&row, "edge_types"), 0);
}

#[test]
fn analyze_describes_itself() {
    let d = AnalyzeOperator::new().describe();
    assert_eq!(d.name, "Analyze");
    assert_eq!(d.details, "recompute planner statistics");
    assert!(d.children.is_empty());
}
