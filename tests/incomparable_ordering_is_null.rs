//! An ordering comparison between values of different types is null (#607).
//!
//! `0 > 'x'` is neither true, nor false, nor an error: Cypher cannot order a
//! number against a string, so the answer is unknown. Two code paths got this
//! wrong in opposite directions — one raised `TypeError("Cannot compare these
//! types")`, aborting the whole query; the other compared them through the
//! total `Ord` that backs the property index and returned a confident `false`.
//!
//! The difference shows up under `OR`: `false OR true` is true, but
//! `null OR true` is also true, while `false OR false` is false and
//! `null OR false` is null. A WHERE keeps rows only for true, so the wrong
//! answer changes which rows come back rather than announcing itself.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn value_of(store: &GraphStore, cypher: &str) -> Value {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run, not error: {e}"));
    out.records[0].get("x").cloned().unwrap_or(Value::Null)
}

fn assert_null(store: &GraphStore, cypher: &str) {
    let got = value_of(store, cypher);
    assert!(
        matches!(got, Value::Null | Value::Property(PropertyValue::Null)),
        "`{cypher}` should be null, got {got:?}"
    );
}

#[test]
fn ordering_across_types_is_null() {
    let store = GraphStore::new();
    for op in ["<", "<=", ">", ">="] {
        assert_null(&store, &format!("RETURN 0 {op} 'x' AS x"));
        assert_null(&store, &format!("RETURN 'x' {op} 0 AS x"));
        assert_null(&store, &format!("RETURN true {op} 1 AS x"));
        assert_null(&store, &format!("RETURN [1] {op} 1 AS x"));
    }
}

#[test]
fn ordering_within_a_type_still_answers() {
    // The guard must not swallow comparisons that are perfectly well defined,
    // including int-vs-float, which is one type for ordering purposes.
    let store = GraphStore::new();
    let t = Value::Property(PropertyValue::Boolean(true));
    assert_eq!(value_of(&store, "RETURN 1 < 2 AS x"), t);
    assert_eq!(value_of(&store, "RETURN 1 < 2.5 AS x"), t);
    assert_eq!(value_of(&store, "RETURN 2.5 > 1 AS x"), t);
    assert_eq!(value_of(&store, "RETURN 'a' < 'b' AS x"), t);
    assert_eq!(value_of(&store, "RETURN 'xx' > 'x' AS x"), t);
}

#[test]
fn a_where_filters_the_incomparable_row_without_aborting() {
    // Comparison2 [1]: the TypeError killed the whole query, so the row that
    // *does* compare never came back either.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:Child {var: 0})");
    run(&mut store, "CREATE (:Child {var: 'xx'})");
    run(&mut store, "CREATE (:Child)");

    let q = parse_query("MATCH (i:Child) WHERE i.var IS NOT NULL AND i.var > 'x' RETURN i.var AS x")
        .expect("parse");
    let out = QueryExecutor::new(&store).execute(&q).expect("should run, not error");
    let got: Vec<Value> = out.records.iter().filter_map(|r| r.get("x").cloned()).collect();
    assert_eq!(got.len(), 1, "only 'xx' compares against 'x', got {got:?}");
    assert_eq!(got[0], Value::Property(PropertyValue::String("xx".into())));
}
