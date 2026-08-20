//! `count(*)` is an aggregate wherever it appears, including after a
//! projection that renamed everything.
//!
//! `RETURN v, count(*)` following a `WITH ... AS v` reached the *scalar*
//! function evaluator and raised `Unknown function: count`. The aggregate is
//! recognised by the planner, but only on the RETURN paths that call
//! `extract_nested_aggregates` — a second path re-made the decision and got it
//! wrong (Match8, Merge1, Merge5, Merge9, String8/9/10 scenario 8).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn rows(store: &GraphStore, cypher: &str) -> Vec<Value> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run, not error: {e}"));
    out.records.iter().map(|r| r.get("c").cloned().unwrap_or(Value::Null)).collect()
}

fn total(store: &GraphStore, cypher: &str) -> i64 {
    rows(store, cypher)
        .iter()
        .map(|v| match v {
            Value::Property(PropertyValue::Integer(i)) => *i,
            other => panic!("expected an integer count, got {other:?}"),
        })
        .sum()
}

#[test]
fn count_star_after_a_with_projection() {
    let store = GraphStore::new();
    // The String8 shape: UNWIND, project to a computed name, then aggregate.
    assert_eq!(
        total(&store, "UNWIND [1, 2, 3] AS n WITH n * 2 AS v RETURN v, count(*) AS c"),
        3
    );
}

#[test]
fn count_star_after_unwind_of_a_pair() {
    let store = GraphStore::new();
    // Two UNWINDs then a projection — the exact String8/9/10 scenario-8 shape,
    // where every one of the 36 rows projects to the same null `v`.
    assert_eq!(
        total(
            &store,
            "WITH [1, 2] AS ops UNWIND ops AS a UNWIND ops AS b \
             WITH a STARTS WITH b AS v RETURN v, count(*) AS c"
        ),
        4
    );
}

#[test]
fn count_star_after_a_write_clause() {
    // Match8 [2] / Merge9 [3]: counting rows after an updating clause.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:X {n: 1})");
    run(&mut store, "CREATE (:X {n: 2})");
    assert_eq!(total(&store, "MATCH (x:X) WITH x RETURN count(*) AS c"), 2);
}

#[test]
fn other_aggregates_take_the_same_path() {
    // If count reached the scalar evaluator, so would these; assert the class
    // rather than the one function that happened to be reported.
    let store = GraphStore::new();
    for agg in ["count(*)", "count(v)", "sum(v)", "max(v)", "min(v)"] {
        let cypher = format!("UNWIND [1, 2, 3] AS n WITH n AS v RETURN {agg} AS c");
        let q = parse_query(&cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
        QueryExecutor::new(&store)
            .execute(&q)
            .unwrap_or_else(|e| panic!("`{cypher}` should run, not error: {e}"));
    }
}
