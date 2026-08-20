//! UNION returns the rows of *every* branch, not just the first.
//!
//! `RETURN 1 AS x UNION RETURN 2 AS x` returned `[1]`. The parser builds
//! `query.union_queries` and the validator checks the branches agree on their
//! column names, but no executor path ever read the field, so every branch
//! after the first was silently discarded.
//!
//! It survived because the four existing union tests all assert
//! `records.len() >= 2` against a fixture whose *first branch alone* returns
//! two rows. A test that cannot distinguish 2 from 4 cannot detect a UNION
//! that never ran. These assert exact counts and exact values.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn column(store: &GraphStore, cypher: &str) -> Vec<i64> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    let mut got: Vec<i64> = out
        .records
        .iter()
        .map(|r| match r.get("x") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("expected integer column x, got {other:?}"),
        })
        .collect();
    got.sort_unstable();
    got
}

#[test]
fn union_returns_rows_from_both_branches() {
    let store = GraphStore::new();
    assert_eq!(column(&store, "RETURN 1 AS x UNION RETURN 2 AS x"), vec![1, 2]);
}

#[test]
fn union_deduplicates_and_union_all_does_not() {
    let store = GraphStore::new();
    // The distinction the two keywords exist for.
    assert_eq!(column(&store, "RETURN 1 AS x UNION RETURN 1 AS x"), vec![1]);
    assert_eq!(column(&store, "RETURN 1 AS x UNION ALL RETURN 1 AS x"), vec![1, 1]);
}

#[test]
fn union_chains_past_two_branches() {
    let store = GraphStore::new();
    assert_eq!(
        column(&store, "RETURN 1 AS x UNION RETURN 2 AS x UNION RETURN 3 AS x"),
        vec![1, 2, 3]
    );
}

#[test]
fn union_over_matches_counts_every_branch() {
    // The shape the old tests used — but asserting the count that
    // distinguishes a working UNION from one that ran only the first branch.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:P {v: 1})");
    run(&mut store, "CREATE (:P {v: 2})");
    let q = "MATCH (n:P) RETURN n.v AS x UNION ALL MATCH (m:P) RETURN m.v AS x";
    assert_eq!(column(&store, q), vec![1, 1, 2, 2], "UNION ALL over 2 nodes is 4 rows, not 2");
}
