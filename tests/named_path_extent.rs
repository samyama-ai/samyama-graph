//! A named path covers the whole pattern, not just its last hop.
//!
//! Every expand in a chain binds the same path variable, and each one bound a
//! fresh two-node path for its own hop — so the last hop overwrote the rest:
//!
//! ```cypher
//! MATCH p = (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN length(p)   -- 1, not 2
//! ```
//!
//! The wrong answer is a plausible one, which is what makes it dangerous:
//! `length(p)` returns a number, just the wrong number, and anything walking
//! `nodes(p)` sees a shorter journey than the one that matched. The
//! variable-length spelling of the same traversal was always right, so the two
//! agreed on which rows matched and disagreed only on what the path was
//! (#631).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn chain() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query(
        "CREATE (a:A {name: 'A'})-[:KNOWS]->(b:B {name: 'B'})-[:KNOWS]->(c:C {name: 'C'})",
    )
    .expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");
    store
}

/// `(node count, edge count)` of the single path a query returns.
fn path_extent(store: &GraphStore, cypher: &str) -> (usize, usize) {
    let q = parse_query(cypher).expect("query should parse");
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    assert_eq!(out.records.len(), 1, "`{cypher}` should match exactly one path");
    match out.records[0].get("p") {
        Some(Value::Path { nodes, edges }) => (nodes.len(), edges.len()),
        other => panic!("`{cypher}` should bind a path, got {other:?}"),
    }
}

#[test]
fn a_two_hop_named_path_holds_both_hops() {
    let store = chain();
    assert_eq!(
        path_extent(&store, "MATCH p = (a:A)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN p"),
        (3, 2)
    );
}

#[test]
fn the_fixed_and_variable_length_spellings_agree_on_the_path() {
    // They already agreed on which rows matched. The point of this test is
    // that they now agree on what the path *is* — that agreement is what was
    // missing, and it is the cheapest way to notice if one of them drifts.
    let store = chain();
    assert_eq!(
        path_extent(&store, "MATCH p = (a:A)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN p"),
        path_extent(&store, "MATCH p = (a:A)-[:KNOWS*2]->(c) RETURN p")
    );
}

#[test]
fn length_of_a_named_path_counts_every_relationship() {
    let store = chain();
    let q = parse_query("MATCH p = (a:A)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN length(p) AS n")
        .expect("query should parse");
    let out = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(
        out.records[0].get("n"),
        Some(&Value::Property(PropertyValue::Integer(2))),
        "two relationships were traversed"
    );
}

#[test]
fn a_single_hop_named_path_is_unchanged() {
    let store = chain();
    assert_eq!(path_extent(&store, "MATCH p = (a:A)-[:KNOWS]->(b) RETURN p"), (2, 1));
}

#[test]
fn a_path_is_not_continued_across_a_disconnected_pattern() {
    // The extension only applies when the walk actually continues from where
    // the path ended. Two comma-separated hops share the variable but not an
    // endpoint, and stitching them would invent a journey that does not exist.
    let mut store = chain();
    let q = parse_query("CREATE (x:X {name: 'X'})-[:KNOWS]->(y:Y {name: 'Y'})")
        .expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");

    let q = parse_query("MATCH (a:A)-[:KNOWS]->(b:B), p = (x:X)-[:KNOWS]->(y:Y) RETURN p")
        .expect("query should parse");
    let out = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(out.records.len(), 1);
    match out.records[0].get("p") {
        Some(Value::Path { nodes, edges }) => {
            assert_eq!((nodes.len(), edges.len()), (2, 1), "just the X->Y hop");
        }
        other => panic!("expected a path, got {other:?}"),
    }
}
