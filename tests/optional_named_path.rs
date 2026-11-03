//! An OPTIONAL MATCH that does not match binds its named path to null.
//!
//! ```cypher
//! MATCH (a:A)
//! OPTIONAL MATCH p = (a)-[:X]->(b)
//! RETURN p            -- "Variable not found: p"
//! ```
//!
//! The left outer join fills its right-hand-only variables with null, and that
//! list came from a set of the pattern's node and edge variables — the named
//! path was never in it. So `b` was nulled correctly the whole time and `p`
//! was invisible, and a query that should return one null row failed as though
//! it referred to something that did not exist (#637).
//!
//! Five separate places built that variable set and none of them included the
//! path; the clause-level ones now share one function.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:A {num: 42}), (c:C) CREATE (a)-[:REL]->(c)")
        .expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");
    store
}

fn one_value(store: &GraphStore, cypher: &str) -> Value {
    let q = parse_query(cypher).expect("query should parse");
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    assert_eq!(out.records.len(), 1, "`{cypher}` should return one row");
    out.records[0].get("p").cloned().unwrap_or(Value::Null)
}

#[test]
fn an_unmatched_optional_named_path_is_null() {
    let store = graph();
    assert_eq!(
        one_value(&store, "MATCH (a:A) OPTIONAL MATCH p = (a)-[:X]->(b) RETURN p"),
        Value::Null,
        "the row survives and p is null"
    );
}

#[test]
fn a_matched_optional_named_path_is_still_the_path() {
    let store = graph();
    match one_value(&store, "MATCH (a:A) OPTIONAL MATCH p = (a)-[:REL]->(b) RETURN p") {
        Value::Path { nodes, edges } => assert_eq!((nodes.len(), edges.len()), (2, 1)),
        other => panic!("expected a path, got {other:?}"),
    }
}

#[test]
fn the_other_optional_variables_are_unaffected() {
    // `b` was always nulled correctly. Asserting it here so a future change to
    // the variable set cannot fix the path by breaking the nodes.
    let store = graph();
    let q = parse_query("MATCH (a:A) OPTIONAL MATCH p = (a)-[:X]->(b) RETURN p, b")
        .expect("query should parse");
    let out = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(out.records.len(), 1);
    assert_eq!(out.records[0].get("p"), Some(&Value::Null));
    assert_eq!(out.records[0].get("b"), Some(&Value::Null));
}
