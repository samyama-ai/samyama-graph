//! A data write with no `RETURN` returns no rows (openCypher TCK).
//!
//! `CREATE ()` produces one node and an **empty result**. It does not return
//! the node — the side effect is the point, and the caller asked for nothing.
//! We were emitting a row per created entity, so 34 TCK scenarios whose entire
//! assertion is "the result should be empty" failed while the write itself was
//! perfectly correct. Nothing about the graph was wrong; only the shape of the
//! answer was.
//!
//! The rule is narrower than "no RETURN means no rows", and the narrowing is
//! the interesting part. Two neighbours produce rows with no RETURN and must
//! keep doing so:
//!
//! * `CALL … YIELD` yields its results directly — the first version of this
//!   fix broke every algorithm invocation in the suite;
//! * DDL such as `CREATE HIERARCHY INDEX …` reports the encoding it chose.
//!
//! So the tests below come in two halves: writes that must be silent, and
//! neighbours that must not be.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("query should run")
        .records
        .len()
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store).execute(&q).expect("query should run").records.len()
}

#[test]
fn create_without_return_yields_no_rows() {
    for cypher in [
        "CREATE ()",
        "CREATE (), ()",
        "CREATE (:A {x: 1})",
        "CREATE (a)-[:R]->(b)",
        "CREATE (a:A), (b:B), (a)-[:R]->(b)",
    ] {
        let mut store = GraphStore::new();
        assert_eq!(run(&mut store, cypher), 0, "{cypher}");
    }
}

#[test]
fn merge_without_return_yields_no_rows() {
    let mut store = GraphStore::new();
    assert_eq!(run(&mut store, "MERGE (a:L)"), 0);
    // …and again, on the branch that matches rather than creates.
    assert_eq!(run(&mut store, "MERGE (a:L)"), 0);
}

#[test]
fn the_write_still_happens() {
    // The whole risk of this change: discarding the rows must not discard the
    // work. The plan is still driven to exhaustion.
    let mut store = GraphStore::new();
    assert_eq!(run(&mut store, "CREATE (:A {x: 1})"), 0);
    assert_eq!(count(&store, "MATCH (n:A) RETURN n"), 1);

    assert_eq!(run(&mut store, "CREATE (a:B), (b:B), (a)-[:R]->(b)"), 0);
    assert_eq!(count(&store, "MATCH (:B)-[:R]->(:B) RETURN 1 AS x"), 1);

    assert_eq!(run(&mut store, "MERGE (:C {k: 7})"), 0);
    assert_eq!(count(&store, "MATCH (n:C) WHERE n.k = 7 RETURN n"), 1);
}

#[test]
fn a_write_with_a_return_still_returns() {
    let mut store = GraphStore::new();
    assert_eq!(run(&mut store, "CREATE (n) RETURN n"), 1);
    assert_eq!(run(&mut store, "MERGE (m:L) RETURN m"), 1);
    // `CREATE (a), (b) RETURN a` is deliberately absent: it fails with
    // "Variable not found", a defect that predates this change (verified
    // against the commit at the start of the sweep) and is filed separately.
    // Asserting it here would mix an unrelated bug into this file's subject.
}

#[test]
fn set_and_delete_are_silent_too() {
    // `SET` and `DELETE` are data writes by the same argument as `CREATE`, so
    // they fall under the same rule — and `MATCH (n:P) SET n.x = 2` was
    // returning one row per matched node.
    let mut store = GraphStore::new();
    assert_eq!(run(&mut store, "CREATE (:P {x: 1})"), 0);
    assert_eq!(run(&mut store, "MATCH (n:P) SET n.x = 2"), 0);
    assert_eq!(count(&store, "MATCH (n:P) WHERE n.x = 2 RETURN n"), 1);
    assert_eq!(run(&mut store, "MATCH (n:P) DELETE n"), 0);
    assert_eq!(count(&store, "MATCH (n:P) RETURN n"), 0);
}

#[test]
fn call_yield_without_a_return_still_yields() {
    // The exception that the first version of this fix got wrong. A `CALL`
    // projects through YIELD, so it has results even with no RETURN clause.
    let mut store = GraphStore::new();
    assert_eq!(run(&mut store, "CREATE (a:N), (b:N), (a)-[:R]->(b)"), 0);
    let rows = run(&mut store, "CALL pagerank() YIELD nodeId, score");
    assert!(rows > 0, "CALL … YIELD must produce rows without a RETURN");
}
