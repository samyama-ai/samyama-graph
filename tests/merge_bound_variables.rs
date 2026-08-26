//! MERGE does not re-create a variable the row already bound (#893).
//!
//! ```cypher
//! CREATE (a) WITH a MERGE (x) MERGE (y) MERGE (x)-[:T]->(y)
//! ```
//!
//! left **three** nodes where the pattern names one. `MERGE (x)` and
//! `MERGE (y)` each matched the existing node and bound it, and then
//! `MERGE (x)-[:T]->(y)` searched the store from scratch: the candidate sets
//! were built from labels and properties alone, so the row's own bindings —
//! the one thing that decides the answer here — were never consulted.
//!
//! A bound variable is not a search. It names one node, and MERGE neither
//! looks for another nor makes one.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` parses: {e:?}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` runs: {e:?}"));
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("count query parses");
    QueryExecutor::new(store)
        .execute(&q)
        .expect("count query runs")
        .records
        .len()
}

/// The relationship is created; its endpoints are not.
#[test]
fn a_relationship_merge_reuses_its_bound_endpoints() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (a) WITH a MERGE (x) MERGE (y) MERGE (x)-[:T]->(y)");
    assert_eq!(count(&store, "MATCH (n) RETURN n"), 1, "the one node the pattern names");
    assert_eq!(count(&store, "MATCH ()-[r:T]->() RETURN r"), 1, "a self-relationship on it");
}

/// Only the unbound end is created, and the bound end keeps its identity.
#[test]
fn only_the_unbound_end_of_a_pattern_is_created() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A {tag: 'first'})");
    run(&mut store, "MATCH (a:A) MERGE (a)-[:T]->(b:B)");
    assert_eq!(count(&store, "MATCH (n:A) RETURN n"), 1, "no second A");
    assert_eq!(count(&store, "MATCH (n:B) RETURN n"), 1, "one new B");
    assert_eq!(
        count(&store, "MATCH (a:A {tag: 'first'})-[:T]->(:B) RETURN a"),
        1,
        "the relationship hangs off the node that was already there"
    );
}

/// An unbound MERGE still searches — the binding shortcut must not disable
/// matching for everything else.
#[test]
fn an_unbound_merge_still_matches_what_exists() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:X {p: 1})");
    run(&mut store, "MERGE (n:X {p: 1})");
    assert_eq!(count(&store, "MATCH (n:X) RETURN n"), 1, "matched, not created");
}

/// A MERGE whose endpoints are **not** all bound must still run.
///
/// The planner chose `MatchMergeEdgeOperator` -- which wires an edge between
/// endpoints that already exist -- whenever a MATCH appeared anywhere in the
/// query, rather than when its endpoints were actually bound. With `b` bound
/// by nothing it wired an edge to no second endpoint, so the whole MERGE was
/// a silent no-op that returned zero rows (#894).
#[test]
fn a_merge_after_a_match_is_not_skipped() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A {tag: 'first'})");
    let q = parse_query("MATCH (a:A) MERGE (a)-[:T]->(b:B) RETURN a, b").expect("parses");
    let rows = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs")
        .records
        .len();
    assert_eq!(rows, 1, "the MERGE produces the row its MATCH fed it");
    assert_eq!(count(&store, "MATCH ()-[r:T]->() RETURN r"), 1);
}

/// With no endpoint bound, the whole pattern is created -- including a second
/// `:A`, because the pattern as written does not exist.
#[test]
fn an_entirely_unbound_pattern_is_created_whole() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A)");
    run(&mut store, "MATCH (x:A) MERGE (c:C)-[:T]->(b:B)");
    assert_eq!(count(&store, "MATCH (n:C) RETURN n"), 1);
    assert_eq!(count(&store, "MATCH (n:B) RETURN n"), 1);
    assert_eq!(count(&store, "MATCH ()-[r:T]->() RETURN r"), 1);
}
