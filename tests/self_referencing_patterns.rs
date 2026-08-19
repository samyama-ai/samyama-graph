//! A repeated variable is a constraint, and a self-relationship is one
//! relationship.
//!
//! Two defects that both show up on patterns whose two ends are the same node,
//! and that fail in opposite directions — one matched too much, the other
//! counted too much.
//!
//! **`MATCH (b)-->(b)` matched every edge.** `ExpandOperator` binds its target
//! unconditionally, so the far end of each edge was bound over the near one
//! and the equality the pattern states was never checked. A graph containing
//! no self-relationships at all returned one row per edge (#639).
//!
//! **`MATCH ()--()` counted a self-relationship twice.** A loop is incident to
//! its node twice, once outgoing and once incoming, and the undirected walk
//! took both (#640). Undirected matching traverses each relationship once —
//! but for an edge between two *different* nodes it still yields both
//! orientations, which is why the fix has to be about the loop and not about
//! the direction.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn store_of(setup: &str) -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query(setup).expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .len()
}

#[test]
fn a_repeated_variable_requires_both_ends_to_be_the_same_node() {
    let store = store_of("CREATE (a), (b), (c) CREATE (a)-[:T]->(b), (b)-[:T]->(c)");
    assert_eq!(rows(&store, "MATCH (b)-->(b) RETURN b"), 0, "no self-relationships exist");
    assert_eq!(
        rows(&store, "MATCH (a)-->(b), (b)-->(b) RETURN b"),
        0,
        "the second path cannot match either"
    );
    // The same graph, the same traversal, without the repetition: still two.
    assert_eq!(rows(&store, "MATCH (a)-->(b) RETURN b"), 2);
}

#[test]
fn a_repeated_variable_matches_a_real_self_relationship() {
    // The constraint must not be so strict that the case it exists for stops
    // working.
    let store = store_of("CREATE (a:A) CREATE (a)-[:LOOP]->(a)");
    assert_eq!(rows(&store, "MATCH (n)-->(n) RETURN n"), 1);
}

#[test]
fn an_undirected_match_traverses_a_self_relationship_once() {
    let store = store_of("CREATE (a:A) CREATE (a)-[:LOOP]->(a)");
    assert_eq!(rows(&store, "MATCH ()--() RETURN 1 AS z"), 1);
    assert_eq!(rows(&store, "MATCH (n)--(n) RETURN n"), 1);
    assert_eq!(rows(&store, "MATCH ()-->() RETURN 1 AS z"), 1);
}

#[test]
fn an_undirected_match_still_yields_both_orientations_between_distinct_nodes() {
    // The dedup is about a loop being one relationship, not about undirected
    // matching being one-way. Suppressing the second orientation here would
    // halve the answer to every undirected query in the engine.
    let store = store_of("CREATE (:A)-[:T]->(:B)");
    assert_eq!(rows(&store, "MATCH ()--() RETURN 1 AS z"), 2, "A--B and B--A");
    assert_eq!(rows(&store, "MATCH ()-->() RETURN 1 AS z"), 1);

    let two_way = store_of("CREATE (a:A), (b:B) CREATE (a)-[:T]->(b), (b)-[:T]->(a)");
    assert_eq!(rows(&two_way, "MATCH ()--() RETURN 1 AS z"), 4, "two edges, two ways each");
}
