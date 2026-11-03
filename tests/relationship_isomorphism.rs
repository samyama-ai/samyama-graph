//! One relationship may not be traversed twice within a single MATCH (#684).
//!
//! openCypher uses **relationship isomorphism**: a pattern that needs two
//! relationships is not satisfied by one edge walked out and back. Nodes are
//! *not* restricted this way — a node may repeat — so this is specifically
//! about edges.
//!
//! The scope was checked against Neo4j 5 rather than assumed, because both
//! halves are easy to get wrong in opposite directions:
//!
//! ```text
//! MATCH (a)-[:R]-(b)-[:R]-(c)          over one edge -> 0   enforced, even with
//!                                                            no repeated variable
//! MATCH (a)-[:R]-(b) MATCH (b)-[:R]-(c) over one edge -> 2   NOT enforced across
//!                                                            separate MATCH clauses
//! ```
//!
//! So the rule is per-clause, and it applies to every multi-segment pattern,
//! not only those that fold back onto a bound variable.
//!
//! The `EXISTS` walker already enforced this (`visited_edges` in
//! `exists_expand_hops`); `ExpandOperator` did not, so the same question got
//! two different answers depending on which evaluator the planner picked.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    match out.records.first().and_then(|r| r.get("c")) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("expected integer c, got {other:?}"),
    }
}

/// Exactly one edge: 1 -[:R]-> 2.
fn one_edge() -> GraphStore {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:S {id: 1})");
    run(&mut store, "CREATE (:S {id: 2})");
    run(&mut store, "MATCH (x:S {id:1}), (y:S {id:2}) CREATE (x)-[:R]->(y)");
    store
}

#[test]
fn an_edge_cannot_be_reused_to_return_to_the_start() {
    // Neo4j 5: 0. We returned 2 — the edge walked out and back, once from each end.
    let store = one_edge();
    assert_eq!(
        count(&store, "MATCH (a:S)-[:R]-(b:S)-[:R]-(a) RETURN count(a) AS c"),
        0,
        "the pattern needs two relationships and the graph has one"
    );
}

#[test]
fn an_edge_cannot_be_reused_even_without_a_repeated_variable() {
    // Nodes may repeat (c can be a), so this is not about node isomorphism —
    // it is the edge that may not be reused. Neo4j 5: 0.
    let store = one_edge();
    assert_eq!(
        count(&store, "MATCH (a:S)-[:R]-(b:S)-[:R]-(c:S) RETURN count(*) AS c"),
        0
    );
}

#[test]
fn separate_match_clauses_may_reuse_an_edge() {
    // The other half of the rule, and the one a too-broad fix breaks. Neo4j 5: 2.
    let store = one_edge();
    assert_eq!(
        count(&store, "MATCH (a:S)-[:R]-(b:S) MATCH (b)-[:R]-(c:S) RETURN count(*) AS c"),
        2,
        "isomorphism is per-clause; a second MATCH starts fresh"
    );
}

#[test]
fn a_genuine_two_edge_path_still_matches() {
    // The fix must not prune legitimate traversals: distinct edges are fine.
    let mut store = GraphStore::new();
    for i in 1..=3 {
        run(&mut store, &format!("CREATE (:T {{id: {i}}})"));
    }
    run(&mut store, "MATCH (x:T {id:1}), (y:T {id:2}) CREATE (x)-[:R]->(y)");
    run(&mut store, "MATCH (x:T {id:2}), (y:T {id:3}) CREATE (x)-[:R]->(y)");
    assert_eq!(
        count(&store, "MATCH (a:T)-[:R]-(b:T)-[:R]-(c:T) RETURN count(*) AS c"),
        2,
        "1-2-3 and 3-2-1 use two distinct edges each"
    );
}

#[test]
fn a_triangle_is_still_found() {
    // BI-17's shape over a real triangle: three distinct edges, so it matches.
    let mut store = GraphStore::new();
    for i in 1..=3 {
        run(&mut store, &format!("CREATE (:P {{id: {i}}})"));
    }
    for (x, y) in [(1, 2), (2, 3), (3, 1)] {
        run(&mut store, &format!(
            "MATCH (x:P {{id: {x}}}), (y:P {{id: {y}}}) CREATE (x)-[:R]->(y)"
        ));
    }
    assert_eq!(
        count(&store,
            "MATCH (a:P)-[:R]-(b:P)-[:R]-(c:P)-[:R]-(a) \
             WHERE a.id < b.id AND b.id < c.id RETURN count(a) AS c"),
        1
    );
}

#[test]
fn a_self_loop_is_unaffected() {
    // Revisiting a *node* is legal; only the edge is restricted (#639).
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:L {id: 1})");
    run(&mut store, "MATCH (x:L {id:1}) CREATE (x)-[:R]->(x)");
    assert_eq!(count(&store, "MATCH (a:L)-[:R]->(a) RETURN count(a) AS c"), 1);
}
