//! The `EXISTS` walker's adjacency walk, at the edges the rewrite touched (#618).
//!
//! `exists_neighbors` used to materialise every incident edge — all types, both
//! directions — as an owned `Edge` with its whole property map, and filter
//! afterwards. It now walks adjacency with an interned type filter and fetches
//! the edge only when the pattern needs it. These are the cases where "needs
//! it" is not obvious: an edge property predicate, a bound edge variable, an
//! undirected match over a self-relationship, and a type the graph has never
//! seen.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn flags(store: &GraphStore, cypher: &str) -> Vec<bool> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .iter()
        .map(|r| match r.get("x") {
            Some(Value::Property(PropertyValue::Boolean(b))) => *b,
            other => panic!("expected boolean x, got {other:?}"),
        })
        .collect()
}

/// `a -[:R {w: 1}]-> b` and `a -[:R {w: 2}]-> c`.
fn weighted() -> GraphStore {
    let mut store = GraphStore::new();
    for n in ["a", "b", "c"] {
        run(&mut store, &format!("CREATE (:N {{name: \"{n}\"}})"));
    }
    run(&mut store, "MATCH (x:N {name:\"a\"}), (y:N {name:\"b\"}) CREATE (x)-[:R {w: 1}]->(y)");
    run(&mut store, "MATCH (x:N {name:\"a\"}), (y:N {name:\"c\"}) CREATE (x)-[:R {w: 2}]->(y)");
    store
}

/// An edge property in the subquery pattern still filters.
///
/// The property map is no longer cloned for every candidate, so the edge has to
/// be fetched for exactly the ones that get this far — and still tested.
#[test]
fn an_edge_property_predicate_inside_exists_still_filters() {
    let store = weighted();
    assert_eq!(
        flags(&store, "MATCH (a:N {name:\"a\"}) RETURN EXISTS { MATCH (a)-[:R {w: 2}]->() } AS x"),
        vec![true],
    );
    assert_eq!(
        flags(&store, "MATCH (a:N {name:\"a\"}) RETURN EXISTS { MATCH (a)-[:R {w: 9}]->() } AS x"),
        vec![false],
    );
}

/// A bound edge variable is available to the subquery's WHERE.
///
/// The `Edge` is materialised lazily now; if that fetch were skipped the
/// variable would be unbound and the predicate would not see `w`.
#[test]
fn an_edge_variable_bound_inside_exists_is_readable() {
    let store = weighted();
    assert_eq!(
        flags(
            &store,
            "MATCH (a:N {name:\"a\"}) RETURN EXISTS { MATCH (a)-[r:R]->() WHERE r.w = 2 } AS x"
        ),
        vec![true],
    );
    assert_eq!(
        flags(
            &store,
            "MATCH (a:N {name:\"a\"}) RETURN EXISTS { MATCH (a)-[r:R]->() WHERE r.w = 7 } AS x"
        ),
        vec![false],
    );
}

/// A relationship type no node carries matches nothing — it does not become a
/// wildcard. Conflating "no types named" with "named types that resolve to
/// nothing" makes `-[:NO_SUCH_TYPE]->` follow every edge in the graph (#520).
#[test]
fn an_unknown_relationship_type_matches_nothing() {
    let store = weighted();
    assert_eq!(
        flags(&store, "MATCH (a:N {name:\"a\"}) RETURN EXISTS { MATCH (a)-[:NOPE]->() } AS x"),
        vec![false],
    );
}

/// Direction is still honoured: `b` has no outgoing `:R`.
#[test]
fn direction_is_honoured_in_both_walks() {
    let store = weighted();
    assert_eq!(
        flags(&store, "MATCH (b:N {name:\"b\"}) RETURN EXISTS { MATCH (b)-[:R]->() } AS x"),
        vec![false],
    );
    assert_eq!(
        flags(&store, "MATCH (b:N {name:\"b\"}) RETURN EXISTS { MATCH (b)<-[:R]-() } AS x"),
        vec![true],
    );
    assert_eq!(
        flags(&store, "MATCH (b:N {name:\"b\"}) RETURN EXISTS { MATCH (b)-[:R]-() } AS x"),
        vec![true],
    );
}

/// A self-relationship is incident to its node twice, once out and once in.
/// Undirected matching traverses it once, so a two-hop undirected pattern over
/// a single loop must not close by taking the same edge from both sides (#640).
#[test]
fn a_self_relationship_is_walked_once_by_an_undirected_pattern() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:N {name: \"a\"})");
    run(&mut store, "MATCH (x:N {name:\"a\"}) CREATE (x)-[:LOOP]->(x)");
    assert_eq!(
        flags(&store, "MATCH (a:N {name:\"a\"}) RETURN EXISTS { MATCH (a)-[:LOOP]-() } AS x"),
        vec![true],
    );
    assert_eq!(
        flags(
            &store,
            "MATCH (a:N {name:\"a\"}) RETURN EXISTS { MATCH (a)-[:LOOP]-()-[:LOOP]-() } AS x"
        ),
        vec![false],
    );
}
