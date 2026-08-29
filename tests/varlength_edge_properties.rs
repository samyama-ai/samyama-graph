//! An inline property predicate on a variable-length relationship (#934).
//!
//! ```cypher
//! MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b:Artist) RETURN *
//! ```
//!
//! returned **every path in the graph**, as though the predicate were not
//! written. It parsed, ran, and reported success.
//!
//! That is worse than a missing feature. A filter that silently does not
//! filter is indistinguishable from one that matched everything — and this one
//! returns *more* rows than it should, so it fails **open**: anyone scoping a
//! traversal this way was getting the unscoped traversal.
//!
//! The predicate applies to **every** hop, not just the first or last, so it
//! is enforced inside the walk. A filter above the operator could not see the
//! intermediate relationships at all.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// `(a)-[1987]->(b)-[1988]->(c)-[1988]->(d)`, all `:Artist`.
fn artists() -> GraphStore {
    let mut store = GraphStore::new();
    let ns: Vec<_> = ["A", "B", "C", "D"]
        .iter()
        .map(|tag| store.create_node_with_labels([Label::new("Artist"), Label::new(*tag)]))
        .collect();
    for (i, year) in [1987i64, 1988, 1988].iter().enumerate() {
        let e = store.create_edge(ns[i], ns[i + 1], "WORKED_WITH").unwrap();
        let _ = store.set_edge_property_sparse(e, "year", PropertyValue::Integer(*year));
    }
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

#[test]
fn the_predicate_narrows_the_paths() {
    let store = artists();
    // Unconstrained: 3 one-hop + 2 two-hop + 1 three-hop = 6.
    assert_eq!(rows(&store, "MATCH (a:Artist)-[:WORKED_WITH*]->(b:Artist) RETURN a, b"), 6);
    // Only the b→c and c→d hops are 1988, and they are adjacent, so: two
    // one-hop paths and one two-hop path.
    assert_eq!(
        rows(&store, "MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b:Artist) RETURN a, b"),
        3
    );
}

#[test]
fn every_hop_must_satisfy_it_not_just_the_first() {
    // The heart of the semantics. `1987` appears only on the first hop, so a
    // predicate of 1987 admits exactly that one path — if it were checked on
    // the first hop alone, the two- and three-hop paths starting there would
    // come back too.
    let store = artists();
    assert_eq!(
        rows(&store, "MATCH (a:Artist)-[:WORKED_WITH* {year: 1987}]->(b:Artist) RETURN a, b"),
        1
    );
}

#[test]
fn a_predicate_nothing_satisfies_returns_nothing() {
    let store = artists();
    assert_eq!(
        rows(&store, "MATCH (a:Artist)-[:WORKED_WITH* {year: 1066}]->(b:Artist) RETURN a, b"),
        0
    );
}

#[test]
fn it_applies_whichever_end_the_planner_anchors() {
    // Anchor selection may walk the segment backwards from the more selective
    // end. The constraint has to hold there too, or the same pattern answers
    // differently depending on a planning decision.
    let store = artists();
    let forward = "MATCH (a:A)-[:WORKED_WITH* {year: 1988}]->(b:Artist) RETURN a, b";
    let backward = "MATCH (a:Artist)-[:WORKED_WITH* {year: 1988}]->(b:D) RETURN a, b";
    assert_eq!(rows(&store, forward), 0, "A's only outgoing hop is 1987");
    assert_eq!(rows(&store, backward), 2, "B->C->D and C->D");
}

#[test]
fn a_bounded_length_is_constrained_too() {
    let store = artists();
    assert_eq!(
        rows(&store, "MATCH (a:Artist)-[:WORKED_WITH*1..1 {year: 1988}]->(b:Artist) RETURN a, b"),
        2
    );
    assert_eq!(
        rows(&store, "MATCH (a:Artist)-[:WORKED_WITH*2..2 {year: 1988}]->(b:Artist) RETURN a, b"),
        1
    );
}

#[test]
fn an_unconstrained_segment_is_unaffected() {
    // The predicate is opt-in; a pattern without one must not start filtering.
    let store = artists();
    assert_eq!(rows(&store, "MATCH (a:Artist)-[:WORKED_WITH*1..1]->(b:Artist) RETURN a, b"), 3);
    assert_eq!(rows(&store, "MATCH (a:Artist)-[:WORKED_WITH*2..3]->(b:Artist) RETURN a, b"), 3);
}
