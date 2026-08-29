//! MERGE with an undirected relationship pattern (#938).
//!
//! ```cypher
//! MATCH (a {id: 2})--(b {id: 1})
//! MERGE (a)-[r:KNOWS]-(b)
//! ```
//!
//! `-[r:T]-` matches a relationship either way round. Both MERGE paths folded
//! `Direction::Both` into `Outgoing`, so an existing `b -> a` did not match and
//! MERGE wrote a second relationship beside it.
//!
//! That is worse than a wrong row. MERGE exists so that a write does *not*
//! happen when the thing is already there; an undirected MERGE against a graph
//! whose relationships run the other way accumulates a duplicate on every run.
//! The TCK scenario asserts "no side effects" for exactly this reason, so these
//! tests count relationships rather than rows.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut ex = MutQueryExecutor::new(store, "default".to_string());
    ex.execute(&query).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
}

fn edge_count(store: &GraphStore) -> i64 {
    let query = parse_query("MATCH ()-[r]->() RETURN count(r) AS c").unwrap();
    match QueryExecutor::new(store).execute(&query).unwrap().records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{other:?}"),
    }
}

/// `a -[:KNOWS]-> b`, and `b` is the one carrying the lower id.
fn pair() -> (GraphStore, samyama::graph::NodeId, samyama::graph::NodeId) {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let _ = store.set_node_property("default", a, "id".to_string(), PropertyValue::Integer(2));
    let _ = store.set_node_property("default", b, "id".to_string(), PropertyValue::Integer(1));
    let e = store.create_edge(a, b, "KNOWS").unwrap();
    let _ = store.set_edge_property_sparse(e, "name", PropertyValue::String("ab".into()));
    (store, a, b)
}

#[test]
fn an_undirected_merge_matches_a_relationship_written_the_other_way() {
    // The existing edge is a -> b; the MERGE is written b -[r]- a. Directional
    // matching misses it and creates a second one.
    let (mut store, ..) = pair();
    assert_eq!(edge_count(&store), 1);
    run(&mut store, "MATCH (a {id: 1}), (b {id: 2}) MERGE (a)-[r:KNOWS]-(b)");
    assert_eq!(edge_count(&store), 1, "matched, not created");
}

#[test]
fn it_is_idempotent_across_runs() {
    // The failure mode that matters: one duplicate per run, forever.
    let (mut store, ..) = pair();
    for _ in 0..3 {
        run(&mut store, "MATCH (a {id: 1}), (b {id: 2}) MERGE (a)-[r:KNOWS]-(b)");
    }
    assert_eq!(edge_count(&store), 1);
}

#[test]
fn a_directed_merge_still_respects_direction() {
    // The fix must not make MERGE undirected everywhere: `->` against an edge
    // running the other way is genuinely absent and must be created.
    let (mut store, ..) = pair();
    run(&mut store, "MATCH (a {id: 1}), (b {id: 2}) MERGE (a)-[r:KNOWS]->(b)");
    assert_eq!(edge_count(&store), 2, "b -> a did not exist");
}

#[test]
fn an_undirected_merge_still_creates_when_nothing_matches() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let _ = store.set_node_property("default", a, "id".to_string(), PropertyValue::Integer(1));
    let _ = store.set_node_property("default", b, "id".to_string(), PropertyValue::Integer(2));
    assert_eq!(edge_count(&store), 0);
    run(&mut store, "MATCH (a {id: 1}), (b {id: 2}) MERGE (a)-[r:KNOWS]-(b)");
    assert_eq!(edge_count(&store), 1);
    // And created in the pattern's written direction, which is what Cypher
    // does for an undirected MERGE.
    let q = parse_query("MATCH (a {id: 1})-[r:KNOWS]->(b {id: 2}) RETURN count(r) AS c").unwrap();
    match QueryExecutor::new(&store).execute(&q).unwrap().records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => assert_eq!(*n, 1),
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_matched_relationship_keeps_its_properties() {
    // The symptom that exposed this: the created duplicate came back as a bare
    // `[:KNOWS]` beside the real one.
    let (mut store, ..) = pair();
    run(&mut store, "MATCH (a {id: 1}), (b {id: 2}) MERGE (a)-[r:KNOWS]-(b)");
    let q = parse_query("MATCH ()-[r:KNOWS]->() RETURN r.name AS n").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    assert_eq!(batch.records.len(), 1);
    match batch.records[0].get("n") {
        Some(Value::Property(PropertyValue::String(s))) => assert_eq!(s, "ab"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_unbound_undirected_merge_also_matches_either_way() {
    // The other MERGE path: endpoints not bound by an earlier MATCH, so the
    // whole pattern is searched for. Both paths had the same defect and a fix
    // to one would not have reached the other.
    let (mut store, ..) = pair();
    run(&mut store, "MERGE ({id: 1})-[r:KNOWS]-({id: 2})");
    assert_eq!(edge_count(&store), 1);
}
