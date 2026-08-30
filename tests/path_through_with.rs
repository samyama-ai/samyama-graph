//! A zero-length named path survives a WITH (#964).
//!
//! ```cypher
//! MATCH p = (a) RETURN p            -- <()>
//! MATCH p = (a) WITH p RETURN p     -- null
//! ```
//!
//! The variable existed right up until something asked the WITH to carry it.
//!
//! `MATCH p = (a)` has no segments, so no expand walks it and nothing binds
//! `p`; #909 added a `BindPathOperator` for exactly that case. It was inserted
//! **above** the WITH barriers, so the barrier never saw `p` and projected a
//! null for it. Moving it below is the whole fix.

use samyama::graph::{GraphStore, Label};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn path_of(store: &GraphStore, cypher: &str, col: &str) -> (usize, usize) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    match batch.records[0].get(col) {
        Some(Value::Path { nodes, edges }) => (nodes.len(), edges.len()),
        other => panic!("{cypher}: expected a path, got {other:?}"),
    }
}

fn one_node() -> GraphStore {
    let mut store = GraphStore::new();
    store.create_node("");
    store
}

#[test]
fn a_zero_length_path_survives_a_with() {
    let store = one_node();
    assert_eq!(path_of(&store, "MATCH p = (a) WITH p RETURN p", "p"), (1, 0));
}

#[test]
fn it_still_works_without_a_with() {
    // #909's own case, pinned: moving the operator must not lose it.
    let store = one_node();
    assert_eq!(path_of(&store, "MATCH p = (a) RETURN p", "p"), (1, 0));
}

#[test]
fn it_survives_an_aliasing_with() {
    let store = one_node();
    assert_eq!(path_of(&store, "MATCH p = (a) WITH p AS q RETURN q", "q"), (1, 0));
}

#[test]
fn it_survives_a_with_star() {
    let store = one_node();
    assert_eq!(path_of(&store, "MATCH p = (a) WITH * RETURN p", "p"), (1, 0));
}

#[test]
fn a_path_with_segments_is_unaffected() {
    // The ordinary case, bound by the expand that walks it.
    let mut store = GraphStore::new();
    let a = store.create_node_with_labels([Label::new("A")]);
    let b = store.create_node_with_labels([Label::new("B")]);
    store.create_edge(a, b, "R").unwrap();
    assert_eq!(path_of(&store, "MATCH p = (:A)-[:R]->(:B) RETURN p", "p"), (2, 1));
    assert_eq!(path_of(&store, "MATCH p = (:A)-[:R]->(:B) WITH p RETURN p", "p"), (2, 1));
}

#[test]
fn the_path_functions_read_it_after_a_with() {
    // A null would have made these answer 0 and null rather than failing, so
    // the projection alone is not enough to pin the fix.
    let store = one_node();
    let q = parse_query("MATCH p = (a) WITH p RETURN length(p) AS len, nodes(p) AS ns").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    let rec = &batch.records[0];
    assert!(
        format!("{:?}", rec.get("len")).contains("Integer(0)"),
        "{:?}",
        rec.get("len")
    );
    match rec.get("ns") {
        Some(Value::List(v)) => assert_eq!(v.len(), 1),
        Some(Value::Property(samyama::graph::PropertyValue::Array(v))) => assert_eq!(v.len(), 1),
        other => panic!("{other:?}"),
    }
}
