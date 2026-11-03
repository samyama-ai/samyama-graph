//! The O(1) count fast paths are blind to a self-loop pattern (#962).
//!
//! ```cypher
//! CREATE (a:A)-[:LOOP]->(a), ()-[:T]->()
//!
//! MATCH (n)-[r]->(n) RETURN count(r)   -- answered 2
//! MATCH (n)-[r]->(n) RETURN r          -- 1 row
//! ```
//!
//! The same query, two different answers, depending on whether it counted.
//!
//! `EdgeCountOperator` reads the edge total straight off the store — which is
//! the point, on a billion-edge graph — but the store knows nothing about
//! `(n)…(n)`, the constraint that both endpoints are the *same* node. The
//! guard already excluded undirected patterns and labels and properties; it
//! did not exclude a repeated endpoint variable.
//!
//! An optimisation may skip **work**. It may not skip a **predicate**.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// One self-loop and one ordinary edge, so total and self-loop counts differ.
fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node_with_labels([Label::new("A")]);
    store.create_edge(a, a, "LOOP").unwrap();
    let x = store.create_node("");
    let y = store.create_node("");
    store.create_edge(x, y, "T").unwrap();
    store
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    match QueryExecutor::new(store).execute(&q).unwrap().records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{cypher}: {other:?}"),
    }
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store).execute(&q).unwrap().records.len()
}

#[test]
fn counting_a_self_loop_pattern_agrees_with_returning_it() {
    // The property that was violated, stated directly: counting a pattern and
    // listing it must give the same number.
    let store = graph();
    assert_eq!(count(&store, "MATCH (n)-[r]->(n) RETURN count(r) AS c"), 1);
    assert_eq!(rows(&store, "MATCH (n)-[r]->(n) RETURN r"), 1);
}

#[test]
fn count_star_over_a_self_loop_pattern_too() {
    let store = graph();
    assert_eq!(count(&store, "MATCH (n)-[r]->(n) RETURN count(*) AS c"), 1);
}

#[test]
fn a_typed_self_loop_pattern_is_counted_correctly() {
    let store = graph();
    assert_eq!(count(&store, "MATCH (n)-[r:LOOP]->(n) RETURN count(r) AS c"), 1);
    assert_eq!(count(&store, "MATCH (n)-[r:T]->(n) RETURN count(r) AS c"), 0);
}

#[test]
fn the_ordinary_fast_path_still_answers_the_total() {
    // The direction a fix that simply disabled the optimisation would break.
    // Distinct endpoints, no labels, no properties: still the store's count.
    let store = graph();
    assert_eq!(count(&store, "MATCH (a)-[r]->(b) RETURN count(r) AS c"), 2);
    assert_eq!(count(&store, "MATCH (a)-[r:T]->(b) RETURN count(r) AS c"), 1);
    assert_eq!(count(&store, "MATCH (a)-[r:LOOP]->(b) RETURN count(r) AS c"), 1);
}

#[test]
fn the_fast_path_is_still_chosen_for_the_ordinary_shape() {
    // Asserted through EXPLAIN, because "the answer is right" would also hold
    // if the optimisation had been silently switched off everywhere.
    let store = graph();
    let q = parse_query("EXPLAIN MATCH (a)-[r]->(b) RETURN count(r) AS c").unwrap();
    let plan = format!("{:?}", QueryExecutor::new(&store).execute(&q).unwrap().records[0]);
    assert!(plan.contains("EdgeCount"), "{plan}");
}

#[test]
fn the_self_loop_shape_does_not_use_it() {
    let store = graph();
    let q = parse_query("EXPLAIN MATCH (n)-[r]->(n) RETURN count(r) AS c").unwrap();
    let plan = format!("{:?}", QueryExecutor::new(&store).execute(&q).unwrap().records[0]);
    assert!(!plan.contains("EdgeCount"), "{plan}");
}

#[test]
fn grouping_by_type_over_a_self_loop_pattern_is_correct_too() {
    // The second fast path had the same blind spot.
    let store = graph();
    let q = parse_query("MATCH (n)-[r]->(n) RETURN type(r) AS t, count(r) AS c").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    assert_eq!(batch.records.len(), 1, "{:?}", batch.records);
    match batch.records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => assert_eq!(*n, 1),
        other => panic!("{other:?}"),
    }
}
