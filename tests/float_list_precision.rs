//! A float in a list literal keeps all 64 of its bits.
//!
//! A list literal became a `Vector(Vec<f32>)` as soon as one element was a
//! float, on the theory that a float list is an embedding. Every element was
//! narrowed to 32 bits on the way in, so `UNWIND [1.3, 1.5] AS v RETURN v`
//! returned **1.2999999523162842** — and `ORDER BY` over those values sorted
//! numbers that were no longer the ones written.
//!
//! Cypher floats are IEEE doubles. The coercion was also unnecessary:
//! `PropertyValue::to_vector` accepts a numeric array, so an embedding written
//! as a list literal still indexes without the literal pretending to be a
//! vector. Vector-ness is the consumer's decision, not the literal's.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn rows(store: &GraphStore, cypher: &str) -> Vec<Value> {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .iter()
        .map(|r| r.get("x").cloned().expect("column x"))
        .collect()
}

fn floats(store: &GraphStore, cypher: &str) -> Vec<f64> {
    rows(store, cypher)
        .into_iter()
        .map(|v| match v {
            Value::Property(PropertyValue::Float(f)) => f,
            other => panic!("expected a float, got {other:?}"),
        })
        .collect()
}

#[test]
fn a_float_in_a_list_literal_survives_unwind_unchanged() {
    let store = GraphStore::new();
    // Exact equality is the assertion. 1.3 as an f32 widened back to f64 is
    // 1.2999999523162842, which is not 1.3 by any tolerance worth having.
    assert_eq!(floats(&store, "UNWIND [1.3, 1.5] AS v RETURN v AS x"), vec![1.3, 1.5]);
    assert_eq!(floats(&store, "UNWIND [999.99] AS v RETURN v AS x"), vec![999.99]);
}

#[test]
fn ordering_floats_from_a_list_compares_the_values_that_were_written() {
    let store = GraphStore::new();
    assert_eq!(
        floats(&store, "UNWIND [1.3, 999.99, 1.5] AS v WITH v ORDER BY v DESC RETURN v AS x"),
        vec![999.99, 1.5, 1.3]
    );
}

#[test]
fn a_list_literal_stays_a_list_whatever_it_contains() {
    let store = GraphStore::new();
    for (cypher, expected) in [
        (
            "RETURN [1.3, 1.5] AS x",
            PropertyValue::Array(vec![PropertyValue::Float(1.3), PropertyValue::Float(1.5)]),
        ),
        (
            "RETURN [1, 2] AS x",
            PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]),
        ),
        (
            "RETURN [1, 2.5] AS x",
            PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Float(2.5)]),
        ),
    ] {
        match rows(&store, cypher).into_iter().next() {
            Some(Value::Property(got)) => assert_eq!(got, expected, "for `{cypher}`"),
            other => panic!("expected a property for `{cypher}`, got {other:?}"),
        }
    }
}

#[test]
fn an_embedding_written_as_a_list_literal_is_still_searchable() {
    // The reason the coercion existed. It has to keep working without it —
    // otherwise this fix trades a precision bug for a silently empty index.
    let mut store = GraphStore::new();
    for cypher in [
        "CREATE VECTOR INDEX doc_idx FOR (n:Doc) ON (n.embedding) OPTIONS {dimensions: 3, similarity: 'cosine'}",
        "CREATE (n:Doc {name: 'near', embedding: [1.0, 0.0, 0.0]})",
        "CREATE (n:Doc {name: 'far', embedding: [0.0, 1.0, 0.0]})",
    ] {
        let q = parse_query(cypher).expect("query should parse");
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    }

    let q = parse_query(
        "CALL db.index.vector.queryNodes('Doc', 'embedding', [1.0, 0.1, 0.0], 1) \
         YIELD node RETURN node.name AS x",
    )
    .expect("query should parse");
    let found = QueryExecutor::new(&store).execute(&q).expect("search should run");
    assert_eq!(found.records.len(), 1, "the index must not be empty");
    assert_eq!(
        found.records[0].get("x").and_then(|v| v.as_property()).and_then(|p| p.as_string()),
        Some("near")
    );
}
