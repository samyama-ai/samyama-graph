//! `db.index.vector.queryNodes` takes its query vector and `k` as `$params`
//! (#2047).
//!
//! A client sending a 768-float vector should not have to print it into the
//! query text on every call: that is a parse of 768 literals per query and a
//! different query string, so a different cache entry, for every vector. These
//! tests go through `QueryEngine::execute_with_params`, the path both HTTP
//! `/api/query` and RESP `GRAPH.QUERY` use, for both spellings of the call.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::{BoundParams, QueryEngine, Value};
use samyama::vector::index::Quantization;
use samyama::vector::DistanceMetric;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    s.create_vector_index_named(
        Some("docs"),
        "Doc",
        "emb",
        3,
        DistanceMetric::Cosine,
        Quantization::None,
    )
    .expect("index");
    for (name, v) in [
        ("x", [1.0f32, 0.0, 0.0]),
        ("y", [0.0, 1.0, 0.0]),
        ("z", [0.0, 0.0, 1.0]),
    ] {
        let id = s.create_node("Doc");
        s.set_node_property("default", id, "name", PropertyValue::String(name.into()))
            .unwrap();
        s.set_node_property("default", id, "emb", PropertyValue::Vector(v.to_vec()))
            .unwrap();
    }
    s
}

fn vec_param(v: &[f64]) -> PropertyValue {
    PropertyValue::Array(v.iter().map(|f| PropertyValue::Float(*f)).collect())
}

fn names(s: &GraphStore, q: &str, params: BoundParams) -> Vec<String> {
    let batch = QueryEngine::new()
        .execute_with_params(q, s, &params)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    batch
        .records
        .iter()
        .map(|r| match r.get("name") {
            Some(Value::Property(PropertyValue::String(n))) => n.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

fn refused(s: &GraphStore, q: &str, params: BoundParams) -> String {
    match QueryEngine::new().execute_with_params(q, s, &params) {
        Ok(b) => panic!("{q}: accepted with {} rows", b.records.len()),
        Err(e) => e.to_string(),
    }
}

fn params(entries: &[(&str, PropertyValue)]) -> BoundParams {
    entries.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

#[test]
fn the_query_vector_and_k_are_parameters_in_our_form() {
    let s = store();
    let got = names(
        &s,
        "CALL db.index.vector.queryNodes('Doc', 'emb', $vec, $k) YIELD node \
         RETURN node.name AS name",
        params(&[("vec", vec_param(&[0.9, 0.1, 0.0])), ("k", PropertyValue::Integer(1))]),
    );
    assert_eq!(got, vec!["x".to_string()]);
}

#[test]
fn the_query_vector_and_k_are_parameters_in_neo4j_form() {
    let s = store();
    let got = names(
        &s,
        "CALL db.index.vector.queryNodes('docs', $k, $vec) YIELD node \
         RETURN node.name AS name",
        params(&[("vec", vec_param(&[0.0, 0.1, 0.9])), ("k", PropertyValue::Integer(1))]),
    );
    assert_eq!(got, vec!["z".to_string()]);
}

#[test]
fn a_parameter_answers_as_the_same_literal_does() {
    let s = store();
    let literal = names(
        &s,
        "CALL db.index.vector.queryNodes('Doc', 'emb', [0.1, 0.9, 0.0], 2) YIELD node \
         RETURN node.name AS name",
        BoundParams::new(),
    );
    let bound = names(
        &s,
        "CALL db.index.vector.queryNodes('Doc', 'emb', $vec, $k) YIELD node \
         RETURN node.name AS name",
        params(&[("vec", vec_param(&[0.1, 0.9, 0.0])), ("k", PropertyValue::Integer(2))]),
    );
    assert_eq!(literal.len(), 2);
    assert_eq!(literal, bound);
}

#[test]
fn a_list_of_parameters_and_an_expression_for_k_are_accepted() {
    let s = store();
    let got = names(
        &s,
        "CALL db.index.vector.queryNodes('Doc', 'emb', [$a, $b, $c], toInteger($k)) YIELD node \
         RETURN node.name AS name",
        params(&[
            ("a", PropertyValue::Float(0.0)),
            ("b", PropertyValue::Float(1.0)),
            ("c", PropertyValue::Float(0.0)),
            ("k", PropertyValue::String("1".into())),
        ]),
    );
    assert_eq!(got, vec!["y".to_string()]);
}

#[test]
fn a_missing_parameter_is_named() {
    let s = store();
    let e = refused(
        &s,
        "CALL db.index.vector.queryNodes('Doc', 'emb', $vec, 1) YIELD node RETURN node.name AS name",
        BoundParams::new(),
    );
    assert!(e.contains("$vec"), "{e}");
}

#[test]
fn a_parameter_of_the_wrong_type_is_refused() {
    let s = store();
    let e = refused(
        &s,
        "CALL db.index.vector.queryNodes('Doc', 'emb', $vec, 1) YIELD node RETURN node.name AS name",
        params(&[("vec", PropertyValue::String("not a vector".into()))]),
    );
    assert!(e.contains("the query vector must be a list of numbers"), "{e}");

    let e = refused(
        &s,
        "CALL db.index.vector.queryNodes('Doc', 'emb', [1.0, 0.0, 0.0], $k) YIELD node \
         RETURN node.name AS name",
        params(&[("k", PropertyValue::Float(1.5))]),
    );
    assert!(e.contains("k must be a non-negative integer"), "{e}");
}

#[test]
fn a_negative_k_is_refused_not_read_as_every_vector() {
    // `-1 as usize` is usize::MAX.
    let s = store();
    let e = refused(
        &s,
        "CALL db.index.vector.queryNodes('Doc', 'emb', [1.0, 0.0, 0.0], $k) YIELD node \
         RETURN node.name AS name",
        params(&[("k", PropertyValue::Integer(-1))]),
    );
    assert!(e.contains("k must be a non-negative integer"), "{e}");
}
