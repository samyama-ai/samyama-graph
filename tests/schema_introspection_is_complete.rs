//! The schema surfaces answer about the whole graph, not a sample of it.
//!
//! `db.schema.visualization()` and `db.propertyKeys()` each walked the first
//! 1000 edges of every type and stopped. On any graph with more edges than that
//! of a type, the answer was a sample presented as the schema -- a
//! `(Label)-[T]->(Label)` triple or an edge property key whose only edges sit
//! past the cap was simply absent, and nothing in the result said the answer
//! was partial.
//!
//! **It is the schema an LLM is handed.** AI-05 asks for a schema description a
//! model can plan against; a missing triple is a relationship the model is told
//! does not exist, and it writes queries accordingly. A silently truncated
//! answer is worse than an error, because the caller cannot tell.
//!
//! The graph here is built through `GraphStore` rather than Cypher: CI runs a
//! debug build, and 100,001 `CREATE`s through the query engine costs minutes
//! for nothing the test is about.

use samyama::graph::{GraphStore, PropertyMap, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

/// One edge past the old cap, of a shape that appears nowhere before it.
const BULK: usize = 100_000;

/// Each row as its bound strings, joined -- so an assertion reads `"A->B"`
/// rather than picking through escaped `Debug` output.
fn rows(engine: &QueryEngine, store: &mut GraphStore, q: &str) -> Vec<String> {
    let batch = engine.execute_mut(q, store, "default").expect("query");
    batch
        .records
        .iter()
        .map(|r| {
            r.bindings()
                .iter()
                .map(|(_, v)| match v {
                    Value::Property(PropertyValue::String(s)) => s.clone(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("->")
        })
        .collect()
}

/// `BULK` edges of `(A)-[T]->(B)`, then one `(C)-[T]->(D)` carrying a property
/// no other edge has.
fn graph_with_a_rare_triple() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    for _ in 0..BULK {
        store.create_edge(a, b, "T").expect("bulk edge");
    }
    let c = store.create_node("C");
    let d = store.create_node("D");
    let mut props = PropertyMap::new();
    props.insert(
        "rare_key".to_string(),
        PropertyValue::String("x".to_string()),
    );
    store
        .create_edge_with_properties(c, d, "T", props)
        .expect("rare edge");
    store
}

#[test]
fn schema_visualization_reports_a_triple_past_the_first_thousand_edges() {
    let mut store = graph_with_a_rare_triple();
    let engine = QueryEngine::new();
    let got = rows(
        &engine,
        &mut store,
        "CALL db.schema.visualization() YIELD source_label, relationship_type, target_label \
         RETURN source_label, target_label",
    );
    let flat = got.join(", ");
    assert!(
        got.iter().any(|r| r == "A->B"),
        "the common triple is missing entirely: {flat}"
    );
    assert!(
        got.iter().any(|r| r == "C->D"),
        "(C)-[T]->(D) exists in the graph and the schema does not report it. \
         Its only edge sits past the first 1000 edges of type T, so a capped \
         scan cannot see it: {flat}"
    );
}

#[test]
fn property_keys_reports_an_edge_key_past_the_first_thousand_edges() {
    let mut store = graph_with_a_rare_triple();
    let engine = QueryEngine::new();
    let got = rows(
        &engine,
        &mut store,
        "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey",
    );
    let flat = got.join(", ");
    assert!(
        got.iter().any(|r| r == "rare_key"),
        "`rare_key` is set on an edge of type T and db.propertyKeys() omits it; \
         that edge is the 100,001st of its type: {flat}"
    );
}

/// The complete answer is also still the *distinct* answer: removing the cap
/// must not start reporting one row per edge.
#[test]
fn schema_visualization_reports_each_triple_once() {
    let mut store = graph_with_a_rare_triple();
    let engine = QueryEngine::new();
    let got = rows(
        &engine,
        &mut store,
        "CALL db.schema.visualization() YIELD source_label, relationship_type, target_label \
         RETURN source_label, relationship_type, target_label",
    );
    assert_eq!(
        got.len(),
        2,
        "two distinct (label, type, label) triples exist over 100,001 edges; \
         got {} rows: {}",
        got.len(),
        got.join(", ")
    );
}
