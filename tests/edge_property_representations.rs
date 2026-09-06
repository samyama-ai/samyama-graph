//! Edge properties written the way bulk loaders write them are readable.
//!
//! `GraphStore` keeps edge properties in two places — the row map in
//! `edge_properties` and the typed `edge_columns` — and which of them a given edge
//! lands in depends on the API used to write it:
//!
//! | writer | row | columnar | reached by |
//! |---|---|---|---|
//! | `create_edge_with_properties` | yes | yes | direct API only |
//! | `set_edge_property` | yes | yes | direct API only |
//! | Cypher `CREATE ()-[:R {..}]->()` | **yes** | **no** | every query |
//! | `create_edge` + `get_edge_properties_mut` | **yes** | **no** | every bulk loader |
//!
//! Measured, not assumed: after `CREATE (a)-[:R {w: 2.5}]->(b)` the row map holds
//! `w` and `edge_columns` holds `Null`. **No path a user exercises populates
//! `edge_columns`** — the two writers that do are called only from direct API code
//! and their own tests, while the query engine and every bulk loader in the repo
//! (LDBC, FinBench, the banking demo) write the row map directly.
//!
//! So `edge_columns` is empty in practice and **every** edge property read is
//! answered by the **row fallback** in `record.rs`.
//!
//! That fallback is therefore load-bearing, and nothing said so. It reads like the
//! slow path of an optimisation — the kind of branch someone deletes when the
//! columnar store looks like the real representation — and deleting it would return
//! `null` for every edge property in every bulk-loaded graph, silently, because
//! `null` is a legal answer for an absent property.
//!
//! These tests pin it. If the columnar store starts being populated on this path,
//! the precondition assertion below fails and says to re-derive the test rather
//! than delete it.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

const T: &str = "default";

/// Build the way `benches/ldbc_common` and `benches/finbench_common` build.
fn bulk_loaded() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    let b = store.create_node("Person");
    store.set_node_property(T, a, "name", "ada").unwrap();
    store.set_node_property(T, b, "name", "grace").unwrap();

    let e = store.create_edge(a, b, "KNOWS").unwrap();
    if let Some(props) = store.get_edge_properties_mut(e) {
        props.insert("since".to_string(), PropertyValue::Integer(2020));
        props.insert("note".to_string(), PropertyValue::String("bulk".into()));
    }
    store
}

#[test]
fn the_row_fallback_is_what_answers_a_bulk_loaded_edge_property() {
    let store = bulk_loaded();
    let e = samyama::graph::EdgeId::new(1);
    let idx = e.as_u64() as usize;

    // The precondition. If this fails, the write path has changed and the test is
    // no longer exercising the fallback — re-derive it, do not delete the assert.
    assert!(
        store.edge_columns.get_property(idx, "since").is_null(),
        "the bulk path now populates edge_columns, so this test no longer covers \
         the row fallback it was written for"
    );

    let engine = QueryEngine::new();
    let batch = engine
        .execute("MATCH ()-[r:KNOWS]->() RETURN r.since, r.note", &store)
        .expect("query");
    assert_eq!(batch.records.len(), 1);
    let row = &batch.records[0];
    assert_eq!(
        format!("{:?}", row.get("r.since")),
        format!("{:?}", Some(&samyama::query::executor::record::Value::Property(
            PropertyValue::Integer(2020)
        ))),
        "the row fallback stopped answering: every edge property in every \
         bulk-loaded graph now reads as null"
    );
}

/// The query engine writes the row map only — the same shape as the bulk loaders,
/// and not what the two columnar-aware writers do.
///
/// Pinned as the measured fact rather than the expected one: this test was first
/// written asserting that `CREATE` populates both, on the reasoning that the query
/// engine is the primary path and the columnar store exists for it. It does not.
/// The assertion is inverted here so the next person reads the behaviour rather
/// than the assumption (#1127).
#[test]
fn the_query_engine_path_writes_the_row_map_only() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "CREATE (a:P {n: 1})-[:R {w: 2.5}]->(b:P {n: 2})",
            &mut store,
            T,
        )
        .expect("create");

    let e = samyama::graph::EdgeId::new(1);
    let idx = e.as_u64() as usize;
    assert_eq!(
        store.get_edge(e).and_then(|x| x.get_property("w").cloned()),
        Some(PropertyValue::Float(2.5)),
        "the query engine no longer populates the row copy"
    );
    assert!(
        store.edge_columns.get_property(idx, "w").is_null(),
        "the query engine now populates edge_columns too — that is an improvement, \
         and it means the row fallback is no longer the only thing answering edge \
         property reads. Re-derive this file's premise before changing the assert."
    );

    // And the answer is still right, because of the fallback.
    let batch = engine
        .execute("MATCH ()-[r:R]->() RETURN r.w", &store)
        .expect("query");
    assert_eq!(batch.records.len(), 1);
}
