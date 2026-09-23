//! Edge properties are readable whichever store they were written to.
//!
//! `GraphStore` keeps edge properties in two places — the row map in
//! `edge_properties` and the typed `edge_columns` — and which of them a given edge
//! lands in depends on the API used to write it:
//!
//! | writer | row | columnar | reached by |
//! |---|---|---|---|
//! | `create_edge_with_properties` | yes | yes | snapshot import, direct API |
//! | `set_edge_property` | yes | yes | direct API |
//! | `set_edge_property_sparse` / `set_edge_properties_sparse` | yes | yes | Cypher `CREATE`/`MERGE`/`SET`, the FinBench loader |
//! | `create_edge` + `get_edge_properties_mut` | **yes** | **no** | the LDBC loader, the banking demo |
//!
//! Until #1059, Cypher `CREATE` wrote the row map only and `edge_columns` was empty
//! in practice (#1127): every edge property read was answered by the **row
//! fallback** in `record.rs`, and a `WHERE r.p ... ORDER BY r.p` scan paid two hash
//! probes into scattered memory per row for it — 63% of FinBench CR-8.
//!
//! The query engine now writes both. The row fallback is still load-bearing for
//! anything written through `get_edge_properties_mut`, and deleting it would return
//! `null` for every such edge property, silently, because `null` is a legal answer
//! for an absent property. The first test pins that; the second pins that the query
//! engine's writes reach the column the reads try first.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

const T: &str = "default";

/// Build the way `benches/ldbc_common` builds: the row map only.
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
        format!(
            "{:?}",
            Some(&samyama::query::executor::record::Value::Property(
                PropertyValue::Integer(2020)
            ))
        ),
        "the row fallback stopped answering: every edge property in every \
         bulk-loaded graph now reads as null"
    );
}

/// The query engine writes both stores (#1059).
///
/// This test used to pin the opposite — `CREATE` writing the row map only — as the
/// measured fact of #1127, with a message saying that populating `edge_columns`
/// would be an improvement and to re-derive the premise when it happened. It
/// happened: the row-map-only write was the cost of FinBench CR-8.
#[test]
fn the_query_engine_path_writes_both_stores() {
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
    assert_eq!(
        store.edge_columns.get_property(idx, "w"),
        PropertyValue::Float(2.5),
        "the query engine stopped writing edge_columns: every relationship property \
         read falls back to the row map again"
    );

    let batch = engine
        .execute("MATCH ()-[r:R]->() RETURN r.w", &store)
        .expect("query");
    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        format!("{:?}", batch.records[0].get("r.w")),
        format!(
            "{:?}",
            Some(&samyama::query::executor::record::Value::Property(
                PropertyValue::Float(2.5)
            ))
        )
    );
}
