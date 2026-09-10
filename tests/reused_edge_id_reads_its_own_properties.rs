//! A relationship created on a reused id reads its own properties, not the
//! deleted relationship's.
//!
//! `delete_edge` cleared the row map (`edge_properties`) but not
//! `edge_columns`, and pushed the id onto `free_edge_ids`. Reads consult the
//! column first. So when the deleted edge's properties had reached the
//! columns -- snapshot import (`create_edge_with_properties`) and
//! `set_edge_property` both write there -- the next edge to take that id
//! inherited them: a property it never had read as the old value, and one it
//! was created with was shadowed by the old value.

use samyama::graph::{GraphStore, PropertyMap, PropertyValue};
use samyama::query::QueryEngine;

fn read(store: &GraphStore, q: &str) -> Vec<String> {
    QueryEngine::new()
        .execute(q, store)
        .unwrap()
        .records
        .iter()
        .map(|r| format!("{:?}", r.get("v")))
        .collect()
}

fn graph_with_imported_edge() -> (GraphStore, samyama::graph::EdgeId) {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    // The path snapshot import takes: properties reach the columns.
    let mut props = PropertyMap::new();
    props.insert("w".to_string(), PropertyValue::Integer(1));
    props.insert(
        "only_old".to_string(),
        PropertyValue::String("stale".into()),
    );
    let old = store.create_edge_with_properties(a, b, "R", props).unwrap();
    (store, old)
}

#[test]
fn a_property_the_new_relationship_never_had_is_null() {
    let (mut store, old) = graph_with_imported_edge();
    store.delete_edge(old).unwrap();
    QueryEngine::new()
        .execute_mut(
            "MATCH (a:A), (b:B) CREATE (a)-[:R {w: 2}]->(b)",
            &mut store,
            "default",
        )
        .unwrap();

    let got = read(&store, "MATCH ()-[r:R]->() RETURN r.only_old AS v");
    assert_eq!(got.len(), 1);
    assert!(
        got[0].contains("Null"),
        "the new relationship reads a property only the deleted one had: {got:?}"
    );
}

#[test]
fn a_property_the_new_relationship_was_created_with_is_its_own() {
    let (mut store, old) = graph_with_imported_edge();
    store.delete_edge(old).unwrap();
    QueryEngine::new()
        .execute_mut(
            "MATCH (a:A), (b:B) CREATE (a)-[:R {w: 2}]->(b)",
            &mut store,
            "default",
        )
        .unwrap();

    let got = read(&store, "MATCH ()-[r:R]->() RETURN r.w AS v");
    assert_eq!(got.len(), 1);
    assert!(
        got[0].contains("Integer(2)"),
        "the deleted relationship's value shadows the new one's: {got:?}"
    );
}
