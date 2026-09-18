//! `GraphStore::node_property` sees a property however it was written.
//!
//! There are two property stores: a node's own map, and the columnar store the
//! Cypher write path uses (ADR-021). `Node::get_property` reads only the first,
//! so it answers `None` for data a query returns — which is how the parity
//! exporter's `or.solve` check silently stopped running (#1313).
//!
//! This pins the invariant that *should* hold rather than the defect: whichever
//! way a property was written, `node_property` finds it. If #1313 is fixed so
//! that both accessors agree, this test keeps passing — a test that encoded the
//! asymmetry would have to be deleted to fix the bug, which is the wrong
//! incentive.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;

#[test]
fn node_property_finds_a_value_whichever_path_wrote_it() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut("CREATE (:Item {cost: 7.0})", &mut store, "default")
        .expect("create through Cypher");
    let via_cypher = store.get_nodes_by_label(&Label::new("Item"))[0].id;

    let via_api = store.create_node("Item");
    store
        .get_node_mut(via_api)
        .unwrap()
        .set_property("cost", PropertyValue::Float(7.0));

    for (id, how) in [(via_cypher, "Cypher"), (via_api, "the Rust API")] {
        assert_eq!(
            store.node_property(id, "cost").and_then(|v| v.as_float()),
            Some(7.0),
            "a property written through {how} must be readable through \
             GraphStore::node_property"
        );
    }

    // And the value is the one a query returns, which is the reason the first
    // assertion matters: the accessor and the engine must not disagree.
    let rows = engine
        .execute("MATCH (i:Item) RETURN i.cost AS c", &store)
        .expect("query");
    assert_eq!(rows.records.len(), 2, "both nodes are visible to a query");
}
