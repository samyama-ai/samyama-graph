//! The cost model's triple statistics must survive a snapshot import.
//!
//! This is the sibling of `anchored_scan_after_import.rs` (#303). That one
//! covered *property* statistics, which were sampled from row storage and so
//! came back empty from a columnar-only import. This covers the **catalog**,
//! which is emptied by a different mechanism:
//!
//!   * `create_node_stub` does call `catalog.on_label_added`, so label counts
//!     survive -- which is exactly what makes the gap hard to notice;
//!   * `create_edge_stub` does **not** call `catalog.on_edge_created`, so
//!     triple statistics are absent entirely.
//!
//! With no triple stats, `estimate_expand_out` returns its documented default
//! of "assume 1 edge per node". On a graph where each node has 20 outgoing
//! edges that is a 20x under-estimate, and it grows with degree -- on LDBC,
//! where a Person knows tens of others, it is the difference between planning
//! an expansion as cheap and planning it correctly.

use samyama::graph::{EdgeType, GraphStore, Label};
use samyama::query::QueryEngine;
use samyama::snapshot::{export_tenant, import_tenant};

const NODES: usize = 200;
const OUT_DEGREE: usize = 20;

/// A graph with a known, uniform out-degree, so the expected estimate is exact.
fn built_store() -> GraphStore {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    for i in 0..NODES {
        engine
            .execute_mut(&format!("CREATE (:Person {{id: {i}}})"), &mut store, "default")
            .unwrap();
    }
    for i in 0..NODES {
        for k in 1..=OUT_DEGREE {
            engine
                .execute_mut(
                    &format!(
                        "MATCH (a:Person {{id: {i}}}), (b:Person {{id: {}}}) CREATE (a)-[:KNOWS]->(b)",
                        (i + k) % NODES
                    ),
                    &mut store,
                    "default",
                )
                .unwrap();
        }
    }
    store
}

fn round_trip(store: &GraphStore) -> GraphStore {
    let mut buf: Vec<u8> = Vec::new();
    export_tenant(store, &mut buf).expect("export");
    let mut imported = GraphStore::new();
    import_tenant(&mut imported, &buf[..]).expect("import");
    imported
}

#[test]
fn triple_statistics_survive_a_snapshot_import() {
    let store = built_store();
    let person = Label::new("Person");
    let knows = EdgeType::new("KNOWS");

    let before = store.catalog().estimate_expand_out(&person, &knows);
    assert!(
        (before - OUT_DEGREE as f64).abs() < 0.5,
        "fixture is wrong: expected ~{OUT_DEGREE}, got {before}"
    );

    let imported = round_trip(&store);
    assert_eq!(imported.node_count(), NODES, "nodes did not survive");
    assert_eq!(imported.edge_count(), NODES * OUT_DEGREE, "edges did not survive");

    let after = imported.catalog().estimate_expand_out(&person, &knows);
    assert!(
        (after - before).abs() < 0.5,
        "expand estimate changed across a snapshot round-trip: {before} -> {after}. \
         A value of 1.0 means the catalog was never rebuilt and the cost model is \
         falling back to 'assume 1 edge per node'."
    );
}

#[test]
fn triple_statistics_are_not_merely_present_but_correct() {
    // A rebuild that produced *some* stats but wrong ones would pass a
    // non-empty check, so assert the value rather than the count.
    let imported = round_trip(&built_store());
    assert!(
        !imported.catalog().all_triple_stats().is_empty(),
        "no triple statistics after import"
    );
    let estimate = imported
        .catalog()
        .estimate_expand_out(&Label::new("Person"), &EdgeType::new("KNOWS"));
    assert!(
        estimate > 1.0,
        "estimate is exactly the 'no statistics' default of 1.0"
    );
}

#[test]
fn label_counts_survive_too() {
    // These always survived -- `create_node_stub` maintains them -- which is
    // why the missing triple stats were easy to miss. Pinned so a future
    // change to the node stub cannot quietly break them either.
    let imported = round_trip(&built_store());
    let n = imported.catalog().estimate_label_scan(&Label::new("Person"));
    assert!((n - NODES as f64).abs() < 0.5, "label count after import: {n}");
}
