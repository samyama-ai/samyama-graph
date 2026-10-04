//! An anchored single-hop must seed from its anchor index, including on a graph loaded
//! from a snapshot (#303).
//!
//! The failure had a two-step cause, and neither step is visible on a graph built by
//! CREATE:
//!
//! 1. Property statistics were sampled from `node.properties` -- row storage. A snapshot
//!    import populates only the columnar store, so sampling found no properties and
//!    produced **zero** statistics.
//! 2. `estimate_equality_selectivity` then fell back to its 10% default. On a large anchor
//!    label that makes an index lookup look more expensive than scanning a *smaller* target
//!    label, so the planner anchored on the target and scanned it -- expanding backwards.
//!
//! Which is why structurally identical anchored 1-hops differed by six orders of magnitude
//! depending on the target label, and why an anchor value that did not exist still scanned
//! instead of returning immediately.

use samyama::graph::{GraphStore, Label};
use samyama::query::QueryEngine;
use samyama::snapshot::{export_tenant, import_tenant};

/// Large indexed anchor label, small target label — the shape that misleads the estimate.
fn imported_store() -> GraphStore {
    let engine = QueryEngine::new();
    let mut source = GraphStore::new();
    for i in 0..5000 {
        engine
            .execute_mut(&format!("CREATE (:Article {{pmid: \"{i}\"}})"), &mut source, "default")
            .unwrap();
    }
    for i in 0..50 {
        engine
            .execute_mut(&format!("CREATE (:Author {{id: {i}}})"), &mut source, "default")
            .unwrap();
    }
    for i in 0..50 {
        engine
            .execute_mut(
                &format!(
                    "MATCH (a:Article {{pmid: \"{i}\"}}), (x:Author {{id: {i}}}) \
                     CREATE (a)-[:AUTHORED_BY]->(x)"
                ),
                &mut source, "default",
            )
            .unwrap();
    }

    let mut buf = Vec::new();
    export_tenant(&source, &mut buf).expect("export");
    let mut store = GraphStore::new();
    import_tenant(&mut store, &buf[..]).expect("import");
    engine
        .execute_mut("CREATE INDEX ON :Article(pmid)", &mut store, "default")
        .unwrap();
    store
}

fn plan_for_unchecked(store: &GraphStore, query: &str) -> String {
    let engine = QueryEngine::new();
    let batch = engine
        .execute(&format!("EXPLAIN {query}"), store)
        .expect("explain");
    format!("{:?}", batch.records[0].get("plan")).replace("\\n", "\n")
}

/// [`plan_for_unchecked`], refusing a plan tree that stops early.
///
/// `PhysicalOperator::describe` has a default printing `Unknown` with no
/// children, so an operator that does not implement it truncates the tree —
/// and an assertion that the plan does *not* contain something is then
/// satisfied by the missing half, whatever the planner did (#1826). Checked
/// here, where the text is read, so the hole fails loudly rather than passing.
fn plan_for(store: &GraphStore, query: &str) -> String {
    let text = plan_for_unchecked(store, query);
    assert!(
        samyama::query::executor::undescribed_plan_lines(&text).is_empty(),
        "the plan tree stops at an operator with no describe, so what is below \
         it cannot be asserted about (#1826):\n{text}"
    );
    text
}

#[test]
fn property_statistics_survive_a_snapshot_import() {
    // Step 1 of the cause. With zero statistics every selectivity is the 10% placeholder,
    // and every cost decision on an imported graph is made on that placeholder.
    let store = imported_store();
    let stats = store.statistics();

    assert!(
        !stats.property_stats.is_empty(),
        "a snapshot-imported graph produced no property statistics at all"
    );

    let selectivity = stats.estimate_equality_selectivity(&Label::new("Article"), "pmid");
    assert!(
        selectivity < 0.1,
        "pmid is near-unique across 5000 articles; got the {selectivity} default instead"
    );
}

#[test]
fn an_anchored_hop_seeds_from_the_anchor_index_not_the_target_label() {
    let store = imported_store();

    for (name, query) in [
        ("present anchor", "MATCH (a:Article {pmid: \"10\"})-[:AUTHORED_BY]->(au:Author) RETURN au"),
        // the anchor value does not exist: this must resolve from the index, not scan
        ("absent anchor", "MATCH (a:Article {pmid: \"999999\"})-[:AUTHORED_BY]->(au:Author) RETURN au"),
    ] {
        let plan = plan_for(&store, query);
        assert!(
            plan.contains("IndexScan (var=a"),
            "{name}: should seed from :Article(pmid), plan was:\n{plan}"
        );
        assert!(
            !plan.contains("NodeScan (var=au"),
            "{name}: scanned the target label instead, plan was:\n{plan}"
        );
    }
}

#[test]
fn the_anchored_hop_still_returns_the_right_rows() {
    // A plan assertion alone would pass on a plan that is fast and wrong.
    let store = imported_store();
    let engine = QueryEngine::new();

    let hit = engine
        .execute("MATCH (a:Article {pmid: \"10\"})-[:AUTHORED_BY]->(au:Author) RETURN au.id AS id", &store)
        .expect("query");
    assert_eq!(hit.records.len(), 1);

    let miss = engine
        .execute("MATCH (a:Article {pmid: \"999999\"})-[:AUTHORED_BY]->(au:Author) RETURN au.id AS id", &store)
        .expect("query");
    assert_eq!(miss.records.len(), 0);
}
