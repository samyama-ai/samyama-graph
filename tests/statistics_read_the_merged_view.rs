//! Statistics sample the merged property view, not one storage tier (LANG-15, #303).
//!
//! A snapshot import populates only the columnar store. Sampling row storage
//! therefore found *no* properties at all, produced zero statistics, and left
//! every selectivity estimate on the 10% default — which made an index lookup
//! look dearer than scanning a bigger label, so the planner anchored on the
//! wrong end of the pattern and scanned it.
//!
//! The fix was one line at the chokepoint: `compute_statistics()` samples
//! `node_properties_full()`, so every import path inherits it. LANG-15's H1
//! target says what to assert, and why this shape rather than one test per
//! import path: *"assert that stats sampling reads the merged view, so a future
//! storage tier cannot silently reintroduce #303"*.
//!
//! So this writes properties through both tiers and asserts the statistics see
//! both. A third tier added later, sampled by `node_properties_full`, passes
//! without anyone editing this file; one that is not, fails here.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;

/// Properties written through Cypher land in the columnar store; properties
/// written through `set_property` land in the node's own map. The two tiers are
/// the thing under test, so the fixture uses both deliberately.
fn store_with_both_tiers() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for i in 0..40 {
        engine
            .execute_mut(
                &format!("CREATE (:Item {{columnar: {i}, shared: 'c'}})"),
                &mut store,
                "default",
            )
            .expect("cypher create");
    }
    for i in 0..40 {
        let n = store.create_node("Item");
        let node = store.get_node_mut(n).expect("node");
        node.set_property("rowmap", PropertyValue::Integer(i));
        node.set_property("shared", PropertyValue::String("r".into()));
    }
    store
}

#[test]
fn statistics_see_properties_from_every_storage_tier() {
    let store = store_with_both_tiers();
    let stats = store.compute_statistics();
    let item = Label::new("Item");

    let seen: Vec<String> = stats
        .property_stats
        .keys()
        .filter(|(l, _)| *l == item)
        .map(|(_, k)| k.clone())
        .collect();

    assert!(
        seen.iter().any(|k| k == "columnar"),
        "a property written through the Cypher path is missing from the statistics: {seen:?}"
    );
    assert!(
        seen.iter().any(|k| k == "rowmap"),
        "a property written through the Rust API is missing from the statistics: {seen:?}"
    );
}

#[test]
fn a_property_present_in_both_tiers_is_counted_from_both() {
    // The sharper version: `shared` exists on all 80 nodes, 40 per tier. If
    // sampling read one tier the presence count would be 40, which still looks
    // like a real statistic — the failure #303 caused was not an empty result
    // but a plausible wrong one.
    let store = store_with_both_tiers();
    let stats = store.compute_statistics();
    let key = (Label::new("Item"), "shared".to_string());
    let s = stats
        .property_stats
        .get(&key)
        .expect("`shared` is on every node and must appear in the statistics");

    // Every node has it, so nothing is null. Sampling one tier would find it on
    // half the nodes and report a null fraction near 0.5 — still a plausible
    // statistic, which is the point: #303 produced a wrong number, not an
    // obviously empty one.
    assert!(
        s.null_fraction < 0.1,
        "`shared` is on all 80 nodes across two tiers, so its null fraction should be \
         ~0; statistics say {:.2}, which is what reading one tier looks like",
        s.null_fraction
    );
}

#[test]
fn statistics_are_not_empty_for_a_graph_built_only_through_cypher() {
    // The original #303 shape on its own: every property in the columnar store
    // and none in the row map.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for i in 0..20 {
        engine
            .execute_mut(&format!("CREATE (:Only {{k: {i}}})"), &mut store, "default")
            .expect("cypher create");
    }
    let stats = store.compute_statistics();
    assert!(
        stats
            .property_stats
            .keys()
            .any(|(l, k)| *l == Label::new("Only") && k == "k"),
        "a graph whose properties live only in the columnar store produced no property \
         statistics, which is #303 exactly"
    );
}
