//! A catalog template is held to the work it did when it was built (#1156).
//!
//! Requirement 4 of #1156: "a cost ceiling per template, recorded at build time
//! ... and re-checked at execution: a call whose estimated cost exceeds the
//! recorded ceiling by a stated factor is refused with an error that names the
//! parameter." The default planner records no plan cost (`chosen_plan_cost` is
//! `0.0` there), so the ceiling is measured instead: the rows every operator
//! produced with the sample values, summed, and a call may do
//! `WORK_CEILING_FACTOR` times that (at least `MIN_WORK_CEILING`).
//!
//! The case the issue names as still open is the one tested here: a selective
//! predicate handed a value that matches most of the graph. Written as an index
//! lookup and blessed with a rare value, the template is cheap; given a common
//! value it becomes a scan of most of the graph in all but name.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::error_code::TEMPLATE_COST_EXCEEDED;
use samyama::query::QueryEngine;
use samyama::snapshot::verify::{
    build_catalog, run_template, work_ceiling, CatalogEntry, QuerySpec, MIN_WORK_CEILING,
    WORK_CEILING_FACTOR,
};
use serde_json::json;
use std::collections::BTreeMap;

const COMMON: usize = 30_000;

/// One `rare` node and `COMMON` `common` ones, indexed on `g`, so a lookup on
/// the rare value touches one row and on the common value touches all of them.
fn skewed() -> GraphStore {
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE INDEX ON :P(g)", &mut s, "default")
        .unwrap();
    for i in 0..=COMMON {
        let n = s.create_node_with_labels([Label::new("P")]);
        let g = if i == 0 { "rare" } else { "common" };
        s.set_node_property("default", n, "g", PropertyValue::String(g.into()))
            .unwrap();
        s.set_node_property("default", n, "i", PropertyValue::Integer(i as i64))
            .unwrap();
        s.set_node_property("default", n, "tier", PropertyValue::Integer((i % 3) as i64))
            .unwrap();
    }
    s
}

fn specs() -> Vec<QuerySpec> {
    serde_json::from_value(json!([
        {
            "id": "by_group",
            "question": "Which nodes are in group {g}?",
            "difficulty": "easy",
            "cypher": "MATCH (p:P) WHERE p.g = $g RETURN p.i",
            "params": [{"name": "g", "type": "string", "sample": "rare"}]
        },
        {
            "id": "by_tier",
            "question": "How many nodes are in tier {t}?",
            "difficulty": "easy",
            "cypher": "MATCH (p:P) WHERE p.tier = $t RETURN count(p) AS n",
            "params": [{"name": "t", "type": "int", "sample": 1, "enum_values": [0, 1, 2]}]
        }
    ]))
    .unwrap()
}

fn entry<'a>(entries: &'a [CatalogEntry], id: &str) -> &'a CatalogEntry {
    entries.iter().find(|e| e.id == id).unwrap()
}

fn values(pairs: &[(&str, serde_json::Value)]) -> BTreeMap<String, serde_json::Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[test]
fn the_build_records_the_work_each_entry_did() {
    let s = skewed();
    let catalog = build_catalog(&s, &specs(), &[]).unwrap();
    let rare = entry(&catalog.entries, "by_group")
        .work
        .expect("work is recorded");
    let tier = entry(&catalog.entries, "by_tier")
        .work
        .expect("work is recorded");
    assert!(rare > 0, "a query that returned a row did some work");
    // The index lookup on the rare value is cheap; the tier count scans the label.
    assert!(
        rare < 100,
        "an indexed lookup on a value held by one node: {rare}"
    );
    assert!(
        tier > COMMON as u64,
        "a label scan produces every node: {tier}"
    );
    // Deterministic, or a rebuilt catalog would churn for no reason.
    let again = build_catalog(&s, &specs(), &[]).unwrap();
    assert_eq!(entry(&again.entries, "by_group").work, Some(rare));
    assert_eq!(entry(&again.entries, "by_tier").work, Some(tier));
}

#[test]
fn a_value_that_matches_most_of_the_graph_is_refused_by_name() {
    let s = skewed();
    let catalog = build_catalog(&s, &specs(), &[]).unwrap();
    let e = entry(&catalog.entries, "by_group");
    assert_eq!(work_ceiling(e.work.unwrap()), MIN_WORK_CEILING);

    // The sample value runs, and agrees with the catalog.
    let ok = run_template(&s, e, &BTreeMap::new()).expect("the samples are within the ceiling");
    assert_eq!(ok.records.len(), e.rows);

    let err = run_template(&s, e, &values(&[("g", json!("common"))]))
        .expect_err("30,000 matches against a template blessed on 1 must be refused");
    assert!(err.contains(TEMPLATE_COST_EXCEEDED), "{err}");
    assert!(
        err.contains("g = \"common\""),
        "the error must name the value: {err}"
    );
    assert!(err.contains("by_group"), "and the template: {err}");
    assert!(
        err.contains(&MIN_WORK_CEILING.to_string()),
        "and the ceiling: {err}"
    );
}

#[test]
fn a_value_within_the_factor_is_not_refused() {
    // The tier template scans the label whatever the value, so every tier does
    // about the same work as the sample: well inside the factor. A ceiling that
    // refused here would refuse ordinary use.
    let s = skewed();
    let catalog = build_catalog(&s, &specs(), &[]).unwrap();
    let e = entry(&catalog.entries, "by_tier");
    for t in [0, 1, 2] {
        let batch = run_template(&s, e, &values(&[("t", json!(t))]))
            .unwrap_or_else(|err| panic!("tier {t} refused: {err}"));
        assert_eq!(batch.records.len(), 1);
    }
    assert!(WORK_CEILING_FACTOR >= 2);
}

#[test]
fn the_declared_contract_is_enforced_before_anything_runs() {
    let s = skewed();
    let catalog = build_catalog(&s, &specs(), &[]).unwrap();
    let tier = entry(&catalog.entries, "by_tier");

    // Wrong type: the engine would answer "1" against an integer with 0 rows.
    let err = run_template(&s, tier, &values(&[("t", json!("1"))])).unwrap_err();
    assert!(err.contains("declared type"), "{err}");

    // Outside the enum.
    let err = run_template(&s, tier, &values(&[("t", json!(7))])).unwrap_err();
    assert!(err.contains("not one of the declared values"), "{err}");

    // A parameter the template does not have.
    let err = run_template(&s, tier, &values(&[("limit", json!(5))])).unwrap_err();
    assert!(err.contains("no parameter named \"limit\""), "{err}");
}

#[test]
fn an_entry_with_no_recorded_work_is_refused_not_run_unbounded() {
    let s = skewed();
    let catalog = build_catalog(&s, &specs(), &[]).unwrap();
    let mut old = entry(&catalog.entries, "by_group").clone();
    old.work = None; // as in a catalog built before #1156
    let err = run_template(&s, &old, &BTreeMap::new()).unwrap_err();
    assert!(err.contains("records no work"), "{err}");
    assert!(
        err.contains("catalog-build"),
        "and says how to fix it: {err}"
    );
}

#[test]
fn an_older_catalog_without_work_still_reads_and_verifies() {
    // `work` is additive: a catalog file written before it existed must load,
    // and `verify` -- which runs the samples, not caller values -- must not
    // need it.
    let s = skewed();
    let catalog = build_catalog(&s, &specs(), &[]).unwrap();
    let mut json = serde_json::to_value(&catalog).unwrap();
    for e in json["entries"].as_array_mut().unwrap() {
        e.as_object_mut().unwrap().remove("work");
    }
    let old: samyama::snapshot::verify::QueryCatalog = serde_json::from_value(json).unwrap();
    assert!(old.entries.iter().all(|e| e.work.is_none()));
    let report = samyama::snapshot::verify::verify(&s, &old).unwrap();
    assert!(report.is_ok());
}
