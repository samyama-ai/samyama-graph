//! A merge that drops a value says so (#1951).
//!
//! When two nodes merge on a dedup key, the node already in the graph keeps
//! each property it has and the incoming value is dropped. That policy stays.
//! What these tests pin is that the import counts what it dropped, per key,
//! with a few examples, instead of dropping it silently.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::snapshot::{export_tenant, import_tenant_with_dedup, PROPERTY_CONFLICT_SAMPLES};
use serde_json::json;

/// One snapshot of `:Gene` nodes, each given as `(property, value)` pairs.
fn genes(rows: &[&[(&str, PropertyValue)]]) -> Vec<u8> {
    let mut src = GraphStore::new();
    for row in rows {
        let id = src.create_node("Gene");
        let node = src.get_node_mut(id).expect("node");
        for (key, value) in row.iter() {
            node.set_property(key.to_string(), value.clone());
        }
    }
    let mut buf = Vec::new();
    export_tenant(&src, &mut buf).expect("export");
    buf
}

fn s(v: &str) -> PropertyValue {
    PropertyValue::String(v.to_string())
}

fn import(
    rows: &[&[(&str, PropertyValue)]],
) -> (GraphStore, samyama::snapshot::format::ImportStats) {
    let mut store = GraphStore::new();
    let stats = import_tenant_with_dedup(&mut store, &genes(rows)[..], &["id"]).expect("import");
    (store, stats)
}

#[test]
fn a_different_value_on_both_sides_is_counted_and_sampled() {
    let (store, stats) = import(&[
        &[("id", s("G1")), ("description", s("tumour suppressor"))],
        &[("id", s("G1")), ("description", s("DNA repair"))],
    ]);
    assert_eq!(stats.merged_count, 1);
    assert_eq!(
        stats.property_conflicts_by_key,
        vec![("description".to_string(), 1)]
    );
    assert_eq!(stats.property_conflict_samples.len(), 1);
    let c = &stats.property_conflict_samples[0];
    assert_eq!(c.key, "description");
    assert_eq!(c.kept, json!("tumour suppressor"));
    assert_eq!(c.discarded, json!("DNA repair"));
    assert_eq!(store.node_count(), 1, "the policy is unchanged: one node");
}

#[test]
fn the_same_value_up_to_case_whitespace_or_number_type_is_not_a_conflict() {
    let (_, stats) = import(&[
        &[
            ("id", s("G1")),
            ("symbol", s("TP53")),
            ("len", PropertyValue::Integer(3)),
        ],
        &[
            ("id", s("G1")),
            ("symbol", s(" tp53 ")),
            ("len", PropertyValue::Float(3.0)),
        ],
    ]);
    assert_eq!(stats.merged_count, 1);
    assert!(
        stats.property_conflicts_by_key.is_empty(),
        "{:?}",
        stats.property_conflicts_by_key
    );
    assert!(stats.property_conflict_samples.is_empty());
}

#[test]
fn a_property_the_existing_node_lacks_is_added_not_a_conflict() {
    let (store, stats) = import(&[
        &[("id", s("G1"))],
        &[("id", s("G1")), ("description", s("DNA repair"))],
    ]);
    assert_eq!(stats.merged_count, 1);
    assert!(stats.property_conflicts_by_key.is_empty());
    let id = store.all_nodes()[0].id;
    assert_eq!(
        store
            .node_columns
            .get_property(id.as_u64() as usize, "description"),
        s("DNA repair")
    );
}

#[test]
fn every_conflict_is_counted_but_only_a_few_are_sampled() {
    let rows: Vec<Vec<(&str, PropertyValue)>> = (0..PROPERTY_CONFLICT_SAMPLES + 3)
        .map(|i| vec![("id", s("G1")), ("note", s(&format!("note {i}")))])
        .collect();
    let rows: Vec<&[(&str, PropertyValue)]> = rows.iter().map(|r| &r[..]).collect();
    let (_, stats) = import(&rows);
    let conflicts = (PROPERTY_CONFLICT_SAMPLES + 2) as u64;
    assert_eq!(
        stats.property_conflicts_by_key,
        vec![("note".to_string(), conflicts)]
    );
    assert_eq!(
        stats.property_conflict_samples.len(),
        PROPERTY_CONFLICT_SAMPLES
    );
    assert!(stats
        .property_conflict_samples
        .iter()
        .all(|c| c.kept == json!("note 0")));
}
