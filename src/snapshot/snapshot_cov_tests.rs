//! Coverage-focused tests for snapshot import and export: the encrypted entry
//! points, the header peek, the legacy v1 path, dedup merges of non-scalar
//! values, index-catalog conflicts and the failure rollback.

use super::*;
use crate::graph::types::EdgeType;
use crate::index::catalog::{IndexDefinition, RestoredIndexes};
use crate::vector::DistanceMetric;
use serde_json::json;

fn gz(lines: &[String]) -> Vec<u8> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    for l in lines {
        e.write_all(l.as_bytes()).unwrap();
        e.write_all(b"\n").unwrap();
    }
    e.finish().unwrap()
}

fn header(version: u32, labels: &[&str]) -> String {
    json!({
        "format": "sgsnap",
        "version": version,
        "tenant": "default",
        "node_count": 0,
        "edge_count": 0,
        "labels": labels,
        "edge_types": [],
        "created_at": "",
        "samyama_version": "test",
    })
    .to_string()
}

fn node(id: u64, labels: &[&str], props: serde_json::Value) -> String {
    json!({ "t": "n", "id": id, "labels": labels, "props": props }).to_string()
}

fn edge(id: u64, src: u64, tgt: u64, ty: &str, props: serde_json::Value) -> String {
    json!({ "t": "e", "id": id, "src": src, "tgt": tgt, "type": ty, "props": props }).to_string()
}

fn catalog_line(defs: &[IndexDefinition]) -> String {
    json!({ "t": "i", "definitions": serde_json::to_value(defs).unwrap() }).to_string()
}

fn two_person_store() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    let b = store.create_node("Person");
    store
        .set_node_property("default", a, "name", "Alice")
        .unwrap();
    store.create_edge(a, b, "KNOWS").unwrap();
    store
}

// ------------------------------------------------------------------
// Encryption entry points and header peeks
// ------------------------------------------------------------------

#[test]
fn an_encrypted_export_imports_only_with_its_key() {
    let key = [9u8; encryption::KEY_BYTES];
    let src = two_person_store();
    let mut sealed = Vec::new();
    let stats = export_tenant_encrypted(&src, &mut sealed, &key).unwrap();
    assert_eq!(stats.node_count, 2);
    assert!(encryption::looks_encrypted(&sealed));

    let mut without = GraphStore::new();
    let err = import_tenant_maybe_encrypted(&mut without, &sealed[..], None).unwrap_err();
    assert!(err.to_string().contains("encrypted and no key"), "{err}");
    assert_eq!(without.node_count(), 0);

    let mut with = GraphStore::new();
    let imported = import_tenant_maybe_encrypted(&mut with, &sealed[..], Some(&key)).unwrap();
    assert_eq!(imported.node_count, 2);
    assert_eq!(imported.edge_count, 1);
    assert_eq!(with.edge_count(), 1);

    let h = peek_header_maybe_encrypted(&sealed[..], Some(&key)).unwrap();
    assert_eq!((h.node_count, h.edge_count), (2, 1));
    let err = peek_header_maybe_encrypted(&sealed[..], None).unwrap_err();
    assert!(err.to_string().contains("no key was given"), "{err}");
}

#[test]
fn a_plain_snapshot_goes_through_the_maybe_encrypted_paths_unchanged() {
    let src = two_person_store();
    let mut plain = Vec::new();
    export_tenant(&src, &mut plain).unwrap();
    let key = [1u8; encryption::KEY_BYTES];
    let h = peek_header_maybe_encrypted(&plain[..], Some(&key)).unwrap();
    assert_eq!(h.format, "sgsnap");
    assert_eq!(h.node_count, 2);
    let mut dst = GraphStore::new();
    let stats = import_tenant_maybe_encrypted(&mut dst, &plain[..], None).unwrap();
    assert_eq!(stats.node_count, 2);
    let alice = dst.get_nodes_by_label(&Label::new("Person"));
    assert!(alice
        .iter()
        .any(|n| dst.node_property(n.id, "name") == Some(PropertyValue::String("Alice".into()))));
}

#[test]
fn peek_header_rejects_empty_and_foreign_files() {
    let err = peek_header(&gz(&[])[..]).unwrap_err();
    assert!(err.to_string().contains("missing header"), "{err}");
    let foreign = gz(&[json!({
        "format": "other",
        "version": 2,
        "tenant": "t",
        "node_count": 0,
        "edge_count": 0,
        "labels": [],
        "edge_types": [],
        "created_at": "",
        "samyama_version": "x",
    })
    .to_string()]);
    let err = peek_header(&foreign[..]).unwrap_err();
    assert!(
        err.to_string()
            .contains("expected \"sgsnap\", got \"other\""),
        "{err}"
    );
}

#[test]
fn an_unsupported_version_is_refused() {
    let mut store = GraphStore::new();
    let err = import_tenant(&mut store, &gz(&[header(3, &[])])[..]).unwrap_err();
    assert!(err.to_string().contains("expected 1 or 2, got 3"), "{err}");
}

// ------------------------------------------------------------------
// Export details
// ------------------------------------------------------------------

#[test]
fn export_prefers_the_row_value_over_the_column() {
    let mut store = GraphStore::new();
    let n = store.create_node("Doc");
    store.set_column_property(n, "k", PropertyValue::String("column".into()));
    store.get_node_mut(n).unwrap().set_property("k", "row");
    let mut buf = Vec::new();
    export_tenant(&store, &mut buf).unwrap();
    let mut dst = GraphStore::new();
    import_tenant(&mut dst, &buf[..]).unwrap();
    let id = dst.node_ids_by_label(&Label::new("Doc"), None)[0];
    assert_eq!(
        dst.node_property(id, "k"),
        Some(PropertyValue::String("row".into()))
    );
}

// ------------------------------------------------------------------
// Legacy v1 import
// ------------------------------------------------------------------

#[test]
fn a_v1_snapshot_imports_through_full_nodes_and_edges() {
    let lines = vec![
        header(1, &["Person"]),
        node(10, &["Person"], json!({ "name": "Ann", "age": 30 })),
        String::new(), // blank lines are skipped
        node(11, &["Person"], json!({ "name": "Bob" })),
        edge(1, 10, 11, "KNOWS", json!({})),
        edge(2, 11, 10, "KNOWS", json!({ "since": 2020 })),
        json!({ "t": "z", "what": "unknown record kinds are skipped" }).to_string(),
    ];
    let mut store = GraphStore::new();
    let stats = import_tenant(&mut store, &gz(&lines)[..]).unwrap();
    assert_eq!(stats.node_count, 2);
    assert_eq!(stats.edge_count, 2);
    assert_eq!(stats.labels, vec!["Person".to_string()]);
    assert_eq!(stats.edge_types, vec!["KNOWS".to_string()]);

    let ids = store.node_ids_by_label(&Label::new("Person"), None);
    let ann = *ids
        .iter()
        .find(|&&id| store.node_property(id, "name") == Some(PropertyValue::String("Ann".into())))
        .unwrap();
    // v1 writes the row map, not the column.
    assert_eq!(
        store.get_node(ann).unwrap().get_property("age"),
        Some(&PropertyValue::Integer(30))
    );
    let edges = store.get_edges_by_type(&EdgeType::new("KNOWS"));
    assert_eq!(edges.len(), 2);
    assert!(edges
        .iter()
        .any(|e| e.properties.get("since") == Some(&PropertyValue::Integer(2020))));
}

#[test]
fn v1_dedup_merges_on_row_values_and_skips_non_scalar_keys() {
    let lines = vec![
        header(1, &["Country"]),
        node(1, &["Country"], json!({ "code": "IN" })),
        node(2, &["Country"], json!({ "code": " in " })),
        node(3, &["Country"], json!({ "code": true })),
        node(4, &["Country"], json!({ "code": 7 })),
        node(5, &["Country"], json!({ "code": 7 })),
    ];
    let mut store = GraphStore::new();
    let stats = import_tenant_with_dedup(&mut store, &gz(&lines)[..], &["code"]).unwrap();
    assert_eq!(stats.merged_count, 2, "node 2 merges into 1, node 5 into 4");
    assert_eq!(store.label_node_count(&Label::new("Country")), 3);
}

// ------------------------------------------------------------------
// v2 dedup: merging non-scalar values into an existing node
// ------------------------------------------------------------------

#[test]
fn a_dedup_merge_adds_list_values_without_overwriting() {
    let mut store = GraphStore::new();
    let existing = store.create_node("Country");
    store
        .set_node_property("default", existing, "code", "IN")
        .unwrap();
    store
        .set_node_property("default", existing, "name", "India")
        .unwrap();

    let lines = vec![
        header(2, &["Country"]),
        node(
            1,
            &["Country"],
            json!({ "code": "in", "name": "Bharat", "tags": ["asia", "south"] }),
        ),
        // A non-scalar dedup value never matches and is not indexed.
        node(2, &["Country"], json!({ "code": ["x"] })),
    ];
    let stats = import_tenant_with_dedup(&mut store, &gz(&lines)[..], &["code"]).unwrap();
    assert_eq!(stats.merged_count, 1);
    assert_eq!(
        store.node_property(existing, "name"),
        Some(PropertyValue::String("India".into())),
        "existing scalar kept"
    );
    let tags = store
        .node_property(existing, "tags")
        .expect("list merged in");
    assert_eq!(
        tags,
        PropertyValue::Array(vec![
            PropertyValue::String("asia".into()),
            PropertyValue::String("south".into())
        ])
    );
    assert!(
        store
            .get_node(existing)
            .unwrap()
            .get_property("tags")
            .is_some(),
        "row copy too"
    );
    assert_eq!(store.label_node_count(&Label::new("Country")), 2);
}

// ------------------------------------------------------------------
// Failure rollback
// ------------------------------------------------------------------

#[test]
fn an_edge_to_an_unknown_node_fails_the_import_and_rolls_it_back() {
    for (src, tgt, what) in [(99, 1, "source node ID 99"), (1, 98, "target node ID 98")] {
        let lines = vec![
            header(2, &["N"]),
            node(1, &["N"], json!({ "k": 1 })),
            edge(1, src, tgt, "R", json!({})),
        ];
        let mut store = GraphStore::new();
        let err = import_tenant(&mut store, &gz(&lines)[..]).unwrap_err();
        assert!(err.to_string().contains(what), "{err}");
        assert_eq!(
            store.node_count(),
            0,
            "nothing from the failed import remains"
        );
    }
}

// ------------------------------------------------------------------
// Hierarchy declarations
// ------------------------------------------------------------------

#[test]
fn hierarchy_declarations_are_rebuilt_and_a_cyclic_one_is_skipped() {
    let lines = vec![
        header(2, &["T"]),
        node(1, &["T"], json!({ "w": 1 })),
        node(2, &["T"], json!({ "w": 2 })),
        node(3, &["T"], json!({})),
        node(4, &["T"], json!({})),
        edge(1, 2, 1, "IS_A", json!({})),
        edge(2, 3, 4, "LOOP", json!({})),
        edge(3, 4, 3, "LOOP", json!({})),
        json!({ "t": "h", "name": "tax", "edge_types": ["IS_A"], "measure_property": "w" })
            .to_string(),
        json!({
            "t": "h", "name": "taxmax", "edge_types": ["IS_A"],
            "measure_label": "T", "measure_property": "w", "ops": ["max", "bogus"]
        })
        .to_string(),
        json!({ "t": "h", "name": "loop", "edge_types": ["LOOP"] }).to_string(),
    ];
    let mut store = GraphStore::new();
    let stats = import_tenant(&mut store, &gz(&lines)[..]).unwrap();
    assert_eq!(stats.hierarchy_count, 2);
    let names: Vec<String> = store
        .hierarchy_index
        .list()
        .into_iter()
        .map(|h| h.name)
        .collect();
    assert_eq!(names, vec!["tax".to_string(), "taxmax".to_string()]);
    let tax = store
        .hierarchy_index
        .list()
        .into_iter()
        .find(|h| h.name == "tax")
        .unwrap();
    assert_eq!(tax.ops, vec!["sum"], "no ops declared means sum");
    let taxmax = store
        .hierarchy_index
        .list()
        .into_iter()
        .find(|h| h.name == "taxmax")
        .unwrap();
    assert_eq!(taxmax.ops, vec!["max"], "unknown op names are dropped");
}

// ------------------------------------------------------------------
// Index catalog conflicts
// ------------------------------------------------------------------

fn target_with_indexes() -> GraphStore {
    let mut store = GraphStore::new();
    let n = store.create_node("A");
    store
        .set_node_property("default", n, "body", "hello")
        .unwrap();
    store.create_fulltext_index("ft", "A", "body");
    store
        .create_vector_index_named(
            Some("vi"),
            "A",
            "emb",
            4,
            DistanceMetric::Cosine,
            Default::default(),
        )
        .unwrap();
    store.clear_index_catalog_dirty();
    store
}

#[test]
fn conflicting_index_definitions_keep_the_targets_and_are_counted() {
    let mut store = target_with_indexes();
    let defs = vec![
        // Same name, different definition: the target's wins.
        IndexDefinition::FullText {
            name: "ft".into(),
            label: "B".into(),
            property: "title".into(),
        },
        // Identical to the target's: not a conflict, simply rebuilt.
        IndexDefinition::FullText {
            name: "ft".into(),
            label: "A".into(),
            property: "body".into(),
        },
        // Same (label, property) under another name and dimension.
        IndexDefinition::Vector {
            name: Some("other".into()),
            label: "A".into(),
            property: "emb".into(),
            dimensions: 8,
            metric: DistanceMetric::Cosine,
            quantization: Default::default(),
        },
        // Same name on a different (label, property).
        IndexDefinition::Vector {
            name: Some("vi".into()),
            label: "C".into(),
            property: "x".into(),
            dimensions: 4,
            metric: DistanceMetric::Cosine,
            quantization: Default::default(),
        },
        IndexDefinition::Property {
            label: "A".into(),
            property: "body".into(),
        },
        IndexDefinition::Property {
            label: "A".into(),
            property: "body".into(),
        },
    ];
    let lines = vec![
        header(2, &["A"]),
        node(1, &["A"], json!({ "body": "hello again" })),
        catalog_line(&defs),
    ];
    let stats = import_tenant(&mut store, &gz(&lines)[..]).unwrap();
    assert_eq!(stats.index_conflicts, 3);
    let restored = stats.indexes.expect("a catalog was present");
    assert_eq!(
        restored.property, 1,
        "a repeated definition is declared once"
    );
    assert_eq!(restored.fulltext, 1);
    assert_eq!(restored.vector, 0);
    assert!(
        store.index_catalog_is_dirty(),
        "imported definitions must be persisted"
    );
    assert_eq!(store.fulltext.search("ft", "hello", 10).unwrap().len(), 2);
    assert_eq!(
        store.resolve_vector_index("vi"),
        Some(("A".into(), "emb".into()))
    );
}

#[test]
fn a_catalog_of_only_conflicts_restores_nothing_and_leaves_the_dirty_flag() {
    let mut store = target_with_indexes();
    store.mark_index_catalog_changed();
    let defs = vec![IndexDefinition::FullText {
        name: "ft".into(),
        label: "Z".into(),
        property: "q".into(),
    }];
    let lines = vec![header(2, &[]), catalog_line(&defs), catalog_line(&[])];
    let stats = import_tenant(&mut store, &gz(&lines)[..]).unwrap();
    assert_eq!(stats.index_conflicts, 1);
    assert_eq!(stats.indexes, Some(RestoredIndexes::default()));
    assert!(
        store.index_catalog_is_dirty(),
        "an unpersisted DDL change is not cleared"
    );
    assert_eq!(stats.node_count, 0);
}

#[test]
fn conflicting_definition_rules() {
    let ft = |name: &str, label: &str| IndexDefinition::FullText {
        name: name.into(),
        label: label.into(),
        property: "p".into(),
    };
    let vec_def = |name: Option<&str>, label: &str| IndexDefinition::Vector {
        name: name.map(str::to_string),
        label: label.into(),
        property: "p".into(),
        dimensions: 2,
        metric: DistanceMetric::Cosine,
        quantization: Default::default(),
    };
    let existing = vec![ft("a", "L"), vec_def(None, "L")];
    assert!(
        conflicting_definition(&existing, &ft("a", "L")).is_none(),
        "identical"
    );
    assert_eq!(
        conflicting_definition(&existing, &ft("a", "M")),
        Some(&existing[0])
    );
    assert!(conflicting_definition(&existing, &ft("b", "M")).is_none());
    assert_eq!(
        conflicting_definition(&existing, &vec_def(Some("n"), "L")),
        Some(&existing[1])
    );
    assert!(
        conflicting_definition(&existing, &vec_def(None, "M")).is_none(),
        "two unnamed indexes on different keys"
    );
    let prop = IndexDefinition::Property {
        label: "L".into(),
        property: "p".into(),
    };
    assert!(conflicting_definition(&existing, &prop).is_none());
}

// ------------------------------------------------------------------
// JSON <-> property conversions
// ------------------------------------------------------------------

#[test]
fn tagged_objects_missing_their_payload_fall_back_to_plain_maps() {
    for tag in [
        "DateTime",
        "Date",
        "LocalTime",
        "Time",
        "LocalDateTime",
        "ZonedDateTime",
        "Vector",
        "Custom",
    ] {
        let v = json_to_property(&json!({ "__type": tag }));
        match v {
            PropertyValue::Map(m) => {
                assert_eq!(
                    m.get("__type"),
                    Some(&PropertyValue::String(tag.to_string()))
                )
            }
            other => panic!("{tag}: expected a plain map, got {other:?}"),
        }
    }
    // Duration defaults each missing component to zero rather than failing.
    assert_eq!(
        json_to_property(&json!({ "__type": "Duration", "days": 2 })),
        PropertyValue::Duration {
            months: 0,
            days: 2,
            seconds: 0,
            nanos: 0
        }
    );
    assert_eq!(
        json_to_property(&json!(u64::MAX)),
        PropertyValue::Float(u64::MAX as f64)
    );
    assert_eq!(json_to_property(&json!(null)), PropertyValue::Null);
}

// ------------------------------------------------------------------
// persist.rs
// ------------------------------------------------------------------

#[test]
fn a_snapshot_without_its_commit_marker_is_not_restored() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().to_string_lossy().to_string();
    let dir = tmp.path().join("snapshots");
    std::fs::create_dir_all(&dir).unwrap();
    let mut bytes = Vec::new();
    export_tenant(&two_person_store(), &mut bytes).unwrap();
    std::fs::write(dir.join("default.sgsnap"), &bytes).unwrap();
    let mut store = GraphStore::new();
    let restored = persist::restore_persisted_snapshots(&data_path, &mut store).unwrap();
    assert!(restored.is_none());
    assert_eq!(store.node_count(), 0);

    std::fs::write(dir.join("default.sgsnap.committed"), b"").unwrap();
    let restored = persist::restore_persisted_snapshots(&data_path, &mut store).unwrap();
    assert_eq!(restored.map(|s| s.node_count), Some(2));
}
