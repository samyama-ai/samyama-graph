//! Entity dedup must not depend on which label happens to iterate first (#317).
//!
//! The dedup index was keyed on `(first_label, key, value)`, where "first" came from
//! iterating a *set* of labels. For a dual-labelled node — the concrete case being ChEMBL
//! targets carrying both `:ChemblTarget` and `:Protein`, merged against UniProt `:Protein`
//! nodes on `accession` — the key could be either label. When the two sides disagreed the
//! lookup missed and the merge silently did not happen, leaving an unmerged duplicate with
//! no error.
//!
//! Label order in a snapshot is a plain JSON array, so these tests set it explicitly:
//! the *same* data must merge either way round.

use std::io::{Read, Write};

use samyama::graph::{GraphStore, PropertyValue};
use samyama::snapshot::{export_tenant, import_tenant_with_dedup};

/// Export a dual-labelled node, then force the label array into a given order.
fn snapshot_with_label_order(order: &str) -> Vec<u8> {
    let mut src = GraphStore::new();
    let id = src.create_node("ChemblTarget");
    let node = src.get_node_mut(id).unwrap();
    node.add_label("Protein");
    node.set_property(
        "accession".to_string(),
        PropertyValue::String("P12345".to_string()),
    );

    let mut buf = Vec::new();
    export_tenant(&src, &mut buf).expect("export");

    let mut plain = String::new();
    flate2::read::GzDecoder::new(&buf[..])
        .read_to_string(&mut plain)
        .expect("decode");
    let rewritten = plain
        .replace("[\"Protein\",\"ChemblTarget\"]", order)
        .replace("[\"ChemblTarget\",\"Protein\"]", order);
    assert!(rewritten.contains(order), "label order not applied");

    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(rewritten.as_bytes()).expect("encode");
    enc.finish().expect("finish")
}

/// A store holding the UniProt side: a single-labelled `:Protein` with the bridge key.
fn uniprot_side() -> GraphStore {
    let mut store = GraphStore::new();
    let id = store.create_node("Protein");
    let node = store.get_node_mut(id).unwrap();
    node.set_property(
        "accession".to_string(),
        PropertyValue::String("P12345".to_string()),
    );
    node.set_property(
        "src".to_string(),
        PropertyValue::String("uniprot".to_string()),
    );
    store
}

#[test]
fn a_dual_labelled_node_merges_whichever_label_comes_first() {
    for order in [
        "[\"ChemblTarget\",\"Protein\"]", // the shared label second — this is what failed
        "[\"Protein\",\"ChemblTarget\"]", // the shared label first — this happened to work
    ] {
        let snapshot = snapshot_with_label_order(order);
        let mut store = uniprot_side();

        let stats = import_tenant_with_dedup(&mut store, &snapshot[..], &["accession"])
            .expect("import");

        assert_eq!(
            stats.merged_count, 1,
            "labels {order}: should merge on the shared :Protein label"
        );
        assert_eq!(
            store.all_nodes().len(),
            1,
            "labels {order}: merged node should not be duplicated"
        );

        // the merged node keeps both labels
        let labels: Vec<String> = store.all_nodes()[0]
            .labels
            .iter()
            .map(|l| l.as_str().to_string())
            .collect();
        for expected in ["Protein", "ChemblTarget"] {
            assert!(labels.contains(&expected.to_string()), "labels {order}: {labels:?}");
        }
    }
}

#[test]
fn nodes_sharing_no_label_are_not_merged() {
    // Matching on *any* shared label must not become matching on the key alone: a :Gene and
    // a :Protein that happen to carry the same accession are different entities.
    let snapshot = snapshot_with_label_order("[\"ChemblTarget\",\"Protein\"]");
    let mut store = GraphStore::new();
    let id = store.create_node("Gene");
    store.get_node_mut(id).unwrap().set_property(
        "accession".to_string(),
        PropertyValue::String("P12345".to_string()),
    );

    let stats = import_tenant_with_dedup(&mut store, &snapshot[..], &["accession"]).expect("import");

    assert_eq!(stats.merged_count, 0, "no shared label, so no merge");
    assert_eq!(store.all_nodes().len(), 2);
}

#[test]
fn dedup_is_still_off_by_default() {
    // Importing without dedup keys must not merge anything, regardless of labels.
    let snapshot = snapshot_with_label_order("[\"ChemblTarget\",\"Protein\"]");
    let mut store = uniprot_side();

    let stats = import_tenant_with_dedup(&mut store, &snapshot[..], &[]).expect("import");

    assert_eq!(stats.merged_count, 0);
    assert_eq!(store.all_nodes().len(), 2);
}
