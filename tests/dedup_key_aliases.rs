//! One identifier, several property names (#1948).
//!
//! Dedup matches on `(label, key, value)` with the key spelled the same on
//! both sides. UMLS writes `cui` and SIDER `umls_cui`; UniProt writes
//! `uniprot_id`, GO `accession` and ChEMBL `uniprot_accession`. Those never
//! merged, and nothing said so. A rule now names its aliases,
//! `cui=umls_cui`, and the import names every key that merged nothing.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::snapshot::format::{ImportStats, UnusedDedupKey};
use samyama::snapshot::{export_tenant, import_tenant_with_dedup};

/// One snapshot of `label` nodes, each given as `(property, value)` pairs.
fn snapshot(label: &str, rows: &[&[(&str, &str)]]) -> Vec<u8> {
    let mut src = GraphStore::new();
    for row in rows {
        let id = src.create_node(label);
        let node = src.get_node_mut(id).expect("node");
        for (key, value) in row.iter() {
            node.set_property(key.to_string(), PropertyValue::String(value.to_string()));
        }
    }
    let mut buf = Vec::new();
    export_tenant(&src, &mut buf).expect("export");
    buf
}

fn load(store: &mut GraphStore, snap: &[u8], keys: &[&str]) -> ImportStats {
    import_tenant_with_dedup(store, snap, keys).expect("import")
}

fn unused(key: &str, n: u64) -> UnusedDedupKey {
    UnusedDedupKey {
        key: key.to_string(),
        incoming_nodes_with_key: n,
    }
}

#[test]
fn an_alias_merges_the_same_value_under_another_name() {
    let mut store = GraphStore::new();
    let umls = snapshot("Concept", &[&[("cui", "C0006142")]]);
    let sider = snapshot("Concept", &[&[("umls_cui", " c0006142 ")]]);
    load(&mut store, &umls, &["cui=umls_cui"]);
    let stats = load(&mut store, &sider, &["cui=umls_cui"]);
    assert_eq!(stats.merged_count, 1);
    assert_eq!(stats.merges_by_key, vec![("cui=umls_cui".to_string(), 1)]);
    assert!(stats.dedup_keys_unused.is_empty());
    assert_eq!(store.node_count(), 1);
}

/// CONTROL: without the alias the two never meet, and the import says the
/// key was on no incoming node.
#[test]
fn without_the_alias_nothing_merges_and_the_key_is_named() {
    let mut store = GraphStore::new();
    load(
        &mut store,
        &snapshot("Concept", &[&[("cui", "C1")]]),
        &["cui"],
    );
    let stats = load(
        &mut store,
        &snapshot("Concept", &[&[("umls_cui", "C1")]]),
        &["cui"],
    );
    assert_eq!(stats.merged_count, 0);
    assert_eq!(store.node_count(), 2);
    assert_eq!(stats.dedup_keys_unused, vec![unused("cui", 0)]);
}

#[test]
fn every_alias_meets_every_other_within_a_label_scope() {
    let rule = "Protein.uniprot_id=accession|uniprot_accession";
    let mut store = GraphStore::new();
    for (key, value) in [
        ("uniprot_id", "P04637"),
        ("accession", "P04637"),
        ("uniprot_accession", "p04637"),
    ] {
        load(
            &mut store,
            &snapshot("Protein", &[&[(key, value)]]),
            &[rule],
        );
    }
    assert_eq!(store.node_count(), 1, "three spellings, one protein");
}

/// A key on no incoming node and a key whose values never matched are
/// reported apart: the first is a typo or an absent property, the second a
/// key with nothing to meet. The existing node carries no `hgvs`, so the
/// conflict check has nothing to refuse the `rs_id` merge over.
#[test]
fn keys_that_merged_nothing_are_named_with_their_cause() {
    let mut store = GraphStore::new();
    load(
        &mut store,
        &snapshot("Variant", &[&[("rs_id", "rs1")]]),
        &[],
    );
    let stats = load(
        &mut store,
        &snapshot(
            "Variant",
            &[
                &[("rs_id", "rs1"), ("hgvs", "b")],
                &[("rs_id", "rs2"), ("hgvs", "c")],
            ],
        ),
        &["rsid", "hgvs", "rs_id"],
    );
    assert_eq!(stats.merges_by_key, vec![("rs_id".to_string(), 1)]);
    assert_eq!(
        stats.dedup_keys_unused,
        vec![unused("rsid", 0), unused("hgvs", 2)]
    );
}

#[test]
fn an_empty_alias_is_refused() {
    for rule in ["cui=", "cui=a||b", "=umls_cui"] {
        let mut store = GraphStore::new();
        let err = import_tenant_with_dedup(
            &mut store,
            &snapshot("Concept", &[&[("cui", "C1")]])[..],
            &[rule],
        )
        .expect_err(rule)
        .to_string();
        assert!(err.contains("empty property"), "{rule}: {err}");
        assert_eq!(store.node_count(), 0, "{rule}: nothing imported");
    }
}
