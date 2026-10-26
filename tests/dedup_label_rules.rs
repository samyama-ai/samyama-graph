use samyama::graph::GraphStore;
use samyama::snapshot::{export_tenant, import_tenant_with_dedup};
fn snapshot() -> Vec<u8> {
    let mut g = GraphStore::new();
    for (label, accession) in [
        ("Gene", "A"),
        ("Gene", "A"),
        ("Protein", "P1"),
        ("Protein", "P2"),
    ] {
        let n = g.create_node(label);
        g.get_node_mut(n).unwrap().set_property("symbol", "SHARED");
        g.get_node_mut(n)
            .unwrap()
            .set_property("accession", accession);
    }
    let mut bytes = Vec::new();
    export_tenant(&g, &mut bytes).unwrap();
    bytes
}
#[test]
fn label_scoping_merges_genes_without_collapsing_distinct_proteins() {
    let mut g = GraphStore::new();
    let stats = import_tenant_with_dedup(
        &mut g,
        &snapshot()[..],
        &["Gene.symbol", "Protein.accession"],
    )
    .unwrap();
    assert_eq!(g.node_count(), 3);
    assert_eq!(stats.merges_by_key, vec![("Gene.symbol".to_string(), 1)]);
}
#[test]
fn bare_keys_keep_the_original_all_labels_behavior() {
    let mut g = GraphStore::new();
    import_tenant_with_dedup(&mut g, &snapshot()[..], &["symbol"]).unwrap();
    assert_eq!(g.node_count(), 2);
}
#[test]
fn unknown_label_rules_fail_before_importing_rows() {
    let mut g = GraphStore::new();
    assert!(import_tenant_with_dedup(&mut g, &snapshot()[..], &["Typo.symbol"]).is_err());
    assert_eq!(g.node_count(), 0);
}
