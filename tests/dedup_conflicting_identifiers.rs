use samyama::graph::GraphStore;
use samyama::snapshot::{export_tenant, import_tenant_with_dedup_options};

fn run(existing: &[(&str, &str)], incoming: &[(&str, &str)], allow: bool) -> (usize, u64) {
    let mut target = GraphStore::new();
    let old = target.create_node("Protein");
    for (k, v) in existing {
        target.get_node_mut(old).unwrap().set_property(*k, *v);
    }
    let mut source = GraphStore::new();
    let new = source.create_node("Protein");
    for (k, v) in incoming {
        source.get_node_mut(new).unwrap().set_property(*k, *v);
    }
    let mut bytes = Vec::new();
    export_tenant(&source, &mut bytes).unwrap();
    let stats =
        import_tenant_with_dedup_options(&mut target, &bytes[..], &["accession", "symbol"], allow)
            .unwrap();
    (target.node_count(), stats.merges_refused_conflicting)
}

#[test]
fn a_matching_annotation_cannot_override_a_conflicting_identifier() {
    assert_eq!(
        run(
            &[("accession", "P04637"), ("symbol", "TP53")],
            &[("accession", "Q9UQ61"), ("symbol", "TP53")],
            false
        ),
        (2, 1)
    );
}
#[test]
fn any_present_declared_key_conflict_refuses_even_an_accession_match() {
    assert_eq!(
        run(
            &[("accession", "P04637"), ("symbol", "TP53")],
            &[("accession", "P04637"), ("symbol", "OTHER")],
            false
        ),
        (2, 1)
    );
}
#[test]
fn missing_identifiers_are_not_conflicts_and_normalized_strings_agree() {
    assert_eq!(
        run(
            &[("accession", "P04637"), ("symbol", "TP53")],
            &[("symbol", " tp53 ")],
            false
        ),
        (1, 0)
    );
}
#[test]
fn explicit_opt_out_preserves_the_legacy_merge() {
    assert_eq!(
        run(
            &[("accession", "P04637"), ("symbol", "TP53")],
            &[("accession", "Q9UQ61"), ("symbol", "TP53")],
            true
        ),
        (1, 0)
    );
}
