//! A dedup key that is not an identifier collapses distinct entities, and the run
//! reports the collapse as a success (#1808).
//!
//! `--dedup-keys` accepts any property key. Passing `symbol` and `gene_name`
//! alongside stable identifiers merged 475,090 UniProt nodes where identifier-only
//! keys merged 48,994: UniProt ships 618,093 proteins and 143,003 survived. The
//! import log showed more merges than the published baseline, no warning and no
//! error, because `ImportStats::merged_count` is a bare count and a larger one
//! reads as a better result.
//!
//! The issue frames the collapse as cross-source, and infers the mechanism from
//! orthologues and isoforms. These tests pin it instead, and locate it one level
//! earlier: a created node registers its own dedup values (`snapshot/mod.rs`,
//! the two `dedup_index.insert` sites on the create path), so two nodes sharing a
//! non-identifier value merge **within a single snapshot**, before any second
//! source is involved. That is what makes the defect reproducible on a fixture of
//! three nodes rather than on 26 million.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::snapshot::{export_tenant, import_tenant_with_dedup};

/// One snapshot holding `(accession, symbol)` pairs, each as a `:Protein`.
///
/// Shaped like the real case: a gene symbol shared by orthologues across species,
/// where the accession — the actual identifier — differs.
fn proteins(rows: &[(&str, &str)]) -> Vec<u8> {
    let mut src = GraphStore::new();
    for (accession, symbol) in rows {
        let id = src.create_node("Protein");
        let node = src.get_node_mut(id).expect("node");
        node.set_property(
            "accession".to_string(),
            PropertyValue::String((*accession).to_string()),
        );
        node.set_property("symbol".to_string(), PropertyValue::String((*symbol).to_string()));
    }
    let mut buf = Vec::new();
    export_tenant(&src, &mut buf).expect("export");
    buf
}

/// Three distinct proteins, one shared gene symbol. BRCA1 orthologues in human,
/// mouse and rat are three entities with three accessions and one symbol.
const ORTHOLOGUES: &[(&str, &str)] =
    &[("P38398", "BRCA1"), ("P48754", "BRCA1"), ("O54952", "BRCA1")];

/// CONTROL: the identifier key is an identifier, so nothing merges.
///
/// If this ever fails, the fixture is wrong and every other result on this page
/// is meaningless — three rows with three distinct accessions must stay three
/// nodes.
#[test]
fn an_identifier_key_merges_nothing_when_identifiers_differ() {
    let mut store = GraphStore::new();
    let stats = import_tenant_with_dedup(&mut store, &proteins(ORTHOLOGUES)[..], &["accession"])
        .expect("import");

    assert_eq!(stats.merged_count, 0, "distinct accessions must not merge");
    assert_eq!(store.node_count(), 3, "three proteins in, three out");
}

/// THE DEFECT, within one snapshot: a shared symbol collapses three entities into
/// one, and two accessions are lost.
///
/// This is the part the issue did not measure. No second source, no federation,
/// no 96-vCPU host: a created node registers its own `symbol` value, so the
/// second and third rows of the *same file* merge into the first.
#[test]
fn a_shared_non_identifier_value_collapses_entities_within_one_snapshot() {
    let mut store = GraphStore::new();
    let stats =
        import_tenant_with_dedup(&mut store, &proteins(ORTHOLOGUES)[..], &["symbol"]).expect("import");

    assert_eq!(
        store.node_count(),
        1,
        "three distinct proteins collapsed to {} on a shared gene symbol",
        store.node_count()
    );
    assert_eq!(stats.merged_count, 2, "two of three entities merged away");

    // The surviving node keeps the first accession; the other two are simply gone.
    // Property merge is additive and does not overwrite, so there is no record
    // anywhere in the store that P48754 and O54952 were ever distinct.
    let survivors: Vec<String> = store
        .all_nodes()
        .iter()
        .filter_map(|n| match n.get_property("accession") {
            Some(PropertyValue::String(s)) => Some(s.clone()),
            _ => store
                .node_columns
                .get_property(n.id.as_u64() as usize, "accession")
                .as_string()
                .map(|s| s.to_string()),
        })
        .collect();
    assert_eq!(survivors.len(), 1, "one survivor, got {survivors:?}");
    assert_eq!(
        survivors[0], "P38398",
        "the first row claims the symbol and the rest merge into it"
    );
}

/// An identifier key listed FIRST does not protect against a bad key listed after
/// it, because the loop takes the first key that *matches*, not the first key
/// given.
///
/// This is why arm 3 of the issue — 13 identifier keys plus `symbol` and
/// `gene_name` — over-merged despite the identifiers coming first. Orthologues
/// have different accessions, so the identifier never matches; the loop falls
/// through to `symbol`, which does.
#[test]
fn an_identifier_key_first_does_not_protect_against_a_bad_key_after_it() {
    let mut store = GraphStore::new();
    let stats = import_tenant_with_dedup(
        &mut store,
        &proteins(ORTHOLOGUES)[..],
        &["accession", "symbol"],
    )
    .expect("import");

    assert_eq!(
        store.node_count(),
        1,
        "listing the identifier first did not prevent the symbol collapse"
    );
    assert_eq!(stats.merged_count, 2);
}

/// The reported count cannot distinguish a good merge from a bad one, which is
/// the reporting half of the defect (#1808 fix 3).
///
/// Two runs, both reporting `merged_count = 2`. One merged two duplicate records
/// of a single entity — correct. The other destroyed two entities. Nothing in
/// `ImportStats` separates them, and the louder number reads as the better run.
#[test]
fn the_merge_count_reads_the_same_for_a_correct_merge_and_a_destructive_one() {
    // Correct: one entity, three records, same accession. Merging is right.
    let duplicates = &[("P38398", "BRCA1"), ("P38398", "BRCA1"), ("P38398", "BRCA1")];
    let mut good = GraphStore::new();
    let good_stats =
        import_tenant_with_dedup(&mut good, &proteins(duplicates)[..], &["accession"]).expect("import");

    // Destructive: three entities, one symbol.
    let mut bad = GraphStore::new();
    let bad_stats =
        import_tenant_with_dedup(&mut bad, &proteins(ORTHOLOGUES)[..], &["symbol"]).expect("import");

    assert_eq!(good_stats.merged_count, 2, "three records of one entity");
    assert_eq!(bad_stats.merged_count, 2, "three entities destroyed");
    assert_eq!(
        good_stats.merged_count, bad_stats.merged_count,
        "the statistic a caller reads is identical for both, which is the defect"
    );
    assert_eq!(good.node_count(), 1);
    assert_eq!(bad.node_count(), 1);
}
