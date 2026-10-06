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

// ---------------------------------------------------------------------------
// The fix: the statistics now carry the shape of the merge, so a caller that
// reads them can tell a duplicate pair from a collapse. #1808 fixes 2 and 3.
// ---------------------------------------------------------------------------

/// A correct merge and a destructive one now differ in the statistics, even
/// though `merged_count` is still 2 for both.
#[test]
fn the_merge_shape_separates_a_correct_merge_from_a_collapse() {
    let duplicates = &[("P38398", "BRCA1"), ("P38398", "BRCA1"), ("P38398", "BRCA1")];
    let mut good = GraphStore::new();
    let good_stats =
        import_tenant_with_dedup(&mut good, &proteins(duplicates)[..], &["accession"]).expect("import");

    let mut bad = GraphStore::new();
    let bad_stats =
        import_tenant_with_dedup(&mut bad, &proteins(ORTHOLOGUES)[..], &["symbol"]).expect("import");

    // Still indistinguishable on the old statistic.
    assert_eq!(good_stats.merged_count, bad_stats.merged_count);

    // Both collapse three records into one group, so group COUNT alone does not
    // separate them either -- the size does, together with which key merged.
    assert_eq!(good_stats.largest_merge_group, 3);
    assert_eq!(bad_stats.largest_merge_group, 3);

    // What does separate them: the key that did the merging is named.
    assert_eq!(good_stats.merges_by_key, vec![("accession".to_string(), 2)]);
    assert_eq!(bad_stats.merges_by_key, vec![("symbol".to_string(), 2)]);
}

/// The key named is the one that MATCHED, not the first one listed -- the
/// attribution #1808 needs to point at `symbol` when identifiers came first.
#[test]
fn the_attributed_key_is_the_one_that_matched_not_the_first_listed() {
    let mut store = GraphStore::new();
    let stats = import_tenant_with_dedup(
        &mut store,
        &proteins(ORTHOLOGUES)[..],
        &["accession", "symbol"],
    )
    .expect("import");

    assert_eq!(
        stats.merges_by_key,
        vec![("symbol".to_string(), 2)],
        "accession was listed first but never matched; symbol did the merging"
    );
    assert!(
        !stats.merges_by_key.iter().any(|(k, _)| k == "accession"),
        "a key that matched nothing must not be credited with merges"
    );
}

/// Group counting distinguishes many pairs from one large collapse, which is the
/// case `merged_count` cannot express at all.
#[test]
fn many_pairs_and_one_large_group_report_different_shapes() {
    // Three separate entities, each duplicated once: three groups of two.
    let pairs = &[
        ("P00001", "AAA"), ("P00001", "AAA"),
        ("P00002", "BBB"), ("P00002", "BBB"),
        ("P00003", "CCC"), ("P00003", "CCC"),
    ];
    let mut many = GraphStore::new();
    let many_stats =
        import_tenant_with_dedup(&mut many, &proteins(pairs)[..], &["accession"]).expect("import");

    // Six records, one shared symbol: one group of six.
    let one_group = &[
        ("P00001", "SHARED"), ("P00002", "SHARED"), ("P00003", "SHARED"),
        ("P00004", "SHARED"), ("P00005", "SHARED"), ("P00006", "SHARED"),
    ];
    let mut collapsed = GraphStore::new();
    let collapsed_stats =
        import_tenant_with_dedup(&mut collapsed, &proteins(one_group)[..], &["symbol"]).expect("import");

    // Identical merge counts, opposite shapes.
    assert_eq!(many_stats.merged_count, 3);
    assert_eq!(collapsed_stats.merged_count, 5);

    assert_eq!(many_stats.merge_groups, 3, "three duplicate pairs");
    assert_eq!(many_stats.largest_merge_group, 2, "a pair is the largest group");

    assert_eq!(collapsed_stats.merge_groups, 1, "one value swallowed everything");
    assert_eq!(collapsed_stats.largest_merge_group, 6, "six records into one node");

    assert_eq!(many.node_count(), 3, "three entities survive");
    assert_eq!(collapsed.node_count(), 1, "five entities destroyed");
}

/// An import that merges nothing reports a zeroed shape rather than a stale or
/// absent one.
#[test]
fn an_import_that_merges_nothing_reports_an_empty_shape() {
    let mut store = GraphStore::new();
    let stats = import_tenant_with_dedup(&mut store, &proteins(ORTHOLOGUES)[..], &["accession"])
        .expect("import");

    assert_eq!(stats.merged_count, 0);
    assert_eq!(stats.merge_groups, 0);
    assert_eq!(stats.largest_merge_group, 0, "no groups, so no largest");
    assert!(stats.merges_by_key.is_empty(), "no key merged anything");
}
