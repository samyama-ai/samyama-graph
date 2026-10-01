//! Materialized results for a read-only snapshot (#1158).
//!
//! Each test is one of the issue's constraints: epoch binding (1), read-only
//! only (2), the caps and their order (3), re-verification (4), and exact
//! match only (5). The end-to-end path through the CLI -- build, link, serve,
//! disclose, tamper -- is in `tests/server_binary.rs`.

use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::snapshot::format::{QueriesRef, SnapshotHeader};
use samyama::snapshot::results::{
    build, read_checked, reverify, sample_values, Limits, Materialized, ResultsFile,
};
use samyama::snapshot::verify::{build_catalog, queries_sha256, QueryCatalog, QuerySpec};
use serde_json::json;
use std::collections::BTreeMap;

fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    for i in 0..50i64 {
        let mut p = PropertyMap::new();
        p.insert("i".into(), PropertyValue::Integer(i));
        p.insert("g".into(), PropertyValue::String(format!("g{}", i % 5)));
        s.create_node_with_properties("default", vec![Label::new("T")], p);
    }
    s
}

fn specs() -> Vec<QuerySpec> {
    // The large row set first, so admitting in catalog order would differ
    // from admitting the smallest first.
    serde_json::from_value(json!([
        {"id": "all", "question": "List them.", "difficulty": "easy",
         "cypher": "MATCH (t:T) RETURN t.i AS i, t.g AS g ORDER BY i"},
        {"id": "by_group", "question": "Which are in {g}?", "difficulty": "easy",
         "cypher": "MATCH (t:T) WHERE t.g = $g RETURN t.i AS i ORDER BY i",
         "params": [{"name": "g", "type": "string", "sample": "g1"}]},
        {"id": "count", "question": "How many?", "difficulty": "easy",
         "cypher": "MATCH (t:T) RETURN count(t) AS n"}
    ]))
    .unwrap()
}

/// A catalog for `store`, its bytes' SHA-256, and a header that is read-only
/// and names that catalog.
fn published(store: &GraphStore) -> (QueryCatalog, String, SnapshotHeader) {
    let catalog = build_catalog(store, &specs(), &[]).unwrap();
    let sha = queries_sha256(serde_json::to_string(&catalog).unwrap().as_bytes());
    let mut bytes = Vec::new();
    samyama::snapshot::export_tenant(store, &mut bytes).unwrap();
    let mut header = samyama::snapshot::peek_header(&bytes[..]).unwrap();
    header.read_only = true;
    header.queries = Some(QueriesRef {
        file: "t.sgqueries".into(),
        sha256: sha.clone(),
    });
    (catalog, sha, header)
}

/// Generous caps, so a test about something else stores everything.
fn roomy() -> Limits {
    Limits {
        max_result_bytes: 1 << 20,
        max_total_bytes: None,
        max_total_pct: 100.0,
    }
}

/// `file` as published: its bytes, and the header linking them.
fn link(file: &ResultsFile, header: &mut SnapshotHeader) -> Vec<u8> {
    let bytes = serde_json::to_vec(file).unwrap();
    header.results = Some(QueriesRef {
        file: "t.sgresults".into(),
        sha256: queries_sha256(&bytes),
    });
    bytes
}

#[test]
fn a_snapshot_not_marked_read_only_gets_no_results() {
    let s = graph();
    let (catalog, sha, mut header) = published(&s);
    header.read_only = false;
    let err = build(&s, &header, 1 << 20, &catalog, &sha, roomy()).unwrap_err();
    assert!(err.contains("not marked read-only"), "{err}");

    // And results already built are not served for one either.
    header.read_only = true;
    let file = build(&s, &header, 1 << 20, &catalog, &sha, roomy()).unwrap();
    let bytes = link(&file, &mut header);
    header.read_only = false;
    let err = Materialized::load(&bytes, &header).err().unwrap();
    assert!(err.contains("not marked read-only"), "{err}");
}

#[test]
fn the_smallest_answers_are_admitted_first_within_both_caps() {
    let s = graph();
    let (catalog, sha, header) = published(&s);
    let all = build(&s, &header, 1 << 20, &catalog, &sha, roomy()).unwrap();
    assert_eq!(all.entries.len(), 3);
    assert!(all.skipped.is_empty());
    let size = |id: &str| all.entries.iter().find(|e| e.id == id).unwrap().bytes;
    let (count, group, list) = (size("count"), size("by_group"), size("all"));
    assert!(count < group && group < list, "{count} {group} {list}");
    assert_eq!(all.caps.used_bytes, count + group + list);

    // Per-result cap: the row set is over it, the others are not.
    let capped = Limits {
        max_result_bytes: list - 1,
        ..roomy()
    };
    let f = build(&s, &header, 1 << 20, &catalog, &sha, capped).unwrap();
    assert_eq!(f.skipped.len(), 1);
    assert_eq!(f.skipped[0].id, "all");
    assert!(
        f.skipped[0].reason.contains("per-result cap"),
        "{:?}",
        f.skipped
    );

    // Total cap, as a percentage of the snapshot: room for the scalar and
    // the small row set, so the large one goes, not whichever came first.
    let snapshot_bytes = 100 * (count + group);
    let pct = Limits {
        max_total_pct: 1.0,
        ..roomy()
    };
    let f = build(&s, &header, snapshot_bytes, &catalog, &sha, pct).unwrap();
    let kept: Vec<&str> = f.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(kept, ["by_group", "count"]);
    assert!(f.skipped[0].reason.contains("total cap"), "{:?}", f.skipped);
    assert_eq!(f.caps.max_total_bytes, count + group);

    // A total cap the large row set alone would fill: catalog order would
    // store it and nothing else; smallest first stores the other two.
    assert!(count + group <= list);
    let crowd = Limits {
        max_total_bytes: Some(list),
        ..roomy()
    };
    let f = build(&s, &header, 1 << 20, &catalog, &sha, crowd).unwrap();
    let kept: Vec<&str> = f.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(kept, ["by_group", "count"]);

    // An absolute total cap applies together with the percentage.
    let bytes_cap = Limits {
        max_total_bytes: Some(count),
        ..roomy()
    };
    let f = build(&s, &header, 1 << 20, &catalog, &sha, bytes_cap).unwrap();
    assert_eq!(f.entries.len(), 1);
    assert_eq!(f.entries[0].id, "count");
}

#[test]
fn a_catalog_that_disagrees_with_the_snapshot_builds_nothing() {
    let s = graph();
    let (_, sha, header) = published(&s);
    // The catalog of a different graph: the same questions, other answers.
    let mut other = graph();
    other.create_node("T");
    let wrong = build_catalog(&other, &specs(), &[]).unwrap();
    let err = build(&s, &header, 1 << 20, &wrong, &sha, roomy()).unwrap_err();
    assert!(err.contains("the catalog records"), "{err}");
}

#[test]
fn only_the_exact_query_with_its_sample_values_is_answered() {
    let s = graph();
    let (catalog, sha, mut header) = published(&s);
    let file = build(&s, &header, 1 << 20, &catalog, &sha, roomy()).unwrap();
    let bytes = link(&file, &mut header);
    let m = Materialized::load(&bytes, &header).unwrap().bind(&s);
    assert_eq!(m.len(), 3);

    let entry = catalog.entries.iter().find(|e| e.id == "by_group").unwrap();
    let samples = sample_values(&entry.params);
    let hit = m
        .lookup(&s, &entry.cypher, &samples)
        .expect("the samples are stored");
    assert_eq!(hit.rows.len(), 10);

    let other: BTreeMap<_, _> = [("g".to_string(), json!("g2"))].into();
    assert!(
        m.lookup(&s, &entry.cypher, &other).is_none(),
        "another value"
    );
    let spaced = format!("{} ", entry.cypher);
    assert!(
        m.lookup(&s, &spaced, &samples).is_none(),
        "not byte-identical"
    );
    let extra: BTreeMap<_, _> = [
        ("g".to_string(), json!("g1")),
        ("limit".to_string(), json!(3)),
    ]
    .into();
    assert!(
        m.lookup(&s, &entry.cypher, &extra).is_none(),
        "an extra parameter"
    );
}

#[test]
fn the_first_write_drops_every_stored_answer_for_good() {
    let mut s = graph();
    let (catalog, sha, mut header) = published(&s);
    let file = build(&s, &header, 1 << 20, &catalog, &sha, roomy()).unwrap();
    let bytes = link(&file, &mut header);
    let m = Materialized::load(&bytes, &header).unwrap().bind(&s);
    let count = &catalog
        .entries
        .iter()
        .find(|e| e.id == "count")
        .unwrap()
        .cypher;
    assert!(m.lookup(&s, count, &BTreeMap::new()).is_some());
    assert!(!m.is_dropped());

    s.create_node("T");
    assert!(
        m.lookup(&s, count, &BTreeMap::new()).is_none(),
        "50 is no longer the answer, and a stored 50 must not be served"
    );
    assert!(m.is_dropped());
}

#[test]
fn a_file_that_is_not_the_one_published_is_refused() {
    let s = graph();
    let (catalog, sha, mut header) = published(&s);
    let file = build(&s, &header, 1 << 20, &catalog, &sha, roomy()).unwrap();
    let bytes = link(&file, &mut header);
    assert!(read_checked(&bytes, &header).is_ok());

    // Bytes that are not the ones the header names.
    let mut other = bytes.clone();
    other.push(b' ');
    let err = read_checked(&other, &header).unwrap_err();
    assert!(
        err.contains("not the results the snapshot was published with"),
        "{err}"
    );

    // Answers to a different catalog.
    let mut h2 = samyama::snapshot::peek_header(
        &{
            let mut b = Vec::new();
            samyama::snapshot::export_tenant(&s, &mut b).unwrap();
            b
        }[..],
    )
    .unwrap();
    h2.read_only = true;
    h2.results = header.results.clone();
    h2.queries = Some(QueriesRef {
        file: "t.sgqueries".into(),
        sha256: "0".repeat(64),
    });
    let err = read_checked(&bytes, &h2).unwrap_err();
    assert!(err.contains("answers the catalog"), "{err}");

    // A stored row edited after the build, with the link updated to match:
    // the per-result digest still catches it.
    let mut edited = file.clone();
    edited.entries[0].rows[0].insert("n".into(), json!(999));
    let mut h3 = samyama::snapshot::peek_header(
        &{
            let mut b = Vec::new();
            samyama::snapshot::export_tenant(&s, &mut b).unwrap();
            b
        }[..],
    )
    .unwrap();
    h3.read_only = true;
    h3.queries = header.queries.clone();
    let edited_bytes = link(&edited, &mut h3);
    let err = read_checked(&edited_bytes, &h3).unwrap_err();
    assert!(err.contains("edited after it was built"), "{err}");
}

#[test]
fn reverify_reports_a_stored_answer_that_execution_does_not_give() {
    let s = graph();
    let (catalog, sha, header) = published(&s);
    let file = build(&s, &header, 1 << 20, &catalog, &sha, roomy()).unwrap();
    assert!(
        reverify(&s, &file).is_empty(),
        "the build's own answers reproduce"
    );

    // The same file against a graph that has moved on.
    let mut moved = graph();
    moved.create_node("T");
    let problems = reverify(&moved, &file);
    assert!(
        problems.iter().any(|p| p.starts_with("count:")),
        "{problems:?}"
    );

    // A stored row changed, as a forged file would be.
    let mut forged = file.clone();
    let i = forged.entries.iter().position(|e| e.id == "count").unwrap();
    forged.entries[i].rows[0].insert("n".into(), json!(51));
    let problems = reverify(&s, &forged);
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(
        problems[0].contains("disagrees with execution"),
        "{problems:?}"
    );
}
