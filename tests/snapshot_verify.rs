//! `samyama verify`: a restored snapshot must reproduce its own answers (#1157).
//!
//! Each test reproduces one of the silent-partial-restore classes this is meant
//! to catch, rather than only checking the happy path. A checksum on the file
//! would pass every one of them.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::snapshot::verify::{
    build_catalog, canonical_hash, verify, CatalogEntry, FailureClass, QueryCatalog,
    CATALOG_FORMAT,
};
use samyama::query::QueryEngine;

fn seeded() -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..12i64 {
        let mut props = samyama::graph::PropertyMap::new();
        props.insert("id".into(), PropertyValue::Integer(i));
        props.insert("name".into(), PropertyValue::String(format!("n{i:02}")));
        ids.push(store.create_node_with_properties("default", vec![Label::new("Thing")], props));
    }
    for w in ids.windows(2) {
        let _ = store.create_edge(w[0], w[1], "LINKS");
    }
    store
}

fn queries() -> Vec<(String, String)> {
    vec![
        ("q_nodes".into(), "MATCH (t:Thing) RETURN count(t) AS n".into()),
        ("q_names".into(), "MATCH (t:Thing) RETURN t.name ORDER BY t.name".into()),
        ("q_edges".into(), "MATCH (:Thing)-[:LINKS]->(:Thing) RETURN count(*) AS n".into()),
        // Not an aggregate, on purpose. `count(*)` returns one row whether it
        // counted 11 edges or none, so an aggregate alone cannot tell a missing
        // edge type from a changed value.
        ("q_edge_pairs".into(),
         "MATCH (a:Thing)-[:LINKS]->(b:Thing) RETURN a.id, b.id ORDER BY a.id".into()),
        ("q_props".into(), "MATCH (t:Thing) WHERE t.id < 5 RETURN t.id, t.name".into()),
    ]
}

/// A snapshot that round-trips cleanly reproduces every answer.
#[test]
fn a_sound_round_trip_verifies() {
    let store = seeded();
    let catalog = build_catalog(&store, &queries(), &[]).expect("build");
    assert_eq!(catalog.format, CATALOG_FORMAT);
    assert_eq!(catalog.entries.len(), 5);

    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(&store, &mut buf).expect("export");
    let mut restored = GraphStore::new();
    samyama::snapshot::import_tenant(&mut restored, &buf[..]).expect("import");

    let report = verify(&restored, &catalog).expect("verify");
    assert!(
        report.is_ok(),
        "a clean round trip failed its own catalog: {:?}",
        report.failed().map(|r| (&r.id, r.failure, &r.detail)).collect::<Vec<_>>()
    );
}

/// The #449 shape: a catalog entry that returns nothing passes against any
/// graph, including one that lost everything, so it is refused at build time.
#[test]
fn a_zero_row_entry_is_refused_at_build_time() {
    let store = seeded();
    let mut q = queries();
    q.push(("q_none".into(), "MATCH (x:Absent) RETURN x".into()));

    let err = build_catalog(&store, &q, &[]).expect_err("a 0-row entry must be refused");
    assert!(err.contains("q_none"), "{err}");
    assert!(err.contains("detects nothing"), "{err}");

    // Unless it is deliberately an unanswerable item, which KG-08 asks for.
    let ok = build_catalog(&store, &q, &["q_none".to_string()]).expect("unanswerable allowed");
    assert!(ok.entries.iter().any(|e| e.id == "q_none" && e.unanswerable && e.rows == 0));
}

/// A run where every entry returns nothing fails whatever the expectations say.
#[test]
fn an_all_empty_run_fails_even_when_expectations_match() {
    let store = seeded();
    // A catalog of nothing but unanswerable entries: every expectation is 0 and
    // every result is 0, so entry-by-entry this run is perfect.
    let catalog = QueryCatalog {
        format: CATALOG_FORMAT.to_string(),
        generated_by: "test".into(),
        entries: vec![CatalogEntry {
            id: "q_none".into(),
            cypher: "MATCH (x:Absent) RETURN x".into(),
            rows: 0,
            hash: canonical_hash(&QueryEngine::new()
                .execute("MATCH (x:Absent) RETURN x", &store).unwrap()),
            unanswerable: true,
        }],
    };
    let report = verify(&store, &catalog).expect("verify");
    assert_eq!(report.failed().count(), 0, "no individual entry should fail");
    assert!(report.everything_empty);
    assert!(
        !report.is_ok(),
        "a run in which nothing returned a row reported success; that is what a \
         catalog run against an empty graph looks like"
    );
}

/// #303/#420 and #1130: the right rows with the properties gone.
#[test]
fn properties_lost_by_a_restore_are_caught_as_values_differing() {
    let store = seeded();
    let catalog = build_catalog(&store, &queries(), &[]).expect("build");

    // Same nodes and edges, different property values -- the shape a restore
    // takes when the row copy is empty and only some properties survive.
    let mut damaged = seeded();
    let targets: Vec<_> = damaged.get_nodes_by_label(&Label::new("Thing"))
        .iter().map(|n| n.id).collect();
    for id in targets {
        damaged.set_node_property("default", id, "name", PropertyValue::String("".into()))
            .expect("blank the name");
    }

    let report = verify(&damaged, &catalog).expect("verify");
    assert!(!report.is_ok(), "blanked properties passed verification");
    let classes: Vec<_> = report.failed().map(|r| r.failure.unwrap()).collect();
    assert!(
        classes.contains(&FailureClass::ValuesDiffer),
        "expected ValuesDiffer, got {classes:?}"
    );
    // Row counts are untouched, which is the point: a count-only check misses this.
    for r in report.failed() {
        assert_eq!(r.actual_rows, r.expected_rows, "{}: rows changed too", r.id);
    }
}

/// #332: a degraded subset published as if complete.
#[test]
fn a_missing_edge_type_is_caught_and_named() {
    let store = seeded();
    let catalog = build_catalog(&store, &queries(), &[]).expect("build");

    let mut damaged = GraphStore::new();
    for i in 0..12i64 {
        let mut props = samyama::graph::PropertyMap::new();
        props.insert("id".into(), PropertyValue::Integer(i));
        props.insert("name".into(), PropertyValue::String(format!("n{i:02}")));
        damaged.create_node_with_properties("default", vec![Label::new("Thing")], props);
    }
    // Nodes restored, edges lost.

    let report = verify(&damaged, &catalog).expect("verify");
    assert!(!report.is_ok(), "a graph with no edges passed a catalog that counts them");
    // The aggregate can only report that its single row changed value.
    let agg = report.results.iter().find(|r| r.id == "q_edges").expect("q_edges");
    assert_eq!(agg.failure, Some(FailureClass::ValuesDiffer),
               "count(*) returns one row either way, so this is a value change");

    // The non-aggregate is the one that names the shape: 11 rows became none.
    let pairs = report.results.iter().find(|r| r.id == "q_edge_pairs").expect("q_edge_pairs");
    assert_eq!(pairs.failure, Some(FailureClass::EmptyResult),
               "expected an empty result where the edges went missing");
    assert!(pairs.failure.unwrap().explain().contains("edge type"),
            "the failure should name a probable class: {}", pairs.failure.unwrap().explain());
}

/// Row order must not decide the verdict, or an unordered query reports a
/// restore defect on every run.
#[test]
fn row_order_does_not_change_the_hash() {
    let mut a = GraphStore::new();
    let mut b = GraphStore::new();
    for i in [3i64, 1, 2] {
        let mut p = samyama::graph::PropertyMap::new();
        p.insert("id".into(), PropertyValue::Integer(i));
        a.create_node_with_properties("default", vec![Label::new("T")], p);
    }
    for i in [1i64, 2, 3] {
        let mut p = samyama::graph::PropertyMap::new();
        p.insert("id".into(), PropertyValue::Integer(i));
        b.create_node_with_properties("default", vec![Label::new("T")], p);
    }
    let engine = QueryEngine::new();
    let q = "MATCH (t:T) RETURN t.id";
    assert_eq!(
        canonical_hash(&engine.execute(q, &a).unwrap()),
        canonical_hash(&engine.execute(q, &b).unwrap()),
        "the hash depends on insertion order, so an unordered query would fail \
         verification for a reason that is not a restore defect"
    );
}

/// A catalog whose format string is unknown is refused rather than guessed at.
#[test]
fn an_unknown_catalog_format_is_refused() {
    let store = seeded();
    let bad = QueryCatalog {
        format: "samyama.queries/99".into(),
        generated_by: "test".into(),
        entries: vec![],
    };
    let err = verify(&store, &bad).expect_err("unknown format must be refused");
    assert!(err.contains("refusing to guess"), "{err}");
}
