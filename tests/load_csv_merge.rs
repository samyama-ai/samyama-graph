//! `LOAD CSV ... AS row MERGE ...` runs the MERGE once per row (#1895).
//!
//! A statement whose only write is a single MERGE, with no MATCH, was planned
//! by a shortcut that builds the MERGE alone. A leading UNWIND was excluded
//! from it, and the CREATE-only shortcut also excluded LOAD CSV and LOAD
//! PARQUET, but the MERGE shortcut did not: the file was never opened, and the
//! MERGE failed with "Variable not found: row". That is the standard
//! idempotent bulk-load idiom, and `compatibility_report` accepted it because
//! it parses and plans.
//!
//! One test, because the import root is process-global.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::csv_source::set_import_root;
use samyama::query::QueryEngine;
use std::collections::BTreeSet;
use std::io::Write;

const T: &str = "default";

fn names(store: &GraphStore, label: &str) -> BTreeSet<String> {
    store
        .get_nodes_by_label(&label.into())
        .iter()
        .filter_map(|n| match store.node_property(n.id, "name") {
            Some(PropertyValue::String(s)) => Some(s),
            _ => None,
        })
        .collect()
}

#[test]
fn load_csv_feeds_a_merge() {
    let dir = tempfile::tempdir().unwrap();
    // Eve twice: a MERGE that ran per row creates her once.
    write!(
        std::fs::File::create(dir.path().join("people.csv")).unwrap(),
        "name\nEve\nAda\nEve\n"
    )
    .unwrap();
    set_import_root(Some(dir.path())).unwrap();
    let engine = QueryEngine::new();
    let expected: BTreeSet<String> = ["Ada", "Eve"].iter().map(|s| s.to_string()).collect();

    // The statement from the issue.
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "LOAD CSV WITH HEADERS FROM 'people.csv' AS row MERGE (:B {name: row.name})",
            &mut store,
            T,
        )
        .expect("LOAD CSV ... MERGE runs");
    assert_eq!(names(&store, "B"), expected);
    assert_eq!(store.node_count(), 2, "one node per distinct name, not per row");

    // Idempotent: running it again creates nothing.
    engine
        .execute_mut(
            "LOAD CSV WITH HEADERS FROM 'people.csv' AS row MERGE (:B {name: row.name})",
            &mut store,
            T,
        )
        .expect("the second run");
    assert_eq!(store.node_count(), 2, "a second load merges into the same nodes");

    // Through an intervening WITH, which failed the same way.
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "LOAD CSV WITH HEADERS FROM 'people.csv' AS row WITH row MERGE (:C {name: row.name})",
            &mut store,
            T,
        )
        .expect("LOAD CSV ... WITH row MERGE runs");
    assert_eq!(names(&store, "C"), expected);

    // With ON CREATE SET reading the row, the usual shape of the idiom.
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "LOAD CSV WITH HEADERS FROM 'people.csv' AS row \
             MERGE (n:D {name: row.name}) ON CREATE SET n.source = 'csv'",
            &mut store,
            T,
        )
        .expect("LOAD CSV ... MERGE ... ON CREATE SET runs");
    assert_eq!(names(&store, "D"), expected);
    for n in store.get_nodes_by_label(&"D".into()) {
        assert_eq!(
            store.node_property(n.id, "source"),
            Some(PropertyValue::String("csv".into()))
        );
    }

    // The forms that already worked still do.
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "LOAD CSV WITH HEADERS FROM 'people.csv' AS row CREATE (:A {name: row.name})",
            &mut store,
            T,
        )
        .expect("LOAD CSV ... CREATE runs");
    assert_eq!(store.node_count(), 3, "CREATE makes one node per row");
    engine
        .execute_mut("MERGE (:E {name: 'Zed'})", &mut store, T)
        .expect("a bare MERGE still takes its own path");
    assert_eq!(names(&store, "E"), ["Zed".to_string()].into_iter().collect());

    set_import_root(None).unwrap();
}
