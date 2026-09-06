//! `LOAD CSV` (LANG-09).
//!
//! The clause is off unless an import directory is configured, so most of these set
//! one first. That is not test scaffolding: `LOAD CSV FROM 'file:///etc/passwd'` on a
//! server reachable over the network is an arbitrary local file read, so the gate is
//! the feature's first property rather than an option on it.

use samyama::graph::GraphStore;
use samyama::query::csv_source::set_import_root;
use samyama::query::QueryEngine;
use std::io::Write;

const T: &str = "default";

fn write_csv(dir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    write!(std::fs::File::create(&p).unwrap(), "{body}").unwrap();
    p
}

/// One test, because the import root is process-global and these would otherwise
/// race each other into whichever directory ran last.
#[test]
fn load_csv_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    write_csv(
        dir.path(),
        "people.csv",
        "name,city\nada,London\n\"Doe, Jane\",Pune\n",
    );
    write_csv(dir.path(), "semi.csv", "name;city\nalan;Wilmslow\n");
    let engine = QueryEngine::new();

    // ---- the gate ----
    set_import_root(None).unwrap();
    let mut store = GraphStore::new();
    let err = engine
        .execute_mut(
            "LOAD CSV WITH HEADERS FROM 'people.csv' AS row CREATE (:P {n: row.name})",
            &mut store,
            T,
        )
        .expect_err("LOAD CSV ran with no import directory configured");
    assert!(
        err.to_string().contains("no import directory"),
        "refused for the wrong reason: {err}"
    );
    assert_eq!(store.node_count(), 0);

    set_import_root(Some(dir.path())).unwrap();

    // ---- WITH HEADERS binds a map, and the CSV is really parsed ----
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "LOAD CSV WITH HEADERS FROM 'people.csv' AS row CREATE (:P {n: row.name, c: row.city})",
            &mut store,
            T,
        )
        .unwrap();
    assert_eq!(store.node_count(), 2);
    let names: std::collections::BTreeSet<String> = store
        .get_nodes_by_label(&"P".into())
        .iter()
        .filter_map(|n| match n.properties.get("n") {
            Some(samyama::graph::PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(
        names.contains("Doe, Jane"),
        "the quoted comma was split into two fields: {names:?}"
    );
    assert!(names.contains("ada"), "{names:?}");

    // ---- without headers the row is a list ----
    let mut store = GraphStore::new();
    let rows = engine
        .execute("LOAD CSV FROM 'people.csv' AS row RETURN row", &store)
        .unwrap();
    assert_eq!(
        rows.records.len(),
        3,
        "without WITH HEADERS the header line is data, so all three lines are rows"
    );
    let _ = &mut store;

    // ---- FIELDTERMINATOR ----
    let rows = engine
        .execute(
            "LOAD CSV WITH HEADERS FROM 'semi.csv' AS row FIELDTERMINATOR ';' RETURN row",
            &GraphStore::new(),
        )
        .unwrap();
    assert_eq!(rows.records.len(), 1);

    // ---- escaping the import directory ----
    let outside = tempfile::tempdir().unwrap();
    write_csv(outside.path(), "secret.csv", "a\n1\n");
    let err = engine
        .execute(
            &format!(
                "LOAD CSV FROM 'file://{}' AS row RETURN row",
                outside.path().join("secret.csv").display()
            ),
            &GraphStore::new(),
        )
        .expect_err("read a file outside the import directory");
    assert!(err.to_string().contains("outside the import directory"), "{err}");

    // ---- http is refused rather than fetched ----
    let err = engine
        .execute(
            "LOAD CSV FROM 'https://example.com/x.csv' AS row RETURN row",
            &GraphStore::new(),
        )
        .expect_err("fetched an http source");
    assert!(err.to_string().contains("cannot read 'https' sources"), "{err}");

    // ---- fields arrive as strings, not guessed types ----
    write_csv(dir.path(), "zeros.csv", "code\n007\n");
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "LOAD CSV WITH HEADERS FROM 'zeros.csv' AS row CREATE (:Z {code: row.code})",
            &mut store,
            T,
        )
        .unwrap();
    let z = store.get_nodes_by_label(&"Z".into())[0].properties.get("code").cloned();
    assert_eq!(
        z,
        Some(samyama::graph::PropertyValue::String("007".into())),
        "a field was type-guessed, which loses the leading zeros"
    );

    set_import_root(None).unwrap();
}
