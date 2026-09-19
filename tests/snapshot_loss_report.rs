//! An export says what it did not carry (INT-06).
//!
//! INT-06 asks for full-fidelity export **plus an explicit loss report**. The
//! `.sgsnap` format has always dropped things — index declarations, edge
//! timestamps — and a user had no way to learn that except by comparing the two
//! graphs afterwards and noticing. An export silent about its losses is the
//! shape of a backup somebody discovers is incomplete during a restore.
//!
//! The property these tests are really about: **a row appears only when there
//! was something to lose**. A standing list of everything the format could drop
//! is a disclaimer, and a check that asserts a disclaimer is present passes on
//! every graph including an empty one.

use samyama::graph::{GraphStore, Label};
use samyama::query::QueryEngine;
use samyama::snapshot::format::Dropped;
use samyama::snapshot::export_tenant;

fn export(store: &GraphStore) -> Vec<Dropped> {
    let mut buf = Vec::new();
    export_tenant(store, &mut buf).expect("export").dropped
}

fn run(store: &mut GraphStore, q: &str) {
    QueryEngine::new()
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn find<'a>(rows: &'a [Dropped], what: &str) -> Option<&'a Dropped> {
    rows.iter().find(|d| d.what == what)
}

#[test]
fn an_empty_graph_loses_nothing_and_says_nothing() {
    // The test that stops this becoming a disclaimer. If the report is a
    // constant list, this fails.
    assert!(export(&GraphStore::new()).is_empty());
}

#[test]
fn edges_carry_no_creation_time_and_the_report_says_so() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A)-[:R]->(:B)");
    let rows = export(&store);
    let d = find(&rows, "edge_creation_timestamps").expect("{rows:?}");
    assert_eq!(d.count, 1);
    assert!(d.detail.contains("Node timestamps do survive"), "{}", d.detail);
}

#[test]
fn a_graph_of_nodes_only_reports_no_edge_loss() {
    // The count is the edges there are, so with none there is nothing to say.
    let mut store = GraphStore::new();
    store.create_node_with_labels([Label::new("A")]);
    let rows = export(&store);
    assert!(find(&rows, "edge_creation_timestamps").is_none(), "{rows:?}");
}

#[test]
fn an_index_declaration_is_reported_with_the_statement_that_restores_it() {
    // Not just "an index was dropped": the row carries the DDL, because the
    // person reading it is trying to make the restored graph equivalent.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:Person {email: 'a@b.c'})");
    run(&mut store, "CREATE INDEX ON :Person(email)");
    let rows = export(&store);
    let d = find(&rows, "property_indexes").expect("{rows:?}");
    assert_eq!(d.count, 1);
    assert!(
        d.detail.contains("CREATE INDEX ON :Person(email)"),
        "the row does not say how to restore it: {}",
        d.detail
    );
}

#[test]
fn a_unique_constraint_is_reported_as_a_correctness_loss_not_a_speed_one() {
    // An index costs speed. A constraint that does not come back lets the
    // restored graph accept duplicates the original refused, which is a
    // different kind of problem and has to read as one.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:Person {email: 'a@b.c'})");
    run(
        &mut store,
        "CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE",
    );
    let rows = export(&store);
    let d = find(&rows, "unique_constraints").expect("{rows:?}");
    assert_eq!(d.count, 1);
    assert!(
        d.detail.contains("Uniqueness is not enforced"),
        "{}",
        d.detail
    );
}

#[test]
fn a_vector_index_declaration_is_reported_and_the_vectors_are_not() {
    // The distinction matters: the embeddings are node properties and survive,
    // so "the vector index was dropped" would read as data loss when it is a
    // declaration loss.
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE VECTOR INDEX vidx FOR (n:Doc) ON (n.embedding) OPTIONS {dimensions: 3}",
    );
    let rows = export(&store);
    let d = find(&rows, "vector_index_declarations").expect("{rows:?}");
    assert_eq!(d.count, 1);
    assert!(d.detail.contains("survive"), "{}", d.detail);
}

#[test]
fn the_report_travels_inside_the_file() {
    // Whoever finds a snapshot later is usually not whoever wrote it, so the
    // losses have to be readable off the artifact and not only from the return
    // value of the call that made it.
    use flate2::read::GzDecoder;
    use std::io::{BufRead, BufReader};

    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A)-[:R]->(:B)");
    run(&mut store, "CREATE INDEX ON :A(name)");

    let mut buf = Vec::new();
    export_tenant(&store, &mut buf).expect("export");

    let mut lines = BufReader::new(GzDecoder::new(&buf[..])).lines();
    let header: serde_json::Value =
        serde_json::from_str(&lines.next().expect("header").expect("read")).expect("json");
    let dropped = header["dropped"].as_array().expect("dropped in the header");
    let kinds: Vec<&str> = dropped.iter().filter_map(|d| d["what"].as_str()).collect();
    assert!(kinds.contains(&"property_indexes"), "{kinds:?}");
    assert!(kinds.contains(&"edge_creation_timestamps"), "{kinds:?}");
}

#[test]
fn an_older_snapshot_without_the_field_still_loads() {
    // The field is additive and the format version does not move, so a file
    // written before this exists must still import. Asserted rather than
    // assumed, because "additive" is a claim about a deserializer.
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    let header = serde_json::json!({
        "format": "sgsnap", "version": 2, "tenant": "default",
        "node_count": 1, "edge_count": 0, "labels": ["A"], "edge_types": [],
        "created_at": "2026-01-01T00:00:00Z", "samyama_version": "1.8.0"
    });
    writeln!(gz, "{header}").unwrap();
    writeln!(
        gz,
        "{}",
        serde_json::json!({"t": "n", "id": 1, "labels": ["A"], "props": {}})
    )
    .unwrap();
    let bytes = gz.finish().unwrap();

    let mut store = GraphStore::new();
    samyama::snapshot::import_tenant(&mut store, &bytes[..]).expect("a v2 file with no losses key");
    assert_eq!(store.node_count(), 1);
}
