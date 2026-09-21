//! A Neo4j `apoc.export.json` dump imports into a queryable graph (INT-02).
//!
//! The tests query the imported store through Cypher rather than reading the
//! report back, because the report is written by the importer and would agree
//! with itself. What a migrating user cares about is that their queries answer
//! the same way, so that is what is asked.

use samyama::graph::GraphStore;
use samyama::migrate::neo4j_json::{import_str, ImportReport};
use samyama::query::QueryEngine;

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benchmarks/migrate")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

fn import(name: &str) -> (GraphStore, ImportReport) {
    let mut store = GraphStore::new();
    let report = import_str(&fixture(name), &mut store, "default").expect("import must succeed");
    (store, report)
}

fn count(store: &GraphStore, query: &str) -> i64 {
    let batch = QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let first = {
        let r = batch
            .records
            .first()
            .unwrap_or_else(|| panic!("{query}: no rows"));
        r.values().next().cloned()
    };
    match first {
        Some(samyama::query::executor::record::Value::Property(
            samyama::graph::PropertyValue::Integer(n),
        )) => n,
        other => panic!("{query}: expected an integer, got {other:?}"),
    }
}

#[test]
fn the_imported_graph_answers_the_queries_the_source_would() {
    let (store, report) = import("neo4j-apoc-export.jsonl");

    assert_eq!(report.nodes_created, 4);
    assert_eq!(report.edges_created, 3);
    assert_eq!(count(&store, "MATCH (n) RETURN count(n)"), 4);
    assert_eq!(count(&store, "MATCH ()-[r]->() RETURN count(r)"), 3);
    assert_eq!(count(&store, "MATCH (n:Person) RETURN count(n)"), 2);
    // A second label on the same node, not a second node.
    assert_eq!(count(&store, "MATCH (n:Employee) RETURN count(n)"), 1);
    assert_eq!(
        count(
            &store,
            "MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*)"
        ),
        1
    );
    assert_eq!(
        count(
            &store,
            "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(q) RETURN count(q)"
        ),
        1,
        "properties must be readable by the same predicate the source used"
    );
}

#[test]
fn a_relationship_written_before_its_endpoints_still_lands() {
    // The first line of the fixture is a relationship whose two nodes are
    // defined below it. APOC does not promise nodes come first, and an
    // importer that resolved endpoints as it read would have reported this
    // edge as dangling and produced a graph missing it — a loss the user would
    // only find by counting.
    let (store, report) = import("neo4j-apoc-export.jsonl");
    assert_eq!(report.dangling_edges, 0, "{:?}", report.missing_endpoints);
    assert_eq!(
        count(
            &store,
            "MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*)"
        ),
        1
    );
    assert!(
        report.lossless(),
        "clean export must import losslessly: {report:?}"
    );
}

#[test]
fn a_node_with_no_labels_gets_none_invented() {
    // Neo4j allows an unlabelled node. Giving it a placeholder label would make
    // the imported graph answer a query the source answers differently — the
    // mistake the RDF round trip made with `Resource`.
    let (store, _) = import("neo4j-apoc-export.jsonl");
    assert_eq!(count(&store, "MATCH (n) RETURN count(n)"), 4);
    assert_eq!(
        count(
            &store,
            "MATCH (n) WHERE size(labels(n)) = 0 RETURN count(n)"
        ),
        1,
        "the unlabelled node must still be unlabelled"
    );
}

#[test]
fn property_types_survive_as_the_types_the_file_carried() {
    let (store, report) = import("neo4j-apoc-export.jsonl");

    assert_eq!(
        count(&store, "MATCH (n:Person) WHERE n.age > 40 RETURN count(n)"),
        1,
        "an integer must arrive as an integer, not a string"
    );
    assert_eq!(
        count(
            &store,
            "MATCH (n:Person) WHERE n.active = true RETURN count(n)"
        ),
        1
    );
    assert_eq!(
        count(
            &store,
            "MATCH (n:Person) WHERE n.score > 91.0 RETURN count(n)"
        ),
        1
    );
    assert_eq!(
        count(
            &store,
            "MATCH (n:Person) WHERE size(n.tags) = 2 RETURN count(n)"
        ),
        1,
        "an array must arrive as an array"
    );
    assert_eq!(
        count(
            &store,
            "MATCH ()-[r:KNOWS]->() WHERE r.since = 2019 RETURN count(r)"
        ),
        1,
        "relationship properties must arrive too"
    );

    // The advisory counts. `joined` and `shift` look temporal; `version`
    // ("1.8.0") and the integer `founded` must not.
    assert_eq!(
        report.values_that_look_temporal, 2,
        "exactly the two ISO-shaped strings, not the version string"
    );
    assert_eq!(report.values_that_look_spatial, 1, "the one `crs` map");
}

#[test]
fn a_string_that_looks_like_a_date_is_still_a_string() {
    // The importer counts convertible-looking values; it must not convert them.
    // A silent conversion turns a version number or an identifier into a date,
    // and the damage is invisible until a comparison behaves oddly.
    let (store, _) = import("neo4j-apoc-export.jsonl");
    assert_eq!(
        count(
            &store,
            "MATCH (n:Person) WHERE n.joined = '2019-06-01T12:00:00Z' RETURN count(n)"
        ),
        1,
        "the value must still compare equal to the string the file held"
    );
    assert_eq!(
        count(
            &store,
            "MATCH (n:Person) WHERE n.version = '1.8.0' RETURN count(n)"
        ),
        1
    );
}

#[test]
fn a_partial_export_names_what_is_missing_instead_of_dropping_it() {
    // `apoc.export.json.query` over a subgraph exports relationships whose far
    // end was not selected. Importing that silently produces a graph with
    // fewer edges than the file described, and nothing says so.
    let (store, report) = import("neo4j-apoc-partial.jsonl");

    assert_eq!(report.nodes_created, 1);
    assert_eq!(report.edges_created, 0);
    assert_eq!(report.dangling_edges, 2);
    assert_eq!(
        report.missing_endpoints,
        vec!["98".to_string(), "99".to_string()],
        "the ids must be named: a count does not say which part of the export was short"
    );
    assert_eq!(report.null_properties_dropped, 1, "`nickname: null`");
    assert_eq!(
        report.unknown_record_types,
        vec!["schema".to_string()],
        "an APOC schema record is not graph data and must be reported, not ignored"
    );
    assert!(!report.lossless());

    assert_eq!(count(&store, "MATCH ()-[r]->() RETURN count(r)"), 0);
    assert_eq!(
        count(
            &store,
            "MATCH (n:Person) WHERE n.nickname IS NULL RETURN count(n)"
        ),
        1,
        "a dropped null must leave the property absent, not stored as a null"
    );
}

#[test]
fn the_single_object_json_format_imports_the_same_graph() {
    // `apoc.export.json.all(file, {jsonFormat:'JSON'})` writes one object with
    // two arrays instead of JSON Lines. Which one a user has depends on an
    // option they may not have chosen deliberately, so both must work — and
    // must produce the same graph, not merely both produce one.
    let lines = fixture("neo4j-apoc-export.jsonl");
    let mut nodes = Vec::new();
    let mut rels = Vec::new();
    for line in lines.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        if v["type"] == "node" {
            nodes.push(v);
        } else {
            rels.push(v);
        }
    }
    let object = serde_json::json!({"nodes": nodes, "rels": rels}).to_string();

    let mut store = GraphStore::new();
    let report = import_str(&object, &mut store, "default").unwrap();

    let (line_store, line_report) = import("neo4j-apoc-export.jsonl");
    assert_eq!(report.nodes_created, line_report.nodes_created);
    assert_eq!(report.edges_created, line_report.edges_created);
    assert_eq!(report.node_properties_set, line_report.node_properties_set);
    assert_eq!(
        count(&store, "MATCH (n:Person)-[:KNOWS]->() RETURN count(*)"),
        count(&line_store, "MATCH (n:Person)-[:KNOWS]->() RETURN count(*)")
    );
}

#[test]
fn a_file_that_is_not_an_apoc_export_is_refused() {
    // Refusing beats importing nothing and reporting success: an empty graph
    // and a successful import are what a user sees either way, and only one of
    // them tells them they pointed at the wrong file.
    let mut store = GraphStore::new();
    let err = import_str("this is not json\nnor is this\n", &mut store, "default")
        .expect_err("a non-JSON file must be refused");
    let msg = format!("{err}");
    assert!(
        msg.contains("apoc.export.json"),
        "the error must say what was expected; got: {msg}"
    );
}

#[test]
fn an_empty_file_imports_an_empty_graph_without_error() {
    // Distinct from the case above: an empty export is a legitimate result of
    // exporting an empty database, and is not a malformed file.
    let mut store = GraphStore::new();
    let report = import_str("", &mut store, "default").unwrap();
    assert_eq!(report.nodes_created, 0);
    assert_eq!(report.records_read, 0);
    assert!(report.lossless());
}
