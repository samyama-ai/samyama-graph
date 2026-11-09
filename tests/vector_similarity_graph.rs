//! `vector.knnGraph` and `vector.similarityJoin` (#1979): turn a vector
//! index's neighbourhoods into rows, or into relationships a graph algorithm
//! can then run over.
//!
//! The fixture is two well-separated clusters of three, so the right answer
//! is known without an oracle: every node's two nearest are the other two in
//! its own cluster. Six nodes are searched exactly (an index of 128 or fewer
//! is), so nothing here depends on HNSW recall.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Record, RecordBatch, Value};
use samyama::query::parser::parse_query;
use samyama::query::QueryEngine;
use samyama::vector::DistanceMetric;

fn write(s: &mut GraphStore, q: &str) -> RecordBatch {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

fn write_error(s: &mut GraphStore, q: &str) -> String {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    match MutQueryExecutor::new(s, "default".into()).execute(&query) {
        Ok(b) => panic!("{q}: expected an error, got {:?}", b.records),
        Err(e) => e.to_string(),
    }
}

fn rows(s: &GraphStore, q: &str) -> Vec<Record> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"))
        .records
}

fn read_error(s: &GraphStore, q: &str) -> String {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    match QueryExecutor::new(s).execute(&query) {
        Ok(b) => panic!("{q}: expected an error, got {:?}", b.records),
        Err(e) => e.to_string(),
    }
}

fn string(r: &Record, col: &str) -> String {
    match r.get(col) {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        other => panic!("{col}: {other:?}"),
    }
}

fn float(r: &Record, col: &str) -> f64 {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Float(f))) => *f,
        other => panic!("{col}: {other:?}"),
    }
}

fn int(r: &Record, col: &str) -> i64 {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("{col}: {other:?}"),
    }
}

const VECTORS: &[(&str, [f64; 3])] = &[
    ("a1", [1.0, 0.0, 0.05]),
    ("a2", [0.98, 0.1, 0.0]),
    ("a3", [0.95, 0.0, 0.2]),
    ("b1", [0.0, 1.0, 0.05]),
    ("b2", [0.1, 0.97, 0.0]),
    ("b3", [0.0, 0.94, 0.2]),
];

fn create(s: &mut GraphStore, label: &str, suffix: &str) {
    for (name, v) in VECTORS {
        write(
            s,
            &format!(
                "CREATE (:{label} {{name: '{name}{suffix}', v: [{}, {}, {}]}})",
                v[0], v[1], v[2]
            ),
        );
    }
}

/// Six `Doc` nodes in two clusters of three, with a cosine index on `v`.
fn clusters() -> GraphStore {
    let mut s = GraphStore::new();
    create(&mut s, "Doc", "");
    s.create_vector_index("Doc", "v", 3, DistanceMetric::Cosine)
        .unwrap();
    s.rebuild_vector_index();
    s
}

fn cluster_of(name: &str) -> char {
    name.chars().next().unwrap()
}

const STREAM: &str = "CALL vector.knnGraph('Doc', 'v', {k: 2}) YIELD node1, node2, similarity \
                      RETURN node1.name AS a, node2.name AS b, similarity";

#[test]
fn knn_graph_streams_each_nodes_nearest_on_the_read_path() {
    let s = clusters();
    assert!(
        !QueryEngine::new().statement_is_write(STREAM).unwrap(),
        "a stream must route as a read"
    );
    let out = rows(&s, STREAM);
    assert_eq!(out.len(), 12, "two neighbours for each of six nodes");
    for r in &out {
        let (a, b) = (string(r, "a"), string(r, "b"));
        assert_ne!(a, b, "a node is not its own neighbour");
        assert_eq!(
            cluster_of(&a),
            cluster_of(&b),
            "{a} -> {b} crosses clusters"
        );
        let sim = float(r, "similarity");
        assert!((0.0..=1.0).contains(&sim), "cosine score in [0, 1]: {sim}");
    }
    assert_eq!(s.edge_count(), 0, "a stream creates nothing");
}

#[test]
fn similarity_is_the_same_score_the_scalar_function_gives() {
    let s = clusters();
    let out = rows(
        &s,
        "CALL vector.knnGraph('Doc', 'v', {k: 2}) YIELD node1, node2, similarity \
         RETURN similarity, vector.similarity.cosine(node1.v, node2.v) AS expected",
    );
    assert!(!out.is_empty());
    for r in &out {
        let (got, want) = (float(r, "similarity"), float(r, "expected"));
        assert!(
            (got - want).abs() < 1e-5,
            "similarity {got} vs vector.similarity.cosine {want}"
        );
    }
}

#[test]
fn rows_are_by_source_then_nearest_first() {
    let s = clusters();
    let out = rows(
        &s,
        "CALL vector.knnGraph('Doc', 'v', {k: 3}) YIELD node1, node2, similarity \
         RETURN id(node1) AS src, similarity",
    );
    for pair in out.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        let (sa, sb) = (int(a, "src"), int(b, "src"));
        assert!(sa <= sb, "sources out of order");
        if sa == sb {
            assert!(
                float(a, "similarity") >= float(b, "similarity"),
                "not nearest-first"
            );
        }
    }
}

#[test]
fn a_threshold_drops_the_pairs_below_it() {
    let s = clusters();
    let all = rows(
        &s,
        "CALL vector.knnGraph('Doc', 'v', {k: 5}) YIELD similarity RETURN similarity",
    );
    assert_eq!(all.len(), 30, "k = 5 reaches every other node");
    let cut = rows(
        &s,
        "CALL vector.knnGraph('Doc', 'v', {k: 5, threshold: 0.9}) YIELD node1, node2, similarity \
         RETURN node1.name AS a, node2.name AS b, similarity",
    );
    assert_eq!(cut.len(), 12, "only the within-cluster pairs clear 0.9");
    for r in &cut {
        assert!(float(r, "similarity") >= 0.9);
        assert_eq!(cluster_of(&string(r, "a")), cluster_of(&string(r, "b")));
    }
}

#[test]
fn write_creates_scored_relationships_and_a_rerun_refreshes_rather_than_duplicates() {
    let mut s = clusters();
    let q =
        "CALL vector.knnGraph('Doc', 'v', {k: 2, write: true}) YIELD node1 RETURN count(*) AS n";
    assert!(QueryEngine::new().statement_is_write(q).unwrap());
    write(&mut s, q);
    assert_eq!(s.edge_count(), 12);
    let edges = rows(
        &s,
        "MATCH (a:Doc)-[r:SIMILAR_TO]->(b:Doc) \
         RETURN a.name AS a, b.name AS b, r.score AS score, vector.similarity.cosine(a.v, b.v) AS expected",
    );
    assert_eq!(edges.len(), 12);
    for r in &edges {
        assert_eq!(cluster_of(&string(r, "a")), cluster_of(&string(r, "b")));
        assert!((float(r, "score") - float(r, "expected")).abs() < 1e-5);
    }

    write(&mut s, q);
    assert_eq!(
        s.edge_count(),
        12,
        "a second run must not duplicate the relationships"
    );
}

#[test]
fn rel_type_and_score_property_are_configurable() {
    let mut s = clusters();
    write(
        &mut s,
        "CALL vector.knnGraph('Doc', 'v', {k: 1, write: true, relType: 'NEAR', scoreProperty: 'sim'}) \
         YIELD node1 RETURN count(*)",
    );
    let out = rows(
        &s,
        "MATCH ()-[r:NEAR]->() RETURN count(r) AS n, min(r.sim) AS lo",
    );
    assert_eq!(int(&out[0], "n"), 6);
    assert!(float(&out[0], "lo") > 0.9);
    assert!(rows(&s, "MATCH ()-[r:SIMILAR_TO]->() RETURN r").is_empty());
}

#[test]
fn a_written_graph_is_a_graph_algorithms_can_run_on() {
    let mut s = clusters();
    write(
        &mut s,
        "CALL vector.knnGraph('Doc', 'v', {k: 2, write: true}) YIELD node1 RETURN count(*)",
    );
    let out = rows(
        &s,
        "CALL algo.wcc() YIELD node, componentId RETURN count(DISTINCT componentId) AS components",
    );
    assert_eq!(int(&out[0], "components"), 2, "one component per cluster");
}

#[test]
fn write_true_is_refused_on_the_read_path() {
    let s = clusters();
    let err = read_error(
        &s,
        "CALL vector.knnGraph('Doc', 'v', {write: true}) YIELD node1 RETURN node1",
    );
    // Refused either by the executor's read-only check or by the operator
    // itself; both name the write.
    assert!(
        err.contains("requires write access") || err.contains("read-only"),
        "{err}"
    );
}

#[test]
fn similarity_join_matches_across_labels() {
    let mut s = GraphStore::new();
    create(&mut s, "Person", "");
    create(&mut s, "Author", "_twin");
    s.create_vector_index("Author", "v", 3, DistanceMetric::Cosine)
        .unwrap();
    s.rebuild_vector_index();
    let out = rows(
        &s,
        "CALL vector.similarityJoin('Person', 'v', 'Author', 'v', {k: 1}) \
         YIELD node1, node2, similarity RETURN node1.name AS p, node2.name AS a, similarity",
    );
    assert_eq!(out.len(), 6, "one match per Person");
    for r in &out {
        assert_eq!(
            string(r, "a"),
            format!("{}_twin", string(r, "p")),
            "each Person's twin"
        );
        assert!(float(r, "similarity") > 0.9999);
    }
    write(
        &mut s,
        "CALL vector.similarityJoin('Person', 'v', 'Author', 'v', {k: 1, write: true, relType: 'SAME_AS'}) \
         YIELD node1 RETURN count(*)",
    );
    let linked = rows(
        &s,
        "MATCH (:Person)-[r:SAME_AS]->(:Author) RETURN count(r) AS n",
    );
    assert_eq!(int(&linked[0], "n"), 6);
}

#[test]
fn yield_aliases_are_honoured() {
    let s = clusters();
    let out = rows(
        &s,
        "CALL vector.knnGraph('Doc', 'v', {k: 1}) YIELD node1 AS x, similarity AS s RETURN x.name AS n, s",
    );
    assert_eq!(out.len(), 6);
    assert!(float(&out[0], "s") > 0.0);
}

#[test]
fn bad_requests_are_refused_with_the_reason() {
    let mut s = clusters();
    for (q, want) in [
        (
            "CALL vector.knnGraph('Doc', 'v', {kk: 2}) YIELD node1 RETURN node1",
            "unknown config key `kk`",
        ),
        (
            "CALL vector.knnGraph('Doc', 'v', {k: 0}) YIELD node1 RETURN node1",
            "`k` must be a positive integer",
        ),
        (
            "CALL vector.knnGraph('Doc', 'v', {threshold: 'high'}) YIELD node1 RETURN node1",
            "`threshold` must be a number",
        ),
        (
            "CALL vector.knnGraph('Doc', 'v', {relType: 'X'}) YIELD node1 RETURN node1",
            "does not write",
        ),
        (
            "CALL vector.knnGraph('Doc', 'w') YIELD node1 RETURN node1",
            "CREATE VECTOR INDEX",
        ),
        (
            "CALL vector.knnGraph('Doc') YIELD node1 RETURN node1",
            "takes (label, property)",
        ),
        (
            "CALL vector.knnGraph('Doc', 'v') YIELD score RETURN score",
            "no `score` column",
        ),
    ] {
        let err = read_error(&s, q);
        assert!(err.contains(want), "{q}: {err} !~ {want}");
    }
    let err = write_error(
        &mut s,
        "CALL vector.knnGraph('Doc', 'v', {write: 'yes'}) YIELD node1 RETURN node1",
    );
    assert!(err.contains("`write` must be a boolean"), "{err}");
}

#[test]
fn names_are_case_insensitive() {
    let s = clusters();
    for q in [
        "CALL vector.knngraph('Doc', 'v', {k: 1}) YIELD node1 RETURN node1",
        "CALL VECTOR.KNNGRAPH('Doc', 'v', {k: 1}) YIELD node1 RETURN node1",
    ] {
        assert_eq!(rows(&s, q).len(), 6, "{q}");
    }
}
