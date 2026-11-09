//! Neo4j 5's vector scores and functions (#1980).
//!
//! `db.index.vector.queryNodes` yielded `score` as a distance, lower is
//! better: the nearest neighbour of a query scored 5.96e-8. Neo4j's `score` is
//! a similarity in [0, 1], higher is better, so a query ported from Neo4j with
//! `ORDER BY score DESC` or `WHERE score > 0.9` returned the worst matches.
//! `score` now follows Neo4j, and the raw distance is yielded as `distance`.
//!
//! The expected numbers in the function tests are the ones Neo4j's Cypher
//! manual prints for the same inputs (Functions > Vector functions). Neo4j
//! computes in float32, so they are compared with a tolerance, never exactly.

use samyama::graph::GraphStore;
use samyama::graph::PropertyValue;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Record, Value};
use samyama::query::parser::parse_query;

const TOL: f64 = 1e-6;

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

fn rows(s: &GraphStore, q: &str) -> Vec<Record> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"))
        .records
}

fn error(s: &GraphStore, q: &str) -> String {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    match QueryExecutor::new(s).execute(&query) {
        Ok(b) => panic!("{q}: expected an error, got {:?}", b.records),
        Err(e) => e.to_string(),
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

fn is_null(r: &Record, col: &str) -> bool {
    matches!(
        r.get(col),
        None | Some(Value::Null) | Some(Value::Property(PropertyValue::Null))
    )
}

/// `RETURN <expr> AS r`, as a float.
fn eval(expr: &str) -> f64 {
    let s = GraphStore::new();
    float(&rows(&s, &format!("RETURN {expr} AS r"))[0], "r")
}

/// Within `TOL` of `want`, relative to its size when that is above 1: the
/// arithmetic is float32, whose error grows with the value.
fn assert_close(got: f64, want: f64, what: &str) {
    assert!(
        (got - want).abs() < TOL * want.abs().max(1.0),
        "{what}: got {got}, want {want}"
    );
}

// ---------------------------------------------------------------------------
// The functions, against Neo4j's documented examples
// ---------------------------------------------------------------------------

/// Neo4j's k-NN example for `vector.similarity.euclidean`: three nodes, the
/// query `[4.0, 5.0, 6.0]`, `ORDER BY score DESC LIMIT 2`.
#[test]
fn euclidean_similarity_matches_the_neo4j_knn_example() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE (:Node {id: 1, vector: [1.0, 4.0, 2.0]}), \
                (:Node {id: 2, vector: [3.0, -2.0, 1.0]}), \
                (:Node {id: 3, vector: [2.0, 8.0, 3.0]})",
    );
    let got = rows(
        &s,
        "MATCH (node:Node) \
         WITH node, vector.similarity.euclidean([4.0, 5.0, 6.0], node.vector) AS score \
         RETURN node.id AS id, score ORDER BY score DESC LIMIT 2",
    );
    assert_eq!(got.len(), 2);
    assert_eq!(int(&got[0], "id"), 3);
    assert_close(float(&got[0], "score"), 0.043478261679410934, "id 3");
    assert_eq!(int(&got[1], "id"), 1);
    assert_close(float(&got[1], "score"), 0.03703703731298447, "id 1");
}

/// Neo4j's examples for `vector_distance`: COSINE over `[1, 2, 3]` and
/// `[1, 2, 4]`, EUCLIDEAN over `[1.0, 5.0, 3.0, 6.7]` and `[5.0, 2.5, 3.1, 9.0]`.
#[test]
fn vector_distance_matches_the_neo4j_examples() {
    for f in ["vector.distance", "vector_distance"] {
        assert_close(
            eval(&format!("{f}([1, 2, 3], [1, 2, 4], 'COSINE')")),
            0.008539855480194092,
            f,
        );
        assert_close(
            eval(&format!(
                "{f}([1.0, 5.0, 3.0, 6.7], [5.0, 2.5, 3.1, 9.0], 'EUCLIDEAN')"
            )),
            5.248809337615967,
            f,
        );
    }
}

/// The remaining metrics, by their definitions in the same manual page.
#[test]
fn vector_distance_takes_every_neo4j_metric() {
    let (a, b) = ("[1.0, 5.0, 3.0, 6.7]", "[5.0, 2.5, 3.1, 9.0]");
    // 4² + 2.5² + 0.1² + 2.3²
    assert_close(
        eval(&format!("vector.distance({a}, {b}, 'EUCLIDEAN_SQUARED')")),
        27.55,
        "sq",
    );
    // 4 + 2.5 + 0.1 + 2.3
    assert_close(
        eval(&format!("vector.distance({a}, {b}, 'MANHATTAN')")),
        8.9,
        "manhattan",
    );
    // -(1 + 4 + 12)
    assert_close(
        eval("vector.distance([1, 2, 3], [1, 2, 4], 'DOT')"),
        -17.0,
        "dot",
    );
    assert_close(
        eval("vector.distance([1, 2, 3], [1, 2, 4], 'HAMMING')"),
        1.0,
        "hamming",
    );
    // The metric name is not case-sensitive.
    assert_close(
        eval("vector.distance([1, 2, 3], [1, 2, 4], 'cosine')"),
        0.008539855480194092,
        "case",
    );
}

/// Cosine similarity is `(1 + cos) / 2`: 1 for the same direction, 0.5 for
/// orthogonal, 0 for opposite. For Neo4j's COSINE example pair it is
/// `1 - 0.0085398... / 2`.
#[test]
fn cosine_similarity_is_one_plus_cos_over_two() {
    assert_close(
        eval("vector.similarity.cosine([1.0, 0.0], [2.0, 0.0])"),
        1.0,
        "same",
    );
    assert_close(
        eval("vector.similarity.cosine([1.0, 0.0], [0.0, 1.0])"),
        0.5,
        "orthogonal",
    );
    assert_close(
        eval("vector.similarity.cosine([1.0, 0.0], [-1.0, 0.0])"),
        0.0,
        "opposite",
    );
    assert_close(
        eval("vector.similarity.cosine([1, 2, 3], [1, 2, 4])"),
        1.0 - 0.008539855480194092 / 2.0,
        "neo4j pair",
    );
    // Euclidean similarity is `1 / (1 + d²)`: 1 for the same point.
    assert_close(
        eval("vector.similarity.euclidean([1.0, 2.0], [1.0, 2.0])"),
        1.0,
        "same point",
    );
    assert_close(
        eval("vector.similarity.euclidean([0.0, 0.0], [1.0, 2.0])"),
        1.0 / 6.0,
        "d²=5",
    );
}

/// Null in, null out, as Neo4j documents for both similarity functions. A
/// zero vector has no direction, so it has no cosine either.
#[test]
fn null_and_zero_vectors_give_null() {
    let s = GraphStore::new();
    for q in [
        "RETURN vector.similarity.cosine(null, [1.0]) AS r",
        "RETURN vector.similarity.euclidean([1.0], null) AS r",
        "RETURN vector.distance(null, [1.0], 'EUCLIDEAN') AS r",
        "RETURN vector.similarity.cosine([0.0, 0.0], [1.0, 0.0]) AS r",
        "RETURN vector.distance([0.0, 0.0], [1.0, 0.0], 'COSINE') AS r",
    ] {
        assert!(is_null(&rows(&s, q)[0], "r"), "{q}");
    }
    // Euclidean is defined at the origin.
    assert_close(
        eval("vector.similarity.euclidean([0.0, 0.0], [0.0, 0.0])"),
        1.0,
        "origin",
    );
}

/// Vectors of different dimensions, a value that is not a vector, and a
/// metric nobody defines are errors, not numbers.
#[test]
fn bad_arguments_are_errors() {
    let s = GraphStore::new();
    let e = error(
        &s,
        "RETURN vector.similarity.cosine([1.0, 2.0], [1.0]) AS r",
    );
    assert!(e.contains("same dimension"), "{e}");
    let e = error(
        &s,
        "RETURN vector.distance([1.0], [1.0, 2.0], 'EUCLIDEAN') AS r",
    );
    assert!(e.contains("same dimension"), "{e}");
    let e = error(&s, "RETURN vector.similarity.euclidean('a', [1.0]) AS r");
    assert!(e.contains("vectors"), "{e}");
    let e = error(&s, "RETURN vector.similarity.euclidean([], []) AS r");
    assert!(e.contains("vectors"), "{e}");
    let e = error(&s, "RETURN vector.distance([1.0], [2.0], 'CHEBYSHEV') AS r");
    assert!(
        e.contains("CHEBYSHEV") && e.contains("EUCLIDEAN_SQUARED"),
        "{e}"
    );
    let e = error(&s, "RETURN vector.distance([1.0], [2.0], 3) AS r");
    assert!(e.contains("metric"), "{e}");
}

// ---------------------------------------------------------------------------
// db.index.vector.queryNodes: score and distance
// ---------------------------------------------------------------------------

/// Vectors in the plane at increasing angles from the x axis, at radius 1
/// and 2 alternately, so that cosine, L2 and inner product rank them
/// differently.
fn plane(s: &mut GraphStore, label: &str, similarity: &str) {
    write(
        s,
        &format!(
            "CREATE VECTOR INDEX FOR (n:{label}) ON (n.v) \
             OPTIONS {{dimensions: 2, similarity: '{similarity}'}}"
        ),
    );
    let nodes: Vec<String> = (0..20)
        .map(|i| {
            let angle = i as f64 * 0.15;
            let r = if i % 2 == 0 { 1.0 } else { 2.0 };
            format!(
                "(:{label} {{id: {i}, v: [{:?}, {:?}]}})",
                r * angle.cos(),
                r * angle.sin()
            )
        })
        .collect();
    write(s, &format!("CREATE {}", nodes.join(", ")));
}

/// For each metric: `score` is the Neo4j mapping of `distance`, the rows come
/// nearest first, which is also highest score first, and `ORDER BY score
/// DESC` returns them in the same order.
#[test]
fn score_is_a_similarity_and_distance_is_the_raw_distance() {
    for (similarity, to_score) in [
        ("cosine", (|d: f64| 1.0 - d / 2.0) as fn(f64) -> f64),
        ("euclidean", |d: f64| 1.0 / (1.0 + d * d)),
        ("inner_product", |d: f64| 1.0 - d / 2.0),
    ] {
        let mut s = GraphStore::new();
        plane(&mut s, "P", similarity);
        let q = "CALL db.index.vector.queryNodes('P', 'v', [1.0, 0.0], 8) \
                 YIELD node, score, distance RETURN node.id AS id, score, distance";
        let got = rows(&s, q);
        assert_eq!(got.len(), 8, "{similarity}");
        for (i, r) in got.iter().enumerate() {
            let (score, distance) = (float(r, "score"), float(r, "distance"));
            assert_close(score, to_score(distance), similarity);
            if i > 0 {
                assert!(
                    distance >= float(&got[i - 1], "distance") - TOL,
                    "{similarity}: not nearest first"
                );
                assert!(
                    score <= float(&got[i - 1], "score") + TOL,
                    "{similarity}: not highest score first"
                );
            }
        }
        let sorted = rows(
            &s,
            "CALL db.index.vector.queryNodes('P', 'v', [1.0, 0.0], 8) \
             YIELD node, score RETURN node.id AS id ORDER BY score DESC, id",
        );
        let ranked: Vec<i64> = got.iter().map(|r| int(r, "id")).collect();
        let by_score: Vec<i64> = sorted.iter().map(|r| int(r, "id")).collect();
        // Equal scores may swap under `, id`; the multiset and the best match
        // may not.
        assert_eq!(by_score[0], ranked[0], "{similarity}");
        let (mut a, mut b) = (ranked.clone(), by_score.clone());
        a.sort();
        b.sort();
        assert_eq!(a, b, "{similarity}");
    }
}

/// The issue's case: the nearest neighbour of a query already in the index
/// scores 1, not 0, and `WHERE score > 0.9` keeps it.
#[test]
fn the_nearest_neighbour_scores_one() {
    let mut s = GraphStore::new();
    plane(&mut s, "P", "cosine");
    let got = rows(
        &s,
        "CALL db.index.vector.queryNodes('P', 'v', [1.0, 0.0], 3) YIELD node, score \
         WHERE score > 0.9 RETURN node.id AS id, score",
    );
    assert!(!got.is_empty());
    assert_eq!(int(&got[0], "id"), 0);
    assert_close(float(&got[0], "score"), 1.0, "nearest");
}

/// The index's score and the function's similarity agree for the same pair,
/// so a query can filter on either.
#[test]
fn the_index_score_equals_the_similarity_function() {
    for (similarity, function) in [
        ("cosine", "vector.similarity.cosine"),
        ("euclidean", "vector.similarity.euclidean"),
    ] {
        let mut s = GraphStore::new();
        plane(&mut s, "P", similarity);
        let got = rows(
            &s,
            &format!(
                "CALL db.index.vector.queryNodes('P', 'v', [0.3, 0.7], 5) YIELD node, score \
                 RETURN score, {function}(node.v, [0.3, 0.7]) AS f"
            ),
        );
        assert_eq!(got.len(), 5);
        for r in &got {
            assert_close(float(r, "score"), float(r, "f"), similarity);
        }
    }
}

/// Both call forms, aliases, and the filtered search: a WHERE on the node is
/// still taken into the search (EXPLAIN shows it), k rows still come back,
/// and their score and distance still agree.
#[test]
fn the_filtered_search_and_explain_still_work() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE VECTOR INDEX doc_emb FOR (n:VDoc) ON (n.v) \
         OPTIONS {dimensions: 2, similarity: 'l2'}",
    );
    let nodes: Vec<String> = (0..400)
        .map(|i| {
            let (x, y) = ((i % 20) as f64 * 0.1, (i / 20) as f64 * 0.1);
            format!("(:VDoc {{id: {i}, cat: {}, v: [{x:?}, {y:?}]}})", i % 10)
        })
        .collect();
    write(&mut s, &format!("CREATE {}", nodes.join(", ")));

    for q in [
        "CALL db.index.vector.queryNodes('doc_emb', 5, [0.5, 0.5]) \
         YIELD node AS d, score AS sc, distance AS dist WHERE d.cat = 3 \
         RETURN d.id AS id, sc, dist",
        "CALL db.index.vector.queryNodes('VDoc', 'v', [0.5, 0.5], 5) \
         YIELD node AS d, score AS sc, distance AS dist WHERE d.cat = 3 \
         RETURN d.id AS id, sc, dist",
    ] {
        let got = rows(&s, q);
        assert_eq!(got.len(), 5, "{q}");
        for r in &got {
            assert_eq!(int(r, "id") % 10, 3, "{q}");
            let d = float(r, "dist");
            assert_close(float(r, "sc"), 1.0 / (1.0 + d * d), q);
        }

        let query = parse_query(&format!("EXPLAIN {q}")).unwrap();
        let batch = QueryExecutor::new(&s).execute(&query).unwrap();
        let text = match batch.records[0].get("plan") {
            Some(Value::Property(PropertyValue::String(t))) => t.clone(),
            other => panic!("{other:?}"),
        };
        let search = text
            .lines()
            .find(|l| l.contains("VectorSearch"))
            .unwrap_or_else(|| panic!("no vector search in\n{text}"));
        assert!(
            search.contains("filter:") && search.contains("cat"),
            "{text}"
        );
    }

    // A threshold on the score is applied above the search, and now keeps
    // the near rows rather than the far ones.
    let near = rows(
        &s,
        "CALL db.index.vector.queryNodes('doc_emb', 20, [0.5, 0.5]) \
         YIELD node, score, distance WHERE score > 0.9 RETURN distance",
    );
    assert!(!near.is_empty());
    for r in &near {
        assert!(float(r, "distance") < (1.0f64 / 0.9 - 1.0).sqrt() + TOL);
    }
}
