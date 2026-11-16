//! Multi-vector retrieval through an ordinary vector index (#1978).
//!
//! `vector.muvera.encode(tokens, role[, options])` turns a multi-vector into
//! one MUVERA fixed-dimensional encoding. Documents encoded as `'DOCUMENT'`
//! go in a dot-product vector index. A query encoded as `'QUERY'` finds
//! candidates there, and `vector.similarity.maxSim` reranks them exactly:
//!
//! ```cypher
//! CALL db.index.vector.queryNodes('Doc', 'fde', vector.muvera.encode($q, 'QUERY'), 10)
//! YIELD node
//! RETURN node.id, vector.similarity.maxSim($q, node.tokens) AS s ORDER BY s DESC
//! ```
//!
//! The algorithm's own properties (additivity, filling, determinism) are unit
//! tests in `src/vector/muvera.rs`. These tests check the Cypher surface and
//! that the pipeline finds the document a query was drawn from.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Record, Value};
use samyama::query::parser::parse_query;

const DIM: usize = 16;
const DOCS: usize = 30;
const TOKENS: usize = 6;
const PLANTED: usize = 7;

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

fn error(q: &str) -> String {
    let s = GraphStore::new();
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    match QueryExecutor::new(&s).execute(&query) {
        Ok(b) => panic!("{q}: expected an error, got {:?}", b.records),
        Err(e) => e.to_string(),
    }
}

fn vector(r: &Record, col: &str) -> Vec<f32> {
    match r.get(col) {
        Some(Value::Property(p)) => p.to_vector().unwrap_or_else(|| panic!("{col}: {p:?}")),
        other => panic!("{col}: {other:?}"),
    }
}

fn integer(r: &Record, col: &str) -> i64 {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("{col}: {other:?}"),
    }
}

/// Deterministic tokens, roughly uniform in [-1, 1).
fn tokens(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            (0..DIM)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    ((x >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
                })
                .collect()
        })
        .collect()
}

fn literal(tokens: &[Vec<f32>]) -> String {
    let rows: Vec<String> = tokens
        .iter()
        .map(|t| {
            let xs: Vec<String> = t.iter().map(|x| format!("{x:?}")).collect();
            format!("[{}]", xs.join(", "))
        })
        .collect();
    format!("[{}]", rows.join(", "))
}

#[test]
fn the_default_encoding_has_a_thousand_and_twenty_four_dimensions() {
    let s = GraphStore::new();
    let t = literal(&tokens(3, 1));
    let r = &rows(
        &s,
        &format!("RETURN vector.muvera.encode({t}, 'query') AS e"),
    )[0];
    assert_eq!(vector(r, "e").len(), 16 * 8 * 8);
    let r = &rows(
        &s,
        &format!(
            "RETURN vector.muvera.encode({t}, 'DOCUMENT', \
             {{bits: 2, projection: 3, repetitions: 5, seed: 11}}) AS e"
        ),
    )[0];
    assert_eq!(vector(r, "e").len(), 4 * 3 * 5);
}

#[test]
fn the_same_tokens_and_settings_encode_the_same_and_the_roles_differ() {
    let s = GraphStore::new();
    let t = literal(&tokens(4, 2));
    let r = &rows(
        &s,
        &format!(
            "RETURN vector.muvera.encode({t}, 'QUERY') AS a, \
                    vector.muvera.encode({t}, 'QUERY') AS b, \
                    vector.muvera.encode({t}, 'DOCUMENT') AS d"
        ),
    )[0];
    assert_eq!(vector(r, "a"), vector(r, "b"));
    assert_ne!(vector(r, "a"), vector(r, "d"));
}

#[test]
fn null_in_null_out() {
    let s = GraphStore::new();
    let r = &rows(&s, "RETURN vector.muvera.encode(null, 'QUERY') AS e")[0];
    assert!(matches!(
        r.get("e"),
        Some(Value::Null) | Some(Value::Property(PropertyValue::Null))
    ));
}

#[test]
fn bad_arguments_are_refused_with_a_reason() {
    for (q, says) in [
        (
            "RETURN vector.muvera.encode([[1.0, 2.0]], 'PASSAGE')",
            "unknown role",
        ),
        (
            "RETURN vector.muvera.encode([[1.0, 2.0]], 1)",
            "role as a string",
        ),
        (
            "RETURN vector.muvera.encode([[1.0, 2.0]], 'QUERY', {width: 3})",
            "unknown option",
        ),
        (
            "RETURN vector.muvera.encode([[1.0, 2.0]], 'QUERY', {bits: -1})",
            "non-negative integer",
        ),
        (
            "RETURN vector.muvera.encode([[1.0, 2.0]], 'QUERY', {bits: 11})",
            "bits must be at most",
        ),
        (
            "RETURN vector.muvera.encode([[1.0, 2.0]], 'QUERY', {repetitions: 0})",
            "repetitions",
        ),
        (
            "RETURN vector.muvera.encode([[1.0, 2.0]], 'QUERY', 4)",
            "options as a map",
        ),
        ("RETURN vector.muvera.encode([], 'QUERY')", "multi-vectors"),
        (
            "RETURN vector.muvera.encode([1.0, 2.0], 'QUERY')",
            "multi-vectors",
        ),
        (
            "RETURN vector.muvera.encode([[1.0, 2.0], [1.0]], 'QUERY')",
            "same dimension",
        ),
    ] {
        let e = error(q);
        assert!(e.contains(says), "{q}: {e}");
    }
}

/// Thirty documents of six tokens each. The query is three of document 7's
/// tokens, so exact MaxSim ranks document 7 first by a wide margin: each
/// query token meets itself. The index search over the encodings must keep
/// document 7 among its candidates, and the rerank must put it first.
#[test]
fn the_encoding_finds_the_document_a_query_was_drawn_from() {
    let mut s = GraphStore::new();
    let options = "{bits: 3, projection: 8, repetitions: 8, seed: 5}";
    let dim = 8 * 8 * 8;
    write(
        &mut s,
        &format!(
            "CREATE VECTOR INDEX doc_fde FOR (d:Doc) ON (d.fde) \
             OPTIONS {{dimensions: {dim}, similarity: 'dot'}}"
        ),
    );
    let docs: Vec<Vec<Vec<f32>>> = (0..DOCS).map(|i| tokens(TOKENS, 100 + i as u64)).collect();
    for (i, d) in docs.iter().enumerate() {
        let t = literal(d);
        write(
            &mut s,
            &format!(
                "CREATE (:Doc {{id: {i}, tokens: {t}, \
                 fde: vector.muvera.encode({t}, 'DOCUMENT', {options})}})"
            ),
        );
    }
    assert_eq!(
        s.vector_index
            .get_index("Doc", "fde")
            .expect("index")
            .read()
            .unwrap()
            .len(),
        DOCS,
        "every document's encoding is indexed"
    );

    let q = literal(&docs[PLANTED][..3]);
    let exact = rows(
        &s,
        &format!(
            "MATCH (d:Doc) RETURN d.id AS id, vector.similarity.maxSim({q}, d.tokens) AS s \
             ORDER BY s DESC LIMIT 1"
        ),
    );
    assert_eq!(
        integer(&exact[0], "id"),
        PLANTED as i64,
        "the fixture's premise"
    );

    let found = rows(
        &s,
        &format!(
            "CALL db.index.vector.queryNodes('Doc', 'fde', \
             vector.muvera.encode({q}, 'QUERY', {options}), 10) YIELD node \
             RETURN node.id AS id, vector.similarity.maxSim({q}, node.tokens) AS s \
             ORDER BY s DESC"
        ),
    );
    assert_eq!(found.len(), 10);
    assert_eq!(integer(&found[0], "id"), PLANTED as i64, "{found:?}");
}
