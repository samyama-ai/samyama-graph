//! `db.hybrid.search` carries a citation payload on every row (AI-08, #1707).
//!
//! A caller could already return node ids, a source URI and a score, but only
//! by naming them in the projection (`RETURN node.src AS uri, score`), which
//! needs knowing the corpus's property names. The `provenance` column carries
//! them without being asked: a bare `CALL` returns it, and its shape is the
//! same on every row, with null for what is unknown.

use std::collections::HashMap;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{Record, Value};
use samyama::query::QueryEngine;

fn write(store: &mut GraphStore, q: &str) {
    QueryEngine::new()
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn rows(store: &GraphStore, q: &str) -> Vec<Record> {
    QueryEngine::new()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .records
}

/// `a` keeps its URI in `source_uri`, `b` in `url`, `c` has none, `d` keeps it
/// in a property no default reads, `link`.
fn corpus() -> GraphStore {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE (:Doc {name: 'a', text: 'aspirin', emb: [1.0, 0.0, 0.0], source_uri: 'https://ex.org/a'}) \
         CREATE (:Doc {name: 'b', text: 'aspirin bleeding', emb: [0.9, 0.1, 0.0], url: 'https://ex.org/b'}) \
         CREATE (:Doc {name: 'c', text: 'aspirin dose', emb: [0.8, 0.2, 0.0]}) \
         CREATE (:Doc {name: 'd', text: 'aspirin trial', emb: [0.7, 0.3, 0.0], link: 'https://ex.org/d'})",
    );
    write(
        &mut s,
        "CREATE FULLTEXT INDEX doc_text FOR (d:Doc) ON (d.text)",
    );
    write(
        &mut s,
        "CREATE VECTOR INDEX doc_emb FOR (d:Doc) ON (d.emb) OPTIONS {dimensions: 3, similarity: 'cosine'}",
    );
    s
}

const CALL: &str = "CALL db.hybrid.search('doc_text', 'aspirin', 'doc_emb', [1.0, 0.0, 0.0], {k: 4";

fn provenance(r: &Record) -> HashMap<String, PropertyValue> {
    match r.get("provenance") {
        Some(Value::Property(PropertyValue::Map(m))) => m.clone(),
        other => panic!("provenance: {other:?}"),
    }
}

/// Provenance by the node's `name`, read back through a projection.
fn by_name(s: &GraphStore, config: &str) -> HashMap<String, HashMap<String, PropertyValue>> {
    rows(
        s,
        &format!("{CALL}{config}}}) YIELD node, provenance RETURN node.name AS name, provenance"),
    )
    .iter()
    .map(|r| match r.get("name") {
        Some(Value::Property(PropertyValue::String(n))) => (n.clone(), provenance(r)),
        other => panic!("name: {other:?}"),
    })
    .collect()
}

fn s(v: &str) -> PropertyValue {
    PropertyValue::String(v.to_string())
}

/// A bare `CALL` -- no `YIELD`, no projection -- carries the payload.
#[test]
fn a_bare_call_carries_provenance_without_being_asked() {
    let st = corpus();
    let out = rows(&st, &format!("{CALL}}})"));
    assert_eq!(out.len(), 4);
    for r in &out {
        let p = provenance(r);
        for key in [
            "nodeId",
            "labels",
            "source",
            "sourceProperty",
            "score",
            "foundBy",
            "bm25Rank",
            "vectorRank",
            "seedId",
            "hops",
            "index",
        ] {
            assert!(p.contains_key(key), "missing {key}: {p:?}");
        }
        assert!(matches!(p["nodeId"], PropertyValue::Integer(_)), "{p:?}");
        assert!(matches!(p["score"], PropertyValue::Float(_)), "{p:?}");
    }
}

/// The source URI comes from the first conventional property the node has.
#[test]
fn the_source_is_read_from_the_conventional_properties() {
    let st = corpus();
    let p = by_name(&st, "");
    assert_eq!(p["a"]["source"], s("https://ex.org/a"));
    assert_eq!(p["a"]["sourceProperty"], s("source_uri"));
    assert_eq!(p["b"]["source"], s("https://ex.org/b"));
    assert_eq!(p["b"]["sourceProperty"], s("url"));
    // Nothing to cite is null, not a missing key and not an empty string.
    assert_eq!(p["c"]["source"], PropertyValue::Null);
    assert_eq!(p["c"]["sourceProperty"], PropertyValue::Null);
    assert_eq!(p["d"]["source"], PropertyValue::Null);
}

/// `sourceProperty` names the property instead, and then only it is read.
#[test]
fn source_property_names_where_the_uri_is() {
    let st = corpus();
    let p = by_name(&st, ", sourceProperty: 'link'");
    assert_eq!(p["d"]["source"], s("https://ex.org/d"));
    assert_eq!(p["d"]["sourceProperty"], s("link"));
    assert_eq!(p["a"]["source"], PropertyValue::Null);
}

/// The payload agrees with the row's own columns and names the indexes used.
#[test]
fn provenance_agrees_with_the_row() {
    let st = corpus();
    let out = rows(
        &st,
        &format!("{CALL}}}) YIELD node, score, foundBy, provenance RETURN id(node) AS id, score, foundBy, provenance"),
    );
    for r in &out {
        let p = provenance(r);
        assert_eq!(Some(&Value::Property(p["nodeId"].clone())), r.get("id"));
        assert_eq!(Some(&Value::Property(p["score"].clone())), r.get("score"));
        assert_eq!(
            Some(&Value::Property(p["foundBy"].clone())),
            r.get("foundBy")
        );
        assert_eq!(p["labels"], PropertyValue::Array(vec![s("Doc")]));
        assert_eq!(p["hops"], PropertyValue::Integer(0));
        assert_eq!(p["seedId"], p["nodeId"]);
        let index: HashMap<String, PropertyValue> = [
            ("fulltext".to_string(), s("doc_text")),
            ("vector".to_string(), s("doc_emb")),
        ]
        .into_iter()
        .collect();
        assert_eq!(p["index"], PropertyValue::Map(index));
    }
}

#[test]
fn a_source_property_that_is_not_a_name_is_refused() {
    let st = corpus();
    let err = QueryEngine::new()
        .execute(&format!("{CALL}, sourceProperty: 3}})"), &st)
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("sourceProperty"), "{err}");
}
