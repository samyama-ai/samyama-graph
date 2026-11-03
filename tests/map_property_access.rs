//! Reading a property off a map value (#571).
//!
//! `m.a` where `m` is a map parsed, ran, and returned `Null`. It did not error,
//! so a query over map values returned confidently wrong answers rather than
//! failing — and the aggregate case was sharpest, because two distinct keys
//! collapsed into one `Null` group while the row count stayed plausible.
//!
//! Map literals were otherwise supported: they parse, `UNWIND` iterates them,
//! and they round-trip as `PropertyValue::Map`. Only the access off them was
//! missing.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// The named column of every row, rendered.
fn column(store: &GraphStore, cypher: &str, name: &str) -> Vec<String> {
    let query = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&query)
        .expect("query should run")
        .records
        .iter()
        .map(|r| match r.get(name) {
            Some(Value::Property(p)) => format!("{p:?}"),
            Some(other) => format!("{other:?}"),
            None => "<unbound>".to_string(),
        })
        .collect()
}

#[test]
fn a_literal_map_yields_its_value() {
    let store = GraphStore::new();
    assert_eq!(column(&store, "WITH {a: 1} AS m RETURN m.a AS a", "a"), vec!["Integer(1)"]);
}

#[test]
fn every_value_type_survives() {
    let store = GraphStore::new();
    let got = column(
        &store,
        "WITH {i: 1, f: 1.5, s: \"x\", b: true} AS m RETURN m.i AS a",
        "a",
    );
    assert_eq!(got, vec!["Integer(1)"]);
    for (key, expected) in [("f", "Float(1.5)"), ("s", "String(\"x\")"), ("b", "Boolean(true)")] {
        let cypher = format!("WITH {{i: 1, f: 1.5, s: \"x\", b: true}} AS m RETURN m.{key} AS a");
        assert_eq!(column(&store, &cypher, "a"), vec![expected], "{key}");
    }
}

#[test]
fn unwinding_a_list_of_maps_reads_each_one() {
    let store = GraphStore::new();
    assert_eq!(
        column(
            &store,
            "UNWIND [{a: 1, b: 10}, {a: 2, b: 20}] AS m RETURN m.a AS a",
            "a"
        ),
        vec!["Integer(1)", "Integer(2)"]
    );
}

#[test]
fn a_key_that_is_absent_is_null() {
    // Cypher's answer for a missing key, and the behaviour the bug made
    // universal. It has to survive the fix.
    let store = GraphStore::new();
    assert_eq!(column(&store, "WITH {a: 1} AS m RETURN m.zzz AS a", "a"), vec!["Null"]);
}

#[test]
fn an_empty_map_is_null_for_every_key() {
    let store = GraphStore::new();
    assert_eq!(column(&store, "WITH {} AS m RETURN m.a AS a", "a"), vec!["Null"]);
}

#[test]
fn a_nested_map_reads_through() {
    let store = GraphStore::new();
    let got = column(&store, "WITH {outer: {inner: 7}} AS m RETURN m.outer AS a", "a");
    assert!(got[0].contains("Map"), "the inner map comes back as a map: {got:?}");
}

#[test]
fn a_map_valued_node_property_reads_the_same_way() {
    // The map need not come from a literal. A node property holding a map is
    // the case that reaches this through storage rather than the parser.
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let mut inner = std::collections::HashMap::new();
    inner.insert("city".to_string(), PropertyValue::String("Pune".to_string()));
    let _ = store.set_node_property("default", id, "addr".to_string(), PropertyValue::Map(inner));

    let got = column(&store, "MATCH (n:N) WITH n.addr AS m RETURN m.city AS a", "a");
    assert_eq!(got, vec!["String(\"Pune\")"]);
}

#[test]
fn grouping_by_a_map_property_no_longer_collapses_to_one_null_group() {
    // The sharpest form of the bug: distinct keys became one Null group while
    // the row count stayed plausible, so nothing about the result looked wrong.
    let store = GraphStore::new();
    let cypher = "UNWIND [{a: 1, b: 10}, {a: 2, b: 20}, {a: 1, b: 30}] AS m \
                  RETURN m.a AS a, count(m) AS c, sum(m.b) AS s";
    let query = parse_query(cypher).unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();

    assert_eq!(batch.records.len(), 2, "two distinct values of a");

    let mut seen: Vec<(i64, i64, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let g = |k: &str| match r.get(k) {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("{k}: {other:?}"),
            };
            (g("a"), g("c"), g("s"))
        })
        .collect();
    seen.sort();
    assert_eq!(seen, vec![(1, 2, 40), (2, 1, 20)]);
}

#[test]
fn filtering_on_a_map_property_selects_rows() {
    // A WHERE over a map property was comparing against Null, so it matched
    // nothing — the silently-empty result this class of bug produces.
    //
    // Written against a node property rather than `UNWIND … WITH … WHERE`,
    // which is the natural way to say this and does not work: `UNWIND` binds a
    // variable that a following `WITH` cannot see, whatever the value type
    // (#572). That is a separate gap and it errors rather than answering
    // wrongly, so it is not this test's business.
    let mut store = GraphStore::new();
    for i in 1..=3i64 {
        let id = store.create_node("N");
        let mut inner = std::collections::HashMap::new();
        inner.insert("a".to_string(), PropertyValue::Integer(i));
        let _ = store.set_node_property("default", id, "m".to_string(), PropertyValue::Map(inner));
    }
    let mut got = column(&store, "MATCH (n:N) WHERE n.m.a > 1 RETURN n.m.a AS a", "a");
    got.sort();
    assert_eq!(got, vec!["Integer(2)", "Integer(3)"]);
}

#[test]
fn ordering_by_a_map_property_sorts() {
    let store = GraphStore::new();
    let got = column(
        &store,
        "UNWIND [{a: 3}, {a: 1}, {a: 2}] AS m RETURN m.a AS a ORDER BY m.a ASC",
        "a",
    );
    assert_eq!(got, vec!["Integer(1)", "Integer(2)", "Integer(3)"]);
}
