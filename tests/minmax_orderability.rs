//! `min()` and `max()` use Cypher's orderability (#960).
//!
//! ```cypher
//! UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x RETURN max(x), min(x)
//! ```
//!
//! answered `max = [1, 2]` and `min = 0.2`, where openCypher answers
//! `max = 1` and `min = [1, 2]`.
//!
//! They compared with `PropertyValue`'s derived `Ord`, which is the **index**
//! order — Boolean, Number, String, …, Array, Map, Null — and exists to back
//! the B-tree property index, where the only requirement is that numbers
//! compare numerically across `Integer` and `Float`.
//!
//! Cypher's orderability is a different total order:
//! `Map < List < String < Boolean < Number < null`. Over mixed input the two
//! disagree about which value is smallest.
//!
//! Both orders exist on purpose and neither can be dropped — see
//! `graph::property::cypher_order`'s doc comment — so the fix is for each
//! caller to ask for the one it means.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn one(store: &GraphStore, cypher: &str, col: &str) -> String {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&q).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    format!("{:?}", batch.records[0].get(col))
}

#[test]
fn max_over_mixed_types_is_the_number() {
    let store = GraphStore::new();
    let got = one(
        &store,
        "UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x RETURN max(x) AS m",
        "m",
    );
    assert!(got.contains("Integer(1)"), "expected 1, got {got}");
}

#[test]
fn min_over_mixed_types_is_the_list() {
    let store = GraphStore::new();
    let got = one(
        &store,
        "UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x RETURN min(x) AS m",
        "m",
    );
    assert!(got.contains("Array"), "expected the list, got {got}");
}

#[test]
fn numbers_alone_still_compare_numerically() {
    // The half the index order got right, and the half a careless fix would
    // break: within one type nothing about the ordering changes.
    let store = GraphStore::new();
    for (q, want) in [
        ("UNWIND [3, 1, 2] AS x RETURN min(x) AS m", "Integer(1)"),
        ("UNWIND [3, 1, 2] AS x RETURN max(x) AS m", "Integer(3)"),
        ("UNWIND [1, 0.5, 2] AS x RETURN min(x) AS m", "Float(0.5)"),
    ] {
        let got = one(&store, q, "m");
        assert!(got.contains(want), "{q}: expected {want}, got {got}");
    }
}

#[test]
fn strings_alone_still_compare_lexically() {
    let store = GraphStore::new();
    let got = one(&store, "UNWIND ['b', 'a', 'c'] AS x RETURN min(x) AS m", "m");
    assert!(got.contains("\"a\""), "{got}");
}

#[test]
fn nulls_are_ignored_not_compared() {
    // Aggregates ignore nulls. Comparing them broke min and max in opposite
    // directions once already: null sorted smallest won every min, and once
    // ordering put null greatest it would have won every max.
    let store = GraphStore::new();
    for (q, want) in [
        ("UNWIND [null, 2, null, 1] AS x RETURN min(x) AS m", "Integer(1)"),
        ("UNWIND [null, 2, null, 1] AS x RETURN max(x) AS m", "Integer(2)"),
    ] {
        let got = one(&store, q, "m");
        assert!(got.contains(want), "{q}: expected {want}, got {got}");
    }
}

#[test]
fn all_null_input_gives_null() {
    let store = GraphStore::new();
    let got = one(&store, "UNWIND [null, null] AS x RETURN min(x) AS m", "m");
    assert!(got.contains("Null") || got == "None", "{got}");
}

#[test]
fn a_string_beats_a_list_and_a_number_beats_a_string() {
    // The order stated directly, one pair at a time, so a future change that
    // reorders two adjacent ranks is caught rather than absorbed.
    let store = GraphStore::new();
    let cases = [
        ("UNWIND [[1], 'a'] AS x RETURN max(x) AS m", "\"a\""),
        ("UNWIND [[1], 'a'] AS x RETURN min(x) AS m", "Array"),
        ("UNWIND ['a', 1] AS x RETURN max(x) AS m", "Integer(1)"),
        ("UNWIND ['a', 1] AS x RETURN min(x) AS m", "\"a\""),
        ("UNWIND [true, 1] AS x RETURN max(x) AS m", "Integer(1)"),
    ];
    for (q, want) in cases {
        let got = one(&store, q, "m");
        assert!(got.contains(want), "{q}: expected {want}, got {got}");
    }
}

#[test]
fn min_and_max_over_a_property_are_unchanged() {
    // The overwhelmingly common case: one type, read from the graph.
    let mut store = GraphStore::new();
    for v in [5i64, 1, 9] {
        let n = store.create_node("N");
        let _ = store.set_node_property("default", n, "v".to_string(), PropertyValue::Integer(v));
    }
    let q = parse_query("MATCH (n:N) RETURN min(n.v) AS lo, max(n.v) AS hi").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    let get = |c: &str| match batch.records[0].get(c) {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{c}: {other:?}"),
    };
    assert_eq!((get("lo"), get("hi")), (1, 9));
}
