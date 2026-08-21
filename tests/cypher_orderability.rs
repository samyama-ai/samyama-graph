//! ORDER BY across mixed types follows Cypher's orderability, not the index's.
//!
//! openCypher defines a total order over values of different types, ascending:
//!
//! ```text
//! Map < Node < Relationship < List < Path < String < Boolean < Number < NaN < null
//! ```
//!
//! `PropertyValue`'s `Ord` is a *different* total order — Boolean, Number,
//! String, DateTime, Array, Map, Vector, Duration, Null — and deliberately so:
//! it backs the B-tree property index, where any consistent order works and
//! numbers must compare numerically across Integer and Float. Reusing it for
//! ORDER BY put strings after numbers and lists after both.
//!
//! Both orders are needed. This pins the query-level one; the index keeps its
//! own, and `property.rs` keeps the comment explaining why they differ.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn column(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .iter()
        .map(|r| match r.get("x") {
            Some(Value::Property(PropertyValue::Array(a))) => format!("{a:?}"),
            Some(v) => format!("{v:?}"),
            None => "<none>".into(),
        })
        .collect()
}

#[test]
fn lists_sort_element_wise_with_a_shorter_prefix_first() {
    // WithOrderBy1 [9] / ReturnOrderBy1 [9]. The TCK's expected first four are
    // [], ['a'], ['a', 1], [1] — which also pins String before Number, since
    // ['a', ...] precedes [1].
    let store = GraphStore::new();
    let got = column(
        &store,
        "UNWIND [[], ['a'], ['a', 1], [1], [1, 'a']] AS l WITH l ORDER BY l LIMIT 4 RETURN l AS x",
    );
    assert_eq!(got.len(), 4, "expected four rows, got {got:?}");
    assert!(got[0].contains("[]"), "empty list sorts first, got {:?}", got[0]);
    assert!(
        got[1].contains("String(\"a\")") && !got[1].contains("Integer"),
        "['a'] second, got {:?}", got[1]
    );
    assert!(
        got[2].contains("String(\"a\")") && got[2].contains("Integer"),
        "['a', 1] third — a shared prefix orders by length, got {:?}", got[2]
    );
    assert!(
        got[3].contains("Integer(1)") && !got[3].contains("String"),
        "[1] fourth — a string element sorts before a numeric one, got {:?}", got[3]
    );
}

#[test]
fn a_string_sorts_before_a_number() {
    // The single comparison the list ordering above depends on, isolated.
    // PropertyValue's index order puts these the other way round.
    let store = GraphStore::new();
    let got = column(&store, "UNWIND [1, 'a'] AS v WITH v ORDER BY v RETURN v AS x");
    assert!(got[0].contains("String"), "string first, got {got:?}");
    assert!(got[1].contains("Integer"), "number second, got {got:?}");
}

#[test]
fn the_cross_type_order_is_map_list_string_boolean_number_null() {
    // The PropertyValue-expressible part of Cypher's orderability.
    let store = GraphStore::new();
    let got = column(
        &store,
        "UNWIND [1.5, 'text', false, null, ['list'], {a: 'map'}] AS v \
         WITH v ORDER BY v RETURN v AS x",
    );
    assert_eq!(got.len(), 6, "got {got:?}");
    assert!(got[0].contains("Map"), "map first, got {:?}", got[0]);
    assert!(got[1].contains("Array") || got[1].contains("["), "list second, got {:?}", got[1]);
    assert!(got[2].contains("String"), "string third, got {:?}", got[2]);
    assert!(got[3].contains("Boolean"), "boolean fourth, got {:?}", got[3]);
    assert!(got[4].contains("Float") || got[4].contains("Integer"), "number fifth, got {:?}", got[4]);
    assert!(got[5].contains("Null"), "null last, got {:?}", got[5]);
}

#[test]
fn descending_reverses_the_whole_order() {
    let store = GraphStore::new();
    let got = column(
        &store,
        "UNWIND [1.5, 'text', false, null, {a: 'map'}] AS v WITH v ORDER BY v DESC RETURN v AS x",
    );
    assert!(got[0].contains("Null"), "null first on DESC, got {:?}", got[0]);
    assert!(got[4].contains("Map"), "map last on DESC, got {:?}", got[4]);
}

#[test]
fn numbers_still_compare_numerically_across_integer_and_float() {
    // The property the index order exists for must survive: 999999 > 6.9 even
    // though they are different variants.
    let store = GraphStore::new();
    let got = column(&store, "UNWIND [999999, 6.9, 2] AS v WITH v ORDER BY v RETURN v AS x");
    assert!(got[0].contains("Integer(2)"), "got {got:?}");
    assert!(got[1].contains("6.9"), "got {got:?}");
    assert!(got[2].contains("999999"), "got {got:?}");
}

#[test]
fn ordinary_same_type_sorts_are_unchanged() {
    let store = GraphStore::new();
    assert_eq!(
        column(&store, "UNWIND ['b', 'a', 'c'] AS v WITH v ORDER BY v RETURN v AS x").len(),
        3
    );
    let nums = column(&store, "UNWIND [3, 1, 2] AS v WITH v ORDER BY v RETURN v AS x");
    assert!(nums[0].contains("Integer(1)") && nums[2].contains("Integer(3)"), "got {nums:?}");
}
