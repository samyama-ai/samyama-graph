//! `+` concatenates a list whichever way the list was written (#986).
//!
//! ```cypher
//! MATCH (a)
//! WITH a ORDER BY [a.list2[1], a.list2[0], a.list[1]] + a.list + a.list2 DESC
//!   LIMIT 3
//! RETURN a
//! ```
//!
//! raised `TypeError("Binary op requires property values")` while
//! `RETURN [1,2] + [3]` worked. The same operator, on the same kind of thing,
//! decided by how the list happened to be written.
//!
//! A list has two spellings. The parser folds a *literal* into
//! `Value::Property(PropertyValue::Array)`; an expression builds
//! `Value::List`, because a `PropertyValue` cannot hold an entity. `+`
//! understood only the first.
//!
//! The failure was invisible from the row count. `SortOperator::key_of` folds
//! an evaluation error to `Null`, so every key compared equal and the rows
//! came back in insertion order -- and the TCK's *ascending* scenario expects
//! a set that insertion order happens to satisfy, so it passed while nothing
//! was being sorted at all. Only the descending twin failed. See #987.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    for (l, list, list2) in [
        ("A", vec![2, -2], vec![3, -2]),
        ("B", vec![1, 2], vec![2, -2]),
        ("C", vec![300, 0], vec![1, -2]),
        ("D", vec![1, -20], vec![4, -2]),
        ("E", vec![2, -2, 100], vec![5, -2]),
    ] {
        let n = store.create_node_with_labels([Label::new(l)]);
        let arr = |v: &Vec<i64>| {
            PropertyValue::Array(v.iter().map(|x| PropertyValue::Integer(*x)).collect())
        };
        store.set_node_property("default", n, "list", arr(&list)).unwrap();
        store.set_node_property("default", n, "list2", arr(&list2)).unwrap();
    }
    store
}

fn one_col(store: &GraphStore, cypher: &str) -> Vec<Value> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let c = r.columns[0].clone();
    r.records.iter().map(|rec| rec.get(&c).cloned().unwrap_or(Value::Null)).collect()
}

/// The label of each row, for readable assertions.
fn labels(store: &GraphStore, cypher: &str) -> Vec<String> {
    one_col(store, cypher)
        .iter()
        .map(|v| format!("{v:?}"))
        .map(|s| {
            let i = s.find("String(\"").expect("a label");
            s[i + 8..i + 9].to_string()
        })
        .collect()
}

const KEY: &str = "[a.list2[1], a.list2[0], a.list[1]] + a.list + a.list2";

#[test]
fn a_list_built_from_expressions_concatenates() {
    let store = graph();
    let got = one_col(&store, &format!("MATCH (a) RETURN {KEY} AS k"));
    assert_eq!(got.len(), 5);
    // A's key: [-2,3,-2] + [2,-2] + [3,-2]
    let a = format!("{:?}", got[0]);
    assert_eq!(a.matches("Integer(").count(), 7, "got {a}");
}

#[test]
fn descending_orders_by_the_key_rather_than_by_nothing() {
    let store = graph();
    assert_eq!(
        labels(&store, &format!(
            "MATCH (a) WITH a ORDER BY {KEY} DESC LIMIT 3 RETURN labels(a) AS l")),
        ["E", "D", "A"],
    );
}

#[test]
fn ascending_still_agrees_and_now_for_the_right_reason() {
    let store = graph();
    // This is the scenario that passed while nothing was sorted: insertion
    // order A,B,C is the same *set* as the expected C,B,A. Asserting the
    // sequence, not the set, is what makes it a real check.
    assert_eq!(
        labels(&store, &format!(
            "MATCH (a) WITH a ORDER BY {KEY} ASC LIMIT 3 RETURN labels(a) AS l")),
        ["C", "B", "A"],
    );
}

#[test]
fn a_literal_list_still_concatenates() {
    let store = graph();
    let got = one_col(&store, "RETURN [1,2] + [3] AS c");
    assert_eq!(format!("{:?}", got[0]).matches("Integer(").count(), 3);
}

#[test]
fn a_value_is_appended_and_prepended() {
    let store = graph();
    for (q, n) in [("RETURN [1,2] + 3 AS c", 3), ("RETURN 1 + [2,3] AS c", 3)] {
        let got = one_col(&store, q);
        assert_eq!(format!("{:?}", got[0]).matches("Integer(").count(), n, "{q}");
    }
}

#[test]
fn null_is_not_appended_to_a_list() {
    let store = graph();
    // `[1] + null` is null in Cypher, not a two-element list. The append arm
    // must not catch it.
    let got = one_col(&store, "RETURN [1, 2] + null AS c");
    assert!(got[0].is_null(), "got {:?}", got[0]);
}

#[test]
fn a_list_of_nodes_concatenates_without_being_flattened_to_properties() {
    let store = graph();
    // The reason `Value::List` exists: a `PropertyValue` cannot hold an
    // entity. Concatenating at the `Value` level keeps them; narrowing first
    // would have destroyed them.
    let got = one_col(&store, "MATCH (a) WITH collect(a) AS ns RETURN ns + ns AS c");
    let s = format!("{:?}", got[0]);
    assert_eq!(s.matches("NodeRef").count() + s.matches("Node(").count(), 10, "got {s}");
}

#[test]
fn the_result_keeps_the_narrow_spelling_so_in_and_slicing_still_read_it() {
    let store = graph();
    // Returning `Value::List` unconditionally was correct arithmetic and a
    // regression anyway: `IN` and slicing read the `Array` spelling, so
    // `[1]+[2] IN [3]+[4]` and `[…] + [...][1..3]` broke -- the mirror image
    // of the bug this file fixes. Precedence3[3], [4] and [5].
    for (q, want) in [
        ("RETURN [1]+[2] IN [3]+[4] AS a", "Boolean(false)"),
        ("RETURN [1]+2 IN [3]+4 AS a", "Boolean(false)"),
        ("RETURN [1]+[2] IN [3]+[[1,2]] AS a", "Boolean(true)"),
    ] {
        let got = one_col(&store, q);
        assert!(format!("{:?}", got[0]).contains(want), "{q}: got {:?}", got[0]);
    }
    // A slice of a concatenation is still sliceable.
    let got = one_col(&store, "RETURN ([1,2,3] + [4,5])[1..3] AS a");
    assert_eq!(format!("{:?}", got[0]).matches("Integer(").count(), 2, "got {:?}", got[0]);
}
