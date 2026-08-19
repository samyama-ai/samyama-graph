//! Gaps found by sweeping standard Cypher expressions against hand-computed
//! answers (#577, #578).
//!
//! The sweep exists because #571 (map property access returning `Null`) and
//! #572 (`UNWIND … WITH` failing) were both found *by accident*, while writing
//! tests for something else. Both were in constructs nothing exercised.
//!
//! `examples/cypher_probe.rs` runs the whole battery and reports wrong answers
//! and errors separately, because those are different problems. These are the
//! cases that were failing, pinned.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn one(cypher: &str) -> PropertyValue {
    let store = GraphStore::new();
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: parse {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: exec {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        Some(Value::Null) | None => PropertyValue::Null,
        other => panic!("{cypher}: {other:?}"),
    }
}

fn ints(cypher: &str) -> Vec<i64> {
    match one(cypher) {
        PropertyValue::Array(items) => items
            .iter()
            .map(|p| match p {
                PropertyValue::Integer(n) => *n,
                other => panic!("{other:?}"),
            })
            .collect(),
        other => panic!("{cypher}: expected a list, got {other:?}"),
    }
}

// ---------------------------------------------------------------- #577

/// The one that matters: not the order, but that it is the *same* order.
#[test]
fn keys_is_the_same_on_every_call() {
    let cypher = "WITH {gamma: 1, alpha: 2, delta: 3, beta: 4} AS m RETURN keys(m) AS r";
    let first = one(cypher);
    for _ in 0..20 {
        assert_eq!(one(cypher), first, "keys() varies between calls");
    }
}

#[test]
fn keys_over_a_map_is_sorted() {
    // A `HashMap` seeds its hasher randomly *per process*, so before this the
    // order differed on every run of the binary — which is why an exact
    // assertion is the right one. An "unspecified but stable" order would pass
    // the test above and still fail across processes.
    match one("WITH {gamma: 1, alpha: 2, delta: 3, beta: 4} AS m RETURN keys(m) AS r") {
        PropertyValue::Array(items) => {
            let names: Vec<String> = items
                .iter()
                .map(|p| match p {
                    PropertyValue::String(s) => s.clone(),
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(names, vec!["alpha", "beta", "delta", "gamma"]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn keys_over_a_node_is_sorted() {
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    for k in ["zeta", "alpha", "mu"] {
        let _ = store.set_node_property("default", id, k.to_string(), PropertyValue::Integer(1));
    }
    let query = parse_query("MATCH (n:N) RETURN keys(n) AS r").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    match batch.records[0].get("r") {
        Some(Value::Property(PropertyValue::Array(items))) => {
            let names: Vec<String> = items
                .iter()
                .map(|p| match p {
                    PropertyValue::String(s) => s.clone(),
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(names, vec!["alpha", "mu", "zeta"]);
        }
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------- #578

#[test]
fn reverse_takes_a_list_as_well_as_a_string() {
    assert_eq!(ints("RETURN reverse([1,2,3]) AS r"), vec![3, 2, 1]);
    assert_eq!(one("RETURN reverse(\"abc\") AS r"), PropertyValue::String("cba".into()));
    assert_eq!(ints("RETURN reverse([]) AS r"), Vec::<i64>::new());
    assert_eq!(one("RETURN reverse(null) AS r"), PropertyValue::Null);
}

#[test]
fn a_list_comprehension_may_omit_its_projection() {
    // `[x IN xs WHERE p]` means `[x IN xs WHERE p | x]`. It is the more
    // commonly written form and it did not parse.
    assert_eq!(ints("RETURN [x IN [1,2,3,4] WHERE x > 2] AS r"), vec![3, 4]);
    // The explicit form still works, and agrees.
    assert_eq!(ints("RETURN [x IN [1,2,3,4] WHERE x > 2 | x] AS r"), vec![3, 4]);
}

#[test]
fn a_list_comprehension_may_omit_its_filter() {
    // The other optional half, which must not have been broken by making the
    // projection optional: with two expressions and no `WHERE`, the second is
    // the projection, not a filter.
    assert_eq!(ints("RETURN [x IN [1,2,3] | x * 2] AS r"), vec![2, 4, 6]);
}

#[test]
fn a_list_comprehension_with_neither_is_the_list_itself() {
    assert_eq!(ints("RETURN [x IN [1,2,3]] AS r"), vec![1, 2, 3]);
}

#[test]
fn the_power_operator_works() {
    assert_eq!(one("RETURN 2 ^ 3 AS r"), PropertyValue::Float(8.0));
    // Float even over integers, which is why `2 ^ -1` must not truncate.
    assert_eq!(one("RETURN 2 ^ -1 AS r"), PropertyValue::Float(0.5));
    assert_eq!(one("RETURN 2.0 ^ 0.5 AS r"), PropertyValue::Float(2.0f64.sqrt()));
    assert_eq!(one("RETURN null ^ 2 AS r"), PropertyValue::Null);
}

#[test]
fn the_power_operator_binds_tightest_and_associates_right() {
    // `2 + 3 ^ 2` is 11, not 25 — it binds tighter than `+`.
    assert_eq!(one("RETURN 2 + 3 ^ 2 AS r"), PropertyValue::Float(11.0));
    // `2 * 3 ^ 2` is 18, not 36 — tighter than `*` too.
    assert_eq!(one("RETURN 2 * 3 ^ 2 AS r"), PropertyValue::Float(18.0));
    // `2 ^ 3 ^ 2` is 2^(3^2) = 512, not (2^3)^2 = 64.
    assert_eq!(one("RETURN 2 ^ 3 ^ 2 AS r"), PropertyValue::Float(512.0));
}

#[test]
fn plus_concatenates_lists() {
    assert_eq!(ints("RETURN [1,2] + [3] AS r"), vec![1, 2, 3]);
    assert_eq!(ints("RETURN [1,2] + 3 AS r"), vec![1, 2, 3], "appending a scalar");
    assert_eq!(ints("RETURN 1 + [2,3] AS r"), vec![1, 2, 3], "prepending a scalar");
    assert_eq!(ints("RETURN [] + [1] AS r"), vec![1]);
}

#[test]
fn plus_still_adds_numbers_and_joins_strings() {
    // The arms added for lists sit above the existing ones, so the ordinary
    // cases are the thing most at risk.
    assert_eq!(one("RETURN 1 + 2 AS r"), PropertyValue::Integer(3));
    assert_eq!(one("RETURN 1.5 + 1 AS r"), PropertyValue::Float(2.5));
    assert_eq!(one("RETURN \"a\" + \"b\" AS r"), PropertyValue::String("ab".into()));
    // And a null operand still makes the result null (#457), rather than being
    // appended to a list.
    assert_eq!(one("RETURN [1,2] + null AS r"), PropertyValue::Null);
    assert_eq!(one("RETURN 1 + null AS r"), PropertyValue::Null);
}

#[test]
fn xor_works_and_sits_between_or_and_and() {
    assert_eq!(one("RETURN true XOR false AS r"), PropertyValue::Boolean(true));
    assert_eq!(one("RETURN true XOR true AS r"), PropertyValue::Boolean(false));
    assert_eq!(one("RETURN false XOR false AS r"), PropertyValue::Boolean(false));
    assert_eq!(one("RETURN null XOR true AS r"), PropertyValue::Null);

    // AND binds tighter: `true XOR true AND false` is `true XOR (true AND false)`
    // = true. Grouped the other way it would be false.
    assert_eq!(one("RETURN true XOR true AND false AS r"), PropertyValue::Boolean(true));
    // OR binds looser: `false OR true XOR true` is `false OR (true XOR true)`
    // = false. Grouped the other way it would be true.
    assert_eq!(one("RETURN false OR true XOR true AS r"), PropertyValue::Boolean(false));
}

#[test]
fn explain_renders_the_new_operators() {
    // The plan is the contract, and a missing arm in the formatter would print
    // a predicate that reads as a different one.
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(1));

    for (cypher, expected) in [
        ("EXPLAIN MATCH (n:N) WHERE n.v ^ 2 > 3 RETURN n", "^"),
        ("EXPLAIN MATCH (n:N) WHERE n.v > 1 XOR n.v < 0 RETURN n", "XOR"),
    ] {
        let query = parse_query(cypher).unwrap();
        let batch = QueryExecutor::new(&store).execute(&query).unwrap();
        let text = match batch.records[0].get("plan") {
            Some(Value::Property(PropertyValue::String(t))) => t.clone(),
            other => panic!("{other:?}"),
        };
        assert!(text.contains(expected), "{cypher} should render {expected}:\n{text}");
    }
}
