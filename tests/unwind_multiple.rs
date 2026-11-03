//! Consecutive `UNWIND` clauses (openCypher TCK).
//!
//! `UNWIND [1,2] AS a UNWIND [3,4] AS b` is a cross product — four rows — and
//! did not parse at all. The TCK leans on the three-way form to enumerate the
//! truth tables for `AND`, `OR` and `XOR` over `{true, false, null}`, so a
//! single missing construct accounted for the whole of `Boolean5` and much of
//! `Comparison3`.
//!
//! The tests assert row *counts and contents*, because the failure mode of a
//! chained operator is not "no rows" but "the wrong number of them" — an
//! implementation that overwrote the previous binding instead of expanding it
//! would return 2 rows here and look plausible.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn pairs(cypher: &str, cols: &[&str]) -> Vec<Vec<i64>> {
    let store = GraphStore::new();
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(&store).execute(&q).expect("query should run");
    let mut out: Vec<Vec<i64>> = batch
        .records
        .iter()
        .map(|r| {
            cols.iter()
                .map(|c| match r.get(c) {
                    Some(Value::Property(PropertyValue::Integer(n))) => *n,
                    other => panic!("expected an integer in {c}, got {other:?}"),
                })
                .collect()
        })
        .collect();
    out.sort();
    out
}

#[test]
fn two_unwinds_produce_the_cross_product() {
    assert_eq!(
        pairs("UNWIND [1, 2] AS a UNWIND [3, 4] AS b RETURN a, b", &["a", "b"]),
        vec![vec![1, 3], vec![1, 4], vec![2, 3], vec![2, 4]],
    );
}

#[test]
fn three_unwinds_produce_the_three_way_cross_product() {
    // The shape the TCK uses for truth tables. 2 x 2 x 2.
    let rows = pairs(
        "UNWIND [1, 2] AS a UNWIND [3, 4] AS b UNWIND [5, 6] AS c RETURN a, b, c",
        &["a", "b", "c"],
    );
    assert_eq!(rows.len(), 8);
    // Every combination appears exactly once.
    let mut expected: Vec<Vec<i64>> = Vec::new();
    for a in [1, 2] {
        for b in [3, 4] {
            for c in [5, 6] {
                expected.push(vec![a, b, c]);
            }
        }
    }
    expected.sort();
    assert_eq!(rows, expected);
}

#[test]
fn a_later_unwind_expands_rather_than_replaces() {
    // The plausible-looking wrong answer: an implementation that rebinds
    // instead of chaining returns the length of the *last* list.
    assert_eq!(
        pairs("UNWIND [1, 2, 3] AS a UNWIND [9] AS b RETURN a, b", &["a", "b"]).len(),
        3,
    );
    assert_eq!(
        pairs("UNWIND [1] AS a UNWIND [7, 8, 9] AS b RETURN a, b", &["a", "b"]).len(),
        3,
    );
}

#[test]
fn unwinding_an_empty_list_yields_no_rows() {
    // A cross product with the empty set is empty, however many rows the
    // earlier clauses produced.
    assert!(pairs("UNWIND [1, 2] AS a UNWIND [] AS b RETURN a, b", &["a", "b"]).is_empty());
}

#[test]
fn a_single_unwind_is_unchanged() {
    assert_eq!(pairs("UNWIND [1, 2, 3] AS a RETURN a", &["a"]).len(), 3);
}

#[test]
fn a_boolean_truth_table_over_three_variables_has_twenty_seven_rows() {
    // The actual TCK shape, with nulls: 3 x 3 x 3.
    let store = GraphStore::new();
    let q = parse_query(
        "UNWIND [true, false, null] AS a UNWIND [true, false, null] AS b \
         UNWIND [true, false, null] AS c RETURN a, b, c",
    )
    .expect("query should parse");
    let batch = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(batch.records.len(), 27);
}
