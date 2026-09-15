//! Two answers the TCK's parameter scenarios exposed once its harness passed
//! parameters; neither is about parameters.
//!
//! `x IN null` raised "IN requires a list on the right". Cypher's answer is
//! null, the same as for any other comparison with null (TCK Null3 [4]).
//!
//! A WITH that aggregates without grouping keys returned **no row** over empty
//! input. An aggregate with no grouping keys always has one group -- the
//! whole input -- so `WITH avg(p.age) AS a` over nothing is one row with
//! `a = null`, and `count(*)` is 0. RETURN already did this; WITH did not
//! (TCK With6 [5], WithOrderBy4 [16]).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Every value of column `c`, one per row.
fn rows(s: &GraphStore, q: &str) -> Vec<String> {
    let parsed = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&parsed).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    out.records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) => "null".into(),
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn in_a_null_list_is_null() {
    let s = GraphStore::new();
    assert_eq!(rows(&s, "RETURN null IN null AS c"), vec!["null"]);
    assert_eq!(rows(&s, "RETURN 1 IN null AS c"), vec!["null"]);
}

#[test]
fn with_aggregate_over_nothing_is_one_row() {
    let s = GraphStore::new();
    assert_eq!(rows(&s, "MATCH (p) WITH avg(p.age) AS c RETURN c"), vec!["null"]);
    assert_eq!(rows(&s, "MATCH (p) WITH count(p) AS c RETURN c"), vec!["0"]);
}

/// TCK With6 [5], with the parameter as its literal.
#[test]
fn with_nested_aggregate_over_nothing_is_one_row() {
    let s = GraphStore::new();
    assert_eq!(rows(&s, "MATCH (p) WITH 38 + avg(p.age) - 1000 AS c RETURN c"), vec!["null"]);
}

/// TCK WithOrderBy4 [16].
#[test]
fn with_aggregate_ordered_by_an_aggregate_over_nothing_is_one_row() {
    let s = GraphStore::new();
    assert_eq!(
        rows(&s, "MATCH (p) WITH avg(p.age) AS c ORDER BY 38 + avg(p.age) - 1000 RETURN c"),
        vec!["null"]
    );
}

/// Grouping keys over nothing are no groups: zero rows stays zero rows.
#[test]
fn grouped_with_over_nothing_is_still_no_rows() {
    let s = GraphStore::new();
    assert!(rows(&s, "MATCH (p) WITH p.k AS k, count(*) AS c RETURN c").is_empty());
}

/// Control: RETURN already answered one row.
#[test]
fn return_aggregate_over_nothing_is_one_row() {
    let s = GraphStore::new();
    assert_eq!(rows(&s, "MATCH (p) RETURN count(p) AS c"), vec!["0"]);
}
