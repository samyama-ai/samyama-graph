//! A scalar function given null returns null — it does not raise a type error.
//!
//! Cypher's rule is that null means "unknown", so asking a question about it
//! yields an unknown answer rather than a refusal. `labels(null)` is not a
//! caller mistake to be reported; it is a row where the node was optional and
//! did not match.
//!
//! The engine instead raised `TypeError("labels() requires a node")` and
//! friends, which aborts the whole query. A single OPTIONAL MATCH that misses
//! therefore killed a query that Cypher answers with a null column. Twenty-two
//! openCypher TCK scenarios fail on this rule; these tests pin the shape of it
//! rather than one function at a time, because the fix belongs at the single
//! point where scalar functions are dispatched, not in each arm.
//!
//! `coalesce` and `exists` are the deliberate exceptions: both exist precisely
//! to be asked about null, so propagating would defeat them.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn result_of(cypher: &str) -> Value {
    let q = parse_query(cypher).expect("query should parse");
    let out = QueryExecutor::new(&GraphStore::new())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run, not error: {e}"));
    out.records[0].get("res").cloned().expect("column res")
}

fn assert_null(cypher: &str) {
    let got = result_of(cypher);
    assert!(
        matches!(got, Value::Null | Value::Property(PropertyValue::Null)),
        "`{cypher}` should be null, got {got:?}"
    );
}

#[test]
fn graph_functions_on_null_are_null() {
    assert_null("RETURN labels(null) AS res");
    assert_null("RETURN type(null) AS res");
    assert_null("RETURN properties(null) AS res");
    assert_null("RETURN keys(null) AS res");
    assert_null("RETURN id(null) AS res");
    assert_null("RETURN startNode(null) AS res");
    assert_null("RETURN endNode(null) AS res");
}

#[test]
fn list_and_string_functions_on_null_are_null() {
    assert_null("RETURN size(null) AS res");
    assert_null("RETURN head(null) AS res");
    assert_null("RETURN last(null) AS res");
    assert_null("RETURN tail(null) AS res");
    assert_null("RETURN toString(null) AS res");
    assert_null("RETURN toInteger(null) AS res");
    assert_null("RETURN toUpper(null) AS res");
    assert_null("RETURN trim(null) AS res");
    assert_null("RETURN reverse(null) AS res");
}

#[test]
fn numeric_functions_on_null_are_null() {
    assert_null("RETURN abs(null) AS res");
    assert_null("RETURN ceil(null) AS res");
    assert_null("RETURN sqrt(null) AS res");
    assert_null("RETURN sign(null) AS res");
}

#[test]
fn a_null_anywhere_in_the_arguments_propagates() {
    // Not just the first argument: the rule is about the call, not arg zero.
    assert_null("RETURN substring('abc', null) AS res");
    assert_null("RETURN replace('abc', null, 'x') AS res");
    assert_null("RETURN split(null, ',') AS res");
    assert_null("RETURN range(1, null) AS res");
}

#[test]
fn coalesce_and_exists_do_not_propagate() {
    // These two are the reason the guard needs an exception list at all:
    // they are the functions whose entire job is to be asked about null.
    assert_eq!(
        result_of("RETURN coalesce(null, 7) AS res"),
        Value::Property(PropertyValue::Integer(7)),
        "coalesce must look past the null rather than become it"
    );
    let got = result_of("RETURN exists(null) AS res");
    assert!(
        matches!(got, Value::Property(PropertyValue::Boolean(false))),
        "exists(null) should be false, got {got:?}"
    );
}
