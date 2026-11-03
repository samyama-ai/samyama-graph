//! A list comprehension over a list holding **entities** (#800).
//!
//! ```text
//! MATCH (n)-[r]->() RETURN [x IN [r, 1] | type(x)]
//!   expected TypeError, got []
//! ```
//!
//! A `PropertyValue` list cannot hold a node or a relationship, so `[r, 1]` is
//! a `Value::List`. `eval_list_comprehension` matched only the `PropertyValue`
//! form and fell to `_ => Ok(Array(vec![]))`.
//!
//! The empty list is the dangerous part. It is indistinguishable from a
//! comprehension that legitimately filtered everything out, so the type error
//! inside never surfaced — and neither did any *other* error raised by the map
//! expression. That is why fixing this moved **25** scenarios across eight
//! features when the failure that led here named only `type()`: every function
//! reached through a comprehension over entities was having its errors
//! swallowed.
//!
//! Same shape as #789 and #791: a value that is wrong and looks exactly like a
//! legitimate one.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn store_with_edge() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:A)-[:T]->(b:B)").expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

fn run(cypher: &str) -> Result<Value, String> {
    let store = store_with_edge();
    let q = parse_query(cypher).map_err(|e| format!("parse: {e:?}"))?;
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .map_err(|e| format!("exec: {e:?}"))?;
    Ok(batch
        .records
        .first()
        .and_then(|r| r.get("r"))
        .cloned()
        .unwrap_or(Value::Null))
}

/// An error inside the map expression **surfaces** instead of becoming `[]`.
#[test]
fn an_error_in_the_map_expression_is_not_swallowed() {
    let e = run("MATCH (n)-[r]->() RETURN [x IN [r, 1] | type(x)] AS r")
        .expect_err("type() on an integer must raise");
    assert!(e.contains("type()"), "{e}");
}

/// A comprehension over entities **works** — the fix is not "reject entity
/// lists", it is "iterate them properly".
#[test]
fn a_comprehension_over_entities_evaluates() {
    match run("MATCH (n)-[r]->() RETURN [x IN [r] | type(x)] AS r") {
        Ok(Value::Property(p)) => assert_eq!(p.to_cypher_string(), "[\"T\"]"),
        other => panic!("got {other:?}"),
    }
    // Nodes too.
    assert!(run("MATCH (n) RETURN [x IN [n] | labels(x)] AS r").is_ok());
}

/// Ordinary comprehensions are undisturbed.
#[test]
fn ordinary_comprehensions_still_work() {
    for q in [
        "MATCH (n) RETURN [x IN [1,2,3] | x * 2] AS r",
        "MATCH (n) RETURN [x IN [1,2,3] WHERE x > 1 | x] AS r",
        "MATCH (n) RETURN [x IN [] | x] AS r",
        "MATCH (n) RETURN [x IN ['a','b'] | toUpper(x)] AS r",
        // An all-float literal parses as a Vector (#605) and must keep working.
        "MATCH (n) RETURN [x IN [1.0, 2.0] | x] AS r",
    ] {
        assert!(run(q).is_ok(), "{q}");
    }
}

/// Null in, null out — not an empty list, and not an error.
///
/// A null arrives as `Value::Property(PropertyValue::Null)`, not `Value::Null`;
/// both spellings exist and matching only the latter made this test fail
/// against correct behaviour. Accepting either is what the assertion means.
#[test]
fn a_null_list_gives_null() {
    let got = run("MATCH (n) RETURN [x IN null | x] AS r").expect("null is not an error");
    assert!(
        matches!(got, Value::Null | Value::Property(samyama::graph::PropertyValue::Null)),
        "expected null, got {got:?}"
    );
}

/// **Something that is not a list at all is an error**, where it used to be an
/// empty list. This is the half that turns a silent wrong answer into a
/// report.
#[test]
fn a_non_list_is_refused() {
    let e = run("MATCH (n) RETURN [x IN 123 | x] AS r").expect_err("123 is not a list");
    assert!(e.contains("needs a list"), "{e}");
}
