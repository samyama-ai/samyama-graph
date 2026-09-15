//! Four small expression divergences from Neo4j (#1241), and the float format
//! behind two of them.
//!
//! - `'a' + 1` was a type error; Cypher concatenates a string and a number.
//! - `toString(1.0)` was "1" and `toString(1e20)` a 21-digit integer; Neo4j
//!   writes floats as Java does: "1.0", "1.0E20".
//! - `substring('abc', -1)` answered "": the negative start was cast to an
//!   unsigned index and wrapped. Neo4j refuses a negative start, length, or
//!   `left`/`right` count.
//! - `coalesce()` answered null; Neo4j refuses it.
//!
//! Every expected value here is Neo4j 2026.04.0's.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(q: &str) -> Result<String, String> {
    let s = GraphStore::new();
    let p = parse_query(q).map_err(|e| e.to_string())?;
    let out = QueryExecutor::new(&s).execute(&p).map_err(|e| e.to_string())?;
    Ok(match out.records[0].get("x") {
        Some(Value::Property(PropertyValue::String(s))) => format!("\"{s}\""),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => "null".into(),
        Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
        other => format!("{other:?}"),
    })
}

fn is(q: &str, expected: &str) {
    assert_eq!(run(q).unwrap_or_else(|e| panic!("`{q}`: {e}")), expected, "{q}");
}

fn refused(q: &str) {
    assert!(run(q).is_err(), "`{q}` should be refused, got {:?}", run(q));
}

#[test]
fn a_string_and_a_number_concatenate() {
    is("RETURN 'a' + 1 AS x", "\"a1\"");
    is("RETURN 1 + 'a' AS x", "\"1a\"");
    is("RETURN 'a' + 10000000000 AS x", "\"a10000000000\"");
    is("RETURN 'a' + 1.5 AS x", "\"a1.5\"");
    is("RETURN 'a' + 1.0 AS x", "\"a1.0\"");
    is("RETURN 1.0 + 'a' AS x", "\"1.0a\"");
    is("RETURN 'x' + 0.1 AS x", "\"x0.1\"");
    is("RETURN 'x' + 1e20 AS x", "\"x1.0E20\"");
    is("RETURN 'x' + 2.5e-8 AS x", "\"x2.5E-8\"");
    // Null still propagates, and a boolean is still not a string operand.
    is("RETURN 'a' + null AS x", "null");
    refused("RETURN 'a' + true AS x");
}

#[test]
fn floats_are_written_as_neo4j_writes_them() {
    for (q, want) in [
        ("RETURN toString(1.0) AS x", "\"1.0\""),
        ("RETURN toString(-2.5) AS x", "\"-2.5\""),
        ("RETURN toString(0.001) AS x", "\"0.001\""),
        ("RETURN toString(0.0001) AS x", "\"1.0E-4\""),
        ("RETURN toString(123456.789) AS x", "\"123456.789\""),
        ("RETURN toString(9999999.0) AS x", "\"9999999.0\""),
        ("RETURN toString(1e7) AS x", "\"1.0E7\""),
        ("RETURN toString(1e20) AS x", "\"1.0E20\""),
        ("RETURN toString(1.5e-7) AS x", "\"1.5E-7\""),
        ("RETURN toString(-0.0) AS x", "\"-0.0\""),
        ("RETURN toString(0.1 + 0.2) AS x", "\"0.30000000000000004\""),
    ] {
        is(q, want);
    }
}

#[test]
fn a_negative_start_length_or_count_is_refused() {
    refused("RETURN substring('abc', -1) AS x");
    refused("RETURN substring('abc', 1, -1) AS x");
    refused("RETURN left('abc', -1) AS x");
    refused("RETURN right('abc', -1) AS x");
    // Zero is not negative.
    is("RETURN substring('abc', 0, 0) AS x", "\"\"");
    is("RETURN left('abc', 0) AS x", "\"\"");
    is("RETURN substring('abc', 1) AS x", "\"bc\"");
    is("RETURN substring('abc', 5) AS x", "\"\"");
}

#[test]
fn coalesce_needs_an_argument() {
    refused("RETURN coalesce() AS x");
    is("RETURN coalesce(null) AS x", "null");
    is("RETURN coalesce(null, 3) AS x", "3");
}
