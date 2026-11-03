//! Integer literals: every radix Cypher defines, and no crash at the edges.
//!
//! `parse_value` did `inner.as_str().parse().unwrap()`, so an out-of-range
//! literal **panicked**. That is reachable from any query string — a server
//! taking queries from anywhere could be stopped with `RETURN
//! 9223372036854775808`. Cypher calls an oversized literal a syntax error, and
//! a syntax error is what it should have been all along.
//!
//! The smallest integer is the awkward one: `-9223372036854775808` is
//! representable, but its magnitude alone is not, so the sign has to be folded
//! into the literal before it is parsed rather than applied afterwards.
//!
//! Hexadecimal and octal were simply missing (19 TCK scenarios).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn value_of(cypher: &str) -> Result<PropertyValue, String> {
    let q = parse_query(cypher).map_err(|e| e.to_string())?;
    let out = QueryExecutor::new(&GraphStore::new())
        .execute(&q)
        .map_err(|e| e.to_string())?;
    match out.records.first().and_then(|r| r.get("x")) {
        Some(Value::Property(p)) => Ok(p.clone()),
        other => Err(format!("expected a property, got {other:?}")),
    }
}

fn int_of(cypher: &str) -> i64 {
    match value_of(cypher) {
        Ok(PropertyValue::Integer(n)) => n,
        other => panic!("`{cypher}` should be an integer, got {other:?}"),
    }
}

#[test]
fn hexadecimal_literals_are_read_in_every_case() {
    assert_eq!(int_of("RETURN 0x1 AS x"), 1);
    assert_eq!(int_of("RETURN 0x0 AS x"), 0);
    assert_eq!(int_of("RETURN 0x162CD4F6 AS x"), 372_036_854);
    assert_eq!(int_of("RETURN 0x1A2b3c4D5E6f7 AS x"), 460_367_961_908_983);
    assert_eq!(int_of("RETURN 0x7FFFFFFFFFFFFFFF AS x"), i64::MAX);
    assert_eq!(int_of("RETURN -0x1 AS x"), -1);
}

#[test]
fn octal_literals_are_read() {
    assert_eq!(int_of("RETURN 0o1 AS x"), 1);
    assert_eq!(int_of("RETURN 0o0 AS x"), 0);
    assert_eq!(int_of("RETURN -0o0 AS x"), 0);
    assert_eq!(int_of("RETURN 0o2613152366 AS x"), 372_036_854);
    assert_eq!(int_of("RETURN -0o2613152366 AS x"), -372_036_854);
    assert_eq!(int_of("RETURN 0o777777777777777777777 AS x"), i64::MAX);
}

#[test]
fn the_smallest_integer_is_writable() {
    // The magnitude 9223372036854775808 is not a valid i64, so this only works
    // if the sign is part of the literal rather than an operator applied to it.
    assert_eq!(int_of("RETURN -9223372036854775808 AS x"), i64::MIN);
    assert_eq!(int_of("RETURN 9223372036854775807 AS x"), i64::MAX);
}

#[test]
fn an_out_of_range_literal_is_refused_rather_than_crashing() {
    // The assertion that matters is that control returns at all. `expect_err`
    // is only reached if the parser did not panic.
    for cypher in [
        "RETURN 9223372036854775808 AS x",
        "RETURN -9223372036854775809 AS x",
        "RETURN 99999999999999999999999999 AS x",
        "RETURN 0xFFFFFFFFFFFFFFFFF AS x",
        "MATCH (n) RETURN n LIMIT 99999999999999999999",
    ] {
        let err = value_of(cypher).expect_err(&format!("`{cypher}` must be refused"));
        assert!(
            err.contains("out of range"),
            "`{cypher}` was refused for the wrong reason: {err}"
        );
    }
}

#[test]
fn ordinary_numbers_are_unaffected() {
    assert_eq!(int_of("RETURN 0 AS x"), 0);
    assert_eq!(int_of("RETURN -5 AS x"), -5);
    assert_eq!(int_of("RETURN 42 AS x"), 42);
    assert_eq!(value_of("RETURN 1.5 AS x"), Ok(PropertyValue::Float(1.5)));
    assert_eq!(value_of("RETURN 1e3 AS x"), Ok(PropertyValue::Float(1000.0)));
    assert_eq!(value_of("RETURN -1.5 AS x"), Ok(PropertyValue::Float(-1.5)));
}
