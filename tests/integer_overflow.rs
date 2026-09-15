//! Integer arithmetic that leaves the 64-bit range is an error, not a wrapped
//! value.
//!
//! `RETURN 9223372036854775807 + 1` answered -9223372036854775808 in a release
//! build (wrapping arithmetic) and panicked in a debug one. Neo4j 2026.04.0
//! raises 22003 "numeric value out of range" for it, for `-9223372036854775807
//! - 2`, and for `4611686018427387904 * 2`. A result right at the edge of the
//! range is still a result.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(q: &str) -> Result<String, String> {
    let s = GraphStore::new();
    let p = parse_query(q).map_err(|e| e.to_string())?;
    let out = QueryExecutor::new(&s).execute(&p).map_err(|e| e.to_string())?;
    Ok(match out.records[0].get("x") {
        Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
        other => format!("{other:?}"),
    })
}

fn out_of_range(q: &str) {
    let err = run(q).expect_err(q);
    assert!(err.contains("out of range"), "{q}: {err}");
}

#[test]
fn leaving_the_range_is_an_error() {
    out_of_range("RETURN 9223372036854775807 + 1 AS x");
    out_of_range("RETURN -9223372036854775807 - 2 AS x");
    out_of_range("RETURN 4611686018427387904 * 2 AS x");
}

/// The two quotients and remainders that leave the range: only i64::MIN
/// divided by -1 has no 64-bit answer.
#[test]
fn the_one_division_that_overflows() {
    out_of_range("WITH -9223372036854775807 - 1 AS m RETURN m / -1 AS x");
    assert_eq!(run("WITH -9223372036854775807 - 1 AS m RETURN m % -1 AS x").unwrap(), "0");
}

#[test]
fn negating_the_minimum_is_an_error() {
    out_of_range("WITH -9223372036854775807 - 1 AS m RETURN -m AS x");
}

#[test]
fn the_edges_of_the_range_are_still_results() {
    assert_eq!(run("RETURN 9223372036854775806 + 1 AS x").unwrap(), "9223372036854775807");
    assert_eq!(run("RETURN -9223372036854775807 - 1 AS x").unwrap(), "-9223372036854775808");
    assert_eq!(run("RETURN 3037000499 * 3037000499 AS x").unwrap(), "9223372030926249001");
    assert_eq!(run("RETURN -7 / 2 AS x").unwrap(), "-3");
    assert_eq!(run("RETURN -7 % 3 AS x").unwrap(), "-1");
}
