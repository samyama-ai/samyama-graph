//! A week's fraction carries, and `toInteger` reads a float-valued string
//! (#885).
//!
//! ```text
//! duration('P2.5W')     P17D,   want P17DT12H
//! toInteger('2.9')      null,   want 2
//! ```
//!
//! Both answer **null or a smaller value** where a number belongs, which is
//! the shape that does not look like a defect: `P17D` is a real duration and
//! `null` is what `toInteger` legitimately gives for `'foo'`.
//!
//! The week case came from #853's parser rewrite, which gave `D` and `M` the
//! carry and left `W` scaling to a whole unit — while the *map* constructor had
//! it right from #829. So both spellings are asserted here, since the working
//! one is what hid the broken one.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn value(expr: &str) -> PropertyValue {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        // `toInteger(null)` yields the bare `Value::Null` rather than a
        // `PropertyValue::Null`; both mean null here.
        Some(Value::Null) | None => PropertyValue::Null,
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

/// The string and map spellings must agree, on fractions and on whole units.
#[test]
fn a_fractional_week_carries_into_the_clock() {
    for (text, map, want) in [
        ("P2.5W", "{weeks: 2.5}", "P17DT12H"),
        ("P2W", "{weeks: 2}", "P14D"),
        ("P1.5W", "{weeks: 1.5}", "P10DT12H"),
        ("P0.5W", "{weeks: 0.5}", "P3DT12H"),
    ] {
        assert_eq!(value(&format!("duration('{text}')")).to_cypher_string(), want, "{text}");
        assert_eq!(value(&format!("duration({map})")).to_cypher_string(), want, "{map}");
    }
}

/// Other fractional units keep working — the fix must not have moved only `W`
/// while breaking its neighbours.
#[test]
fn the_other_fractional_units_are_unchanged() {
    assert_eq!(value("duration('P0.75M')").to_cypher_string(), "P22DT19H51M49.5S");
    assert_eq!(value("duration('P1.5D')").to_cypher_string(), "P1DT12H");
    assert_eq!(value("duration('PT1.5H')").to_cypher_string(), "PT1H30M");
}

/// A string holding a float converts, truncating; a string holding nothing
/// numeric is still null.
#[test]
fn to_integer_reads_a_float_valued_string() {
    for (expr, want) in [
        ("toInteger('2')", Some(2)),
        ("toInteger('2.9')", Some(2)),
        ("toInteger('1.7')", Some(1)),
        ("toInteger('-1.7')", Some(-1)),
        ("toInteger(2.9)", Some(2)),
        ("toInteger(2)", Some(2)),
        // Not a number: still null, which is the answer that means exactly that.
        ("toInteger('foo')", None),
        ("toInteger('')", None),
        ("toInteger(null)", None),
    ] {
        let got = value(expr);
        match want {
            Some(n) => assert_eq!(got, PropertyValue::Integer(n), "{expr}"),
            None => assert_eq!(got, PropertyValue::Null, "{expr}"),
        }
    }
}
