//! Float literals: overflow is refused, negative zero prints unsigned (#883).
//!
//! ```text
//! RETURN 1.34E999   -- returned inf; must be a compile-time error
//! RETURN -0.0       -- printed "-0"; Cypher shows 0.0
//! ```
//!
//! Rust's `str::parse::<f64>` returns `Ok(inf)` on overflow rather than an
//! error, so the literal became a perfectly usable infinity.
//!
//! Only a **literal**. An infinity a computation produces is a legitimate
//! value, and `a_computed_infinity_is_still_allowed` pins that — a fix that
//! rejected infinity everywhere would satisfy the overflow cases and break
//! division.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn value(expr: &str) -> Result<PropertyValue, String> {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).map_err(|e| format!("parse: {e:?}"))?;
    let batch = QueryExecutor::new(&store).execute(&q).map_err(|e| format!("exec: {e:?}"))?;
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => Ok(p.clone()),
        other => Err(format!("got {other:?}")),
    }
}

/// An over-large literal is refused before it runs.
#[test]
fn an_overflowing_literal_is_refused() {
    for expr in ["1.34E999", "-1.34E999", "1e400", "9" .repeat(400).as_str()] {
        assert!(value(expr).is_err(), "accepted `{expr}`");
    }
}

/// **A computed infinity is still a value.** This is the half a blanket
/// "reject infinity" rule would break.
#[test]
fn a_computed_infinity_is_still_allowed() {
    assert_eq!(value("1.0 / 0.0"), Ok(PropertyValue::Float(f64::INFINITY)));
    assert_eq!(value("-1.0 / 0.0"), Ok(PropertyValue::Float(f64::NEG_INFINITY)));
}

/// Literals that fit are unaffected — a fix that rejected large-but-finite
/// values would pass the tests above.
#[test]
fn large_but_finite_literals_still_parse() {
    for expr in ["1.0E308", "1.7e308", "-1.0E308", "0.5", "1.0", "3.14159"] {
        assert!(value(expr).is_ok(), "refused `{expr}`");
    }
}

/// Negative zero renders without a sign, and keeps its numeric identity.
#[test]
fn negative_zero_prints_unsigned() {
    assert_eq!(value("-0.0").unwrap().to_string(), "0");
    assert_eq!(value("0.0").unwrap().to_string(), "0");
    assert_eq!(value("-.0").unwrap().to_string(), "0");
    // Still equal, and still not equal to something else.
    assert_eq!(
        value("-0.0 = 0.0"),
        Ok(PropertyValue::Boolean(true)),
        "-0.0 and 0.0 must compare equal"
    );
    // A non-zero negative keeps its sign.
    assert_eq!(value("-1.5").unwrap().to_string(), "-1.5");
}
