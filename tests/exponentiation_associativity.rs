//! `^` associates to the **left** in Cypher (#835).
//!
//! ```text
//! 2 ^ 3 ^ 2  is  (2^3)^2 = 64,  not  2^(3^2) = 512
//! ```
//!
//! This is where Cypher parts company with mathematical convention, and the
//! convention is what was implemented — with a comment in the Pratt table and
//! an assertion in `cypher_sweep_gaps.rs` both stating it as though it were the
//! rule. openCypher's grammar makes `PowerOfExpression` left-recursive, and two
//! `Precedence2` scenarios pin it independently.
//!
//! The *precedence* was already right: `4 ^ 3 * 2 ^ 3` is `(4^3) * (2^3)`. Only
//! the associativity was wrong, which is why those scenarios failed on one
//! column out of three.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn num(expr: &str) -> f64 {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(PropertyValue::Float(f))) => *f,
        Some(Value::Property(PropertyValue::Integer(i))) => *i as f64,
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

/// A chain of `^` groups left, and the two groupings differ in every case here.
#[test]
fn exponentiation_groups_left() {
    for (expr, left, right) in [
        ("2 ^ 3 ^ 2", 64.0, 512.0),
        ("4 ^ 6 ^ 3", 68719476736.0, f64::INFINITY),
        ("4 ^ 5 ^ 3", 1073741824.0, f64::INFINITY),
        ("4 ^ 1 ^ 3", 64.0, 4.0),
        ("2 ^ 2 ^ 2 ^ 2", 256.0, 65536.0),
    ] {
        let got = num(expr);
        assert_eq!(got, left, "{expr} should group left");
        assert_ne!(got, right, "{expr}: the two groupings agree, so this case proves nothing");
    }
}

/// The TCK's own scenarios, whose `a` and `b` columns already passed — included
/// so a later change to associativity cannot silently take precedence with it.
#[test]
fn precedence_over_multiplicative_and_additive_is_unchanged() {
    for (mult, right) in [("*", 512.0), ("/", 8.0), ("%", 0.0)] {
        assert_eq!(num(&format!("4 ^ 3 {mult} 2 ^ 3")), right, "{mult}");
        assert_eq!(num(&format!("(4 ^ 3) {mult} (2 ^ 3)")), right, "{mult} parenthesised");
    }
    for (add, right) in [("+", 72.0), ("-", 56.0)] {
        assert_eq!(num(&format!("4 ^ 3 {add} 2 ^ 3")), right, "{add}");
        assert_eq!(num(&format!("(4 ^ 3) {add} (2 ^ 3)")), right, "{add} parenthesised");
    }
}

/// Unary minus binds tighter than `^`: `-3 ^ 2` is `(-3)^2 = 9`.
#[test]
fn unary_minus_still_binds_tighter() {
    assert_eq!(num("-3 ^ 2"), 9.0);
    assert_eq!(num("(-3) ^ 2"), 9.0);
    assert_eq!(num("-(3 ^ 2)"), -9.0);
}
