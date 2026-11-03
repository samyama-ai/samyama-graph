//! Lists order lexicographically, and NaN orders as false (#855).
//!
//! Two defects that both produced **null** — the answer the engine also gives
//! for values it genuinely cannot compare, so neither looked like a bug.
//!
//! `cypher_ordering` had no `Array` arm at all, so every list comparison fell
//! to `_ => None`. And `partial_cmp` returns `None` for NaN, which the same
//! branch maps to null; Cypher wants false there.
//!
//! The line between them is the interesting part: **incomparable is null, NaN
//! is false, and a NaN against a string is null again** — because comparing
//! across types is settled before NaN is considered. A blanket NaN rule cost
//! exactly that one scenario.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `Some(bool)` for a definite answer, `None` for null.
fn truth(expr: &str) -> Option<bool> {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(PropertyValue::Boolean(b))) => Some(*b),
        _ => None,
    }
}

/// The TCK's rows, plus the ordinary cases that had also been answering null.
#[test]
fn lists_order_lexicographically_then_by_length() {
    for (expr, want) in [
        ("[1, 0] >= [1]", Some(true)),
        ("[1, 2] >= [3, null]", Some(false)),
        ("[1, null] >= [1]", Some(true)),
        ("[1] >= [1]", Some(true)),
        ("[2] >= [1]", Some(true)),
        ("[1] >= [2]", Some(false)),
        ("[1, 2] > [1]", Some(true)),
        ("[1] < [1, 0]", Some(true)),
        ("[] < [1]", Some(true)),
        ("['a', 'b'] < ['a', 'c']", Some(true)),
    ] {
        assert_eq!(truth(expr), want, "{expr}");
    }
}

/// A null only makes the answer null when it is at a position that has to be
/// compared. `[1, null] >= [1]` is decided by the length.
#[test]
fn a_null_matters_only_where_it_is_compared() {
    assert_eq!(truth("[1, null] >= [1]"), Some(true));
    assert_eq!(truth("[1, 2] >= [3, null]"), Some(false));
    // Here it does have to be compared.
    assert_eq!(truth("[1, null] >= [1, 2]"), None);
    assert_eq!(truth("[null] >= [1]"), None);
}

/// All four operators are false against NaN, `>=` against itself included.
#[test]
fn nan_orders_as_false_against_a_number() {
    for other in ["0.0 / 0.0", "1", "1.0", "-1"] {
        for op in [">", ">=", "<", "<="] {
            assert_eq!(
                truth(&format!("0.0 / 0.0 {op} {other}")),
                Some(false),
                "0.0/0.0 {op} {other}"
            );
            assert_eq!(
                truth(&format!("{other} {op} 0.0 / 0.0")),
                Some(false),
                "{other} {op} 0.0/0.0"
            );
        }
    }
}

/// **But NaN against a non-number is still null**, because comparing across
/// types is settled first. This is the row a blanket NaN rule breaks.
#[test]
fn nan_against_another_type_is_still_null() {
    for other in ["'a'", "true", "[1]", "null"] {
        assert_eq!(truth(&format!("0.0 / 0.0 < {other}")), None, "0.0/0.0 < {other}");
        assert_eq!(truth(&format!("0.0 / 0.0 >= {other}")), None, "0.0/0.0 >= {other}");
    }
}

/// Ordinary numeric and string ordering is untouched — a change that made
/// every comparison false or null would satisfy several tests above.
#[test]
fn ordinary_ordering_is_unchanged() {
    assert_eq!(truth("1 < 2"), Some(true));
    assert_eq!(truth("2.5 >= 2"), Some(true));
    assert_eq!(truth("'a' < 'b'"), Some(true));
    assert_eq!(truth("1 < 'a'"), None);
    assert_eq!(truth("1.0 / 0.0 > 1"), Some(true));
}
