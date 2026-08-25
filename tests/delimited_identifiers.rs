//! Backtick-delimited property names (#847).
//!
//! Cypher spells a name that is a keyword, starts with a digit, or contains
//! punctuation by delimiting it in backticks, doubling any literal backtick
//! inside. The grammar had no such rule.
//!
//! The fix has two halves, and the second is the one that matters: the rule
//! stays **atomic**, so `as_str()` returns the text with its delimiters still
//! on, and every read site has to strip them. Fourteen sites are spelled
//! `Rule::property_key => … .as_str()` and a grep finds them all. The
//! fifteenth, `parse_property_access`, indexes positionally — and with it
//! missed, `map.`name`` *parsed* and then looked up a key literally named
//! `` `name` ``. Null, from a query that had just been taught to accept it,
//! which is worse than the parse error it replaced.
//!
//! So every case below is asserted through **both** spellings: a map in a
//! variable (`property_access`) and a map literal (`member_op`, which desugars
//! to an index). The literal form worked from the first change alone, which is
//! exactly what made the variable form look like a separate feature.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn value(cypher: &str) -> Option<PropertyValue> {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
        Some(Value::Property(p)) => Some(p.clone()),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

fn s(text: &str) -> Option<PropertyValue> {
    Some(PropertyValue::String(text.to_string()))
}

/// Both spellings, every row of the TCK's table.
fn both_ways(map: &str, key: &str, want: Option<PropertyValue>) {
    assert_eq!(
        value(&format!("WITH {map} AS map RETURN map.{key} AS r")),
        want,
        "through a variable: map.{key}"
    );
    assert_eq!(
        value(&format!("RETURN {map}.{key} AS r")),
        want,
        "on a literal: {map}.{key}"
    );
}

#[test]
fn a_delimited_name_reads_the_field_it_names() {
    let m = "{name: 'Mats', nome: 'Pontus'}";
    both_ways(m, "`name`", s("Mats"));
    both_ways(m, "`nome`", s("Pontus"));
    // A delimited name that matches a *value* rather than a key is still absent.
    both_ways(m, "`Mats`", None);
}

/// **Keywords as keys**, which is the reason delimiting exists — and `null` and
/// `NULL` are two different names.
#[test]
fn a_keyword_may_be_a_field_name_and_case_matters() {
    let m = "{null: 'Mats', NULL: 'Pontus'}";
    both_ways(m, "`null`", s("Mats"));
    both_ways(m, "`NULL`", s("Pontus"));
    both_ways("{name: 'Mats', nome: 'Pontus'}", "`null`", None);
}

/// Punctuation, spaces, and an escaped backtick.
#[test]
fn punctuation_and_escaped_backticks() {
    both_ways("{`a b`: 1}", "`a b`", Some(PropertyValue::Integer(1)));
    both_ways("{`a-b`: 1}", "`a-b`", Some(PropertyValue::Integer(1)));
    both_ways("{`a``b`: 1}", "`a``b`", Some(PropertyValue::Integer(1)));
}

/// Undelimited names are unchanged — a fix that stripped the first and last
/// character unconditionally would pass everything above.
#[test]
fn undelimited_names_are_unchanged() {
    both_ways("{name: 'Mats'}", "name", s("Mats"));
    both_ways("{n: 'x'}", "n", s("x"));
    assert_eq!(
        value("WITH {name: 'Mats'} AS map RETURN map.missing AS r"),
        None
    );
}
