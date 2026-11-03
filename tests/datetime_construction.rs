//! Building a `datetime` from a map (#595).
//!
//! `datetime({epochMillis: 1700000000000})` returned the **epoch**. The map
//! form handled `year`/`month`/`day`/`hour`/`minute`/`second` and nothing else,
//! so a map naming only `epochMillis` matched no branch, fell through to the
//! defaults, and produced `1970-01-01T00:00:00`.
//!
//! Silent, and in a value type where wrongness hides: `1970` reads as a
//! plausible date rather than a failure, and every comparison against it is
//! quietly wrong. A millisecond epoch is also what a machine caller has
//! (`Axiom 4`), so it is the natural way to construct a timestamp.
//!
//! The second half of the fix matters as much as the first: a map naming
//! **none** of the understood keys now errors. Returning the epoch for it is
//! exactly how the missing arm stayed invisible.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn one(cypher: &str) -> Result<PropertyValue, String> {
    let store = GraphStore::new();
    let query = parse_query(cypher).map_err(|e| format!("parse: {e:?}"))?;
    let batch = QueryExecutor::new(&store)
        .execute(&query)
        .map_err(|e| format!("exec: {e:?}"))?;
    Ok(match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        _ => PropertyValue::Null,
    })
}

const NOV_2023: i64 = 1_700_000_000_000;

/// The instant `NOV_2023` names, as the type `datetime()` now returns.
///
/// Since #689 `datetime()` produces a `ZonedDateTime` rather than a bare
/// millisecond `DateTime`. The *instant* asserted here is unchanged — only the
/// type it is carried in — so these tests still check what #595 was about.
fn nov_2023() -> PropertyValue {
    PropertyValue::ZonedDateTime {
        secs: NOV_2023 / 1000,
        nanos: 0,
        offset_seconds: 0,
        zone: None,
    }
}

#[test]
fn epoch_millis_yields_that_instant() {
    assert_eq!(
        one("RETURN datetime({epochMillis: 1700000000000}) AS r").unwrap(),
        nov_2023()
    );
}

#[test]
fn it_round_trips() {
    // The property that makes the value usable rather than merely non-1970.
    assert_eq!(
        one("WITH datetime({epochMillis: 1700000000000}) AS d RETURN d.epochMillis AS r").unwrap(),
        PropertyValue::Integer(NOV_2023)
    );
}

#[test]
fn its_components_are_right() {
    // 1700000000000 is 2023-11-14T22:13:20Z.
    for (expr, expected) in [
        ("d.year", 2023),
        ("d.month", 11),
        ("d.day", 14),
        ("d.hour", 22),
        ("d.minute", 13),
        ("d.second", 20),
    ] {
        let cypher = format!("WITH datetime({{epochMillis: 1700000000000}}) AS d RETURN {expr} AS r");
        assert_eq!(one(&cypher).unwrap(), PropertyValue::Integer(expected), "{expr}");
    }
}

#[test]
fn epoch_seconds_works_too() {
    assert_eq!(
        one("RETURN datetime({epochSeconds: 1700000000}) AS r").unwrap(),
        nov_2023()
    );
}

#[test]
fn the_calendar_form_still_works() {
    // The branch that already existed, and which the new arms sit in front of.
    let got = one("WITH datetime({year: 2020, month: 2, day: 29}) AS d RETURN d.year AS r").unwrap();
    assert_eq!(got, PropertyValue::Integer(2020));
    let day = one("WITH datetime({year: 2020, month: 2, day: 29}) AS d RETURN d.day AS r").unwrap();
    assert_eq!(day, PropertyValue::Integer(29));
}

#[test]
fn the_string_form_still_works() {
    let got = one("WITH datetime(\"2023-11-14T22:13:20Z\") AS d RETURN d.year AS r").unwrap();
    assert_eq!(got, PropertyValue::Integer(2023));
}

#[test]
fn a_map_of_unknown_keys_errors_rather_than_returning_1970() {
    // This is the half that keeps the first half honest. Before, *any*
    // unrecognised map produced the epoch, which is why a missing `epochMillis`
    // arm could go unnoticed.
    let err = one("RETURN datetime({nonsense: 1}) AS r").unwrap_err();
    assert!(err.contains("understands none of the keys"), "{err}");
    assert!(err.contains("nonsense"), "the message should name what was given: {err}");
}

#[test]
fn an_empty_map_errors() {
    let err = one("RETURN datetime({}) AS r").unwrap_err();
    assert!(err.contains("understands none of the keys"), "{err}");
}

#[test]
fn a_partial_calendar_map_is_still_accepted() {
    // `{year: 2020}` names a key the constructor understands, so the remaining
    // components default. That behaviour predates this change and stays.
    assert_eq!(
        one("WITH datetime({year: 2020}) AS d RETURN d.year AS r").unwrap(),
        PropertyValue::Integer(2020)
    );
}

#[test]
fn epoch_millis_wins_over_calendar_components() {
    // Both given: the epoch is a complete specification, so it decides. Stated
    // as a test because the precedence is a choice, not an accident.
    assert_eq!(
        one("RETURN datetime({epochMillis: 1700000000000, year: 1999}) AS r").unwrap(),
        nov_2023()
    );
}
