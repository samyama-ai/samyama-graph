//! `duration(toString(d)) = d`, and a time drops a duration's date part (#853).
//!
//! Two defects found by the same pair of scenarios.
//!
//! **The round trip returned a different duration.** `toString` was correct
//! throughout; the parser dropped per-component minus signs — its scanner
//! accepted only digits and `.`, so a `-` was silently skipped — and computed
//! the fraction as `(val - val.floor()) * 1e9`, turning `.001` into 999,999
//! nanoseconds. `PT-2.001S` came back as `PT2.000999999S`: wrong sign, one
//! nanosecond short, and reported as success.
//!
//! **A duration with months could not be added to a time.** A clock has no
//! calendar, so the date part is dropped — the exact mirror of #817, where a
//! date has no clock and the sub-day part is dropped.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn one(cypher: &str) -> PropertyValue {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

fn rendered(expr: &str) -> String {
    one(&format!("RETURN {expr} AS r")).to_cypher_string()
}

/// The TCK's serialization table, asserted as the round trip it actually is:
/// the rendered text **and** that parsing it back gives the same value.
#[test]
fn every_duration_survives_a_round_trip() {
    for (map, text) in [
        ("{days: 1, milliseconds: -1}", "P1DT-0.001S"),
        ("{seconds: -2, milliseconds: -1}", "PT-2.001S"),
        ("{seconds: -2, milliseconds: 1}", "PT-1.999S"),
        ("{seconds: -60, milliseconds: -1}", "PT-1M-0.001S"),
        ("{seconds: -60, milliseconds: 1}", "PT-59.999S"),
        ("{years: 12, months: 5, days: -14, hours: 16}", "P12Y5M-14DT16H"),
        ("{seconds: 2, milliseconds: -1}", "PT1.999S"),
        ("{nanoseconds: -1}", "PT-0.000000001S"),
    ] {
        assert_eq!(rendered(&format!("toString(duration({map}))")), format!("\"{text}\""), "{map}");
        assert_eq!(
            one(&format!("WITH duration({map}) AS d RETURN duration(toString(d)) = d AS r")),
            PropertyValue::Boolean(true),
            "round trip of {map} (rendered {text})"
        );
    }
}

/// **The fraction is exact.** Computed through an `f64` subtraction, `.001`
/// became 999,999 nanoseconds — a value that renders plausibly and is wrong.
#[test]
fn a_fraction_is_read_from_its_digits() {
    assert_eq!(rendered("duration('PT2.001S')"), "PT2.001S");
    assert_eq!(rendered("duration('PT-2.001S')"), "PT-2.001S");
    assert_eq!(rendered("duration('PT0.000000001S')"), "PT0.000000001S");
    assert_eq!(rendered("duration('PT1.5S')"), "PT1.5S");
}

/// Signs attach to components, not to the whole duration.
#[test]
fn each_component_carries_its_own_sign() {
    assert_eq!(rendered("duration('P1DT-0.001S')"), "P1DT-0.001S");
    assert_eq!(rendered("duration('P12Y5M-14DT16H')"), "P12Y5M-14DT16H");
    assert_eq!(rendered("duration('P-1DT1H')"), "P-1DT1H");
}

/// A clock has no calendar: months and days are dropped, and only the time
/// part moves it. The TCK's own expected values.
#[test]
fn a_time_drops_the_date_part_of_a_duration() {
    let x = "localtime({hour: 12, minute: 31, second: 14, nanosecond: 1})";
    let d1 = "duration({months: 1, days: -14, hours: 16, minutes: -12, seconds: 70})";
    let d2 = "duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 2})";
    assert_eq!(rendered(&format!("{x} + {d2}")), "04:44:24.000000003");
    assert_eq!(rendered(&format!("{x} - {d2}")), "20:18:03.999999999");
    assert_eq!(rendered(&format!("{x} + {d1}")), "04:20:24.000000001");
    assert_eq!(rendered(&format!("{x} - {d1}")), "20:42:04.000000001");

    // A zoned time keeps its offset while its clock moves.
    let t = "time({hour: 12, minute: 31, second: 14, nanosecond: 1, timezone: '+01:00'})";
    assert_eq!(rendered(&format!("{t} + duration({{hours: 1}})")), "13:31:14.000000001+01:00");
}

/// A date-time still applies **both** parts, so the two mirrored rules cannot
/// be over-applied to the type that has a calendar and a clock.
#[test]
fn a_datetime_applies_both_parts() {
    assert_eq!(
        rendered("localdatetime({year: 1984, month: 10, day: 11, hour: 12}) + duration({months: 1, hours: 3})"),
        "1984-11-11T15:00"
    );
}
