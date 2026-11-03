//! All twenty duration accessors, both families (#819).
//!
//! Accessors split into **totals** (`minutes` = the entire time part in
//! minutes) and **remainders** (`minutesOfHour` = the same quantity modulo the
//! next unit up). Every bug here was a confusion between the two, or an
//! accessor that simply did not exist — and a missing accessor returns null,
//! which is indistinguishable from a legitimate zero.
//!
//! The table below is the TCK's own (`Temporal5` scenario 7), transcribed
//! whole rather than sampled, so a future accessor change has to face all
//! twenty at once instead of the two someone happened to think of.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn accessors(dur: &str, fields: &[&str]) -> Vec<i64> {
    let store = GraphStore::new();
    let projection: Vec<String> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| format!("d.{f} AS c{i}"))
        .collect();
    let cypher = format!("WITH {dur} AS d RETURN {}", projection.join(", "));
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    let rec = batch.records.first().expect("one row");
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| match rec.get(&format!("c{i}")) {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            // Null is the failure this test exists for: it is what an absent
            // accessor returns, and it reads exactly like a zero.
            other => panic!("d.{f} on {dur} gave {other:?}, not an integer"),
        })
        .collect()
}

fn check(dur: &str, expected: &[(&str, i64)]) {
    let fields: Vec<&str> = expected.iter().map(|(f, _)| *f).collect();
    let got = accessors(dur, &fields);
    let mismatched: Vec<String> = expected
        .iter()
        .zip(&got)
        .filter(|((_, want), have)| *want != **have)
        .map(|((f, want), have)| format!("  d.{f}: want {want}, got {have}"))
        .collect();
    assert!(mismatched.is_empty(), "{dur}\n{}", mismatched.join("\n"));
}

/// The TCK's table, entire.
#[test]
fn every_accessor_on_a_positive_duration() {
    check(
        "duration({years: 1, months: 4, days: 10, hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111})",
        &[
            // Date part: totals...
            ("years", 1),
            ("quarters", 5),
            ("months", 16),
            ("weeks", 1),
            ("days", 10),
            // ...then remainders.
            ("quartersOfYear", 1),
            ("monthsOfQuarter", 1),
            ("monthsOfYear", 4),
            ("daysOfWeek", 3),
            // Time part: totals...
            ("hours", 1),
            ("minutes", 61),
            ("seconds", 3661),
            ("milliseconds", 3661111),
            ("microseconds", 3661111111),
            ("nanoseconds", 3661111111111),
            // ...then remainders.
            ("minutesOfHour", 1),
            ("secondsOfMinute", 1),
            ("millisecondsOfSecond", 111),
            ("microsecondsOfSecond", 111111),
            ("nanosecondsOfSecond", 111111111),
        ],
    );
}

/// `minutes` is a total and `minutesOfHour` a remainder; they agreed only
/// because `minutes` was computing the remainder. Two hours makes them differ.
#[test]
fn totals_and_remainders_are_not_the_same_accessor() {
    check(
        "duration({hours: 2, minutes: 5, seconds: 7})",
        &[
            ("hours", 2),
            ("minutes", 125),
            ("minutesOfHour", 5),
            ("seconds", 7507),
            ("secondsOfMinute", 7),
        ],
    );
    check(
        "duration({years: 2, months: 5})",
        &[("years", 2), ("months", 29), ("monthsOfYear", 5), ("quarters", 9), ("quartersOfYear", 1)],
    );
}

/// The nanosecond remainder is **always non-negative**, with seconds floored to
/// compensate — the value below is -86399.9s and reports `-86400 / +100000000`.
///
/// The stored components stay sign-consistent (#806) so the duration still
/// renders as `PT-23H-59M-59.9S`; this is a presentation split derived from the
/// total, which is why a Euclidean division is correct here and wrong there.
#[test]
fn a_negative_duration_floors_its_second_and_keeps_nanos_non_negative() {
    let dur = "duration.between(localdatetime('2018-01-02T10:00:00.1'), localdatetime('2018-01-01T10:00:00.2'))";
    check(dur, &[("days", 0), ("seconds", -86400), ("nanosecondsOfSecond", 100000000)]);

    // Exactly-divisible negatives have no remainder to borrow and must not
    // gain a spurious one.
    let exact = "duration.between(localdatetime('2018-01-02T10:00'), localdatetime('2018-01-01T12:00'))";
    check(exact, &[("seconds", -79200), ("nanosecondsOfSecond", 0)]);
}

/// A duration with only a date part still answers its time accessors with zero
/// rather than null, and the reverse.
#[test]
fn absent_parts_read_as_zero_not_null() {
    check(
        "duration({days: 3})",
        &[("days", 3), ("weeks", 0), ("months", 0), ("seconds", 0), ("nanosecondsOfSecond", 0)],
    );
    check(
        "duration({seconds: 3})",
        &[("days", 0), ("months", 0), ("seconds", 3), ("hours", 0), ("minutes", 0)],
    );
}
