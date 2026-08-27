//! Selecting a time carries the source's offset, and a `timezone` converts
//! only when there is an instant to convert (#838).
//!
//! Three defects, each producing a perfectly ordinary-looking value:
//!
//! * `time(<zoned>)` returned `12:00Z` for `12:00+01:00` — a **different
//!   instant**, rendered as a good time.
//! * `{time: <zoned>, timezone: X}` relabelled the offset instead of
//!   converting the moment.
//! * `{date: d, quarter: 3}` reset the day to the 1st, so naming one component
//!   silently changed two others.
//!
//! The asymmetry in the middle one is the part worth pinning: a **local**
//! source has no instant to convert from, so its clock stays put and only
//! gains a label. Converting unconditionally fixes the zoned rows and breaks
//! the local ones — the same shape as #821, one constructor over.

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn rendered(expr: &str) -> String {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.to_cypher_string(),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

fn check(rows: &[(String, &str)]) {
    let wrong: Vec<String> = rows
        .iter()
        .filter_map(|(expr, want)| {
            let got = rendered(expr);
            (got != *want).then(|| format!("  {expr}\n    want {want}\n    got  {got}"))
        })
        .collect();
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

const ZONED_DT: &str =
    "datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'})";
const ZONED_TIME: &str =
    "time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'})";

/// A zoned source keeps its offset, and a `timezone` moves the clock with it.
#[test]
fn a_zoned_source_is_converted() {
    check(&[
        (format!("time({ZONED_DT})"), "12:00+01:00"),
        (format!("time({{time: {ZONED_DT}}})"), "12:00+01:00"),
        (format!("time({{time: {ZONED_DT}, second: 42}})"), "12:00:42+01:00"),
        (format!("time({{time: {ZONED_DT}, timezone: '+05:00'}})"), "16:00+05:00"),
        (
            format!("time({{time: {ZONED_DT}, second: 42, timezone: '+05:00'}})"),
            "16:00:42+05:00",
        ),
        (format!("time({ZONED_TIME})"), "12:31:14.645876+01:00"),
        (format!("time({{time: {ZONED_TIME}}})"), "12:31:14.645876+01:00"),
        (format!("time({{time: {ZONED_TIME}, second: 42}})"), "12:31:42.645876+01:00"),
        (
            format!("time({{time: {ZONED_TIME}, timezone: '+05:00'}})"),
            "16:31:14.645876+05:00",
        ),
        (
            format!("time({{time: {ZONED_TIME}, second: 42, timezone: '+05:00'}})"),
            "16:31:42.645876+05:00",
        ),
    ]);
}

/// **A local source is labelled, not moved.** There is no instant to convert
/// from, so `12:31` in `+05:00` is `12:31+05:00` and not `16:31`.
#[test]
fn a_local_source_is_only_labelled() {
    let local_t = "localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123})";
    let local_dt =
        "localdatetime({year: 1984, month: 3, day: 7, hour: 12, minute: 31, second: 14, millisecond: 645})";
    check(&[
        (format!("time({{time: {local_t}, timezone: '+05:00'}})"), "12:31:14.645876123+05:00"),
        (
            format!("time({{time: {local_t}, second: 42, timezone: '+05:00'}})"),
            "12:31:42.645876123+05:00",
        ),
        (format!("time({{time: {local_dt}, timezone: '+05:00'}})"), "12:31:14.645+05:00"),
        (
            format!("time({{time: {local_dt}, second: 42, timezone: '+05:00'}})"),
            "12:31:42.645+05:00",
        ),
    ]);
}

/// Built from components there is no source at all, so `timezone` names the
/// offset the clock is already in.
#[test]
fn components_are_not_converted_either() {
    check(&[
        ("time({hour: 12, timezone: '+05:00'})".into(), "12:00+05:00"),
        ("time({hour: 12})".into(), "12:00Z"),
    ]);
}

/// Overriding the quarter keeps the day within it: 11 November is day 42 of Q4,
/// and day 42 of Q3 is 11 August.
#[test]
fn a_quarter_override_keeps_the_day_within_the_quarter() {
    check(&[
        (
            "date({date: date({year: 1984, month: 11, day: 11}), quarter: 3})".into(),
            "1984-08-11",
        ),
        (
            "date({date: date({year: 1984, month: 11, day: 11}), quarter: 1})".into(),
            "1984-02-11",
        ),
        // An explicit dayOfQuarter still wins.
        (
            "date({date: date({year: 1984, month: 11, day: 11}), quarter: 3, dayOfQuarter: 1})".into(),
            "1984-07-01",
        ),
    ]);
}

/// `datetime()` was already correct here (#809) and must stay so — this fix
/// touched `time()` only.
#[test]
fn datetime_selection_is_undisturbed() {
    check(&[
        (
            format!("datetime({{datetime: {ZONED_DT}, timezone: '+05:00'}})"),
            "1984-10-11T16:00+05:00",
        ),
        (
            format!("datetime({{datetime: {ZONED_DT}}})"),
            "1984-10-11T12:00+01:00[Europe/Stockholm]",
        ),
    ]);
}
