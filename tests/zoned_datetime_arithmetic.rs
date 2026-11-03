//! Adding a duration to a date-time in a named zone re-resolves its offset
//! (samyama-graph#824).
//!
//! `shift_temporal` moved the instant and carried the old offset through. For
//! a named zone that produced values whose offset contradicts their own zone:
//!
//!   2017-10-29T00:00+02:00[Europe/Stockholm] + P1D  ->  2017-10-30T00:00+02:00
//!
//! Stockholm is +01:00 on 30 October. Calendar parts (months, days) now move the
//! local date-time and the offset is re-resolved from the zone; clock parts
//! (hours and below) move the instant exactly -- java.time's split, which
//! Neo4j follows. Fixed offsets are unchanged.
//!
//! Every check reads the result back through Cypher's own accessors.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// `[day, hour, minute, offsetSeconds]` of `start + dur`.
fn shifted(start: &str, dur: &str) -> Vec<String> {
    let q = format!(
        "WITH {start} + {dur} AS d RETURN d.day AS day, d.hour AS hour, d.minute AS minute, d.offsetSeconds AS off"
    );
    let store = GraphStore::new();
    let parsed = parse_query(&q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(&store).execute(&parsed).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let r = &out.records[0];
    ["day", "hour", "minute", "off"]
        .iter()
        .map(|c| format!("{:?}", r.get(c)))
        .collect()
}

fn expect(day: i64, hour: i64, minute: i64, off: i64) -> Vec<String> {
    [day, hour, minute, off]
        .iter()
        .map(|v| format!("{:?}", Some(samyama::query::executor::Value::Property(samyama::graph::PropertyValue::Integer(*v)))))
        .collect()
}

const OCT29: &str = "datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'})";

/// The issue's case: one calendar day across the October fall-back keeps the
/// local midnight and takes the new offset.
#[test]
fn a_day_across_the_fall_back_lands_on_the_new_offset() {
    assert_eq!(shifted(OCT29, "duration({days: 1})"), expect(30, 0, 0, 3600));
}

/// Hours are exact: 24 hours after midnight on the 25-hour day is 23:00.
#[test]
fn twenty_four_hours_across_the_fall_back_is_exact() {
    assert_eq!(shifted(OCT29, "duration({hours: 24})"), expect(29, 23, 0, 3600));
}

#[test]
fn a_day_across_the_spring_forward_lands_on_the_new_offset() {
    let start = "datetime({year: 2017, month: 3, day: 25, hour: 12, timezone: 'Europe/Stockholm'})";
    assert_eq!(shifted(start, "duration({days: 1})"), expect(26, 12, 0, 7200));
}

/// Months move the calendar too: one month from 29 September is before the
/// change (still +02:00), two months is after it (+01:00).
#[test]
fn months_re_resolve_the_offset_as_well() {
    let start = "datetime({year: 2017, month: 9, day: 29, hour: 0, timezone: 'Europe/Stockholm'})";
    assert_eq!(shifted(start, "duration({months: 1})"), expect(29, 0, 0, 7200));
    assert_eq!(shifted(start, "duration({months: 2})"), expect(29, 0, 0, 3600));
}

/// Control: no boundary crossed, nothing changes but the day.
#[test]
fn an_ordinary_day_keeps_its_offset() {
    let start = "datetime({year: 2017, month: 10, day: 20, hour: 0, timezone: 'Europe/Stockholm'})";
    assert_eq!(shifted(start, "duration({days: 1})"), expect(21, 0, 0, 7200));
}

/// A fixed offset is a constant; there is nothing to re-resolve.
#[test]
fn a_fixed_offset_is_kept() {
    assert_eq!(shifted("datetime('2017-10-29T00:00+02:00')", "duration({days: 1})"), expect(30, 0, 0, 7200));
}
