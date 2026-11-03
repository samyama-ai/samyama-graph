//! Duration arithmetic carries its nanoseconds, and a date keeps the whole
//! days inside a duration's seconds (#1001).
//!
//! Two bugs, both silent, both off by exactly one unit.
//!
//! 1. `Duration + Duration` and `Duration - Duration` added the fields
//!    straight across, so adding a duration to itself produced
//!    `nanos: 1000000006` and rendered `…M26.1000000006S` where openCypher
//!    says `…M27.000000006S`. The whole second was not lost — it was sitting
//!    unrendered inside the nanoseconds field. `duration()` itself has always
//!    normalised (#814); only the arithmetic did not.
//!
//! 2. `shift_temporal` drops a duration's sub-day part for a `Date` (#817),
//!    but dropped the entire `seconds` field with it. A duration's seconds can
//!    hold whole days — `T67H56M27S` is nearly three — and those are calendar
//!    days a date can move by.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

const D: &str = "duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, \
                 minutes: 12.5, seconds: 70.5, nanoseconds: 3})";

fn text(cypher: &str) -> String {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let c = r.columns[0].clone();
    let v = format!("{:?}", r.records[0].get(&c));
    let a = v.find('"').expect("a string result");
    let b = v.rfind('"').unwrap();
    v[a + 1..b].to_string()
}

#[test]
fn adding_a_duration_to_itself_carries_the_nanoseconds() {
    assert_eq!(text(&format!("RETURN toString({D} + {D}) AS a")),
               "P25Y10M58DT67H56M27.000000006S");
}

#[test]
fn subtracting_a_duration_from_itself_is_zero() {
    assert_eq!(text(&format!("RETURN toString({D} - {D}) AS a")), "PT0S");
}

#[test]
fn a_negative_sub_second_duration_keeps_its_sign_in_the_nanoseconds() {
    // The representation carries the sign in `nanos`, not as a borrow from
    // seconds. A normaliser using floor division gives `{seconds: -1,
    // nanos: 999999999}` -- the same instant, printed as `PT-1.999999999S`.
    // A correct value that renders wrong.
    assert_eq!(text("RETURN toString(duration({nanoseconds: -1}) + duration({seconds: 0})) AS a"),
               "PT-0.000000001S");
    assert_eq!(text("RETURN toString(duration({seconds: 1}) - duration({nanoseconds: 1})) AS a"),
               "PT0.999999999S");
}

#[test]
fn months_and_days_are_not_merged_into_each_other() {
    // Cypher keeps the three groups separate on purpose: a month is not 30
    // days, and a day is not always 86,400 seconds across a DST boundary.
    // Normalising them would change answers rather than tidy them.
    assert_eq!(text("RETURN toString(duration({months: 1}) + duration({days: 1})) AS a"), "P1M1D");
}

#[test]
fn a_date_moves_by_the_whole_days_inside_the_seconds() {
    assert_eq!(text(&format!("RETURN toString(date('1984-10-11') + {D}) AS a")), "1997-10-11");
    assert_eq!(text(&format!("RETURN toString(date('1984-10-11') - {D}) AS a")), "1971-10-12");
}

#[test]
fn a_sub_day_remainder_is_still_dropped_by_a_date() {
    // #817's rule, which this must not break: `days: -14` with a `+15h49m`
    // remainder stays -14, rather than combining to -13.34 and truncating to
    // -13. Fifteen hours is less than a day, so it yields zero whole days.
    assert_eq!(
        text("RETURN toString(date('1984-10-11') - duration({days: 14, hours: -15, minutes: -49})) AS a"),
        text("RETURN toString(date('1984-10-11') - duration({days: 14})) AS a"),
    );
}

#[test]
fn an_ordinary_date_plus_days_is_unaffected() {
    assert_eq!(text("RETURN toString(date('1984-10-11') + duration({days: 1})) AS a"), "1984-10-12");
    assert_eq!(text("RETURN toString(date('1984-10-11') + duration({hours: 23})) AS a"), "1984-10-11");
    assert_eq!(text("RETURN toString(date('1984-10-11') + duration({hours: 25})) AS a"), "1984-10-12");
}
