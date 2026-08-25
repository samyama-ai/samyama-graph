//! Every `Duration` the engine produces has sign-consistent components (#806).
//!
//! A duration's `months`, `days`, `seconds` and `nanos` must all share a sign
//! (zero counting as either). Mixing them describes the right instants and
//! renders as a perfectly good duration — `PT-1.6S` for what should be
//! `PT0.4S`, `-33.858S` for `-32.142S` — so nothing downstream notices.
//!
//! The cause is always the same: `div_euclid`/`rem_euclid` floor toward
//! negative infinity, so `-400_000_000ns` splits into `(-1s, +600_000_000ns)`.
//! `/` and `%` truncate toward zero and do not.
//!
//! This has now been fixed twice — #775 in `temporal_difference`, and #804 in
//! `temporal_difference_calendar`, which I wrote **an hour later and got wrong
//! the same way**. Euclidean division is correct for time-of-day (genuinely
//! 0..86400) and wrong for durations, and the two live side by side in the same
//! file.
//!
//! So this asserts the invariant over every duration-producing path rather than
//! over specific values. A value test catches the case you thought of; this
//! catches the next function someone adds.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn duration_of(expr: &str) -> PropertyValue {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

/// No two components may disagree in sign.
fn assert_sign_consistent(expr: &str) {
    let v = duration_of(expr);
    let PropertyValue::Duration { months, days, seconds, nanos } = v else {
        panic!("{expr} did not produce a Duration: {v:?}");
    };
    let signs: Vec<i32> = [months as i128, days as i128, seconds as i128, nanos as i128]
        .iter()
        .map(|x| if *x > 0 { 1 } else if *x < 0 { -1 } else { 0 })
        .filter(|s| *s != 0)
        .collect();
    let mixed = signs.windows(2).any(|w| w[0] != w[1]);
    assert!(
        !mixed,
        "{expr} produced mixed signs: months={months} days={days} seconds={seconds} nanos={nanos}\n  \
         renders as {}\n  \
         This is the div_euclid/rem_euclid trap: use `/` and `%`, which truncate toward zero.",
        PropertyValue::Duration { months, days, seconds, nanos }.to_cypher_string()
    );
}

/// Every duration-producing path, in both directions.
///
/// The backwards cases are the point: forwards, Euclidean and truncating
/// division agree, so a suite of only-positive examples proves nothing.
#[test]
fn every_duration_path_is_sign_consistent() {
    let exprs = [
        // duration.between, same month and across months
        "duration.between(localtime('12:34:54.7'), localtime('12:34:54.3'))",
        "duration.between(localtime('12:34:54.3'), localtime('12:34:54.7'))",
        "duration.between(date('2015-06-24'), date('1984-10-11'))",
        "duration.between(date('1984-10-11'), date('2015-06-24'))",
        "duration.between(localdatetime('2015-07-21T21:40:32.142'), date('2015-06-24'))",
        "duration.between(localdatetime('2018-01-02T10:00'), localdatetime('2018-01-01T12:00'))",
        // the in* family
        "duration.inSeconds(localtime('12:34:56.3'), localtime('12:34:54.7'))",
        "duration.inSeconds(localtime('12:44:54.7'), localtime('12:34:55.3'))",
        "duration.inDays(date('2017-10-13'), date('2017-10-11'))",
        // scaling, which reaches the same splitting code
        "duration({days: 3, hours: 4}) * -2",
        "duration({years: 12, months: 5, days: 14, hours: 16}) * -0.5",
        "duration({days: 3, hours: 4}) / -2",
        // subtraction of two temporals
        "date('2015-06-24') - date('2015-07-21')",
        "localdatetime('2018-01-01T12:00') - localdatetime('2018-01-02T10:00')",
        // and durations of each other
        "duration({days: 1}) - duration({days: 3, hours: 4})",
    ];
    for e in exprs {
        assert_sign_consistent(e);
    }
}

/// The invariant check itself must be able to fail, or this file proves
/// nothing. A hand-built mixed-sign duration is what the bug looked like.
#[test]
fn the_invariant_would_catch_a_mixed_sign_duration() {
    let bad = PropertyValue::Duration { months: 0, days: 0, seconds: -1, nanos: 600_000_000 };
    // This is exactly what `div_euclid` produced for -0.4s.
    assert_eq!(bad.to_cypher_string(), "PT-1.6S");
    let signs: Vec<i32> = [bad_seconds(&bad), bad_nanos(&bad)]
        .iter()
        .map(|x| if *x > 0 { 1 } else if *x < 0 { -1 } else { 0 })
        .filter(|s| *s != 0)
        .collect();
    assert!(
        signs.windows(2).any(|w| w[0] != w[1]),
        "the detector must flag a mixed-sign duration"
    );
}

fn bad_seconds(p: &PropertyValue) -> i64 {
    match p { PropertyValue::Duration { seconds, .. } => *seconds, _ => 0 }
}
fn bad_nanos(p: &PropertyValue) -> i64 {
    match p { PropertyValue::Duration { nanos, .. } => *nanos as i64, _ => 0 }
}
