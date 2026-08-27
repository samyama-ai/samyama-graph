//! A local temporal is read **in the other operand's zone**, not as UTC (#821).
//!
//! ```text
//! duration.between(localdatetime('2015-07-21T21:40:32.142'),
//!                  datetime('2015-07-21T21:40:32.142+0100'))
//!   expected PT0S, got PT-1H
//! ```
//!
//! Every wrong answer in this class was off by exactly one offset, which is why
//! it read as a sign or rounding bug rather than a missing rule.
//!
//! The rule has two halves that a single example cannot distinguish, so both are
//! pinned here:
//!
//! 1. **A date-less value borrows the other's day.** Otherwise a `time`
//!    compared against a `datetime` sits at the epoch and the answer is decades.
//! 2. **Daylight saving resolves at the *local* side's own wall clock.** This is
//!    the half that rules out the plausible-but-wrong reading of the same
//!    examples — see [`daylight_saving_is_resolved_at_each_side_s_own_clock`].

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn duration(expr: &str) -> String {
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

fn between(a: &str, b: &str) -> String {
    duration(&format!("duration.between({a}, {b})"))
}

/// The daylight-saving rows use `duration.inSeconds`, which puts the whole
/// elapsed time in the seconds field. `duration.between` would split 25 hours
/// into `P1DT1H`, so the two spellings are not interchangeable here and the
/// tests below use the one the TCK uses.
fn in_seconds(a: &str, b: &str) -> String {
    duration(&format!("duration.inSeconds({a}, {b})"))
}

/// The plain case: the same wall clock in a zone one hour east is the same
/// instant, so the difference is zero and not an hour.
#[test]
fn a_local_datetime_is_read_in_the_zoned_operands_offset() {
    assert_eq!(
        between(
            "localdatetime('2015-07-21T21:40:32.142')",
            "datetime('2015-07-21T21:40:32.142+0100')"
        ),
        "PT0S"
    );
}

/// **This is the test that rules out "subtract the wall clocks".**
///
/// Europe/Stockholm falls back at 03:00 on 2017-10-29, so on that date local
/// midnight is still +02:00 while 04:00 is already +01:00. The two operands are
/// in *different offsets of the same zone*, and the honest answer is five
/// hours, not the four the clock faces show.
///
/// Every variant below borrows something different — a `localdatetime` brings
/// its own date, a `date` brings a date and no clock, a `localtime` brings a
/// clock and no date — and all three must reach the same instant.
#[test]
fn daylight_saving_is_resolved_at_each_side_s_own_clock() {
    let stockholm_4am =
        "datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})";

    for local in [
        "localdatetime({year: 2017, month: 10, day: 29, hour: 0})",
        "date({year: 2017, month: 10, day: 29})",
        "localtime({hour: 0})",
    ] {
        assert_eq!(in_seconds(local, stockholm_4am), "PT5H", "{local}");
    }

    // And in the other direction, with the zoned side at the earlier instant.
    let stockholm_midnight =
        "datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'})";
    assert_eq!(
        in_seconds(stockholm_midnight, "localdatetime({year: 2017, month: 10, day: 29, hour: 4})"),
        "PT5H"
    );
    assert_eq!(in_seconds(stockholm_midnight, "localtime({hour: 4})"), "PT5H");
    // Crossing midnight into a day that is 25 hours long.
    assert_eq!(
        in_seconds(stockholm_midnight, "date({year: 2017, month: 10, day: 30})"),
        "PT25H"
    );
}

/// A `time` has no date and must borrow the other's, or the two are measured
/// forty-five years apart.
#[test]
fn a_date_less_value_borrows_the_others_day() {
    assert_eq!(
        between("time('14:30')", "datetime('2015-07-21T21:40:32.142+0100')"),
        "PT6H10M32.142S"
    );
    assert_eq!(
        between("time('14:30')", "localdatetime('2016-07-21T21:45:22.142')"),
        "PT7H15M22.142S"
    );
}

/// **The offset applies only to sides that carry one**, which is the rule #807
/// established and which this change must not undo.
///
/// `time('14:30')` against `time('16:30+0100')` is `PT1H` — both are zoned, so
/// the second is 15:30 UTC. `localtime('14:30')` against the same value is
/// `PT2H`: the unzoned side is read *in* +01:00, which cancels.
#[test]
fn zoned_and_unzoned_times_still_differ() {
    assert_eq!(between("time('14:30')", "time('16:30+0100')"), "PT1H");
    assert_eq!(between("localtime('14:30')", "time('16:30+0100')"), "PT2H");
}

/// Comparisons with no zone anywhere keep the shared-component rule: a date has
/// no clock and a time has no date, so only what they share is compared.
#[test]
fn local_only_comparisons_are_untouched() {
    assert_eq!(between("date({year: 1984, month: 10, day: 11})", "localtime('16:30')"), "PT16H30M");
    assert_eq!(between("localtime('14:30')", "localtime('16:30')"), "PT2H");
    assert_eq!(
        between("localdatetime('2018-01-01T12:00')", "localdatetime('2018-01-02T10:00')"),
        "PT22H"
    );
}
