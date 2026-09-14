//! `duration.between` across an offset change in a named zone counts days on
//! the local date-time (samyama-graph#825).
//!
//! 2017-10-29 is 25 hours long in Stockholm. `duration.between` divided the
//! elapsed 25 hours by 86,400 and answered `P1DT1H` for two midnights one
//! calendar day apart -- and `start + P1DT1H` is an hour past the end.
//!
//! java.time's rule, which Neo4j follows: months and days are counted on the
//! local date-time, and the remainder is measured on the instant after adding
//! them. Every expected value below is Neo4j 2026.04.0's answer to the same
//! expression. `inSeconds` and two fixed offsets stay elapsed time.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn eval(expr: &str) -> String {
    let q = format!("RETURN toString({expr}) AS d");
    let store = GraphStore::new();
    let parsed = parse_query(&q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(&store).execute(&parsed).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    match out.records[0].get("d") {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        other => panic!("`{q}` gave {other:?}"),
    }
}

fn sthlm(month: u32, day: u32, hour: u32, minute: u32) -> String {
    format!(
        "datetime({{year: 2017, month: {month}, day: {day}, hour: {hour}, minute: {minute}, timezone: 'Europe/Stockholm'}})"
    )
}

fn between(a: &str, b: &str) -> String {
    eval(&format!("duration.between({a}, {b})"))
}

/// The issue's case: midnight to the next day's date across the fall-back.
#[test]
fn the_25_hour_day_is_one_day() {
    assert_eq!(between(&sthlm(10, 29, 0, 0), "date({year: 2017, month: 10, day: 30})"), "P1D");
    assert_eq!(between(&sthlm(10, 29, 0, 0), &sthlm(10, 30, 0, 0)), "P1D");
}

#[test]
fn the_23_hour_day_is_one_day() {
    assert_eq!(between(&sthlm(3, 25, 12, 0), &sthlm(3, 26, 12, 0)), "P1D");
}

/// Twenty-four elapsed hours inside the long day are not a calendar day.
#[test]
fn twenty_four_hours_on_the_long_day_stay_hours() {
    assert_eq!(between(&sthlm(10, 29, 0, 0), &sthlm(10, 29, 23, 0)), "PT24H");
}

#[test]
fn the_month_path_counts_days_locally_too() {
    assert_eq!(between(&sthlm(10, 28, 12, 0), &sthlm(11, 29, 12, 0)), "P1M1D");
    assert_eq!(between(&sthlm(9, 29, 0, 0), "date({year: 2017, month: 10, day: 30})"), "P1M1D");
}

#[test]
fn backwards_is_the_negation() {
    assert_eq!(between(&sthlm(10, 30, 0, 0), &sthlm(10, 29, 0, 0)), "P-1D");
}

/// 02:00-03:00 does not exist on 2017-03-26: 01:30 to 03:00 is 30 minutes.
#[test]
fn across_the_gap_is_elapsed_time() {
    assert_eq!(between(&sthlm(3, 26, 1, 30), &sthlm(3, 26, 3, 0)), "PT30M");
}

/// One day after 02:30 on the 25th is 02:30 on the 26th, which does not
/// exist and resolves to 03:30.
#[test]
fn a_day_that_lands_in_the_gap_is_one_day() {
    assert_eq!(between(&sthlm(3, 25, 2, 30), &sthlm(3, 26, 3, 30)), "P1D");
}

/// Controls: elapsed time is still elapsed time.
#[test]
fn in_seconds_and_fixed_offsets_stay_elapsed() {
    assert_eq!(
        eval(&format!("duration.inSeconds({}, date({{year: 2017, month: 10, day: 30}}))", sthlm(10, 29, 0, 0))),
        "PT25H"
    );
    assert_eq!(between("datetime('2017-10-29T00:00+02:00')", "datetime('2017-10-30T00:00+01:00')"), "P1DT1H");
    assert_eq!(between(&sthlm(10, 20, 0, 0), "date({year: 2017, month: 10, day: 21})"), "P1D");
}
