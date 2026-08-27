//! A week-date override uses the **ISO week year**, and every constructor
//! applies the same override rule (#851).
//!
//! ```text
//! date({date: date('1816-12-31'), week: 2})            1817-01-07, was 1816-01-05
//! localdatetime({date: date('1816-12-31'), week: 2})   1817-01-07T00:00, was unchanged
//! ```
//!
//! Two defects. The year in a week date is the ISO week year, not the calendar
//! year: 1816-12-30 is a Monday belonging to 1817-W01. And
//! `compose_date_and_time` had a **second implementation** of the override
//! rule, which tested for `week`, `quarter` and the rest in its condition and
//! then handled only `year`/`month`/`day` — so the composite constructors
//! entered the branch, recomputed the date they already had, and returned it
//! unchanged.
//!
//! Every case is therefore asserted through `date()`, `localdatetime()` and
//! `datetime()`, because a fix to one of them says nothing about the others.

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

/// The year boundary, where the week year and the calendar year differ.
#[test]
fn a_week_override_uses_the_iso_week_year() {
    check(&[
        ("date({date: date('1816-12-30'), week: 2, dayOfWeek: 3})".into(), "1817-01-08"),
        ("date({date: date('1816-12-31'), week: 2})".into(), "1817-01-07"),
        // An explicit year is read as the week year too.
        ("date({date: date('1816-12-31'), year: 1817, week: 2})".into(), "1817-01-07"),
    ]);
}

/// **The same three cases through the composite constructors**, which had
/// their own copy of the rule and applied none of it.
#[test]
fn the_composite_constructors_apply_the_same_rule() {
    check(&[
        ("localdatetime({date: date('1816-12-31'), week: 2})".into(), "1817-01-07T00:00"),
        (
            "localdatetime({date: date('1816-12-30'), week: 2, dayOfWeek: 3})".into(),
            "1817-01-08T00:00",
        ),
        ("datetime({date: date('1816-12-31'), week: 2})".into(), "1817-01-07T00:00Z"),
        ("datetime({date: date('1816-12-31'), year: 1817, week: 2})".into(), "1817-01-07T00:00Z"),
        // And the `quarter` rule from #838, which the composite path never had.
        ("localdatetime({date: date('1984-11-11'), quarter: 3})".into(), "1984-08-11T00:00"),
    ]);
}

/// Away from the boundary the two years agree, so these would pass either way —
/// they are here to show the fix did not simply shift everything by a year.
///
/// 11 October 1984 is a **Thursday**, and the Thursday of 1984-W02 is
/// 12 January. My first version of this test said the 11th.
#[test]
fn mid_year_week_dates_are_unchanged() {
    check(&[
        ("date({date: date('1984-10-11'), week: 2})".into(), "1984-01-12"),
        ("date({year: 1984, week: 10, dayOfWeek: 3})".into(), "1984-03-07"),
    ]);
}

/// Ordinary overrides and selections still work through every constructor — a
/// change that routed everything through one function could have broken these
/// and satisfied every case above.
#[test]
fn ordinary_overrides_are_undisturbed() {
    check(&[
        ("date({year: 1984, month: 10, day: 11})".into(), "1984-10-11"),
        ("date({date: date('1984-10-11'), month: 3})".into(), "1984-03-11"),
        ("localdatetime({date: date('1984-10-11'), month: 3})".into(), "1984-03-11T00:00"),
        (
            "localdatetime({date: date('1984-10-11'), time: localtime('12:31')})".into(),
            "1984-10-11T12:31",
        ),
    ]);
}
