//! `duration.between`, `inDays` and `inSeconds` agree on which components two
//! temporals share (#849).
//!
//! ```text
//! duration.inDays(date('1984-10-11'), localtime('16:30'))
//!   expected PT0S, got P-5396D
//! ```
//!
//! A date has no time and a time has no date, so only the components the two
//! values **share** are compared — #807's rule. It lived inside
//! `temporal_difference_calendar`, which only `duration.between` and
//! `inMonths` reach; `inDays` and `inSeconds` used the plain difference, which
//! reads a `localtime` as a clock at the epoch and measures fifteen years of
//! nothing.
//!
//! The same difference, measured two ways, disagreed by decades — **and only
//! on mixed pairs**, which is why every date-to-date row was right and the
//! family looked correct. The cross-check below is the point of this file: for
//! each pair, the three spellings must tell the same story.

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

fn check(rows: &[(&str, &str, &str, &str)]) {
    let wrong: Vec<String> = rows
        .iter()
        .filter_map(|(f, lhs, rhs, want)| {
            let got = duration(&format!("duration.{f}({lhs}, {rhs})"));
            (got != *want)
                .then(|| format!("  duration.{f}({lhs}, {rhs})\n    want {want}\n    got  {got}"))
        })
        .collect();
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// The TCK's mixed pairs: a value with no date contributes no days.
#[test]
fn a_pair_with_no_shared_date_measures_zero_days() {
    check(&[
        ("inDays", "date('1984-10-11')", "localtime('16:30')", "PT0S"),
        ("inDays", "date('1984-10-11')", "time('16:30+0100')", "PT0S"),
        ("inDays", "localtime('14:30')", "date('2015-06-24')", "PT0S"),
        ("inDays", "localtime('14:30')", "localdatetime('2016-07-21T21:45:22.142')", "PT0S"),
        ("inDays", "localtime('14:30')", "datetime('2015-07-21T21:40:32.142+0100')", "PT0S"),
        ("inDays", "time('14:30')", "date('2015-06-24')", "PT0S"),
        ("inDays", "localdatetime('2015-07-21T21:40:32.142')", "localtime('16:30')", "PT0S"),
    ]);
}

/// And the clock difference is the whole answer in seconds.
#[test]
fn the_clock_difference_is_the_whole_answer() {
    check(&[
        ("inSeconds", "date('1984-10-11')", "localtime('16:30')", "PT16H30M"),
        ("inSeconds", "localtime('14:30')", "date('2015-06-24')", "PT-14H-30M"),
        (
            "inSeconds",
            "localtime('14:30')",
            "localdatetime('2016-07-21T21:45:22.142')",
            "PT7H15M22.142S",
        ),
    ]);
}

/// **The cross-check.** For a pair with no shared date, `between` and
/// `inSeconds` must agree, and `inDays` must be zero. Disagreement between the
/// spellings is the defect this file exists for, and it is invisible from any
/// one of them.
#[test]
fn the_three_spellings_agree() {
    for (lhs, rhs) in [
        ("date('1984-10-11')", "localtime('16:30')"),
        ("localtime('14:30')", "date('2015-06-24')"),
        ("localdatetime('2015-07-21T21:40:32.142')", "localtime('16:30')"),
        ("time('14:30')", "datetime('2015-07-21T21:40:32.142+0100')"),
    ] {
        let between = duration(&format!("duration.between({lhs}, {rhs})"));
        let in_seconds = duration(&format!("duration.inSeconds({lhs}, {rhs})"));
        let in_days = duration(&format!("duration.inDays({lhs}, {rhs})"));
        assert_eq!(between, in_seconds, "between vs inSeconds for ({lhs}, {rhs})");
        assert_eq!(in_days, "PT0S", "inDays for ({lhs}, {rhs})");
    }
}

/// Pairs that *do* share a date are unchanged — the calendar rule and the
/// elapsed rule still differ from each other, which #804 established.
#[test]
fn pairs_sharing_a_date_are_unchanged() {
    check(&[
        ("inDays", "date('1984-10-11')", "date('2015-06-24')", "P11213D"),
        ("inDays", "localdatetime('2015-07-21T21:40:32.142')", "date('2015-06-24')", "P-27D"),
        (
            "inDays",
            "datetime('2014-07-21T21:40:36.143+0200')",
            "datetime('2015-07-21T21:40:32.142+0100')",
            "P365D",
        ),
        ("between", "date('1984-10-11')", "date('2015-06-24')", "P30Y8M13D"),
    ]);
}
