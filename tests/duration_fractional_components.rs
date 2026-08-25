//! `duration()` accepts fractional components, which carry down (#829).
//!
//! Read with `as_integer()`, a float component returned `None` and
//! `unwrap_or(0)` turned it into zero — so `duration({months: 0.75})` was
//! `PT0S`, a **well-formed duration nothing downstream can question**. Same
//! shape as #787, one component over.
//!
//! Two details make this more than a division, and each is pinned below:
//!
//! * A month's fraction becomes **whole days first, then time**: 0.75 months is
//!   22 days *and* 19:51:49.5, not 22.83 days.
//! * A mean Gregorian month is 365.2425/12 days, which is **exactly 2,629,746
//!   seconds**. Carried in seconds the arithmetic is exact; carried in days
//!   (`0.75 × 30.436875`) it is not representable in binary and lands a hundred
//!   nanoseconds short of `49.5S`.

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn rendered(map: &str) -> String {
    let store = GraphStore::new();
    let cypher = format!("RETURN duration({map}) AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.to_cypher_string(),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

fn check(rows: &[(&str, &str)]) {
    let wrong: Vec<String> = rows
        .iter()
        .filter_map(|(map, want)| {
            let got = rendered(map);
            (got != *want).then(|| format!("  duration({map})\n    want {want}\n    got  {got}"))
        })
        .collect();
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// The TCK's table (`Temporal1` scenario 12), transcribed whole — the integer
/// rows included, since they are what a carry rule can most easily break.
#[test]
fn the_tck_construction_table() {
    check(&[
        ("{days: 14, hours: 16, minutes: 12}", "P14DT16H12M"),
        ("{months: 5, days: 1.5}", "P5M1DT12H"),
        ("{months: 0.75}", "P22DT19H51M49.5S"),
        ("{weeks: 2.5}", "P17DT12H"),
        (
            "{years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}",
            "P12Y5M14DT16H13M10S",
        ),
        ("{days: 14, seconds: 70, milliseconds: 1}", "P14DT1M10.001S"),
        ("{days: 14, seconds: 70, microseconds: 1}", "P14DT1M10.000001S"),
        ("{days: 14, seconds: 70, nanoseconds: 1}", "P14DT1M10.000000001S"),
        ("{minutes: 1.5, seconds: 1}", "PT1M31S"),
    ]);
}

/// **The exact-halves case.** `49.5S` is the digit that distinguishes carrying
/// in seconds from carrying in days: the days constant is not representable in
/// binary and gives `49.4999999S`.
#[test]
fn a_months_fraction_carries_exactly() {
    check(&[
        ("{months: 0.75}", "P22DT19H51M49.5S"),
        ("{months: 0.5}", "P15DT5H14M33S"),
        ("{months: 1.5}", "P1M15DT5H14M33S"),
    ]);
}

/// Each unit's fraction lands in the next unit down, not two down and not in
/// the same one.
#[test]
fn every_unit_carries_into_the_next() {
    check(&[
        ("{years: 1.5}", "P1Y6M"),
        ("{weeks: 1.5}", "P10DT12H"),
        ("{days: 0.5}", "PT12H"),
        ("{hours: 1.5}", "PT1H30M"),
        ("{minutes: 0.5}", "PT30S"),
        ("{seconds: 0.5}", "PT0.5S"),
    ]);
}

/// Negative and mixed-sign components keep sign-consistent parts, which needs
/// the seconds and nanoseconds summed **before** they are split — see
/// `duration_sign_invariant.rs` for why neither split works alone.
#[test]
fn negative_and_mixed_sign_components() {
    check(&[
        ("{nanoseconds: -1}", "PT-0.000000001S"),
        ("{seconds: 2, milliseconds: -1}", "PT1.999S"),
        ("{seconds: -2, milliseconds: 1}", "PT-1.999S"),
        ("{days: -1.5}", "P-1DT-12H"),
        ("{weeks: -2.5}", "P-17DT-12H"),
    ]);
}
