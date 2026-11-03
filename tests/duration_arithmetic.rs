//! Scaling a duration by a number (#787).
//!
//! `duration * n` and `duration / n` were unimplemented (`Mul requires numeric
//! operands`), and `duration()` silently discarded its sub-second components
//! — `duration({nanoseconds: 1})` lost the 1 at construction, so the value was
//! already wrong before any arithmetic touched it.
//!
//! Three rules here were **derived from the TCK's expected values**, not
//! assumed, and each has a plausible-looking wrong alternative:
//!
//! 1. A fractional month carries into days at the **mean Gregorian month**,
//!    365.2425/12 = 30.4369 days — not 30. Using 30 gives `08:06:35` where
//!    `13:21:08` is expected.
//! 2. Hours do **not** carry into days: doubling `14D16H` gives `28DT32H`, not
//!    `29DT8H`. A day is not always 24 hours once zones are involved.
//!    Normalising looks tidier and is wrong.
//! 3. Rounding is **ties-to-even**. `58390000000001 * 0.5` is exactly
//!    `...000.5`; Rust's `.round()` takes it away from zero to `...001` where
//!    the TCK expects a whole `8S`.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn rendered(cypher: &str) -> String {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.to_cypher_string(),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

const D: &str = "duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1})";

/// The TCK's own table, verbatim. Every row of `Temporal8 [7]`.
#[test]
fn the_tck_multiply_and_divide_table() {
    for (expr, want) in [
        ("* 1", "P12Y5M14DT16H13M10.000000001S"),
        ("* 2", "P24Y10M28DT32H26M20.000000002S"),
        ("* 0.5", "P6Y2M22DT13H21M8S"),
        ("/ 1", "P12Y5M14DT16H13M10.000000001S"),
        ("/ 2", "P6Y2M22DT13H21M8S"),
        ("/ 0.5", "P24Y10M28DT32H26M20.000000002S"),
    ] {
        assert_eq!(rendered(&format!("RETURN {D} {expr} AS r")), want, "{D} {expr}");
    }
}

/// `duration()` keeps its sub-second components, additively.
///
/// They were hardcoded to zero, so the `nanoseconds: 1` above was lost at
/// construction — the `* 1` row of the table above is really a test of *this*.
#[test]
fn duration_keeps_its_sub_second_components() {
    assert_eq!(rendered("RETURN duration({nanoseconds: 1}) AS r"), "PT0.000000001S");
    assert_eq!(rendered("RETURN duration({milliseconds: 645}) AS r"), "PT0.645S");
    // Additive with each other, as everywhere else in the temporal surface.
    assert_eq!(
        rendered("RETURN duration({milliseconds: 123, microseconds: 456, nanoseconds: 789}) AS r"),
        "PT0.123456789S"
    );
    // And they carry into seconds when they overflow.
    assert_eq!(rendered("RETURN duration({milliseconds: 1500}) AS r"), "PT1.5S");
}

/// `weeks` is seven days.
#[test]
fn weeks_are_days() {
    assert_eq!(rendered("RETURN duration({weeks: 2}) AS r"), "P14D");
    assert_eq!(rendered("RETURN duration({weeks: 1, days: 3}) AS r"), "P10D");
}

/// **Hours do not carry into days.**
///
/// A day is not always 24 hours once zones are involved, so Cypher does not
/// normalise. `28DT32H` is the answer; `29DT8H` is the tidy-looking wrong one.
#[test]
fn hours_do_not_normalise_into_days() {
    assert_eq!(rendered("RETURN duration({days: 14, hours: 16}) * 2 AS r"), "P28DT32H");
    assert_eq!(rendered("RETURN duration({hours: 30}) AS r"), "PT30H");
}

/// **A fractional month carries at 30.4369 days, not 30.**
///
/// Halving 12Y5M leaves half a month to convert. At 30 days/month the result
/// is `13:21:08` short by nearly five hours — a wrong answer that renders
/// perfectly well.
#[test]
fn a_fractional_month_uses_the_mean_gregorian_month() {
    // Half of 30.436875 days is 15.2184375 = 15d 5h 14m 33s exactly.
    //
    // At 30 days/month it would be 15d 0h 0m 0s, so the two rules are 5h 14m
    // apart on a single month — the same discrepancy the TCK table exposes at
    // 12Y5M, just visible without the arithmetic in between. My first
    // expectation here said 24s; the code said 33s and the code was right.
    assert_eq!(rendered("RETURN duration({months: 1}) * 0.5 AS r"), "P15DT5H14M33S");
}

/// Scaling by zero and by a negative number.
#[test]
fn zero_and_negative_factors() {
    assert_eq!(rendered("RETURN duration({days: 3}) * 0 AS r"), "PT0S");
    assert_eq!(rendered("RETURN duration({days: 3}) * -1 AS r"), "P-3D");
    assert_eq!(rendered("RETURN duration({days: 3, hours: 4}) * -2 AS r"), "P-6DT-8H");
}

/// Dividing by zero is an error, not an infinite duration.
#[test]
fn division_by_zero_is_refused() {
    let store = GraphStore::new();
    let q = parse_query("RETURN duration({days: 1}) / 0 AS r").expect("parses");
    let e = QueryExecutor::new(&store).execute(&q).expect_err("should refuse");
    assert!(format!("{e:?}").contains("zero"), "{e:?}");
}

/// Adding and subtracting two durations is componentwise, and unaffected.
#[test]
fn duration_addition_is_undisturbed() {
    let store = GraphStore::new();
    let q = parse_query(
        "RETURN duration({days: 1, hours: 2}) + duration({days: 3, hours: 4}) AS r",
    )
    .expect("parses");
    let batch = QueryExecutor::new(&store).execute(&q).expect("runs");
    match batch.records[0].get("r") {
        Some(Value::Property(PropertyValue::Duration { days, seconds, .. })) => {
            assert_eq!(*days, 4);
            assert_eq!(*seconds, 6 * 3600);
        }
        other => panic!("expected a Duration, got {other:?}"),
    }
}
