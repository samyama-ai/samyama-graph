//! `duration.between` counts calendar components, not elapsed days (#804).
//!
//! ```text
//! duration.between(date('1984-10-11'), date('2015-06-24'))
//!   expected P30Y8M13D, got P11213D
//! ```
//!
//! A month has no fixed length, so the answer must be the one you get by
//! *counting off* years and months on a calendar — not by dividing elapsed
//! time. The `-` operator stays a plain elapsed difference; only
//! `duration.between` is calendar-aware, and collapsing the two would make the
//! same subtraction disagree with itself depending on spelling.
//!
//! Three rules here were found by measurement, each after a wrong version that
//! produced a **well-formed duration describing the right instants**:
//!
//! 1. Within one month the answer is the plain elapsed form — `PT6H`, not
//!    `P0M0DT6H`. Going through month arithmetic for those cost four
//!    regressions: correct values, wrong shape.
//! 2. Borrowing runs **toward zero**. Backwards, a partial month stays as days:
//!    `P-27D`, not `P-1M3D`.
//! 3. The (seconds, nanos) split must be **truncating**. Euclidean division
//!    gives `-33.858S` where `-32.142S` belongs — the same trap #775 fixed in
//!    the sibling function, which I then reintroduced here.

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

/// The TCK's own rows.
#[test]
fn the_calendar_form_matches_the_reference() {
    assert_eq!(
        rendered("duration.between(date('1984-10-11'), date('2015-06-24'))"),
        "P30Y8M13D"
    );
    assert_eq!(
        rendered("duration.between(date('1984-10-11'), datetime('2015-07-21T21:40:32.142+0100'))"),
        "P30Y9M10DT21H40M32.142S"
    );
}

/// **Within one month the plain elapsed form is the answer**, in the shape the
/// TCK expects.
#[test]
fn a_same_month_difference_is_plain_elapsed() {
    assert_eq!(
        rendered("duration.between(localdatetime('2018-01-01T12:00'), localdatetime('2018-01-02T10:00'))"),
        "PT22H"
    );
    assert_eq!(
        rendered("duration.between(localdatetime('2018-01-02T10:00'), localdatetime('2018-01-01T12:00'))"),
        "PT-22H"
    );
}

/// **Borrowing runs toward zero.** Backwards, a partial month stays as days.
#[test]
fn a_backwards_partial_month_stays_in_days() {
    assert_eq!(
        rendered("duration.between(localdatetime('2015-07-21T21:40:32.142'), date('2015-06-24'))"),
        "P-27DT-21H-40M-32.142S"
    );
}

/// **The (seconds, nanos) split is truncating.**
///
/// Every component of the answer above is negative. Euclidean division floors
/// toward negative infinity and produces `-33.858S` — the same instants, with
/// the seconds and the fraction disagreeing in sign, which a duration may not
/// do. Asserted separately because it is the rule most easily lost in a later
/// edit: the value still looks like a duration.
#[test]
fn negative_components_share_a_sign() {
    let got = rendered("duration.between(localdatetime('2015-07-21T21:40:32.142'), date('2015-06-24'))");
    assert!(!got.contains("T2H"), "the clock part borrowed the wrong way: {got}");
    assert!(got.contains("-32.142S"), "the fraction lost its sign: {got}");
}

/// A month boundary one day apart still crosses a month — the threshold is a
/// differing (year, month), not a day count.
#[test]
fn one_day_across_a_month_boundary_counts_as_a_month() {
    // 31 Jan to 1 Feb: one day elapsed, and the months differ.
    let got = rendered("duration.between(date('2018-01-31'), date('2018-02-01'))");
    assert_eq!(got, "P1D", "one day, and no phantom month: {got}");
}

/// Time-only pairs are untouched — they have no calendar part.
#[test]
fn time_only_pairs_use_the_plain_difference() {
    assert_eq!(
        rendered("duration.inSeconds(localtime('12:34:54.7'), localtime('12:34:54.3'))"),
        "PT-0.4S"
    );
}
