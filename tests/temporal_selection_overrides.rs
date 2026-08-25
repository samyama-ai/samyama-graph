//! Overrides layer onto a **selected** temporal, in every constructor (#802).
//!
//! ```text
//! date({date: other, day: 28})
//!   expected 1984-11-28, got 1984-11-11
//! ```
//!
//! #772 taught the *composite* constructors (`datetime`, `localdatetime`) to
//! layer component overrides onto a selected value. `date()`, `localtime()`
//! and `time()` have their own selection paths and returned the selected value
//! unchanged, silently discarding every override.
//!
//! The same rule implemented in two places, one of them missed — the shape
//! that keeps recurring here. `apply_date_overrides` and
//! `apply_time_overrides` now hold it once each so the paths cannot drift.

use samyama::graph::GraphStore;
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

const D: &str = "date({year: 1984, month: 11, day: 11})";
const T: &str = "localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123})";

/// A calendar override replaces only the field it names.
#[test]
fn a_date_override_replaces_only_that_field() {
    assert_eq!(rendered(&format!("WITH {D} AS o RETURN date({{date: o, day: 28}}) AS r")), "1984-11-28");
    assert_eq!(rendered(&format!("WITH {D} AS o RETURN date({{date: o, month: 1}}) AS r")), "1984-01-11");
    assert_eq!(rendered(&format!("WITH {D} AS o RETURN date({{date: o, year: 2000}}) AS r")), "2000-11-11");
}

/// **`ordinalDay` replaces the whole day-of-year**, so it moves the month too.
///
/// The four date spellings do not mix: naming `ordinalDay` means the calendar
/// fields are not consulted. Defaulting each field independently would give
/// `1984-11-28` here instead of `1984-01-28`.
#[test]
fn an_ordinal_override_moves_the_month() {
    assert_eq!(
        rendered(&format!("WITH {D} AS o RETURN date({{date: o, ordinalDay: 28}}) AS r")),
        "1984-01-28"
    );
}

/// With no override the selected value comes back unchanged.
#[test]
fn selection_without_an_override_is_unchanged() {
    assert_eq!(rendered(&format!("WITH {D} AS o RETURN date({{date: o}}) AS r")), "1984-11-11");
    assert_eq!(rendered(&format!("WITH {T} AS o RETURN localtime({{time: o}}) AS r")), "12:31:14.645876123");
}

/// Clock overrides layer the same way, keeping the fraction.
#[test]
fn a_time_override_keeps_the_rest_of_the_clock() {
    assert_eq!(
        rendered(&format!("WITH {T} AS o RETURN localtime({{time: o, second: 42}}) AS r")),
        "12:31:42.645876123"
    );
    assert_eq!(
        rendered(&format!("WITH {T} AS o RETURN localtime({{time: o, hour: 1}}) AS r")),
        "01:31:14.645876123"
    );
}

/// Naming any sub-second component replaces the whole fraction, and the three
/// are additive with each other.
#[test]
fn a_sub_second_override_replaces_the_fraction() {
    assert_eq!(
        rendered(&format!("WITH {T} AS o RETURN localtime({{time: o, millisecond: 5}}) AS r")),
        "12:31:14.005"
    );
    assert_eq!(
        rendered(&format!(
            "WITH {T} AS o RETURN localtime({{time: o, millisecond: 1, microsecond: 2, nanosecond: 3}}) AS r"
        )),
        "12:31:14.001002003"
    );
}

/// `time()` keeps its offset while layering the clock.
#[test]
fn a_zoned_time_override_keeps_the_offset() {
    assert_eq!(
        rendered(&format!(
            "WITH {T} AS o RETURN time({{time: o, second: 42, timezone: '+02:00'}}) AS r"
        )),
        "12:31:42.645876123+02:00"
    );
}

/// Construction from components alone is undisturbed — the override path must
/// not have displaced it.
#[test]
fn component_only_construction_still_works() {
    assert_eq!(rendered("RETURN date({year: 1984, month: 11, day: 11}) AS r"), "1984-11-11");
    assert_eq!(rendered("RETURN localtime({hour: 12, minute: 31}) AS r"), "12:31");
    assert_eq!(rendered("RETURN date({year: 1984, ordinalDay: 202}) AS r"), "1984-07-20");
}

// ---------------------------------------------------------------------------
// Re-zoning a selected value (#809).
// ---------------------------------------------------------------------------

/// Selecting a **zoned** source into a different zone converts the instant.
///
/// 12:00 in Europe/Stockholm (+01:00 in October) is 11:00 UTC, which is 01:00
/// in Pacific/Honolulu (−10:00). Copying the wall clock gives 12:00 — the same
/// numbers in a different zone, describing a moment eleven hours away.
#[test]
fn re_zoning_a_zoned_source_converts_the_instant() {
    let src = "datetime({year: 1984, month: 10, day: 28, hour: 12, timezone: 'Europe/Stockholm'})";
    assert_eq!(
        rendered(&format!(
            "WITH {src} AS s RETURN datetime({{datetime: s, timezone: 'Pacific/Honolulu'}}) AS r"
        )),
        "1984-10-28T01:00-10:00[Pacific/Honolulu]"
    );
}

/// **An unzoned source is read as local time in the target zone.**
///
/// This is the other half, and the two are easy to conflate: with no source
/// zone there is no instant to convert *from*, so the components mean what
/// they say in the zone being applied. Converting here instead would shift
/// every plain construction by the target offset.
#[test]
fn an_unzoned_source_is_local_time_in_the_target_zone() {
    let src = "localdatetime({year: 1984, month: 10, day: 28, hour: 12})";
    assert_eq!(
        rendered(&format!(
            "WITH {src} AS s RETURN datetime({{datetime: s, timezone: 'Pacific/Honolulu'}}) AS r"
        )),
        "1984-10-28T12:00-10:00[Pacific/Honolulu]"
    );
    // And plain component construction is unaffected.
    assert_eq!(
        rendered("RETURN datetime({year: 1984, month: 10, day: 28, hour: 12, timezone: '+02:00'}) AS r"),
        "1984-10-28T12:00+02:00"
    );
}

/// Selecting with no target zone keeps the source's own.
#[test]
fn selecting_without_a_target_zone_keeps_the_source_zone() {
    let src = "datetime({year: 1984, month: 10, day: 28, hour: 12, timezone: '+02:00'})";
    assert_eq!(
        rendered(&format!("WITH {src} AS s RETURN datetime({{datetime: s}}) AS r")),
        "1984-10-28T12:00+02:00"
    );
}
