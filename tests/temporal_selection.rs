//! Building one temporal type out of another (#772).
//!
//! `datetime({date: d, time: t})` — the *selection* form. Two independent
//! things were wrong and each accounted for a share of `Temporal3`'s 174
//! failures:
//!
//! 1. A map literal containing variables evaluates to `Value::Map`, not
//!    `Value::Property(PropertyValue::Map)`, so the map arm never matched and
//!    every selection was "requires a string or map argument". The feature was
//!    implemented and unreachable through the shape the executor produces —
//!    the same class of gap as #769, one layer in.
//! 2. Overrides replaced the whole clock instead of layering onto the selected
//!    value, so `{date: d, time: t, second: 42}` gave `00:00:42` rather than
//!    keeping `12:31` from `t`. A plausible-looking wrong answer.

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

const D: &str = "date({year: 1984, month: 10, day: 11})";
const T: &str = "localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123})";

/// The straightforward selection, and the case that never matched at all.
#[test]
fn a_date_and_a_time_select_into_a_datetime() {
    assert_eq!(
        rendered(&format!("WITH {D} AS d, {T} AS t RETURN datetime({{date: d, time: t}}) AS r")),
        "1984-10-11T12:31:14.645876123Z"
    );
    assert_eq!(
        rendered(&format!("WITH {D} AS d, {T} AS t RETURN localdatetime({{date: d, time: t}}) AS r")),
        "1984-10-11T12:31:14.645876123"
    );
}

/// An offset given alongside the selection is applied to it.
#[test]
fn a_timezone_can_be_given_with_the_selection() {
    assert_eq!(
        rendered(&format!(
            "WITH {D} AS d, {T} AS t RETURN datetime({{date: d, time: t, timezone: '+05:00'}}) AS r"
        )),
        "1984-10-11T12:31:14.645876123+05:00"
    );
}

/// **Overrides layer onto the selected value; they do not replace it.**
///
/// `{date: d, time: t, day: 28, second: 42}` keeps the year and month from `d`
/// and the hour, minute and fraction from `t`, replacing only the day and the
/// second. Reading the components as a whole clock gives `1984-10-28T00:00:42`
/// — which is exactly what this returned before, and looks entirely reasonable.
#[test]
fn overrides_replace_only_the_fields_they_name() {
    assert_eq!(
        rendered(&format!(
            "WITH {D} AS d, {T} AS t RETURN datetime({{date: d, time: t, day: 28, second: 42}}) AS r"
        )),
        "1984-10-28T12:31:42.645876123Z"
    );
    // Only the hour, with the fraction preserved.
    assert_eq!(
        rendered(&format!("WITH {D} AS d, {T} AS t RETURN datetime({{date: d, time: t, hour: 1}}) AS r")),
        "1984-10-11T01:31:14.645876123Z"
    );
}

/// With nothing selected, the components *are* the whole clock — the other
/// half of the rule above.
#[test]
fn without_a_selection_the_components_are_the_whole_value() {
    assert_eq!(
        rendered("RETURN datetime({year: 1984, month: 10, day: 11, second: 42}) AS r"),
        "1984-10-11T00:00:42Z"
    );
}

/// A bare temporal value widens into a composite type.
#[test]
fn a_bare_temporal_value_can_be_widened() {
    let ldt = "localdatetime({year: 1984, month: 3, day: 7, hour: 12, minute: 31, second: 14, millisecond: 645})";
    assert_eq!(rendered(&format!("RETURN datetime({ldt}) AS r")), "1984-03-07T12:31:14.645Z");
    assert_eq!(rendered(&format!("RETURN localdatetime({ldt}) AS r")), "1984-03-07T12:31:14.645");
    assert_eq!(rendered(&format!("RETURN date({ldt}) AS r")), "1984-03-07");
}

/// Selecting out of a datetime takes both parts from it.
#[test]
fn a_datetime_selects_into_its_parts() {
    let dt = "datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: '+01:00'})";
    assert_eq!(rendered(&format!("WITH {dt} AS x RETURN date({{date: x}}) AS r")), "1984-10-11");
    assert_eq!(rendered(&format!("WITH {dt} AS x RETURN localtime({{time: x}}) AS r")), "12:31:14");
}

/// A map naming nothing the constructor understands is still refused, and the
/// selection work did not weaken that (#595).
#[test]
fn an_unrecognised_map_is_still_refused() {
    let store = GraphStore::new();
    let q = parse_query("RETURN datetime({nonsense: 1}) AS r").expect("parses");
    let e = QueryExecutor::new(&store).execute(&q).expect_err("should be refused");
    let msg = format!("{e:?}");
    assert!(msg.contains("nonsense"), "the message should name what was given: {msg}");
}
