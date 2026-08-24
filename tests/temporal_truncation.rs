//! `<type>.truncate(unit, value, map)` and the namespaced function family (#769).
//!
//! Every scenario in `Temporal9` (truncation, 322) and `Temporal10`
//! (`duration.between`, 131) was a **parse error** — 453 of them, none
//! reaching the evaluator. `function_name` in the grammar had no dot, so the
//! whole namespaced family was unreachable from Cypher. `duration.between` had
//! been *implemented* the entire time and could never be called.
//!
//! The tests below are mostly about the two rules that are easy to get subtly
//! wrong: the namespace decides the result type, and the override map is
//! applied *after* truncation rather than being rounded away by it.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn one(cypher: &str) -> Result<PropertyValue, String> {
    let store = GraphStore::new();
    let q = parse_query(cypher).map_err(|e| format!("parse: {e:?}"))?;
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .map_err(|e| format!("exec: {e:?}"))?;
    Ok(match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        _ => PropertyValue::Null,
    })
}

fn rendered(cypher: &str) -> String {
    one(cypher).unwrap_or_else(|e| panic!("{cypher}\n  -> {e}")).to_cypher_string()
}

/// A dotted function name parses at all. This is the whole unlock.
#[test]
fn a_namespaced_function_call_parses() {
    assert!(parse_query("RETURN date.truncate('year', date('2017-10-11')) AS r").is_ok());
    assert!(parse_query("RETURN duration.between(date('2017-10-11'), date('2018-10-11')) AS r").is_ok());
}

/// Property access is unaffected: `n.name` has no `(` after it, so
/// `function_call` fails the rule and the parser falls back as before.
///
/// This is the risk the grammar change carries, and the reason the namespace
/// segment is optional and the rule still demands a paren.
#[test]
fn property_access_still_parses_as_property_access() {
    let store = GraphStore::new();
    let q = parse_query("MATCH (n) WHERE n.name = 'x' RETURN n.name AS r, n.age AS a").expect("parses");
    // Executing over an empty store is enough: it must plan and run, not error.
    QueryExecutor::new(&store).execute(&q).expect("runs");

    // A nested access is the harder case, and also unchanged.
    assert!(parse_query("MATCH (n) RETURN n.address.city AS r").is_ok());
}

/// Truncation to each unit coarser than a day moves the date to the start of
/// the period and zeroes the clock.
#[test]
fn coarse_units_move_to_the_start_of_the_period() {
    let d = "date({year: 2017, month: 10, day: 11})";
    assert_eq!(rendered(&format!("RETURN date.truncate('millennium', {d}) AS r")), "2000-01-01");
    assert_eq!(rendered(&format!("RETURN date.truncate('century', {d}) AS r")), "2000-01-01");
    assert_eq!(rendered(&format!("RETURN date.truncate('decade', {d}) AS r")), "2010-01-01");
    assert_eq!(rendered(&format!("RETURN date.truncate('year', {d}) AS r")), "2017-01-01");
    assert_eq!(rendered(&format!("RETURN date.truncate('quarter', {d}) AS r")), "2017-10-01");
    assert_eq!(rendered(&format!("RETURN date.truncate('month', {d}) AS r")), "2017-10-01");
    // 2017-10-11 is a Wednesday; the week starts on the Monday.
    assert_eq!(rendered(&format!("RETURN date.truncate('week', {d}) AS r")), "2017-10-09");
}

/// The override map is applied **after** truncation, so a component the unit
/// would have zeroed survives.
///
/// `{day: 2}` on a millennium truncation is `2000-01-02`. An implementation
/// that applied the map first, or that treated truncation as "zero everything
/// below the unit" and ignored the map, gives `2000-01-01` — which looks
/// entirely reasonable and is wrong.
#[test]
fn the_override_map_is_applied_after_truncation() {
    let d = "date({year: 2017, month: 10, day: 11})";
    assert_eq!(rendered(&format!("RETURN date.truncate('millennium', {d}, {{day: 2}}) AS r")), "2000-01-02");
    assert_eq!(rendered(&format!("RETURN date.truncate('year', {d}, {{month: 5}}) AS r")), "2017-05-01");
}

/// The **namespace** decides the result type, not the input.
///
/// `date.truncate` over a datetime returns a Date. Reading the type off the
/// input instead would return a datetime and render with a clock attached.
#[test]
fn the_namespace_decides_the_result_type() {
    let dt = "datetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: '+01:00'})";
    assert_eq!(rendered(&format!("RETURN date.truncate('year', {dt}) AS r")), "2017-01-01");

    let got = one(&format!("RETURN date.truncate('year', {dt}) AS r")).unwrap();
    assert!(matches!(got, PropertyValue::Date(_)), "expected a Date, got {got:?}");

    let got = one(&format!("RETURN localdatetime.truncate('day', {dt}) AS r")).unwrap();
    assert!(matches!(got, PropertyValue::LocalDateTime { .. }), "got {got:?}");
}

/// Units finer than a day keep the date and zero only what is below them.
#[test]
fn fine_units_keep_the_date_and_zero_below() {
    let dt = "localdatetime({year: 2017, month: 10, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123})";
    assert_eq!(rendered(&format!("RETURN localdatetime.truncate('day', {dt}) AS r")), "2017-10-11T00:00");
    assert_eq!(rendered(&format!("RETURN localdatetime.truncate('hour', {dt}) AS r")), "2017-10-11T12:00");
    assert_eq!(rendered(&format!("RETURN localdatetime.truncate('minute', {dt}) AS r")), "2017-10-11T12:31");
    assert_eq!(rendered(&format!("RETURN localdatetime.truncate('second', {dt}) AS r")), "2017-10-11T12:31:14");
    assert_eq!(
        rendered(&format!("RETURN localdatetime.truncate('millisecond', {dt}) AS r")),
        "2017-10-11T12:31:14.645"
    );
    assert_eq!(
        rendered(&format!("RETURN localdatetime.truncate('microsecond', {dt}) AS r")),
        "2017-10-11T12:31:14.645876"
    );
}

/// An unknown unit is an error, not a silent no-op.
#[test]
fn an_unknown_unit_is_refused() {
    let e = one("RETURN date.truncate('fortnight', date('2017-10-11')) AS r").unwrap_err();
    assert!(e.contains("fortnight"), "the message should name the unit: {e}");
}

/// `duration.between` was implemented all along and unreachable. These are the
/// scenarios the grammar alone unblocked.
#[test]
fn duration_between_is_reachable_now() {
    let r = one("RETURN duration.between(date('2017-10-11'), date('2017-10-13')) AS r").unwrap();
    match r {
        PropertyValue::Duration { days, .. } => assert_eq!(days, 2),
        other => panic!("expected a Duration, got {other:?}"),
    }
}

/// Null in, null out — truncation included.
#[test]
fn truncation_propagates_null() {
    assert_eq!(one("RETURN date.truncate('year', null) AS r").unwrap(), PropertyValue::Null);
}
