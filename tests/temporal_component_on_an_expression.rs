//! `date('2024-05-06').year` answers, like `d.year` always did (LANG-16).
//!
//! Component access on a **bound variable** worked. Component access on an
//! **expression** went through the executor's index operator — which knows
//! about lists and maps — and was refused:
//!
//! ```text
//! cannot index Date: it is not a list or a map
//! ```
//!
//! Two spellings of the same question, taking two code paths, with only one of
//! them implemented. They now call the same function, and the tests below
//! assert the two spellings **agree** rather than that each works: a second
//! implementation that returned a plausible-but-different answer would pass a
//! test for either one alone.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

fn scalar(q: &str) -> Result<PropertyValue, String> {
    let mut store = GraphStore::new();
    let batch = QueryEngine::new()
        .execute_mut(q, &mut store, "default")
        .map_err(|e| e.to_string())?;
    match &batch.records.first().ok_or("no rows")?.bindings()[0].1 {
        Value::Property(p) => Ok(p.clone()),
        other => Err(format!("{other:?}")),
    }
}

/// The same component, asked both ways.
fn both_ways(constructor: &str, component: &str) -> (PropertyValue, PropertyValue) {
    let direct = scalar(&format!("RETURN {constructor}.{component} AS v"))
        .unwrap_or_else(|e| panic!("{constructor}.{component}: {e}"));
    let bound = scalar(&format!(
        "WITH {constructor} AS t RETURN t.{component} AS v"
    ))
    .unwrap_or_else(|e| panic!("bound {constructor}.{component}: {e}"));
    (direct, bound)
}

#[test]
fn a_date_component_reads_off_the_constructor() {
    let (direct, bound) = both_ways("date('2024-05-06')", "year");
    assert_eq!(direct, PropertyValue::Integer(2024));
    assert_eq!(direct, bound, "the two spellings disagree");
}

#[test]
fn every_date_component_agrees_between_the_two_spellings() {
    for component in ["year", "month", "day"] {
        let (direct, bound) = both_ways("date('2024-05-06')", component);
        assert_eq!(direct, bound, "date().{component} disagrees with d.{component}");
    }
}

#[test]
fn a_datetime_component_reads_off_the_constructor() {
    let (direct, bound) = both_ways("datetime('2024-05-06T10:30:00Z')", "hour");
    assert_eq!(direct, PropertyValue::Integer(10));
    assert_eq!(direct, bound);
}

#[test]
fn every_datetime_component_agrees() {
    for component in ["year", "month", "day", "hour", "minute", "second"] {
        let (direct, bound) = both_ways("datetime('2024-05-06T10:30:45Z')", component);
        assert_eq!(
            direct, bound,
            "datetime().{component} disagrees with dt.{component}"
        );
    }
}

#[test]
fn a_localtime_component_reads_off_the_constructor() {
    let (direct, bound) = both_ways("localtime('10:30:45')", "minute");
    assert_eq!(direct, PropertyValue::Integer(30));
    assert_eq!(direct, bound);
}

#[test]
fn a_value_with_no_components_is_still_refused() {
    // The half that stops this becoming "index anything". An integer has no
    // `.year`, and the error that said so must survive.
    let e = scalar("RETURN 1.year AS v").unwrap_err();
    assert!(
        e.contains("cannot index") || e.contains("Parse error"),
        "an integer should not have components: {e}"
    );
    let e = scalar("RETURN 'abc'.year AS v").unwrap_err();
    assert!(e.contains("cannot index"), "a string should not either: {e}");
}

#[test]
fn a_list_still_indexes_by_position() {
    // The path this shares with lists and maps must not have moved.
    assert_eq!(scalar("RETURN [10, 20, 30][1] AS v").expect("list"), PropertyValue::Integer(20));
}

#[test]
fn a_map_still_indexes_by_key() {
    assert_eq!(
        scalar("RETURN {a: 7}['a'] AS v").expect("map"),
        PropertyValue::Integer(7)
    );
}

#[test]
fn an_unknown_component_is_null_rather_than_an_error() {
    // Consistent with the bound-variable path, which answers null for a
    // component the type does not have. Divergence here would be the same
    // defect in a new place.
    let (direct, bound) = both_ways("date('2024-05-06')", "hour");
    assert_eq!(direct, bound);
}
