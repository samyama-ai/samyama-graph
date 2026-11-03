//! ISO 8601's alternative duration form (#1005).
//!
//! ```cypher
//! RETURN duration('P2012-02-02T14:37:21.545')
//! ```
//!
//! returned `PT0S`. The string is **valid** — it is ISO 8601's alternative
//! duration notation, which spells the components as a date and a clock rather
//! than with unit letters, and openCypher expects `P2012Y2M2DT14H37M21.545S`.
//!
//! The unit scanner could not read it: there are no units to read, and its `-`
//! handling is a *per-component sign*, so the date separators looked like signs
//! on empty numbers and the whole string scanned to zero. A valid duration
//! became a zero duration with no error.
//!
//! The two notations are mutually exclusive — one has unit letters, the other
//! has separators — so a shape test routes between them and neither parser
//! needs to know about the other.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn text(cypher: &str) -> String {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let c = r.columns[0].clone();
    let v = format!("{:?}", r.records[0].get(&c));
    let a = v.find('"').expect("a string result");
    v[a + 1..v.rfind('"').unwrap()].to_string()
}

fn dur(s: &str) -> String {
    text(&format!("RETURN toString(duration('{s}')) AS d"))
}

#[test]
fn the_extended_form_parses() {
    assert_eq!(dur("P2012-02-02T14:37:21.545"), "P2012Y2M2DT14H37M21.545S");
}

#[test]
fn the_basic_extended_form_parses_too() {
    // The same notation without separators.
    assert_eq!(dur("P20120202T143721.545"), "P2012Y2M2DT14H37M21.545S");
}

#[test]
fn it_agrees_with_the_unit_form() {
    assert_eq!(dur("P2012-02-02T14:37:21.545"), dur("P2012Y2M2DT14H37M21.545S"));
}

#[test]
fn the_fields_are_durations_not_calendar_positions() {
    // A month field is a count of months, not a month of the year, so no date
    // validation applies and the year may exceed any real year.
    assert_eq!(dur("P0001-13-01T00:00:00"), "P2Y1M1D");
}

#[test]
fn every_unit_form_case_is_untouched() {
    // Temporal2[7]'s other six examples, which the shape test must not divert.
    for (input, want) in [
        ("P14DT16H12M", "P14DT16H12M"),
        ("P5M1.5D", "P5M1DT12H"),
        ("P0.75M", "P22DT19H51M49.5S"),
        ("PT0.75M", "PT45S"),
        ("P2.5W", "P17DT12H"),
        ("P12Y5M14DT16H12M70S", "P12Y5M14DT16H13M10S"),
    ] {
        assert_eq!(dur(input), want, "{input}");
    }
}

#[test]
fn a_per_component_sign_still_works() {
    // #853: a `-` in the unit form is a sign, and `duration(toString(d)) = d`
    // must hold for a mixed-sign duration. The shape test must not read that
    // `-` as a date separator.
    assert_eq!(dur("P1DT-0.001S"), "P1DT-0.001S");
}
