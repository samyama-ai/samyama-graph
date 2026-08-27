//! ISO-8601 in every spelling the corpus uses, and duration signs (#775).
//!
//! Two unrelated defects, both found by reading failure *detail* rather than
//! counts.
//!
//! **Parsing.** The string constructors used a short list of `chrono` format
//! strings — four shapes out of the dozen openCypher permits. Compact forms
//! (`20150721T21:40`), week dates (`2015-W30-2T214032.142`), ordinal dates
//! (`2015-202T21:40:32`) and bracketed zones
//! (`...+02:00[Europe/Stockholm]`) were all parse errors.
//!
//! **Duration signs.** `duration.inSeconds` had its operands reversed, and the
//! (seconds, nanos) split used Euclidean division so the two components could
//! disagree in sign. Together those produced `PT-1.6S` where `PT0.4S` was
//! expected — and looked *correct* on exactly the half of the corpus whose
//! answers happen to be positive.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::temporal as tmp;
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

/// Every date spelling, extended and compact.
#[test]
fn dates_parse_in_every_iso_spelling() {
    for (input, want) in [
        ("2015-07-21", "2015-07-21"),
        ("20150721", "2015-07-21"),
        ("2015-W30-2", "2015-07-21"),
        ("2015W302", "2015-07-21"),
        ("2015-202", "2015-07-21"),
        ("2015202", "2015-07-21"),
    ] {
        assert_eq!(
            PropertyValue::Date(tmp::parse_iso_date(input).unwrap_or_else(|e| panic!("{input}: {e}")))
                .to_cypher_string(),
            want,
            "date({input})"
        );
    }
}

/// Every time spelling, including a comma fraction, which ISO-8601 allows.
#[test]
fn times_parse_in_every_iso_spelling() {
    for (input, want) in [
        ("21:40:32.142", "21:40:32.142"),
        ("214032.142", "21:40:32.142"),
        ("21:40:32", "21:40:32"),
        ("214032", "21:40:32"),
        ("21:40", "21:40"),
        ("2140", "21:40"),
        ("21", "21:00"),
        ("21:40:32,142", "21:40:32.142"),
    ] {
        assert_eq!(
            PropertyValue::LocalTime(tmp::parse_iso_time(input).unwrap_or_else(|e| panic!("{input}: {e}")))
                .to_cypher_string(),
            want,
            "time({input})"
        );
    }
}

/// A bracketed zone suffix, with and without a written offset.
///
/// They are not redundant: the offset is what the value had when written, the
/// zone is the rule it follows. Dropping either loses information Cypher keeps.
#[test]
fn a_bracketed_zone_suffix_is_understood() {
    assert_eq!(
        rendered("RETURN datetime('2015-07-21T21:40:32.142+02:00[Europe/Stockholm]') AS r"),
        "2015-07-21T21:40:32.142+02:00[Europe/Stockholm]"
    );
    // No written offset: resolved from the zone at that date (CEST, +02:00).
    assert_eq!(
        rendered("RETURN datetime('2015-07-21T21:40:32.142[Europe/Stockholm]') AS r"),
        "2015-07-21T21:40:32.142+02:00[Europe/Stockholm]"
    );
    // A different date in the same zone gives a different offset (CET).
    assert_eq!(
        rendered("RETURN datetime('2015-01-21T21:40:32[Europe/Stockholm]') AS r"),
        "2015-01-21T21:40:32+01:00[Europe/Stockholm]"
    );
}

/// A `-` inside a date is not an offset sign.
///
/// `2015-07-21T21:40:32-04` ends with an offset; `2015-07-21` does not. The
/// rule is that an offset sign must come after the `T`.
#[test]
fn a_date_dash_is_not_mistaken_for_an_offset() {
    assert_eq!(rendered("RETURN date('2015-07-21') AS r"), "2015-07-21");
    assert_eq!(
        rendered("RETURN datetime('2015-07-21T21:40:32-04:00') AS r"),
        "2015-07-21T21:40:32-04:00"
    );
    assert_eq!(
        rendered("RETURN datetime('2015-07-21T21:40:32+0100') AS r"),
        "2015-07-21T21:40:32+01:00"
    );
}

/// Mixed date and time spellings compose.
#[test]
fn compact_and_extended_forms_compose() {
    for (input, want) in [
        ("20150721T21:40", "2015-07-21T21:40"),
        ("2015-W30-2T214032.142", "2015-07-21T21:40:32.142"),
        ("2015-202T21:40:32", "2015-07-21T21:40:32"),
    ] {
        assert_eq!(rendered(&format!("RETURN localdatetime('{input}') AS r")), want, "{input}");
    }
}

/// **A duration's components must share a sign.**
///
/// Euclidean division splits -0.4s into (-1s, +600ms) and renders `PT-1.6S`.
/// Every row here comes from the TCK, and the negative ones are the point: the
/// bug was invisible on positive answers, which is half the corpus.
#[test]
fn duration_components_share_a_sign() {
    let cases = [
        ("12:34:54.7", "12:34:54.3", "PT-0.4S"),
        ("12:34:54.3", "12:34:54.7", "PT0.4S"),
        ("12:34:54.7", "12:34:55.3", "PT0.6S"),
        ("12:34:54.7", "12:44:55.3", "PT10M0.6S"),
        ("12:44:54.7", "12:34:55.3", "PT-9M-59.4S"),
        ("12:34:56", "12:34:55.7", "PT-0.3S"),
        ("12:34:56.3", "12:34:54.7", "PT-1.6S"),
        ("12:34:54.7", "12:34:56.3", "PT1.6S"),
    ];
    for (lhs, rhs, want) in cases {
        assert_eq!(
            rendered(&format!("RETURN duration.inSeconds(localtime('{lhs}'), localtime('{rhs}')) AS r")),
            want,
            "duration.inSeconds({lhs}, {rhs})"
        );
    }
}

/// `duration.inX(lhs, rhs)` is **rhs - lhs** — the duration you would add to
/// `lhs` to reach `rhs`, the same orientation as `duration.between`.
#[test]
fn the_operand_order_is_from_then_to() {
    assert_eq!(
        rendered("RETURN duration.inDays(date('2017-10-11'), date('2017-10-13')) AS r"),
        "P2D"
    );
    assert_eq!(
        rendered("RETURN duration.inDays(date('2017-10-13'), date('2017-10-11')) AS r"),
        "P-2D"
    );
}
