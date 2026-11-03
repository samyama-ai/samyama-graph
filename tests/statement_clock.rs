//! "Now" is fixed once per statement, not once per call (#793).
//!
//! ```text
//! RETURN duration.inSeconds(datetime(), datetime())
//!   expected PT0S, got PT0.00000016S
//! ```
//!
//! Each `datetime()` read the wall clock independently, so two calls in one
//! query landed microseconds apart.
//!
//! Not merely a test artefact. `WHERE n.created < datetime() AND n.expires >
//! datetime()` should test one instant against both bounds; with a moving
//! clock a row arriving between the two reads is judged against a target that
//! shifted underneath it. The bug is small in magnitude and unbounded in
//! consequence.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn eval(cypher: &str) -> PropertyValue {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

/// Two calls to the same constructor in one statement give the same instant.
#[test]
fn two_calls_in_one_statement_agree() {
    for f in ["localtime()", "time()", "datetime()", "localdatetime()", "date()"] {
        assert_eq!(
            eval(&format!("RETURN duration.inSeconds({f}, {f}) AS r")).to_cypher_string(),
            "PT0S",
            "duration between two {f} calls"
        );
    }
}

/// ...and compare equal, which is the same property stated where a user would
/// meet it.
#[test]
fn the_clock_is_stable_across_an_expression() {
    assert_eq!(eval("RETURN datetime() = datetime() AS r"), PropertyValue::Boolean(true));
    assert_eq!(eval("RETURN date() = date() AS r"), PropertyValue::Boolean(true));
    // Different constructors read the same underlying instant, so the date
    // part of `datetime()` is `date()`.
    assert_eq!(eval("RETURN date(datetime()) = date() AS r"), PropertyValue::Boolean(true));
}

/// **The clock is fixed per statement, not frozen.**
///
/// This is the half that a naive "cache now() forever" implementation gets
/// wrong, and it would not show up in any TCK scenario — every one of them is
/// a single statement.
#[test]
fn separate_statements_see_time_move() {
    let a = eval("RETURN datetime() AS r");
    std::thread::sleep(std::time::Duration::from_millis(30));
    let b = eval("RETURN datetime() AS r");
    assert_ne!(a, b, "a later statement must see a later instant");
}

/// A statement that errors must still release the clock, or every later
/// statement on the thread inherits a stale "now".
#[test]
fn a_failed_statement_does_not_leak_its_clock() {
    let store = GraphStore::new();
    if let Ok(q) = parse_query("RETURN 1/0 AS r") {
        let _ = QueryExecutor::new(&store).execute(&q);
    }
    std::thread::sleep(std::time::Duration::from_millis(30));
    let after = eval("RETURN datetime() AS r");
    std::thread::sleep(std::time::Duration::from_millis(30));
    let later = eval("RETURN datetime() AS r");
    assert_ne!(after, later, "the clock must still advance after a failed statement");
}
