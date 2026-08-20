//! `STARTS WITH`, `ENDS WITH` and `CONTAINS` on a non-string yield null.
//!
//! Not only on null — on *any* non-string operand. The TCK asks for all 36
//! pairings drawn from `[1, 3.14, true, [], {}, null]` and expects null for
//! every one of them (String8/9/10 scenario 8). The engine raised
//! `TypeError("STARTS WITH requires string operands")`, which aborts the
//! query rather than yielding the unknown answer.
//!
//! The comparison is implemented twice, at two call sites that had drifted
//! into the same wrong answer independently; these tests exercise the
//! behaviour through queries so they pin whichever copy the planner routes to.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn result_of(cypher: &str) -> Value {
    let q = parse_query(cypher).expect("query should parse");
    let out = QueryExecutor::new(&GraphStore::new())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run, not error: {e}"));
    out.records[0].get("res").cloned().expect("column res")
}

fn assert_null(cypher: &str) {
    let got = result_of(cypher);
    assert!(
        matches!(got, Value::Null | Value::Property(PropertyValue::Null)),
        "`{cypher}` should be null, got {got:?}"
    );
}

#[test]
fn non_string_operands_yield_null_not_an_error() {
    for op in ["STARTS WITH", "ENDS WITH", "CONTAINS"] {
        assert_null(&format!("RETURN 1 {op} 'a' AS res"));
        assert_null(&format!("RETURN 'a' {op} 1 AS res"));
        assert_null(&format!("RETURN 3.14 {op} 'a' AS res"));
        assert_null(&format!("RETURN true {op} 'a' AS res"));
        assert_null(&format!("RETURN null {op} 'a' AS res"));
        assert_null(&format!("RETURN 'a' {op} null AS res"));
        assert_null(&format!("RETURN [] {op} 'a' AS res"));
    }
}

#[test]
fn string_operands_still_answer_normally() {
    // The guard must not swallow the cases the operators exist for.
    assert_eq!(
        result_of("RETURN 'abc' STARTS WITH 'ab' AS res"),
        Value::Property(PropertyValue::Boolean(true))
    );
    assert_eq!(
        result_of("RETURN 'abc' ENDS WITH 'bc' AS res"),
        Value::Property(PropertyValue::Boolean(true))
    );
    assert_eq!(
        result_of("RETURN 'abc' CONTAINS 'zz' AS res"),
        Value::Property(PropertyValue::Boolean(false))
    );
}
