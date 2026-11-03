//! Indexing something that is not a list or map is a type error (#789).
//!
//! `eval_index` ended in `_ => Ok(Value::Null)`, so every unhandled pair
//! answered null:
//!
//! ```text
//! WITH true AS list, 0 AS idx RETURN list[idx]     -- expected TypeError, got null
//! ```
//!
//! That is the failure this codebase keeps producing — a wrong answer that
//! looks like a legitimate "no such element". Nothing distinguishes "the list
//! has no element 5" from "that was never a list".
//!
//! The risk in fixing it runs the other way: an over-eager error breaks
//! queries that legitimately index past the end, or index a null. Most of this
//! file is therefore about what must **not** raise.

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(cypher: &str) -> Result<Value, String> {
    let store = GraphStore::new();
    let q = parse_query(cypher).map_err(|e| format!("parse: {e:?}"))?;
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .map_err(|e| format!("exec: {e:?}"))?;
    Ok(batch
        .records
        .first()
        .and_then(|r| r.get("r"))
        .cloned()
        .unwrap_or(Value::Null))
}

fn raises(cypher: &str) -> bool {
    run(cypher).is_err()
}
fn is_null(cypher: &str) -> bool {
    matches!(run(cypher), Ok(Value::Null))
}

/// Indexing a non-list, every type the TCK lists.
#[test]
fn indexing_a_non_list_raises() {
    for expr in ["true", "123", "4.7", "'1'"] {
        assert!(
            raises(&format!("WITH {expr} AS list, 0 AS idx RETURN list[idx] AS r")),
            "indexing {expr} should raise"
        );
    }
}

/// Indexing a list with a non-integer.
#[test]
fn a_non_integer_index_raises() {
    for idx in ["true", "4.7", "'x'", "[1]", "{a: 1}"] {
        assert!(
            raises(&format!("WITH [1, 2, 3] AS list, {idx} AS idx RETURN list[idx] AS r")),
            "index {idx} should raise"
        );
    }
}

/// **Out of range is null, not an error.**
///
/// A list that has no element 5 is a different thing from a value that was
/// never a list. This is the distinction the old catch-all destroyed, and the
/// one most easily destroyed again from the other side.
#[test]
fn an_out_of_range_index_is_still_null() {
    assert!(is_null("WITH [1, 2, 3] AS list RETURN list[10] AS r"));
    assert!(is_null("WITH [1, 2, 3] AS list RETURN list[-10] AS r"));
    assert!(is_null("WITH [] AS list RETURN list[0] AS r"));
    assert!(is_null("WITH {a: 1} AS m RETURN m['nope'] AS r"));
}

/// **Null in, null out.** An unknown collection has an unknown element.
#[test]
fn indexing_null_is_null_not_an_error() {
    assert!(is_null("WITH null AS list RETURN list[0] AS r"));
    assert!(is_null("WITH [1, 2] AS list RETURN list[null] AS r"));
    assert!(is_null("WITH null AS list RETURN list[null] AS r"));
}

/// Ordinary indexing is undisturbed, including the cases earlier fixes added.
#[test]
fn ordinary_indexing_still_works() {
    let cases = [
        ("WITH [1, 2, 3] AS list RETURN list[0] AS r", 1i64),
        ("WITH [1, 2, 3] AS list RETURN list[2] AS r", 3),
        ("WITH [1, 2, 3] AS list RETURN list[-1] AS r", 3),
    ];
    for (q, want) in cases {
        match run(q) {
            Ok(Value::Property(samyama::graph::PropertyValue::Integer(i))) => assert_eq!(i, want, "{q}"),
            other => panic!("{q}\n  got {other:?}"),
        }
    }
    // A map by string key, and an all-float literal that parses as a Vector.
    assert!(run("WITH {a: 7} AS m RETURN m['a'] AS r").is_ok());
    assert!(run("WITH [1.0, 2.0] AS v RETURN v[0] AS r").is_ok());
}

/// The message names the operand that is wrong, so the reader is sent to the
/// right half of the expression.
#[test]
fn the_error_says_which_operand_is_wrong() {
    let e = run("WITH 123 AS list, 0 AS idx RETURN list[idx] AS r").unwrap_err();
    assert!(e.contains("not a list or a map"), "{e}");

    let e = run("WITH [1, 2] AS list, 'x' AS idx RETURN list[idx] AS r").unwrap_err();
    assert!(e.contains("index must be an integer"), "{e}");
}
