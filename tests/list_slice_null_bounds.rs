//! A null slice bound is not an absent one (#845).
//!
//! ```text
//! [1,2,3][1..null]   is null,  was [2, 3]
//! [1,2,3][1..]       is [2, 3]
//! ```
//!
//! `eval_list_slice` matched an integer bound and sent everything else to one
//! `_` arm, so a bound that was **present and null** became indistinguishable
//! from one that was **omitted**. The result is a perfectly good list, which is
//! why nothing downstream noticed.
//!
//! Both halves are pinned: a fix that returned null for an omitted bound would
//! satisfy the null cases and break every ordinary slice.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn slice(expr: &str) -> PropertyValue {
    let store = GraphStore::new();
    let cypher = format!("WITH [1, 2, 3] AS list RETURN list{expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        Some(Value::Null) | None => PropertyValue::Null,
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

fn list(items: &[i64]) -> PropertyValue {
    PropertyValue::Array(items.iter().map(|i| PropertyValue::Integer(*i)).collect())
}

/// Every position a null can occupy.
#[test]
fn a_null_bound_makes_the_whole_slice_null() {
    for expr in ["[..null]", "[1..null]", "[null..3]", "[null..]", "[null..null]"] {
        assert_eq!(slice(expr), PropertyValue::Null, "list{expr}");
    }
}

/// **An omitted bound is a different thing** and still means "to the end".
#[test]
fn an_omitted_bound_is_unchanged() {
    assert_eq!(slice("[1..2]"), list(&[2]));
    assert_eq!(slice("[..2]"), list(&[1, 2]));
    assert_eq!(slice("[1..]"), list(&[2, 3]));
    assert_eq!(slice("[..]"), list(&[1, 2, 3]));
    assert_eq!(slice("[-2..]"), list(&[2, 3]));
    assert_eq!(slice("[..-1]"), list(&[1, 2]));
    // Out of range stays an empty list, not null.
    assert_eq!(slice("[5..9]"), list(&[]));
    assert_eq!(slice("[2..1]"), list(&[]));
}

/// A null bound reached through a variable behaves the same as a literal one —
/// the check is on the value, not on the syntax.
#[test]
fn a_null_bound_from_a_variable_behaves_the_same() {
    let store = GraphStore::new();
    let cypher = "WITH [1, 2, 3] AS list, null AS n RETURN list[1..n] AS r";
    let q = parse_query(cypher).expect("parses");
    let batch = QueryExecutor::new(&store).execute(&q).expect("runs");
    assert!(matches!(
        batch.records.first().and_then(|r| r.get("r")),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None
    ));
}
