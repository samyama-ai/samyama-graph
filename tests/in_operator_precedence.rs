//! `IN` binds tighter than every comparison operator (#833).
//!
//! ```text
//! true < false IN [false]
//!   is  true < (false IN [false])   ->  true < true   ->  false
//!   not (true < false) IN [false]   ->  false IN [false]  ->  true
//! ```
//!
//! `in_op` shared a Pratt level with `comparison_op`, so the two grouped left
//! to right. The readings agree whenever the list happens to hold what the
//! comparison would have produced — which is most hand-written queries and
//! every example anyone reaches for.
//!
//! So each case below asserts the bare form against the **right**-grouped form
//! *and* checks that the left grouping would have given something else. A case
//! where both groupings agree proves nothing, and this file would be quietly
//! vacuous without that second half.

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `Some(bool)` for a definite answer, `None` for null.
fn truth(expr: &str) -> Option<bool> {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(samyama::graph::PropertyValue::Boolean(b))) => Some(*b),
        _ => None,
    }
}

/// `lhs op rhs IN list` must equal `lhs op (rhs IN list)`.
fn groups_right(lhs: &str, op: &str, rhs: &str, list: &str) {
    let bare = truth(&format!("{lhs} {op} {rhs} IN {list}"));
    let right = truth(&format!("{lhs} {op} ({rhs} IN {list})"));
    let left = truth(&format!("({lhs} {op} {rhs}) IN {list}"));
    assert_eq!(bare, right, "{lhs} {op} {rhs} IN {list} did not group as `{lhs} {op} ({rhs} IN {list})`");
    assert_ne!(
        right, left,
        "{lhs} {op} {rhs} IN {list}: both groupings agree, so this case cannot detect the bug"
    );
}

/// Every comparison operator, each on a case where the two groupings differ.
#[test]
fn in_binds_tighter_than_every_comparison() {
    groups_right("true", "<", "false", "[false]");
    groups_right("1", "<", "2", "[true]");
    groups_right("1", "=", "2", "[false]");
    groups_right("3", ">=", "2", "[false]");
    groups_right("1", "<=", "2", "[true, false]");
    groups_right("2", ">", "1", "[false]");
    groups_right("1", "<>", "2", "[false]");
}

/// The TCK's own formulation: over every combination of truth values and lists,
/// the bare form always equals the right grouping, and *somewhere* differs from
/// the left one. The second conjunct is what makes it a test.
#[test]
fn the_tck_enumeration() {
    let store = GraphStore::new();
    let cypher = "
        UNWIND [true, false, null] AS a
        UNWIND [true, false, null] AS b
        UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
        WITH collect((a < b IN c) = (a < (b IN c))) AS eq,
             collect((a < b IN c) <> ((a < b) IN c)) AS neq
        RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result";
    let q = parse_query(cypher).expect("parses");
    let batch = QueryExecutor::new(&store).execute(&q).expect("runs");
    assert_eq!(
        batch.records.first().and_then(|r| r.get("result")),
        Some(&Value::Property(samyama::graph::PropertyValue::Boolean(true)))
    );
}

/// Arithmetic still binds tighter than `IN`, which the new level must not
/// disturb: `1 + 1 IN [2]` is `(1 + 1) IN [2]`, true.
#[test]
fn arithmetic_still_binds_tighter_than_in() {
    assert_eq!(truth("1 + 1 IN [2]"), Some(true));
    assert_eq!(truth("2 * 3 IN [6]"), Some(true));
    assert_eq!(truth("2 ^ 3 IN [8.0]"), Some(true));
}

/// And `AND`/`OR`/`NOT` still bind looser.
#[test]
fn boolean_operators_still_bind_looser() {
    assert_eq!(truth("1 IN [1] AND 2 IN [2]"), Some(true));
    assert_eq!(truth("1 IN [9] OR 2 IN [2]"), Some(true));
    assert_eq!(truth("NOT 1 IN [9]"), Some(true));
}
