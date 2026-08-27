//! An aggregate in the `RETURN` of a write query (#907).
//!
//! ```cypher
//! CREATE (a) RETURN count(*) AS n   -- Runtime error: Unknown function: count
//! MERGE (a) RETURN count(*) AS n    -- the same
//! CREATE (a) WITH a RETURN count(*) -- worked
//! ```
//!
//! An everyday query, failing on a function the engine implements. The tell is
//! the third line: inserting a `WITH` made it work, so the aggregation was
//! present and merely unreachable from that planner branch.
//!
//! The write paths built their projection inline — `return_clause.items` mapped
//! straight into a `ProjectOperator` — with no aggregate handling at all, so
//! `count` was evaluated as an ordinary scalar function and no such function
//! exists. Two such copies, neither of which had ever been taught.
//!
//! They now share `plan_return_projection` with each other. The read path keeps
//! its own assembly because it also picks between O(1) shortcuts (label count,
//! edge count) that need the MATCH to decide, and none apply after a write.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, Value};
use samyama::query::parser::parse_query;

fn scalar(cypher: &str, setup: &[&str]) -> Value {
    let mut store = GraphStore::new();
    for s in setup {
        let q = parse_query(s).expect("setup parses");
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .expect("setup runs");
    }
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` parses: {e:?}"));
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` runs: {e}"))
        .records
        .first()
        .and_then(|r| r.get("n"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn int(v: Value) -> i64 {
    match v {
        Value::Property(PropertyValue::Integer(n)) => n,
        other => panic!("expected an integer, got {other:?}"),
    }
}

#[test]
fn create_and_merge_can_count_what_they_produced() {
    assert_eq!(int(scalar("CREATE (a) RETURN count(*) AS n", &[])), 1);
    assert_eq!(int(scalar("MERGE (a) RETURN count(*) AS n", &[])), 1);
    assert_eq!(int(scalar("MERGE (a) RETURN count(a) AS n", &[])), 1);
    assert_eq!(int(scalar("CREATE (a), (b) RETURN count(*) AS n", &[])), 1);
}

/// The form that already worked must keep working, and agree.
#[test]
fn the_with_form_gives_the_same_answer() {
    assert_eq!(
        int(scalar("CREATE (a) WITH a RETURN count(*) AS n", &[])),
        int(scalar("CREATE (a) RETURN count(*) AS n", &[])),
    );
}

/// Other aggregates, and grouping, go through the same path.
#[test]
fn other_aggregates_work_too() {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:X) RETURN collect(a) AS n").expect("parses");
    let rows = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs")
        .records;
    match rows.first().and_then(|r| r.get("n")) {
        Some(Value::List(items)) => assert_eq!(items.len(), 1),
        other => panic!("expected a list, got {other:?}"),
    }
}

/// A non-aggregating RETURN after a write is untouched.
#[test]
fn a_plain_projection_after_a_write_is_unchanged() {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:X {v: 1}) RETURN a.v AS n").expect("parses");
    let rows = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs")
        .records;
    assert_eq!(
        rows.first().and_then(|r| r.get("n")),
        Some(&Value::Property(PropertyValue::Integer(1)))
    );
}

/// `toBoolean()` asks a question about a string, and "no" is an answer.
///
/// Erroring killed the whole query: this returned nothing at all rather than
/// four rows.
#[test]
fn to_boolean_answers_null_for_a_string_that_is_not_one() {
    let mut store = GraphStore::new();
    let q = parse_query("UNWIND [null, '', ' tru ', 'f alse'] AS t RETURN toBoolean(t) AS b")
        .expect("parses");
    let rows = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs")
        .records;
    assert_eq!(rows.len(), 4);
    for row in &rows {
        assert!(
            matches!(row.get("b"), Some(Value::Null) | Some(Value::Property(PropertyValue::Null))),
            "{:?}", row.get("b")
        );
    }

    let q = parse_query("UNWIND ['true', 'FALSE'] AS t RETURN toBoolean(t) AS b").expect("parses");
    let rows = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs")
        .records;
    assert_eq!(rows[0].get("b"), Some(&Value::Property(PropertyValue::Boolean(true))));
    assert_eq!(rows[1].get("b"), Some(&Value::Property(PropertyValue::Boolean(false))));
}
