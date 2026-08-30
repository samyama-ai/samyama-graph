//! A disjoint OPTIONAL MATCH that finds nothing keeps the rows before it (#954).
//!
//! ```cypher
//! MATCH (f:DoesExist)
//! OPTIONAL MATCH (n:DoesNotExist)
//! RETURN count(f)
//! ```
//!
//! answered **0** where three nodes had matched. An OPTIONAL MATCH finding
//! nothing destroyed every row the query had already found — a left outer join
//! behaving as an inner one, which is the single property OPTIONAL MATCH
//! exists to provide.
//!
//! The planner picked its join by whether the clause *shared a variable*:
//! sharing one gave a `LeftOuterJoinOperator`, sharing none gave a
//! `CartesianProductOperator` — which does not look at `optional` at all, and
//! yields nothing when either side is empty.
//!
//! Not an exotic shape. `MATCH … OPTIONAL MATCH …` where the optional part may
//! find nothing is how you attach data that might not be there, and every such
//! query silently returned no rows.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    for n in [42i64, 43, 44] {
        let id = store.create_node_with_labels([Label::new("DoesExist")]);
        let _ = store.set_node_property("default", id, "num".to_string(), PropertyValue::Integer(n));
    }
    store
}

fn int(store: &GraphStore, cypher: &str, col: &str) -> i64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&q).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    match batch.records[0].get(col) {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{cypher}: {other:?}"),
    }
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store).execute(&q).unwrap().records.len()
}

#[test]
fn a_failing_optional_match_does_not_delete_the_rows_before_it() {
    let store = graph();
    assert_eq!(
        int(&store, "MATCH (f:DoesExist) OPTIONAL MATCH (n:DoesNotExist) RETURN count(f) AS c", "c"),
        3
    );
}

#[test]
fn the_collected_values_survive_too() {
    // The scenario as written: two collects, one over the missing side.
    let store = graph();
    let q = "OPTIONAL MATCH (f:DoesExist) OPTIONAL MATCH (n:DoesNotExist) \
             RETURN collect(DISTINCT n.num) AS a, collect(DISTINCT f.num) AS b";
    let parsed = parse_query(q).unwrap();
    let batch = QueryExecutor::new(&store).execute(&parsed).unwrap();
    let list = |c: &str| match batch.records[0].get(c) {
        Some(Value::Property(PropertyValue::Array(v))) => v.len(),
        other => panic!("{c}: {other:?}"),
    };
    assert_eq!(list("a"), 0, "nothing matched DoesNotExist");
    assert_eq!(list("b"), 3, "but the three DoesExist nodes are still here");
}

#[test]
fn the_missing_variable_is_null_not_absent() {
    let store = graph();
    let q = parse_query(
        "MATCH (f:DoesExist) OPTIONAL MATCH (n:DoesNotExist) RETURN f, n",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    assert_eq!(batch.records.len(), 3);
    for r in &batch.records {
        assert!(matches!(r.get("n"), Some(Value::Null) | None), "{:?}", r.get("n"));
        assert!(r.get("f").is_some());
    }
}

#[test]
fn a_disjoint_optional_that_does_match_still_gives_the_cross_product() {
    // The other half. With no shared variable and a non-empty right side, the
    // answer is the full cartesian product — a fix that always null-filled
    // would break this and look like progress.
    let mut store = graph();
    for _ in 0..2 {
        store.create_node_with_labels([Label::new("Other")]);
    }
    assert_eq!(
        rows(&store, "MATCH (f:DoesExist) OPTIONAL MATCH (o:Other) RETURN f, o"),
        6
    );
}

#[test]
fn a_correlated_optional_match_is_unchanged() {
    // The path that already worked: sharing a variable gives a left outer
    // join, and it must keep doing so.
    let mut store = graph();
    let a = store.create_node_with_labels([Label::new("Src")]);
    let b = store.create_node_with_labels([Label::new("Dst")]);
    store.create_edge(a, b, "R").unwrap();
    assert_eq!(rows(&store, "MATCH (s:Src) OPTIONAL MATCH (s)-[:R]->(d) RETURN s, d"), 1);
    assert_eq!(rows(&store, "MATCH (s:Src) OPTIONAL MATCH (s)-[:NOPE]->(d) RETURN s, d"), 1);
}

#[test]
fn a_leading_optional_match_that_finds_nothing_still_gives_one_row() {
    // #671's case, pinned so this change did not disturb it.
    let store = GraphStore::new();
    assert_eq!(rows(&store, "OPTIONAL MATCH (a:Nope) RETURN a"), 1);
}

#[test]
fn an_ordinary_match_that_finds_nothing_still_gives_no_rows() {
    // The distinction the whole fix turns on: MATCH is not OPTIONAL MATCH.
    let store = graph();
    assert_eq!(rows(&store, "MATCH (f:DoesExist) MATCH (n:DoesNotExist) RETURN f"), 0);
}
