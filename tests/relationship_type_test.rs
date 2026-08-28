//! `r:T` on a relationship is a **type** test (#914).
//!
//! `MATCH ()-[r]->() RETURN r:T2` raised `TypeError("a label test requires a
//! node")` and killed the whole query. The parser turns postfix `:A:B` into a
//! `hasLabels` call regardless of what the subject is, and `hasLabels` only
//! knew about nodes.
//!
//! Cypher asks the question of relationships too, where it means "is this a
//! T2?". A relationship has exactly one type, so `r:A:B` is a question about
//! something no relationship can be — that is `false`, not an error. The
//! distinction matters because the error form takes the *entire* query down,
//! including the rows that had nothing to do with the test.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    store.create_edge(a, b, "T1").unwrap();
    store.create_edge(a, b, "T2").unwrap();
    store
}

fn bools(store: &GraphStore, cypher: &str) -> Vec<Option<bool>> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut out: Vec<Option<bool>> = batch
        .records
        .iter()
        .map(|r| match r.get("result") {
            Some(Value::Property(PropertyValue::Boolean(b))) => Some(*b),
            Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
            other => panic!("{cypher}: {other:?}"),
        })
        .collect();
    out.sort_by_key(|b| (b.is_none(), *b));
    out
}

#[test]
fn a_type_test_answers_true_for_the_matching_type() {
    let store = graph();
    assert_eq!(
        bools(&store, "MATCH ()-[r:T2]->() RETURN r:T2 AS result"),
        vec![Some(true)]
    );
}

#[test]
fn a_type_test_answers_per_row_without_failing_the_query() {
    let store = graph();
    // Both relationships are returned; the T1 row answers false rather than
    // aborting the query the T2 row would have succeeded in.
    assert_eq!(
        bools(&store, "MATCH ()-[r]->() RETURN r:T2 AS result"),
        vec![Some(false), Some(true)]
    );
}

#[test]
fn a_relationship_cannot_carry_two_types() {
    let store = graph();
    assert_eq!(
        bools(&store, "MATCH ()-[r]->() RETURN r:T1:T2 AS result"),
        vec![Some(false), Some(false)]
    );
}

#[test]
fn a_label_test_on_a_node_is_unchanged() {
    let store = graph();
    assert_eq!(
        bools(&store, "MATCH (n) RETURN n:A AS result"),
        vec![Some(false), Some(true)]
    );
}

#[test]
fn a_label_test_on_null_is_null() {
    let store = graph();
    assert_eq!(
        bools(&store, "MATCH (n) WITH n LIMIT 1 RETURN null:A AS result"),
        vec![None]
    );
}

#[test]
fn a_type_test_on_a_number_is_still_an_error() {
    let store = graph();
    let query = parse_query("MATCH (n) RETURN 1:A AS result").unwrap();
    assert!(QueryExecutor::new(&store).execute(&query).is_err());
}
