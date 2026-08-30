//! MERGE binds every match, not the first (#956).
//!
//! ```cypher
//! MATCH (a) MATCH (b) RETURN a, b   -- 4 rows over two nodes
//! MATCH (a) MERGE (b) RETURN a, b   -- 2 rows. Should be 4.
//! ```
//!
//! `MergeOperator` took the first matching node and `break`, and returned
//! exactly one record per input row, so the other matches had nowhere to go.
//!
//! The rows that went missing are indistinguishable from rows that never
//! existed: the query looked like it had correlated `a` and `b` when it had
//! silently picked one `b`, and *which* one depended on scan order. Nothing
//! errored and the count was plausible.
//!
//! These tests count rows, and one of them counts nodes — because the other
//! half of MERGE is that it must not create when it matched.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(store: &mut GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap();
    match QueryExecutor::new(store).execute(&q).unwrap().records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{other:?}"),
    }
}

fn two_nodes() -> GraphStore {
    let mut store = GraphStore::new();
    store.create_node_with_labels([Label::new("A")]);
    store.create_node_with_labels([Label::new("B")]);
    store
}

#[test]
fn merge_binds_each_matching_node() {
    let mut store = two_nodes();
    assert_eq!(write(&mut store, "MERGE (b) RETURN b"), 2);
    assert_eq!(count(&store, "MATCH (n) RETURN count(n) AS c"), 2, "matched, not created");
}

#[test]
fn it_multiplies_against_the_incoming_rows() {
    let mut store = two_nodes();
    assert_eq!(write(&mut store, "MATCH (a) MERGE (b) RETURN a, b"), 4);
    assert_eq!(count(&store, "MATCH (n) RETURN count(n) AS c"), 2);
}

#[test]
fn a_labelled_merge_binds_only_its_own_label() {
    let mut store = two_nodes();
    store.create_node_with_labels([Label::new("A")]);
    assert_eq!(write(&mut store, "MERGE (a:A) RETURN a"), 2);
    assert_eq!(write(&mut store, "MERGE (b:B) RETURN b"), 1);
}

#[test]
fn a_property_pattern_narrows_the_matches() {
    let mut store = GraphStore::new();
    for v in [1i64, 1, 2] {
        let n = store.create_node_with_labels([Label::new("N")]);
        let _ = store.set_node_property("default", n, "v".to_string(), PropertyValue::Integer(v));
    }
    assert_eq!(write(&mut store, "MERGE (n:N {v: 1}) RETURN n"), 2);
    assert_eq!(count(&store, "MATCH (n) RETURN count(n) AS c"), 3, "nothing created");
}

#[test]
fn nothing_matching_still_creates_exactly_one() {
    // The other half of match-or-create, and the direction a fix that always
    // enumerated could break.
    let mut store = two_nodes();
    assert_eq!(write(&mut store, "MERGE (n:Absent) RETURN n"), 1);
    assert_eq!(count(&store, "MATCH (n) RETURN count(n) AS c"), 3);
    assert_eq!(count(&store, "MATCH (n:Absent) RETURN count(n) AS c"), 1);
}

#[test]
fn a_bound_variable_is_refused_by_an_earlier_rule() {
    // `MATCH (a) MERGE (a)` never reaches the matching code: validation
    // rejects it first. Recorded rather than asserted as correct — Neo4j
    // accepts the bare form, so this rule may be over-strict, which is
    // #764's territory and not this change's.
    let mut store = two_nodes();
    let q = parse_query("MATCH (a) MERGE (a) RETURN a");
    assert!(q.is_err(), "if this starts parsing, the bound path needs a test");
    let _ = &mut store;
}

#[test]
fn on_match_set_runs_for_every_match() {
    // It used to run on the first match only, so a MERGE meant to touch every
    // matching node touched one.
    let mut store = GraphStore::new();
    for _ in 0..3 {
        store.create_node_with_labels([Label::new("N")]);
    }
    write(&mut store, "MERGE (n:N) ON MATCH SET n.seen = 1 RETURN n");
    assert_eq!(count(&store, "MATCH (n:N) WHERE n.seen = 1 RETURN count(n) AS c"), 3);
}

#[test]
fn on_create_set_still_runs_for_the_created_node() {
    let mut store = GraphStore::new();
    write(&mut store, "MERGE (n:Fresh) ON CREATE SET n.made = 1 RETURN n");
    assert_eq!(count(&store, "MATCH (n:Fresh) WHERE n.made = 1 RETURN count(n) AS c"), 1);
}
