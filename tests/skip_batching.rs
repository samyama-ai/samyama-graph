//! `SKIP` returns everything after the first n rows, batched or not (#523).
//!
//! The executor pulls results with `next_batch(store, 1024)`, so `SkipOperator`
//! is exercised through its batched path and almost never through `next`. The
//! batched path consumed a whole batch to count off the skip and discarded the
//! rows past the boundary, which made `SKIP n` alone return an empty result —
//! a well-formed empty result, from a query that succeeded.
//!
//! Every test here therefore goes through `QueryExecutor`, which is the batched
//! path. A test that drove the operator row-at-a-time would have passed
//! throughout.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn numbered(n: i64) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..n {
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(i));
    }
    store
}

fn values(store: &GraphStore, cypher: &str) -> Vec<i64> {
    let query = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("query should execute");
    batch
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("expected an integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn skip_alone_returns_the_tail() {
    // Was `[]`.
    let store = numbered(5);
    assert_eq!(values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 2"), vec![2, 3, 4]);
}

#[test]
fn skip_of_zero_returns_everything() {
    let store = numbered(5);
    assert_eq!(
        values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 0"),
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn skip_past_the_end_returns_nothing() {
    let store = numbered(5);
    assert!(values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 99").is_empty());
}

#[test]
fn skip_of_exactly_the_row_count_returns_nothing() {
    let store = numbered(5);
    assert!(values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 5").is_empty());
}

#[test]
fn skip_with_limit_is_correct_when_the_boundary_does_not_align() {
    // `SKIP 2 LIMIT 2` passed even while the operator was broken, because the
    // limit made the request size equal the skip. A skip that does not divide
    // evenly into the requested batch is the case that actually tests it.
    let store = numbered(10);
    assert_eq!(
        values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 3 LIMIT 4"),
        vec![3, 4, 5, 6]
    );
    assert_eq!(
        values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 1 LIMIT 7"),
        vec![1, 2, 3, 4, 5, 6, 7]
    );
}

#[test]
fn skip_spans_more_than_one_batch() {
    // The executor pulls 1024 rows at a time, so a skip larger than that has
    // to consume whole batches and then return the remainder of the one it
    // lands in.
    let store = numbered(2500);
    let rows = values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 1500");
    assert_eq!(rows.len(), 1000);
    assert_eq!(rows.first(), Some(&1500));
    assert_eq!(rows.last(), Some(&2499));
}

#[test]
fn a_skip_landing_exactly_on_a_batch_boundary_loses_nothing() {
    let store = numbered(2048);
    let rows = values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 1024");
    assert_eq!(rows.len(), 1024);
    assert_eq!(rows.first(), Some(&1024));
}

#[test]
fn every_prefix_length_is_skippable() {
    // Sweeping the skip catches an off-by-one at either end of the boundary
    // arithmetic, which a single hand-picked value does not.
    let store = numbered(37);
    for skip in 0..=37usize {
        let rows = values(&store, &format!("MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP {skip}"));
        assert_eq!(rows.len(), 37 - skip, "SKIP {skip} returned {} rows", rows.len());
        if let Some(first) = rows.first() {
            assert_eq!(*first, skip as i64, "SKIP {skip} started at {first}");
        }
    }
}
