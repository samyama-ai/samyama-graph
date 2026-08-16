//! `ORDER BY … LIMIT k` keeps k rows, not n (#518).
//!
//! The correctness bar for a bounded sort is higher than for an unbounded one,
//! because every way of getting it wrong loses rows silently: an off-by-one in
//! the bound, a `SKIP` that is not added to it, or a cardinality-changing
//! operator between the sort and the limit that the bound was not allowed to
//! cross. Each of those has a test here.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `n` nodes with `v` counting up, inserted in an order that is *not* the sort
/// order — so a bounded sort that trimmed on arrival order rather than on key
/// order would fail.
fn shuffled(n: i64) -> GraphStore {
    let mut store = GraphStore::new();
    // A stride coprime with n visits every value exactly once in a scrambled
    // order, deterministically.
    let stride = 7;
    for i in 0..n {
        let v = (i * stride) % n;
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(v));
        let _ = store.set_node_property(
            "default",
            id,
            "band".to_string(),
            PropertyValue::Integer(v % 3),
        );
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
fn ascending_top_k_is_the_k_smallest_in_order() {
    let store = shuffled(5000);
    assert_eq!(
        values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c LIMIT 5"),
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn descending_top_k_is_the_k_largest_in_order() {
    let store = shuffled(5000);
    assert_eq!(
        values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c DESC LIMIT 5"),
        vec![4999, 4998, 4997, 4996, 4995]
    );
}

#[test]
fn skip_is_added_to_the_bound() {
    // The bound a sort may apply is SKIP + LIMIT. If only LIMIT reaches it,
    // the sort keeps 5 rows, SKIP throws away 3 of them, and the page comes
    // back short.
    let store = shuffled(5000);
    assert_eq!(
        values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 3 LIMIT 5"),
        vec![3, 4, 5, 6, 7]
    );
}

#[test]
fn a_large_skip_past_several_trim_thresholds_still_pages_correctly() {
    let store = shuffled(5000);
    let rows = values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP 4990 LIMIT 10");
    assert_eq!(rows, (4990..5000).collect::<Vec<_>>());
}

#[test]
fn every_page_of_a_full_scan_is_correct() {
    // Paging all the way through catches a bound that is right at the start
    // and drifts, which a single spot-check does not.
    let store = shuffled(200);
    let mut seen = Vec::new();
    for page in 0..10 {
        let rows = values(
            &store,
            &format!("MATCH (n:N) RETURN n.v AS c ORDER BY c SKIP {} LIMIT 20", page * 20),
        );
        assert_eq!(rows.len(), 20, "page {page} was short: {rows:?}");
        seen.extend(rows);
    }
    assert_eq!(seen, (0..200).collect::<Vec<_>>());
}

#[test]
fn a_limit_larger_than_the_input_returns_everything() {
    let store = shuffled(50);
    let rows = values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c LIMIT 1000");
    assert_eq!(rows, (0..50).collect::<Vec<_>>());
}

#[test]
fn a_limit_of_zero_returns_nothing() {
    let store = shuffled(50);
    assert!(values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c LIMIT 0").is_empty());
}

#[test]
fn without_a_limit_every_row_survives_in_order() {
    let store = shuffled(300);
    let rows = values(&store, "MATCH (n:N) RETURN n.v AS c ORDER BY c");
    assert_eq!(rows, (0..300).collect::<Vec<_>>());
}

#[test]
fn a_multi_key_sort_orders_by_every_key() {
    let store = shuffled(300);
    let query = parse_query(
        "MATCH (n:N) RETURN n.band AS b, n.v AS c ORDER BY b ASC, c DESC LIMIT 4",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let rows: Vec<(i64, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let get = |k: &str| match r.get(k) {
                Some(Value::Property(PropertyValue::Integer(i))) => *i,
                other => panic!("{other:?}"),
            };
            (get("b"), get("c"))
        })
        .collect();
    // band 0 holds 0, 3, 6, … 297; descending within the band.
    assert_eq!(rows, vec![(0, 297), (0, 294), (0, 291), (0, 288)]);
}

#[test]
fn distinct_between_the_sort_and_the_limit_does_not_lose_rows() {
    // The hazard the bound must not cross. DISTINCT can discard rows, so a
    // LIMIT above it may need more than `limit` rows from below; a sort that
    // accepted the bound through a DISTINCT would return short.
    let mut store = GraphStore::new();
    for i in 0..300 {
        let id = store.create_node("N");
        // Ten distinct values, each repeated thirty times.
        let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(i % 10));
    }
    let rows = values(&store, "MATCH (n:N) RETURN DISTINCT n.v AS c ORDER BY c LIMIT 10");
    assert_eq!(rows, (0..10).collect::<Vec<_>>());
}

#[test]
fn a_filter_between_the_sort_and_the_limit_does_not_lose_rows() {
    let store = shuffled(500);
    let rows = values(
        &store,
        "MATCH (n:N) WHERE n.v > 100 RETURN n.v AS c ORDER BY c LIMIT 5",
    );
    assert_eq!(rows, vec![101, 102, 103, 104, 105]);
}

#[test]
fn nulls_sort_consistently_under_a_bound() {
    // ORDER BY over a property some nodes lack must place the missing ones the
    // same way whether or not a bound is applied, or a LIMIT would change the
    // membership of the answer rather than only its length.
    let mut store = GraphStore::new();
    for i in 0..100 {
        let id = store.create_node("N");
        if i % 2 == 0 {
            let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(i));
        }
    }
    let query = parse_query("MATCH (n:N) RETURN n.v AS c ORDER BY c").unwrap();
    let unbounded = QueryExecutor::new(&store).execute(&query).unwrap();
    let first_ten: Vec<String> = unbounded
        .records
        .iter()
        .take(10)
        .map(|r| format!("{:?}", r.get("c")))
        .collect();

    let query = parse_query("MATCH (n:N) RETURN n.v AS c ORDER BY c LIMIT 10").unwrap();
    let bounded = QueryExecutor::new(&store).execute(&query).unwrap();
    let bounded_ten: Vec<String> = bounded.records.iter().map(|r| format!("{:?}", r.get("c"))).collect();

    assert_eq!(bounded_ten, first_ten, "a bound must not change which rows win");
}
