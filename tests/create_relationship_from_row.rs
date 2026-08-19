//! `UNWIND [...] AS x CREATE ()-[r:R {num: x}]->() RETURN r.num`
//!
//! The bulk-load idiom for relationships, and three separate things stopped it
//! working (#649). Each failed differently, and two of the three reported
//! success.
//!
//! **The edge kept no data.** `edges_to_create` carried only the *literal*
//! properties; `{num: x}` is an expression, and it was dropped. The right
//! number of relationships were created with none of their data — the same
//! defect the node side of this operator had in #467, in the half that was
//! not fixed then.
//!
//! **The edge had no name.** The created relationship was bound under the
//! internal key `_edge` and the pattern's own variable was discarded, so
//! `RETURN r.num` could not find `r` at all.
//!
//! **SKIP and LIMIT could not sit above a write.** Both are pass-through
//! operators whose default `next_mut` delegates to `next`, which reads its
//! input read-only, so the write below refused outright — the same defect
//! class as the barriers in #622 and the joins in #624.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) -> Vec<Option<i64>> {
    let q = parse_query(cypher).expect("query should parse");
    let out = MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    let mut got: Vec<Option<i64>> = out
        .records
        .iter()
        .map(|r| match r.get("num") {
            Some(Value::Property(PropertyValue::Integer(n))) => Some(*n),
            _ => None,
        })
        .collect();
    got.sort();
    got
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store).execute(&q).expect("should run").records.len()
}

#[test]
fn a_created_relationship_keeps_the_property_that_came_from_the_row() {
    let mut store = GraphStore::new();
    let got = run(
        &mut store,
        "UNWIND [1, 2, 3] AS x CREATE ()-[r:R {num: x}]->() RETURN r.num AS num",
    );
    assert_eq!(got, vec![Some(1), Some(2), Some(3)], "each edge keeps its own value");
    assert_eq!(count(&store, "MATCH ()-[r:R]->() RETURN r"), 3);
    assert_eq!(count(&store, "MATCH ()-[r:R {num: 2}]->() RETURN r"), 1);
}

#[test]
fn the_created_relationship_is_bound_under_its_own_name() {
    // It was bound under an internal key, so the edge existed with the right
    // properties under a name the query never wrote.
    let mut store = GraphStore::new();
    let got = run(
        &mut store,
        "UNWIND [7] AS x CREATE ()-[r:R {num: x}]->() WITH r RETURN r.num AS num",
    );
    assert_eq!(got, vec![Some(7)], "`r` survives a WITH");
}

#[test]
fn a_filter_after_the_write_sees_the_new_relationship() {
    let mut store = GraphStore::new();
    let got = run(
        &mut store,
        "UNWIND [1, 2, 3, 4, 5] AS x CREATE ()-[r:R {num: x}]->() \
         WITH r WHERE r.num % 2 = 0 RETURN r.num AS num",
    );
    assert_eq!(got, vec![Some(2), Some(4)]);
    assert_eq!(
        count(&store, "MATCH ()-[r:R]->() RETURN r"),
        5,
        "filtering the result must not filter the writes"
    );
}

#[test]
fn skip_and_limit_above_a_write_do_not_reduce_the_writes() {
    // The rows are narrowed; the side effects are not. A LIMIT that stopped
    // pulling would silently create fewer nodes than the query asks for, which
    // is the failure worth pinning here rather than the row count.
    let mut store = GraphStore::new();
    let got = run(
        &mut store,
        "UNWIND [42, 42, 42, 42, 42] AS x CREATE (n:N {num: x}) RETURN n.num AS num SKIP 2 LIMIT 2",
    );
    assert_eq!(got.len(), 2, "two rows survive SKIP 2 LIMIT 2");
    assert_eq!(count(&store, "MATCH (n:N) RETURN n"), 5, "all five were created");
}
