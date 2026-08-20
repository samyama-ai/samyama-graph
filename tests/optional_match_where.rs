//! `WHERE` after an `OPTIONAL MATCH` scopes to the optional match.
//!
//! ```cypher
//! MATCH (x:X)
//! OPTIONAL MATCH (x)-[:E1]->(y:Y)
//! WHERE y.val > 4
//! RETURN x, y
//! ```
//!
//! Cypher keeps every `x` and nulls `y` where the predicate fails. Applied as
//! an ordinary filter above the left outer join it deletes those rows instead:
//! this query returned **one** row where Cypher returns three (#667).
//!
//! The distinction is between a filter and a join condition. A pair failing
//! the predicate is *not a match*, and a left row with no match is exactly
//! what an OPTIONAL MATCH is for.
//!
//! There were two halves to getting this right. The planner has to route the
//! predicate into the join, and the top-level WHERE — which deliberately
//! re-applies the whole predicate whenever an OPTIONAL MATCH is present,
//! because a filter pushed inside the optional side cannot see null-filled
//! rows — has to *stop* re-applying this one. That reasoning predates join
//! conditions and was correct for the case it was written for.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query(
        "CREATE (:X {val: 1})-[:E1]->(:Y {val: 2})-[:E2]->(:Z {val: 3}), \
         (:X {val: 4})-[:E1]->(:Y {val: 5}), (:X {val: 6})",
    )
    .expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");
    store
}

/// `(x.val, y.val)` pairs, sorted, with `None` for a null `y`.
fn pairs(store: &GraphStore, cypher: &str) -> Vec<(i64, Option<i64>)> {
    let q = parse_query(cypher).expect("query should parse");
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    let int = |v: Option<&Value>| match v {
        Some(Value::Property(samyama::graph::PropertyValue::Integer(n))) => Some(*n),
        _ => None,
    };
    let mut rows: Vec<(i64, Option<i64>)> = out
        .records
        .iter()
        .map(|r| (int(r.get("a")).expect("x.val is never null"), int(r.get("b"))))
        .collect();
    rows.sort();
    rows
}

#[test]
fn a_predicate_on_the_optional_side_nulls_the_row_rather_than_dropping_it() {
    let store = graph();
    assert_eq!(
        pairs(
            &store,
            "MATCH (x:X) OPTIONAL MATCH (x)-[:E1]->(y:Y) WHERE y.val > 4 \
             RETURN x.val AS a, y.val AS b"
        ),
        vec![(1, None), (4, Some(5)), (6, None)],
        "every x survives; only y is nulled where the predicate fails"
    );
}

#[test]
fn a_predicate_spanning_both_sides_is_a_join_condition() {
    let store = graph();
    assert_eq!(
        pairs(
            &store,
            "MATCH (x:X) OPTIONAL MATCH (x)-[:E1]->(y:Y) WHERE x.val < y.val \
             RETURN x.val AS a, y.val AS b"
        ),
        vec![(1, Some(2)), (4, Some(5)), (6, None)]
    );
}

#[test]
fn an_optional_match_without_a_where_is_unchanged() {
    let store = graph();
    assert_eq!(
        pairs(&store, "MATCH (x:X) OPTIONAL MATCH (x)-[:E1]->(y:Y) RETURN x.val AS a, y.val AS b"),
        vec![(1, Some(2)), (4, Some(5)), (6, None)]
    );
}

#[test]
fn a_predicate_on_the_outer_side_still_filters() {
    // Not everything after an OPTIONAL MATCH becomes a join condition. A
    // predicate naming only outer variables is an ordinary filter and must
    // still remove rows — treating it as a join condition would return `x`
    // rows the query excludes.
    let store = graph();
    assert_eq!(
        pairs(
            &store,
            "MATCH (x:X) OPTIONAL MATCH (x)-[:E1]->(y:Y) WHERE x.val > 3 \
             RETURN x.val AS a, y.val AS b"
        ),
        vec![(4, Some(5)), (6, None)],
        "x.val = 1 is excluded entirely"
    );
}
