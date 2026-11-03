//! A leading `UNWIND` followed by `WITH` (#572).
//!
//! `UNWIND [1, 2, 3] AS x WITH x WHERE x > 1 RETURN x` failed with
//! `VariableNotFound("x")`, and so did every other shape whatever the value
//! type. A `WITH` barrier projects the variables the clause names, and the
//! `UNWIND` was being planned *after* it — so there was nothing bound for the
//! barrier to project.
//!
//! Since `WHERE` cannot follow `UNWIND` directly, that left **no way to filter
//! an unwound list at all**, and no way to write the "unwind a list of
//! parameters, then match on each" shape a machine caller uses for a batch.
//!
//! The risk in the fix is the other direction: a *trailing* `UNWIND` belongs to
//! its stage and must still be applied there, and a leading one must not be
//! applied twice. Both have tests below, because a double `UNWIND` multiplies
//! rows rather than erroring.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn ints(store: &GraphStore, cypher: &str, name: &str) -> Vec<i64> {
    let query = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&query)
        .expect("query should run")
        .records
        .iter()
        .map(|r| match r.get(name) {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn a_leading_unwind_is_visible_to_a_following_with() {
    let store = GraphStore::new();
    assert_eq!(
        ints(&store, "UNWIND [1, 2, 3] AS x WITH x WHERE x > 1 RETURN x AS a", "a"),
        vec![2, 3]
    );
}

#[test]
fn the_with_may_rename_it() {
    let store = GraphStore::new();
    assert_eq!(
        ints(&store, "UNWIND [1, 2, 3] AS x WITH x AS y WHERE y > 1 RETURN y AS a", "a"),
        vec![2, 3]
    );
}

#[test]
fn the_with_may_project_an_expression_of_it() {
    let store = GraphStore::new();
    assert_eq!(
        ints(&store, "UNWIND [1, 2, 3] AS x WITH x * 10 AS y WHERE y > 15 RETURN y AS a", "a"),
        vec![20, 30]
    );
}

#[test]
fn a_map_element_works_the_same_way() {
    let store = GraphStore::new();
    assert_eq!(
        ints(
            &store,
            "UNWIND [{a: 1}, {a: 2}, {a: 3}] AS m WITH m WHERE m.a > 1 RETURN m.a AS a",
            "a"
        ),
        vec![2, 3]
    );
    assert_eq!(
        ints(
            &store,
            "UNWIND [{a: 1}, {a: 2}, {a: 3}] AS m WITH m.a AS a WHERE a > 1 RETURN a",
            "a"
        ),
        vec![2, 3]
    );
}

#[test]
fn every_row_survives_when_the_where_keeps_them_all() {
    // A double UNWIND would multiply rows rather than error, so the count is
    // the thing to assert, not just the values.
    let store = GraphStore::new();
    let got = ints(&store, "UNWIND [1, 2, 3] AS x WITH x RETURN x AS a", "a");
    assert_eq!(got, vec![1, 2, 3], "three rows in, three out — not nine");
}

#[test]
fn a_with_can_aggregate_over_the_unwound_rows() {
    let store = GraphStore::new();
    assert_eq!(
        ints(&store, "UNWIND [1, 2, 3, 4] AS x WITH sum(x) AS s RETURN s AS a", "a"),
        vec![10]
    );
    assert_eq!(
        ints(&store, "UNWIND [1, 2, 3, 4] AS x WITH count(x) AS c RETURN c AS a", "a"),
        vec![4]
    );
}

#[test]
fn a_leading_unwind_still_reaches_a_where_with_no_with() {
    // The shape the old placement existed for: the predicate references the
    // unwound variable, so the UNWIND has to sit below the filter. Moving it
    // earlier must not have broken this.
    let mut store = GraphStore::new();
    for i in 1..=4i64 {
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "n".to_string(), PropertyValue::Integer(i));
    }
    let got = ints(
        &store,
        "UNWIND [2, 3] AS x MATCH (p:N) WHERE p.n = x RETURN p.n AS a",
        "a",
    );
    let mut got = got;
    got.sort();
    assert_eq!(got, vec![2, 3]);
}

#[test]
fn a_leading_unwind_with_a_match_and_a_with() {
    // Both at once: the UNWIND must be bound before the MATCH's predicate and
    // before the barrier.
    let mut store = GraphStore::new();
    for i in 1..=4i64 {
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "n".to_string(), PropertyValue::Integer(i));
    }
    let mut got = ints(
        &store,
        "UNWIND [1, 2, 3] AS x MATCH (p:N) WHERE p.n = x WITH p.n AS n WHERE n > 1 RETURN n AS a",
        "a",
    );
    got.sort();
    assert_eq!(got, vec![2, 3]);
}

#[test]
fn a_trailing_unwind_still_belongs_to_its_stage() {
    // The other direction. `WITH … UNWIND …` unwinds *after* the barrier, and
    // suppressing the stage's UNWIND for the leading case must not have
    // suppressed it here.
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let _ = store.set_node_property(
        "default",
        id,
        "tags".to_string(),
        PropertyValue::Array(vec![
            PropertyValue::Integer(7),
            PropertyValue::Integer(8),
            PropertyValue::Integer(9),
        ]),
    );
    let got = ints(&store, "MATCH (n:N) WITH n.tags AS t UNWIND t AS x RETURN x AS a", "a");
    assert_eq!(got, vec![7, 8, 9]);
}

#[test]
fn a_leading_unwind_with_no_with_is_unchanged() {
    let store = GraphStore::new();
    assert_eq!(ints(&store, "UNWIND [5, 6] AS x RETURN x AS a", "a"), vec![5, 6]);
    // Aggregation over a bare leading UNWIND — the case the SingleRowOperator
    // seed was introduced for.
    assert_eq!(ints(&store, "UNWIND [5, 6, 7] AS x RETURN count(x) AS a", "a"), vec![3]);
}

#[test]
fn ordering_and_limiting_after_the_with_work() {
    let store = GraphStore::new();
    assert_eq!(
        ints(
            &store,
            "UNWIND [3, 1, 4, 1, 5] AS x WITH x WHERE x > 1 RETURN x AS a ORDER BY x DESC LIMIT 2",
            "a"
        ),
        vec![5, 4]
    );
}

#[test]
fn two_with_stages_after_a_leading_unwind() {
    let store = GraphStore::new();
    assert_eq!(
        ints(
            &store,
            "UNWIND [1, 2, 3, 4] AS x WITH x WHERE x > 1 WITH x * 2 AS y WHERE y < 8 RETURN y AS a",
            "a"
        ),
        vec![4, 6]
    );
}
