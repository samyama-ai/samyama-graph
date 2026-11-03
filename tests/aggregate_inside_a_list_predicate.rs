//! An aggregate inside a list predicate is hoisted, not handed to the scalar
//! evaluator (#997).
//!
//! ```cypher
//! UNWIND [true, true] AS x RETURN ALL(ok IN collect(x) WHERE ok) AS okay
//! ```
//!
//! failed with `Unknown function: collect` — an error naming one of Cypher's
//! most ordinary functions. `collect(x)` works as a top-level aggregate and
//! works through a `WITH`; it failed only inside the list a predicate
//! iterates.
//!
//! `extract_agg_inner` hoists nested aggregates into the `AggregateOperator`.
//! It handled `Function`, `Binary`, `Unary`, `Case`, `ListExpr`, `MapExpr` and
//! `ListComprehension`, and fell through everything else. `PredicateFunction`
//! is the sibling that fix (#670) did not reach, and so are `Reduce`, `Index`
//! and `ListSlice`.
//!
//! `expression_has_aggregate` had the same missing arms, and it is the one
//! that decides whether an `AggregateOperator` is planned at all — so fixing
//! only the extractor leaves a shape it can rewrite but is never asked to.
//! Both walkers have to agree arm for arm.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn nums() -> GraphStore {
    let mut store = GraphStore::new();
    for n in [1i64, 2, 3] {
        let id = store.create_node_with_labels([Label::new("N")]);
        store.set_node_property("default", id, "v", PropertyValue::Integer(n)).unwrap();
    }
    store
}

fn one(store: &GraphStore, cypher: &str) -> Value {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let c = r.columns[0].clone();
    r.records[0].get(&c).cloned().unwrap_or(Value::Null)
}

#[test]
fn all_over_a_collect_is_planned_as_an_aggregate() {
    let store = nums();
    assert!(format!("{:?}", one(&store, "UNWIND [true, true] AS x RETURN ALL(ok IN collect(x) WHERE ok) AS okay"))
        .contains("Boolean(true)"));
}

#[test]
fn the_other_three_predicates_too() {
    let store = nums();
    for (q, want) in [
        ("MATCH (n:N) RETURN ANY(v IN collect(n.v) WHERE v = 2) AS r", "Boolean(true)"),
        ("MATCH (n:N) RETURN NONE(v IN collect(n.v) WHERE v > 9) AS r", "Boolean(true)"),
        ("MATCH (n:N) RETURN SINGLE(v IN collect(n.v) WHERE v = 3) AS r", "Boolean(true)"),
    ] {
        assert!(format!("{:?}", one(&store, q)).contains(want), "{q}");
    }
}

#[test]
fn reduce_hoists_its_seed_and_its_list() {
    let store = nums();
    let got = one(&store, "MATCH (n:N) RETURN reduce(t = 0, v IN collect(n.v) | t + v) AS total");
    assert!(format!("{got:?}").contains("Integer(6)"), "got {got:?}");
}

#[test]
fn indexing_and_slicing_an_aggregate() {
    let store = nums();
    assert!(format!("{:?}", one(&store, "MATCH (n:N) RETURN collect(n.v)[0] AS r")).contains("Integer(1)"));
    let sliced = one(&store, "MATCH (n:N) RETURN collect(n.v)[0..2] AS r");
    assert_eq!(format!("{sliced:?}").matches("Integer(").count(), 2, "got {sliced:?}");
}

#[test]
fn a_loop_body_is_left_alone() {
    // The body runs once per element with the loop variable bound, so an
    // aggregate there is a different question and must not be hoisted out of
    // the loop. Only the *list* is rewritten.
    let store = nums();
    let got = one(&store, "MATCH (n:N) RETURN [v IN collect(n.v) WHERE v > 1 | v * 2] AS r");
    assert_eq!(format!("{got:?}").matches("Integer(").count(), 2, "got {got:?}");
}

#[test]
fn a_predicate_over_an_ordinary_list_still_works() {
    let store = nums();
    assert!(format!("{:?}", one(&store, "RETURN ALL(x IN [1,2,3] WHERE x > 0) AS r")).contains("Boolean(true)"));
    assert!(format!("{:?}", one(&store, "UNWIND [1,2] AS x WITH collect(x) AS c RETURN ALL(v IN c WHERE v > 0) AS r"))
        .contains("Boolean(true)"));
}

#[test]
fn the_full_tck_shape() {
    // List11[3]. `range()` was correct throughout; the failure was the final
    // `ALL(ok IN collect(...) WHERE ok)`.
    let store = nums();
    let got = one(&store,
        "WITH 0 AS start, [1, 2, 500, 1000, 1500] AS stopList, \
         [-1000, -3, -2, -1, 1, 2, 3, 1000] AS stepList \
         UNWIND stopList AS stop UNWIND stepList AS step \
         WITH start, stop, step, range(start, stop, step) AS list \
         WITH start, stop, step, list, sign(stop-start) <> sign(step) AS empty \
         RETURN ALL(ok IN collect((size(list) = 0) = empty) WHERE ok) AS okay");
    assert!(format!("{got:?}").contains("Boolean(true)"), "got {got:?}");
}
