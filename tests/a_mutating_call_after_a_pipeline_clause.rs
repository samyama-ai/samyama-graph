//! A mutating `CALL` reaches the write executor even with a clause before it (#1479).
//!
//! #1471 made `Query::is_write()` ask the procedure registry, so
//! `CALL algo.or.solve(...)` routes to `MutQueryExecutor`. Only the **bare**
//! form worked. Any preceding clause still came back with the read path's
//! refusal from inside a statement that had been routed correctly:
//!
//!     CALL algo.or.solve(...)                          -> ok
//!     WITH 1 AS x CALL algo.or.solve(...)              -> requires write access
//!     MATCH (i:Item) WITH count(i) AS n CALL ...       -> requires write access
//!     UNWIND [1] AS x CALL algo.or.solve(...)          -> requires write access
//!     CREATE (:Tmp) WITH 1 AS x CALL algo.or.solve(..) -> requires write access
//!
//! # The mechanism, and why it is three operators and not one
//!
//! A bare `CALL` lands in `query.call_clause`; anything else puts it in
//! `query.clauses` as `Clause::Call`, which the planner joins into the pipeline.
//! `JoinOperator::next_mut` drained the **left** through `next_mut` and then
//! called `self.next(store)` — the read path — which drives the **right**
//! through `next`. The joined-in `CALL` is always the right.
//!
//! `LeftOuterJoinOperator` and `CartesianProductOperator` had the identical
//! asymmetry, so the fix is symmetric in all three: drain the right through
//! `next_mut` as well. The two AST shapes are the reason the bare form was
//! fine and every other form was not.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

fn store_with_items() -> GraphStore {
    let mut store = GraphStore::new();
    for (name, cost) in [("a", 3.0_f64), ("b", 2.0), ("c", 5.0)] {
        let n = store.create_node("Item");
        store.set_node_property("default", n, "name", name).unwrap();
        store.set_node_property("default", n, "cost", PropertyValue::Float(cost)).unwrap();
        store.set_node_property("default", n, "alloc", PropertyValue::Float(0.0)).unwrap();
    }
    store
}

const CFG: &str = "{label: 'Item', property: 'alloc', cost_property: 'cost', \
                    algorithm: 'Jaya', budget: 18.0, population_size: 4, max_iterations: 5}";

/// Every shape that failed, and the bare one that did not, in one place.
#[test]
fn a_mutating_call_runs_whatever_precedes_it() {
    let engine = QueryEngine::new();
    let shapes = [
        ("bare", format!("CALL algo.or.solve({CFG}) YIELD fitness RETURN fitness")),
        ("WITH", format!("WITH 1 AS x CALL algo.or.solve({CFG}) YIELD fitness RETURN fitness")),
        ("MATCH+WITH", format!(
            "MATCH (i:Item) WITH count(i) AS n CALL algo.or.solve({CFG}) YIELD fitness RETURN fitness")),
        ("UNWIND", format!("UNWIND [1] AS x CALL algo.or.solve({CFG}) YIELD fitness RETURN fitness")),
        ("CREATE+WITH", format!(
            "CREATE (:Tmp) WITH 1 AS x CALL algo.or.solve({CFG}) YIELD fitness RETURN fitness")),
    ];
    for (label, q) in shapes {
        let mut store = store_with_items();
        let result = engine.execute_mut(&q, &mut store, "default");
        let err = result.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            !err.contains("requires write access"),
            "{label}: got the read path's refusal inside a write-routed statement \
             — this is #1479. Error: {err}"
        );
    }
}

/// The other direction. "Drive the right through `next_mut` always" must not
/// turn a read-only pipeline into one that takes the write path pointlessly —
/// the read-only procedure must still answer.
#[test]
fn a_read_only_call_after_a_clause_still_answers() {
    let engine = QueryEngine::new();
    let mut store = store_with_items();
    let out = engine
        .execute_mut("WITH 1 AS x CALL algo.pageRank() YIELD node RETURN count(*)",
                     &mut store, "default")
        .expect("a read-only CALL after a WITH must still run");
    assert_eq!(out.len(), 1, "one aggregate row");
}

/// A cartesian product over two reads is the shape most likely to be disturbed
/// by draining the right side differently.
#[test]
fn a_cartesian_read_is_unchanged() {
    let engine = QueryEngine::new();
    let mut store = store_with_items();
    let out = engine
        .execute_mut("MATCH (a:Item), (b:Item) RETURN count(*)", &mut store, "default")
        .expect("cartesian read");
    let n = out.records[0].get("count(*)").and_then(|v| v.as_property())
        .and_then(|p| p.as_integer()).expect("a count");
    assert_eq!(n, 9, "3 items x 3 items");
}
