//! `algo.or.solve` writes its solution back whole or not at all (OPT-07, #1773).
//!
//! The solver writes one property per node, one `set_node_property` at a time.
//! It used to discard each result (`let _ = ...`), so a write the store refused
//! part-way left some nodes holding the new solution and the rest the old one,
//! and the call reported success. Since #1637 a refused write fails the call,
//! and since #1593 a failed statement is undone whole. Nothing tested that the
//! two together hold for `or.solve`, which is what this file does.
//!
//! The fault is injected through the store, not mocked: a `UNIQUE` constraint
//! on the property being written, and bounds that admit only two values
//! (`5.0` and the next float above it). Four nodes cannot take four different
//! values from two, so a write is refused part-way through every run, after at
//! least one node has already been written. The control runs the same call
//! without the constraint, to show that it does write every node when nothing
//! refuses -- otherwise "nothing changed" would pass for a call that never
//! wrote at all.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

const TENANT: &str = "default";

/// The lowest bound, and the next representable float above it. A solver
/// clamps its variables to `[LOWER, UPPER]`, so it can only produce these two.
const LOWER: &str = "5.0";
const UPPER: &str = "5.000000000000001";

/// The values the nodes hold before the solve. Distinct, so the constraint
/// accepts them, and none of them in `[LOWER, UPPER]`.
const BEFORE: [f64; 4] = [1.0, 2.0, 3.0, 4.0];

fn store(unique: bool) -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for (i, v) in BEFORE.iter().enumerate() {
        engine
            .execute_mut(
                &format!("CREATE (:Item {{name: 'n{i}', alloc: {v:?}, c1: {}.0, c2: {}.0}})", i + 1, 4 - i),
                &mut store,
                TENANT,
            )
            .unwrap();
    }
    if unique {
        engine
            .execute_mut("CREATE CONSTRAINT FOR (n:Item) REQUIRE n.alloc IS UNIQUE", &mut store, TENANT)
            .unwrap();
    }
    (store, engine)
}

/// `alloc` on every node, in creation order.
fn allocs(store: &GraphStore, engine: &QueryEngine) -> Vec<f64> {
    engine
        .execute("MATCH (n:Item) RETURN n.alloc AS a ORDER BY n.name", store)
        .unwrap()
        .records
        .iter()
        .map(|r| match r.get("a") {
            Some(Value::Property(PropertyValue::Float(f))) => *f,
            other => panic!("alloc is not a float: {other:?}"),
        })
        .collect()
}

/// The single-objective call (Jaya) and the multi-objective one (NSGA-II, two
/// cost properties) write back through different loops, so both are covered.
fn solve_query(multi: bool) -> String {
    let objective = if multi {
        "algorithm: 'NSGA2', costProperties: ['c1', 'c2']"
    } else {
        "algorithm: 'Jaya', costProperty: 'c1'"
    };
    format!(
        "CALL algo.or.solve({{label: 'Item', property: 'alloc', {objective}, \
         lowerBound: {LOWER}, upperBound: {UPPER}, populationSize: 8, maxIterations: 5}}) \
         YIELD algorithm RETURN algorithm"
    )
}

fn a_refused_write_changes_nothing(multi: bool) {
    let (mut store, engine) = store(true);
    let err = engine
        .execute_mut(&solve_query(multi), &mut store, TENANT)
        .expect_err("a write the constraint refuses must fail the call, not be discarded");
    let msg = err.to_string();
    // "already has value ... on node N" means the refused write was not the
    // first: another node had already been written in this statement, so what
    // follows tests the undo and not a call that stopped before writing.
    assert!(
        msg.contains("Constraint violation") && msg.contains("already has value"),
        "the error should be the store's refusal: {msg}"
    );
    assert_eq!(
        allocs(&store, &engine),
        BEFORE,
        "a failed solve must leave every node as it was, not part-written"
    );
}

fn without_the_constraint_every_node_is_written(multi: bool) {
    let (mut store, engine) = store(false);
    engine
        .execute_mut(&solve_query(multi), &mut store, TENANT)
        .expect("nothing refuses the write");
    let upper: f64 = UPPER.parse().unwrap();
    let after = allocs(&store, &engine);
    assert!(
        after.iter().all(|&v| v == 5.0 || v == upper),
        "every node holds the solution, from the only two values the bounds admit: {after:?}"
    );
}

#[test]
fn a_single_objective_solve_whose_write_is_refused_changes_nothing() {
    a_refused_write_changes_nothing(false);
}

#[test]
fn a_multi_objective_solve_whose_write_is_refused_changes_nothing() {
    a_refused_write_changes_nothing(true);
}

#[test]
fn the_single_objective_control_writes_every_node() {
    without_the_constraint_every_node_is_written(false);
}

#[test]
fn the_multi_objective_control_writes_every_node() {
    without_the_constraint_every_node_is_written(true);
}

/// Equal or inverted bounds leave the solvers an empty range to draw from.
/// They panicked with "cannot sample empty range"; now the call is refused
/// before any solver runs, and nothing is written.
#[test]
fn bounds_that_leave_no_room_are_refused_not_a_panic() {
    for (lower, upper) in [("5.0", "5.0"), ("10.0", "1.0")] {
        let (mut store, engine) = store(false);
        let q = format!(
            "CALL algo.or.solve({{label: 'Item', property: 'alloc', costProperty: 'c1', \
             lowerBound: {lower}, upperBound: {upper}}}) YIELD algorithm RETURN algorithm"
        );
        let err = engine
            .execute_mut(&q, &mut store, TENANT)
            .expect_err("bounds with no room between them must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("Samyama.ClientError.Statement.ArgumentError")
                && msg.contains("must be less than `upperBound`"),
            "lowerBound {lower}, upperBound {upper}: {msg}"
        );
        assert_eq!(allocs(&store, &engine), BEFORE);
    }
}
