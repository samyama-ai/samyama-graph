//! Every `or.solve` row carries the result envelope (OPT-09, #1776).
//!
//! # What was missing
//!
//! A solve returned `algorithm`, `fitness`, `history` and `iterations`. The
//! requirement is "objective value, feasibility status, gap to best known/bound,
//! iterations, and wall-clock; never a bare answer", and three of those were
//! absent. The one that matters most is feasibility: `fitness` is objective
//! *plus penalty*, so a budget the solver could not meet came back as a large
//! number in a row that looked like any other.
//!
//! # What these tests pin
//!
//! - `objective`, `feasible`, `gap`, `total_time_ms`, `iterations` and
//!   `max_iterations` are on every row, for every solver the engine
//!   advertises, single- and multi-objective (the last two on multi-objective
//!   rows since #2265);
//! - `feasible` is false when a constraint cannot be met, and `fitness` then
//!   exceeds `objective` by the penalty;
//! - `gap` is `objective - bestKnown`, and `null` when no best known value is
//!   supplied (no solver here can prove a bound, so there is nothing else to
//!   subtract);
//! - a `bestKnown` that is not a finite number, or one passed to a
//!   multi-objective solve, is refused rather than ignored.
//!
//! The solver list is read from the engine's own refusal message, not copied
//! here, so a solver added later is covered without editing this file.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, Record};
use samyama::query::parse_query;

const T: &str = "default";

/// Three resources and their unit costs.
const RESOURCES: &[(&str, f64)] = &[("A", 10.0), ("B", 20.0), ("C", 15.0)];

/// Small, so the sweep over every solver stays fast.
const POPULATION: usize = 10;
const ITERATIONS: usize = 20;

/// The allocation bound per resource. With three resources the largest
/// reachable total is `RESOURCES.len() * UPPER_BOUND`.
const UPPER_BOUND: f64 = 1.0;

/// A minimum total no allocation within the bounds can reach, so every answer
/// breaks the constraint.
const UNREACHABLE_MIN_TOTAL: f64 = 1_000.0;

/// A budget every allocation within the bounds satisfies: the most expensive
/// allocation costs `sum(cost) * UPPER_BOUND` = 45.
const GENEROUS_BUDGET: f64 = 1_000.0;

/// A best known objective value to measure the gap against.
const BEST_KNOWN: f64 = 5.0;

/// The prefix of the engine's refusal that lists every solver.
const AVAILABLE_PREFIX: &str = "Available: ";

/// The columns the envelope puts on every row. `iterations` and
/// `max_iterations` were on single-objective rows only until #2265.
const ENVELOPE: &[&str] = &["objective", "feasible", "gap", "total_time_ms", "iterations", "max_iterations"];

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    for &(name, cost) in RESOURCES {
        let n = s.create_node("Resource");
        s.set_node_property(T, n, "name", name).unwrap();
        s.set_node_property(T, n, "cost", PropertyValue::Float(cost))
            .unwrap();
    }
    s
}

/// Run `or.solve` with `extra` appended to the config map and return its one
/// row, or the engine's error message.
fn solve(algorithm: &str, extra: &str) -> Result<Record, String> {
    let mut s = store();
    let q = parse_query(&format!(
        "CALL algo.or.solve({{label: 'Resource', property: 'allocation', \
         cost_property: 'cost', algorithm: '{algorithm}', upperBound: {UPPER_BOUND:?}, \
         populationSize: {POPULATION}, maxIterations: {ITERATIONS}{extra}}})"
    ))
    .map_err(|e| e.to_string())?;
    let mut ex = MutQueryExecutor::new(&mut s, T.to_string());
    let r = ex.execute(&q).map_err(|e| e.to_string())?;
    assert_eq!(r.records.len(), 1, "or.solve returns one row");
    Ok(r.records[0].clone())
}

fn prop<'a>(row: &'a Record, column: &str) -> Option<&'a PropertyValue> {
    row.get(column).and_then(|v| v.as_property())
}

fn float(row: &Record, column: &str) -> f64 {
    prop(row, column)
        .and_then(|p| p.as_float())
        .unwrap_or_else(|| panic!("`{column}` is a float: {:?}", row.get(column)))
}

fn feasible(row: &Record) -> bool {
    match prop(row, "feasible") {
        Some(PropertyValue::Boolean(b)) => *b,
        other => panic!("`feasible` is a boolean, got {other:?}"),
    }
}

/// Whether a column is bound and null.
fn is_null(row: &Record, column: &str) -> bool {
    row.get(column)
        .is_some_and(|v| v.as_property().is_none_or(|p| p.is_null()))
}

/// Every solver the engine advertises, from its own refusal of an unknown one.
fn advertised_solvers() -> Vec<String> {
    let err = solve("NoSuchSolver", "").expect_err("an unknown solver is refused");
    let list = err
        .split(AVAILABLE_PREFIX)
        .nth(1)
        .unwrap_or_else(|| panic!("the refusal lists the solvers: {err}"));
    list.split(", ").map(|s| s.trim().to_string()).collect()
}

#[test]
fn every_advertised_solver_returns_the_whole_envelope() {
    let solvers = advertised_solvers();
    assert!(!solvers.is_empty());
    for algorithm in &solvers {
        let row = solve(algorithm, &format!(", budget: {GENEROUS_BUDGET:?}"))
            .unwrap_or_else(|e| panic!("{algorithm}: {e}"));
        for column in ENVELOPE {
            assert!(
                row.get(column).is_some(),
                "{algorithm}: no `{column}` column"
            );
        }
        let int = |column: &str| match prop(&row, column) {
            Some(PropertyValue::Integer(n)) => *n,
            other => panic!("{algorithm}: `{column}` is an integer, got {other:?}"),
        };
        // The iterations the solver ran, at least one and at most the cap,
        // and the cap as asked: a solve that stops early says so.
        assert_eq!(int("max_iterations"), ITERATIONS as i64, "{algorithm}");
        assert!(
            (1..=ITERATIONS as i64).contains(&int("iterations")),
            "{algorithm}: iterations = {}",
            int("iterations")
        );
        let ms = float(&row, "total_time_ms");
        assert!(
            ms.is_finite() && ms >= 0.0,
            "{algorithm}: total_time_ms = {ms}"
        );
        assert!(
            feasible(&row),
            "{algorithm}: a generous budget is always met"
        );
        assert!(is_null(&row, "gap"), "{algorithm}: no bestKnown, so no gap");
    }
}

#[test]
fn an_unreachable_minimum_is_reported_infeasible() {
    let row = solve("Jaya", &format!(", minTotal: {UNREACHABLE_MIN_TOTAL:?}")).unwrap();
    assert!(!feasible(&row));
    // The penalty is in `fitness`, not in `objective`.
    assert!(
        float(&row, "fitness") > float(&row, "objective"),
        "fitness {} should exceed objective {} by the penalty",
        float(&row, "fitness"),
        float(&row, "objective")
    );
}

#[test]
fn a_feasible_answer_has_its_fitness_equal_to_its_objective() {
    let row = solve("Jaya", &format!(", budget: {GENEROUS_BUDGET:?}")).unwrap();
    assert!(feasible(&row));
    assert_eq!(float(&row, "fitness"), float(&row, "objective"));
}

#[test]
fn the_gap_is_measured_against_the_best_known_value() {
    let row = solve("Jaya", &format!(", bestKnown: {BEST_KNOWN:?}")).unwrap();
    assert_eq!(float(&row, "gap"), float(&row, "objective") - BEST_KNOWN);
}

#[test]
fn best_known_accepts_the_snake_case_spelling_and_an_integer() {
    let integer = BEST_KNOWN as i64;
    let row = solve("Jaya", &format!(", best_known: {integer}")).unwrap();
    assert_eq!(
        float(&row, "gap"),
        float(&row, "objective") - integer as f64
    );
}

#[test]
fn a_best_known_that_is_not_a_number_is_refused() {
    let err = solve("Jaya", ", bestKnown: 'five'").expect_err("a string is refused");
    assert!(err.contains("bestKnown"), "{err}");
}

#[test]
fn a_best_known_on_a_multi_objective_solve_is_refused() {
    let err = solve("NSGA2", &format!(", bestKnown: {BEST_KNOWN:?}"))
        .expect_err("a front has no scalar gap");
    assert!(err.contains("bestKnown"), "{err}");
}

#[test]
fn the_multi_objective_refusal_does_not_depend_on_the_data() {
    // No node has this label, so the solve would return no row at all; the
    // misuse is still reported, not hidden behind the empty result.
    let mut s = store();
    let q = parse_query(&format!(
        "CALL algo.or.solve({{label: 'NoSuchLabel', property: 'allocation', \
         cost_property: 'cost', algorithm: 'NSGA2', bestKnown: {BEST_KNOWN:?}}})"
    ))
    .expect("parse");
    let mut ex = MutQueryExecutor::new(&mut s, T.to_string());
    let err = ex
        .execute(&q)
        .expect_err("refused before the graph is read");
    assert!(err.to_string().contains("bestKnown"), "{err}");
}

#[test]
fn a_multi_objective_row_reports_the_written_point() {
    let row = solve("NSGA2", &format!(", budget: {GENEROUS_BUDGET:?}")).unwrap();
    // The objective vector is the point's fitness: no penalty is added to it.
    assert_eq!(prop(&row, "objective"), prop(&row, "fitness"));
    assert!(feasible(&row));
    assert!(is_null(&row, "gap"));
}

#[test]
fn a_multi_objective_point_that_breaks_the_minimum_is_infeasible() {
    // The multi-objective solvers are given no penalties, so their own
    // `constraint_violation` is 0 here; `feasible` must not be read from it.
    let row = solve("NSGA2", &format!(", minTotal: {UNREACHABLE_MIN_TOTAL:?}")).unwrap();
    assert!(!feasible(&row));
}

#[test]
fn the_envelope_columns_can_be_yielded_by_name() {
    let mut s = store();
    let q = parse_query(&format!(
        "CALL algo.or.solve({{label: 'Resource', property: 'allocation', \
         cost_property: 'cost', algorithm: 'Jaya', bestKnown: {BEST_KNOWN:?}, \
         populationSize: {POPULATION}, maxIterations: {ITERATIONS}}}) \
         YIELD objective, feasible, gap, total_time_ms"
    ))
    .expect("parse");
    let mut ex = MutQueryExecutor::new(&mut s, T.to_string());
    let r = ex.execute(&q).expect("execute");
    for column in ENVELOPE {
        assert!(r.records[0].get(column).is_some(), "YIELD {column}");
    }
}
