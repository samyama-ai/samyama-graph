//! `algo.or.solve` at graph scale, within a stated budget (OPT-11, #1798).
//!
//! # What was missing
//!
//! The issue asks for a graph-derived problem of 10,000 variables solved
//! within a stated budget. `algo.or.solve` already derives one variable per
//! node of a label, so its size is the graph's. What it could not do was take
//! a budget: `/optimize/solve` accepted `time_budget_ms` and `max_evaluations`
//! (#2012), but its problems are fixed benchmarks, and the one graph-shaped
//! one, `uc2_dosing`, is 6 variables whatever `dim` says. The Cypher solve
//! ran `maxIterations` iterations and nothing else could stop it. (`budget`
//! there is the cost constraint, not a limit on the run.)
//!
//! # What these tests pin
//!
//! - The problem has one variable per node: 10,000 nodes, 10,000 values
//!   written back, each within bounds.
//! - `timeBudgetMs` and `maxEvaluations` stop the run, for every solver, and
//!   `stopped_by` says which limit did; `evaluations` is what was spent.
//! - A run with no budget reports `stopped_by: 'iterations'`.
//! - A limit that is 0, negative, fractional or not a number is refused.
//! - The same seed and evaluation budget give the same answer.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parse_query;
use std::collections::HashMap;

const T: &str = "default";

/// Every single-objective solver `algo.or.solve` dispatches to.
const SINGLE: &[&str] = &[
    "Jaya", "Rao1", "Rao2", "Rao3", "TLBO", "Firefly", "Cuckoo", "GWO", "GA", "SA", "Bat", "ABC",
    "GSA", "HS", "FPA", "PSO", "DE", "BMR", "BWR", "BMWR", "QOJaya", "SAMPJaya", "EHRJaya",
    "ITLBO", "GOTLBO", "QORao", "SAPHR", "LSHADE", "CMAES",
];

/// Every multi-objective one.
const MULTI: &[&str] = &["NSGA2", "MOTLBO", "MOBMWR", "MORaoDE", "SPEA2", "MOEAD"];

/// `n` nodes labelled `Site`, each with a `cost` and a `risk` that vary.
fn store(n: usize) -> GraphStore {
    let mut s = GraphStore::new();
    for i in 0..n {
        let id = s.create_node("Site");
        let cost = 1.0 + (i % 7) as f64;
        let risk = 1.0 + (i % 5) as f64;
        s.set_node_property(T, id, "cost", PropertyValue::Float(cost))
            .unwrap();
        s.set_node_property(T, id, "risk", PropertyValue::Float(risk))
            .unwrap();
    }
    s
}

fn run(s: &mut GraphStore, config: &str, yields: &str) -> Result<HashMap<String, Value>, String> {
    let q = parse_query(&format!(
        "CALL algo.or.solve({{label: 'Site', property: 'x', {config}}}) YIELD {yields}"
    ))
    .expect("parse");
    let mut ex = MutQueryExecutor::new(s, T.to_string());
    let r = ex.execute(&q).map_err(|e| e.to_string())?;
    assert_eq!(r.records.len(), 1, "one row per solve");
    Ok(yields
        .split(", ")
        .map(|k| {
            (
                k.to_string(),
                r.records[0].get(k).cloned().unwrap_or(Value::Null),
            )
        })
        .collect())
}

fn int(row: &HashMap<String, Value>, k: &str) -> i64 {
    row[k]
        .as_property()
        .and_then(|p| p.as_integer())
        .unwrap_or_else(|| panic!("{k}: {:?}", row[k]))
}

fn text(row: &HashMap<String, Value>, k: &str) -> String {
    row[k]
        .as_property()
        .and_then(|p| p.as_string())
        .unwrap_or_else(|| panic!("{k}: {:?}", row[k]))
        .to_string()
}

fn float(row: &HashMap<String, Value>, k: &str) -> f64 {
    row[k]
        .as_property()
        .and_then(|p| p.as_float())
        .unwrap_or_else(|| panic!("{k}: {:?}", row[k]))
}

const BUDGET_YIELDS: &str = "evaluations, stopped_by, iterations, max_iterations, total_time_ms";

#[test]
fn ten_thousand_nodes_are_ten_thousand_variables() {
    let n = 10_000;
    let mut s = store(n);
    let row = run(
        &mut s,
        "costProperty: 'cost', lowerBound: 2.0, upperBound: 9.0, algorithm: 'Jaya', \
         populationSize: 6, maxIterations: 3, maxEvaluations: 1000000, seed: 7",
        BUDGET_YIELDS,
    )
    .unwrap();
    assert_eq!(text(&row, "stopped_by"), "iterations", "{row:?}");
    let written: Vec<f64> = s
        .get_nodes_by_label(&Label::new("Site"))
        .iter()
        .map(|n| {
            s.node_property(n.id, "x")
                .and_then(|v| v.as_float())
                .expect("every node written")
        })
        .collect();
    assert_eq!(written.len(), n, "one variable per node");
    assert!(
        written.iter().all(|v| (2.0..=9.0).contains(v)),
        "every value within its bounds"
    );
    // Not all one value: the solve chose 10,000 values, not one broadcast.
    assert!(written.iter().any(|v| *v != written[0]));
}

#[test]
fn a_run_with_no_budget_runs_every_iteration_and_counts_its_evaluations() {
    let (pop, cap) = (8, 12);
    let row = run(
        &mut store(20),
        &format!("costProperty: 'cost', algorithm: 'Jaya', populationSize: {pop}, maxIterations: {cap}, seed: 1"),
        BUDGET_YIELDS,
    )
    .unwrap();
    assert_eq!(text(&row, "stopped_by"), "iterations");
    assert_eq!(int(&row, "iterations"), cap);
    // Jaya scores its starting population, then one candidate per member each
    // iteration.
    assert_eq!(int(&row, "evaluations"), pop * (cap + 1), "{row:?}");
}

#[test]
fn every_single_objective_solver_stops_at_its_evaluation_budget() {
    let (max, cap) = (300, 100_000);
    for algorithm in SINGLE {
        let row = run(
            &mut store(10),
            &format!(
                "costProperty: 'cost', algorithm: '{algorithm}', populationSize: 10, \
                 maxIterations: {cap}, maxEvaluations: {max}, seed: 3"
            ),
            BUDGET_YIELDS,
        )
        .unwrap_or_else(|e| panic!("{algorithm}: {e}"));
        assert_eq!(
            text(&row, "stopped_by"),
            "evaluation_budget",
            "{algorithm}: {row:?}"
        );
        assert!(
            int(&row, "iterations") < cap,
            "{algorithm} ran every iteration: {row:?}"
        );
        assert!(
            int(&row, "evaluations") >= max,
            "{algorithm} stopped before its budget: {row:?}"
        );
    }
}

#[test]
fn every_multi_objective_solver_stops_at_its_evaluation_budget() {
    let (max, cap) = (300, 100_000);
    for algorithm in MULTI {
        let row = run(
            &mut store(10),
            &format!(
                "costProperties: ['cost', 'risk'], algorithm: '{algorithm}', populationSize: 10, \
                 maxIterations: {cap}, max_evaluations: {max}, seed: 3"
            ),
            BUDGET_YIELDS,
        )
        .unwrap_or_else(|e| panic!("{algorithm}: {e}"));
        assert_eq!(
            text(&row, "stopped_by"),
            "evaluation_budget",
            "{algorithm}: {row:?}"
        );
        assert!(
            int(&row, "iterations") < cap,
            "{algorithm} ran every iteration: {row:?}"
        );
        assert!(int(&row, "evaluations") >= max, "{algorithm}: {row:?}");
    }
}

#[test]
fn jaya_overshoots_its_evaluation_budget_by_at_most_one_iteration() {
    let pop = 10;
    let row = run(
        &mut store(10),
        &format!("costProperty: 'cost', algorithm: 'Jaya', populationSize: {pop}, maxIterations: 100000, maxEvaluations: 205, seed: 3"),
        BUDGET_YIELDS,
    )
    .unwrap();
    let used = int(&row, "evaluations");
    assert!((205..205 + pop).contains(&used), "{row:?}");
}

#[test]
fn the_time_budget_stops_a_large_solve() {
    let cap = 100_000_000;
    let row = run(
        &mut store(2_000),
        &format!("costProperty: 'cost', algorithm: 'Jaya', populationSize: 10, maxIterations: {cap}, timeBudgetMs: 100, seed: 5"),
        BUDGET_YIELDS,
    )
    .unwrap();
    assert_eq!(text(&row, "stopped_by"), "time_budget", "{row:?}");
    assert!(int(&row, "iterations") < cap);
    // A lower bound only: the run cannot stop before its budget is spent.
    // How far past it a run goes depends on the machine (see CLAUDE.md).
    assert!(float(&row, "total_time_ms") >= 100.0, "{row:?}");
}

#[test]
fn the_snake_case_time_budget_is_the_same_key() {
    let row = run(
        &mut store(200),
        "costProperty: 'cost', populationSize: 10, maxIterations: 100000000, time_budget_ms: 20, seed: 5",
        BUDGET_YIELDS,
    )
    .unwrap();
    assert_eq!(text(&row, "stopped_by"), "time_budget", "{row:?}");
}

#[test]
fn a_small_evaluation_budget_is_not_hidden_by_a_large_time_budget() {
    // A generous time budget does not hide a small evaluation budget.
    let row = run(
        &mut store(10),
        "costProperty: 'cost', populationSize: 10, maxIterations: 100000, maxEvaluations: 50, timeBudgetMs: 600000, seed: 2",
        BUDGET_YIELDS,
    )
    .unwrap();
    assert_eq!(text(&row, "stopped_by"), "evaluation_budget", "{row:?}");
}

#[test]
fn a_budget_that_is_zero_negative_fractional_or_not_a_number_is_refused() {
    for key in [
        "timeBudgetMs",
        "maxEvaluations",
        "time_budget_ms",
        "max_evaluations",
    ] {
        for bad in ["0", "-5", "1.5", "'ten'"] {
            let err = run(
                &mut store(3),
                &format!("costProperty: 'cost', maxIterations: 2, {key}: {bad}"),
                "evaluations",
            )
            .expect_err(&format!("{key}: {bad} was accepted"));
            assert!(err.contains(key), "{key}: {bad}: {err}");
        }
    }
}

#[test]
fn the_same_seed_and_evaluation_budget_give_the_same_answer() {
    let solve = || {
        run(
            &mut store(500),
            "costProperty: 'cost', minTotal: 300.0, algorithm: 'DE', populationSize: 12, \
             maxIterations: 100000, maxEvaluations: 600, seed: 11",
            "objective, evaluations, iterations",
        )
        .unwrap()
    };
    let (a, b) = (solve(), solve());
    assert_eq!(float(&a, "objective"), float(&b, "objective"));
    assert_eq!(int(&a, "evaluations"), int(&b, "evaluations"));
    assert_eq!(int(&a, "iterations"), int(&b, "iterations"));
}
