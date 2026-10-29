//! Every solver stops when its budget is spent (OPT-11, #1798).
//!
//! A budgeted run must end exactly as a run with fewer iterations would: it
//! stops at the top of the first iteration that finds the budget spent. So
//! for a seeded solver, a run stopped after `k` iterations gives the same
//! answer and the same evaluation count as an unbudgeted run of `k`
//! iterations, the budget was not yet spent after `k - 1`, and it was after
//! `k`. Each of the 29 solvers is held to that.
//!
//! Two solvers follow a schedule over `max_iterations`: GWO's step and GSA's
//! gravity shrink from the first iteration to the last. Stopped early, they
//! are partway along a longer schedule, which is not the search a shorter run
//! makes, so for them the answer is not compared; the iteration and
//! evaluation counts still are.

use ndarray::Array1;
use samyama_optimization::algorithms::*;
use samyama_optimization::budget::{Budget, Budgeted, StopReason};
use samyama_optimization::common::*;
use std::time::Duration;

const SEED: u64 = 7;
const POPULATION: usize = 10;
/// More iterations than any budget below allows.
const MANY: usize = 1_000;
/// Enough for several iterations of every solver, far short of `MANY`.
const EVALUATIONS: u64 = 500;
const SPHERE_DIM: usize = 4;
const ZDT_DIM: usize = 6;
/// Solvers whose updates depend on `max_iterations`; see the module comment.
const SCHEDULED: [&str; 2] = ["gwo", "gsa"];

fn config(iterations: usize) -> SolverConfig {
    SolverConfig {
        population_size: POPULATION,
        max_iterations: iterations,
    }
}

fn sphere() -> SimpleProblem<fn(&Array1<f64>) -> f64> {
    SimpleProblem {
        objective_func: |x| x.iter().map(|v| v * v).sum(),
        dim: SPHERE_DIM,
        lower: Array1::from_elem(SPHERE_DIM, -5.0),
        upper: Array1::from_elem(SPHERE_DIM, 5.0),
    }
}

struct Zdt1;

impl MultiObjectiveProblem for Zdt1 {
    fn dim(&self) -> usize {
        ZDT_DIM
    }
    fn num_objectives(&self) -> usize {
        2
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(ZDT_DIM), Array1::ones(ZDT_DIM))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (ZDT_DIM as f64 - 1.0);
        vec![x[0], g * (1.0 - (x[0] / g).sqrt())]
    }
}

/// What one run produced, in a form both kinds of solver share.
#[derive(Debug, PartialEq)]
struct Run {
    iterations: usize,
    answer: Vec<f64>,
    evaluations: u64,
}

type Single =
    fn(SolverConfig, &Budgeted<SimpleProblem<fn(&Array1<f64>) -> f64>>) -> OptimizationResult;
type Multi = fn(SolverConfig, &Budgeted<Zdt1>) -> MultiObjectiveResult;

fn single_solvers() -> Vec<(&'static str, Single)> {
    vec![
        ("jaya", |c, p| JayaSolver::new(c).with_seed(SEED).solve(p)),
        ("rao1", |c, p| {
            RaoSolver::new(c, RaoVariant::Rao1).with_seed(SEED).solve(p)
        }),
        ("tlbo", |c, p| TLBOSolver::new(c).with_seed(SEED).solve(p)),
        ("bmr", |c, p| BMRSolver::new(c).with_seed(SEED).solve(p)),
        ("bwr", |c, p| BWRSolver::new(c).with_seed(SEED).solve(p)),
        ("qojaya", |c, p| {
            QOJayaSolver::new(c).with_seed(SEED).solve(p)
        }),
        ("itlbo", |c, p| ITLBOSolver::new(c).with_seed(SEED).solve(p)),
        ("pso", |c, p| PSOSolver::new(c).with_seed(SEED).solve(p)),
        ("de", |c, p| DESolver::new(c).with_seed(SEED).solve(p)),
        ("gotlbo", |c, p| {
            GOTLBOSolver::new(c).with_seed(SEED).solve(p)
        }),
        ("firefly", |c, p| {
            FireflySolver::new(c).with_seed(SEED).solve(p)
        }),
        ("cuckoo", |c, p| {
            CuckooSolver::new(c).with_seed(SEED).solve(p)
        }),
        ("gwo", |c, p| GWOSolver::new(c).with_seed(SEED).solve(p)),
        ("ga", |c, p| GASolver::new(c).with_seed(SEED).solve(p)),
        ("sa", |c, p| SASolver::new(c).with_seed(SEED).solve(p)),
        ("bat", |c, p| BatSolver::new(c).with_seed(SEED).solve(p)),
        ("abc", |c, p| ABCSolver::new(c).with_seed(SEED).solve(p)),
        ("gsa", |c, p| GSASolver::new(c).with_seed(SEED).solve(p)),
        ("hs", |c, p| HSSolver::new(c).with_seed(SEED).solve(p)),
        ("fpa", |c, p| FPASolver::new(c).with_seed(SEED).solve(p)),
        ("bmwr", |c, p| BMWRSolver::new(c).with_seed(SEED).solve(p)),
        ("samp_jaya", |c, p| {
            SAMPJayaSolver::new(c).with_seed(SEED).solve(p)
        }),
        ("ehrjaya", |c, p| {
            EHRJayaSolver::new(c).with_seed(SEED).solve(p)
        }),
        ("qo_rao", |c, p| {
            QORaoSolver::new(c, RaoVariant::Rao1)
                .with_seed(SEED)
                .solve(p)
        }),
        ("saphr", |c, p| SAPHRSolver::new(c).with_seed(SEED).solve(p)),
    ]
}

fn multi_solvers() -> Vec<(&'static str, Multi)> {
    vec![
        ("nsga2", |c, p| NSGA2Solver::new(c).with_seed(SEED).solve(p)),
        ("motlbo", |c, p| {
            MOTLBOSolver::new(c).with_seed(SEED).solve(p)
        }),
        ("mo_bmwr", |c, p| {
            MOBMWRSolver::new(c, MOBMWRVariant::MOBMWR)
                .with_seed(SEED)
                .solve(p)
        }),
        ("mo_rao_de", |c, p| {
            MORaoDESolver::new(c).with_seed(SEED).solve(p)
        }),
    ]
}

fn run_single(solve: Single, iterations: usize, budget: Budget) -> (Run, StopReason) {
    let problem = Budgeted::new(sphere(), budget);
    let r = solve(config(iterations), &problem);
    let run = Run {
        iterations: r.history.len(),
        answer: vec![r.best_fitness],
        evaluations: problem.evaluations(),
    };
    (run, problem.stop_reason())
}

fn run_multi(solve: Multi, iterations: usize, budget: Budget) -> (Run, StopReason) {
    let problem = Budgeted::new(Zdt1, budget);
    let r = solve(config(iterations), &problem);
    let run = Run {
        iterations: r.history.len(),
        answer: r
            .pareto_front
            .iter()
            .flat_map(|i| i.fitness.clone())
            .collect(),
        evaluations: problem.evaluations(),
    };
    (run, problem.stop_reason())
}

fn evaluation_budget() -> Budget {
    Budget {
        evaluations: Some(EVALUATIONS),
        ..Budget::default()
    }
}

/// The checks every solver is held to; `run(iterations, budget)` runs it.
fn assert_stops_as_fewer_iterations_would(
    name: &str,
    run: impl Fn(usize, Budget) -> (Run, StopReason),
) {
    let (budgeted, reason) = run(MANY, evaluation_budget());
    let k = budgeted.iterations;
    assert_eq!(reason, StopReason::EvaluationBudget, "{name}: {budgeted:?}");
    assert!(
        k > 0 && k < MANY,
        "{name}: stopped after {k} of {MANY} iterations"
    );

    let (unbudgeted, reason) = run(k, Budget::default());
    assert_eq!(reason, StopReason::Iterations, "{name}");
    let same = if SCHEDULED.contains(&name) {
        (budgeted.iterations, budgeted.evaluations)
            == (unbudgeted.iterations, unbudgeted.evaluations)
    } else {
        budgeted == unbudgeted
    };
    assert!(
        same,
        "{name}: a run stopped after {k} iterations must be a run of {k} iterations:\n\
         {budgeted:?}\n{unbudgeted:?}"
    );

    assert!(
        budgeted.evaluations >= EVALUATIONS,
        "{name}: stopped before the budget was spent"
    );
    let (one_fewer, _) = run(k - 1, Budget::default());
    assert!(
        one_fewer.evaluations < EVALUATIONS,
        "{name}: the budget was already spent after {} iterations, so it ran one too many",
        k - 1
    );
}

#[test]
fn every_single_objective_solver_stops_at_its_evaluation_budget() {
    let solvers = single_solvers();
    assert_eq!(solvers.len(), 25);
    for (name, solve) in solvers {
        assert_stops_as_fewer_iterations_would(name, |n, b| run_single(solve, n, b));
    }
}

#[test]
fn every_multi_objective_solver_stops_at_its_evaluation_budget() {
    let solvers = multi_solvers();
    assert_eq!(solvers.len(), 4);
    for (name, solve) in solvers {
        assert_stops_as_fewer_iterations_would(name, |n, b| run_multi(solve, n, b));
    }
}

#[test]
fn without_a_budget_every_solver_runs_every_iteration() {
    const FEW: usize = 5;
    for (name, solve) in single_solvers() {
        let (run, reason) = run_single(solve, FEW, Budget::default());
        assert_eq!(
            (run.iterations, reason),
            (FEW, StopReason::Iterations),
            "{name}"
        );
    }
    for (name, solve) in multi_solvers() {
        let (run, reason) = run_multi(solve, FEW, Budget::default());
        assert_eq!(
            (run.iterations, reason),
            (FEW, StopReason::Iterations),
            "{name}"
        );
    }
}

#[test]
fn a_time_budget_stops_a_slow_solve() {
    const EVALUATION_COST: Duration = Duration::from_millis(1);
    const LIMIT: Duration = Duration::from_millis(50);
    let slow = SimpleProblem {
        objective_func: |x: &Array1<f64>| {
            std::thread::sleep(EVALUATION_COST);
            x.iter().map(|v| v * v).sum()
        },
        dim: SPHERE_DIM,
        lower: Array1::from_elem(SPHERE_DIM, -5.0),
        upper: Array1::from_elem(SPHERE_DIM, 5.0),
    };
    let problem = Budgeted::new(
        slow,
        Budget {
            time: Some(LIMIT),
            ..Budget::default()
        },
    );
    let started = std::time::Instant::now();
    let r = JayaSolver::new(config(MANY))
        .with_seed(SEED)
        .solve(&problem);
    let took = started.elapsed();
    assert_eq!(problem.stop_reason(), StopReason::TimeBudget);
    assert!(r.history.len() < MANY, "ran {} iterations", r.history.len());
    // Without the budget this solve sleeps POPULATION * MANY ms, ten seconds.
    // The bound is loose so a slow CI runner's sleeps cannot fail it; it still
    // fails a solve that ignored the budget by a factor of ten.
    let ignored_the_budget = EVALUATION_COST * (POPULATION * MANY) as u32 / 10;
    assert!(
        took < ignored_the_budget,
        "took {took:?} against a {LIMIT:?} budget"
    );
    assert!(r.best_fitness.is_finite());
}

#[test]
fn whichever_limit_comes_first_is_the_one_reported() {
    let (_, reason) = run_single(
        single_solvers()[0].1,
        MANY,
        Budget {
            time: Some(Duration::from_secs(3_600)),
            evaluations: Some(EVALUATIONS),
        },
    );
    assert_eq!(reason, StopReason::EvaluationBudget);
}
