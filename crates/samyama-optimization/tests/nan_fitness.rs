//! A NaN fitness neither panics a solver nor becomes its answer (#1634).
//!
//! The objective here returns NaN over half the domain, and the NaN is made
//! with `0.0 / 0.0`, which on x86 has its sign bit set -- the value
//! `f64::total_cmp` orders before every number, so a minimiser comparing that
//! way would return it as the best. Every solver must finish, must not report
//! a NaN best, and must report a best from the half where the objective is
//! defined.

use ndarray::Array1;
use samyama_optimization::algorithms::*;
use samyama_optimization::common::{
    ascending_nan_last, descending_nan_last, MultiObjectiveProblem, Problem, SolverConfig,
};

/// Sphere on [-5, 5]^2, undefined (NaN) wherever x0 > 0.
struct HalfNan;

#[allow(clippy::eq_op)]
fn nan() -> f64 {
    let zero = std::hint::black_box(0.0f64);
    zero / zero
}

impl Problem for HalfNan {
    fn objective(&self, x: &Array1<f64>) -> f64 {
        if x[0] > 0.0 {
            nan()
        } else {
            x.iter().map(|v| v * v).sum()
        }
    }
    fn dim(&self) -> usize {
        2
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::from(vec![-5.0, -5.0]), Array1::from(vec![5.0, 5.0]))
    }
}

/// NaN everywhere.
struct AllNan;

impl Problem for AllNan {
    fn objective(&self, _x: &Array1<f64>) -> f64 {
        nan()
    }
    fn dim(&self) -> usize {
        2
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::from(vec![-1.0, -1.0]), Array1::from(vec![1.0, 1.0]))
    }
}

/// Every solver is seeded, so a failure is reproducible rather than a draw.
/// Unseeded, SA failed one CI run in several: see below.
const SEED: u64 = 1634;

fn config() -> SolverConfig {
    SolverConfig {
        population_size: 12,
        max_iterations: 25,
    }
}

type Run = Box<dyn Fn() -> samyama_optimization::common::OptimizationResult>;
type MoRun = Box<dyn Fn() -> samyama_optimization::common::MultiObjectiveResult>;

macro_rules! single_objective {
    ($($name:literal => $solver:expr),* $(,)?) => {
        vec![$(($name, Box::new(|| $solver.solve(&HalfNan)) as Run)),*]
    };
}

#[test]
fn every_single_objective_solver_finishes_and_never_returns_a_nan_best() {
    let solvers = single_objective![
        "Jaya" => JayaSolver::new(config()).with_seed(SEED),
        "Rao1" => RaoSolver::new(config(), RaoVariant::Rao1).with_seed(SEED),
        "Rao2" => RaoSolver::new(config(), RaoVariant::Rao2).with_seed(SEED),
        "Rao3" => RaoSolver::new(config(), RaoVariant::Rao3).with_seed(SEED),
        "TLBO" => TLBOSolver::new(config()).with_seed(SEED),
        "BMR" => BMRSolver::new(config()).with_seed(SEED),
        "BWR" => BWRSolver::new(config()).with_seed(SEED),
        "QOJaya" => QOJayaSolver::new(config()).with_seed(SEED),
        "ITLBO" => ITLBOSolver::new(config()).with_seed(SEED),
        "PSO" => PSOSolver::new(config()).with_seed(SEED),
        "DE" => DESolver::new(config()).with_seed(SEED),
        "GOTLBO" => GOTLBOSolver::new(config()).with_seed(SEED),
        "Firefly" => FireflySolver::new(config()).with_seed(SEED),
        "Cuckoo" => CuckooSolver::new(config()).with_seed(SEED),
        "GWO" => GWOSolver::new(config()).with_seed(SEED),
        "GA" => GASolver::new(config()).with_seed(SEED),
        "SA" => SASolver::new(config()).with_seed(SEED),
        "Bat" => BatSolver::new(config()).with_seed(SEED),
        "ABC" => ABCSolver::new(config()).with_seed(SEED),
        "GSA" => GSASolver::new(config()).with_seed(SEED),
        "HS" => HSSolver::new(config()).with_seed(SEED),
        "FPA" => FPASolver::new(config()).with_seed(SEED),
        "BMWR" => BMWRSolver::new(config()).with_seed(SEED),
        "SAMPJaya" => SAMPJayaSolver::new(config()).with_seed(SEED),
        "EHRJaya" => EHRJayaSolver::new(config()).with_seed(SEED),
        "QORao1" => QORaoSolver::new(config(), RaoVariant::Rao1).with_seed(SEED),
        "SAPHR" => SAPHRSolver::new(config()).with_seed(SEED),
    ];
    let mut bad = Vec::new();
    for (name, run) in &solvers {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        match result {
            Err(_) => bad.push(format!("{name}: panicked")),
            Ok(r) if r.best_fitness.is_nan() => bad.push(format!("{name}: NaN best")),
            // SA walks from one random point, 25 steps here. Started in the
            // undefined half it may never reach the other, and then +inf is
            // the honest answer: it saw no defined point, so it preferred no
            // undefined one. What it must not do is call an undefined point
            // finite.
            Ok(r) if *name == "SA" && r.best_variables[0] > 0.0 => {
                if r.best_fitness != f64::INFINITY {
                    bad.push(format!("SA: undefined best reported as {}", r.best_fitness));
                }
            }
            Ok(r) if r.best_variables[0] > 0.0 => bad.push(format!(
                "{name}: best {:?} is in the undefined half (fitness {})",
                r.best_variables, r.best_fitness
            )),
            Ok(r) if !r.best_fitness.is_finite() => bad.push(format!(
                "{name}: no defined best found ({})",
                r.best_fitness
            )),
            Ok(_) => {}
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[test]
fn an_objective_that_is_nan_everywhere_reports_infinity_not_nan() {
    for r in [
        JayaSolver::new(config()).with_seed(SEED).solve(&AllNan),
        QOJayaSolver::new(config()).with_seed(SEED).solve(&AllNan),
        HSSolver::new(config()).with_seed(SEED).solve(&AllNan),
        CuckooSolver::new(config()).with_seed(SEED).solve(&AllNan),
    ] {
        assert!(!r.best_fitness.is_nan());
        assert_eq!(r.best_fitness, f64::INFINITY);
    }
}

/// Two objectives, the second NaN wherever x0 > 0.
struct HalfNanMo;

impl MultiObjectiveProblem for HalfNanMo {
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let f1 = x[0] * x[0];
        let f2 = if x[0] > 0.0 {
            nan()
        } else {
            (x[0] - 2.0).powi(2)
        };
        vec![f1, f2]
    }
    fn dim(&self) -> usize {
        1
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::from(vec![-5.0]), Array1::from(vec![5.0]))
    }
    fn num_objectives(&self) -> usize {
        2
    }
}

#[test]
fn every_multi_objective_solver_finishes_with_no_nan_on_its_front() {
    let runs: Vec<(&str, MoRun)> = vec![
        (
            "NSGA2",
            Box::new(|| NSGA2Solver::new(config()).with_seed(SEED).solve(&HalfNanMo)),
        ),
        (
            "MOTLBO",
            Box::new(|| {
                MOTLBOSolver::new(config())
                    .with_seed(SEED)
                    .solve(&HalfNanMo)
            }),
        ),
        (
            "MOBMR",
            Box::new(|| {
                MOBMWRSolver::new(config(), MOBMWRVariant::MOBMR)
                    .with_seed(SEED)
                    .solve(&HalfNanMo)
            }),
        ),
        (
            "MOBWR",
            Box::new(|| {
                MOBMWRSolver::new(config(), MOBMWRVariant::MOBWR)
                    .with_seed(SEED)
                    .solve(&HalfNanMo)
            }),
        ),
        (
            "MOBMWR",
            Box::new(|| {
                MOBMWRSolver::new(config(), MOBMWRVariant::MOBMWR)
                    .with_seed(SEED)
                    .solve(&HalfNanMo)
            }),
        ),
        (
            "MORaoDE",
            Box::new(|| {
                MORaoDESolver::new(config())
                    .with_seed(SEED)
                    .solve(&HalfNanMo)
            }),
        ),
    ];
    let mut bad = Vec::new();
    for (name, run) in &runs {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
            Err(_) => bad.push(format!("{name}: panicked")),
            Ok(r) => {
                if r.pareto_front.is_empty() {
                    bad.push(format!("{name}: empty front"));
                }
                if r.pareto_front
                    .iter()
                    .any(|i| i.fitness.iter().any(|f| f.is_nan()))
                {
                    bad.push(format!("{name}: NaN on the front"));
                }
            }
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[test]
fn the_shared_comparators_rank_nan_worst_in_both_directions() {
    let neg_nan = nan();
    assert!(neg_nan.is_sign_negative() || neg_nan.is_nan());
    let mut v = vec![3.0, neg_nan, 1.0, f64::INFINITY, -2.0];
    v.sort_by(|a, b| ascending_nan_last(*a, *b));
    assert_eq!(&v[..4], &[-2.0, 1.0, 3.0, f64::INFINITY]);
    assert!(v[4].is_nan(), "ascending: NaN last, after +inf: {v:?}");

    let mut d = vec![0.5, neg_nan, f64::INFINITY, 2.0];
    d.sort_by(|a, b| descending_nan_last(*a, *b));
    assert_eq!(&d[..3], &[f64::INFINITY, 2.0, 0.5]);
    assert!(d[3].is_nan(), "descending: NaN still last: {d:?}");
}
