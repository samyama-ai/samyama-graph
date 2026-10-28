//! A seeded solver must still search.
//!
//! NSGA-II, GA, Cuckoo, FPA and MOTLBO rebuilt their RNG from the seed inside
//! the helpers they call on every step (selection, crossover, mutation, Levy
//! flights, teacher choice), so each call drew the same numbers. Firefly gave
//! every firefly in every iteration the same stream. A seeded run therefore
//! stopped moving after its first step: seeded NSGA-II on ZDT1 returned the
//! same front after 150 iterations as after 2. An unseeded run drew from
//! entropy and was unaffected, which is why the reproducibility tests, which
//! compare one seeded run with another, could not see it.
//!
//! Each test runs the solver with a seed for a few iterations and for many,
//! and requires the longer run to be better. The same seed must still give
//! the same answer.

use ndarray::Array1;
use samyama_optimization::algorithms::*;
use samyama_optimization::common::*;
use samyama_optimization::moo::igd;

const SEED: u64 = 11;
const POPULATION: usize = 30;
const FEW: usize = 2;
const MANY: usize = 150;
const SPHERE_DIM: usize = 5;
const SPHERE_BOUND: f64 = 5.0;
const ZDT_DIM: usize = 10;
/// Points on ZDT1's known front used to measure IGD.
const ZDT_FRONT_POINTS: usize = 100;

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
        lower: Array1::from_elem(SPHERE_DIM, -SPHERE_BOUND),
        upper: Array1::from_elem(SPHERE_DIM, SPHERE_BOUND),
    }
}

/// ZDT1, minimised: its front is `f2 = 1 - sqrt(f1)` where the tail is 0.
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

fn zdt1_front() -> Vec<Vec<f64>> {
    (0..ZDT_FRONT_POINTS)
        .map(|i| {
            let f1 = i as f64 / (ZDT_FRONT_POINTS - 1) as f64;
            vec![f1, 1.0 - f1.sqrt()]
        })
        .collect()
}

/// Run a seeded single-objective solver for `FEW` and `MANY` iterations.
fn single(solve: impl Fn(SolverConfig) -> OptimizationResult) -> (f64, f64) {
    (
        solve(config(FEW)).best_fitness,
        solve(config(MANY)).best_fitness,
    )
}

/// Run a seeded multi-objective solver for `FEW` and `MANY` iterations and
/// measure each front's IGD to ZDT1's front.
fn multi(solve: impl Fn(SolverConfig) -> MultiObjectiveResult) -> (f64, f64) {
    let front = zdt1_front();
    let igd_of = |r: MultiObjectiveResult| {
        let points: Vec<Vec<f64>> = r.pareto_front.into_iter().map(|i| i.fitness).collect();
        igd(&points, &front)
    };
    (igd_of(solve(config(FEW))), igd_of(solve(config(MANY))))
}

fn assert_improves(name: &str, (few, many): (f64, f64)) {
    assert!(
        many < few,
        "seeded {name}: {MANY} iterations gave {many}, no better than {FEW} gave {few}"
    );
}

#[test]
fn seeded_nsga2_searches() {
    assert_improves(
        "NSGA-II",
        multi(|c| NSGA2Solver::new(c).with_seed(SEED).solve(&Zdt1)),
    );
}

#[test]
fn seeded_motlbo_searches() {
    assert_improves(
        "MOTLBO",
        multi(|c| MOTLBOSolver::new(c).with_seed(SEED).solve(&Zdt1)),
    );
}

#[test]
fn seeded_ga_searches() {
    assert_improves(
        "GA",
        single(|c| GASolver::new(c).with_seed(SEED).solve(&sphere())),
    );
}

#[test]
fn seeded_cuckoo_searches() {
    assert_improves(
        "Cuckoo",
        single(|c| CuckooSolver::new(c).with_seed(SEED).solve(&sphere())),
    );
}

#[test]
fn seeded_fpa_searches() {
    assert_improves(
        "FPA",
        single(|c| FPASolver::new(c).with_seed(SEED).solve(&sphere())),
    );
}

#[test]
fn seeded_firefly_searches() {
    assert_improves(
        "Firefly",
        single(|c| FireflySolver::new(c).with_seed(SEED).solve(&sphere())),
    );
}

#[test]
fn the_same_seed_still_gives_the_same_answer() {
    let ga = || GASolver::new(config(MANY)).with_seed(SEED).solve(&sphere());
    assert_eq!(ga().best_fitness, ga().best_fitness);
    let nsga2 = || {
        NSGA2Solver::new(config(MANY))
            .with_seed(SEED)
            .solve(&Zdt1)
            .pareto_front
            .into_iter()
            .map(|i| i.fitness)
            .collect::<Vec<_>>()
    };
    assert_eq!(nsga2(), nsga2());
}
