//! Seeded runs must be re-derivable (#455).
//!
//! The property under test is not "seeding exists" but "the same seed gives
//! the same answer, and a different seed gives a different one". The first
//! half alone would pass for a solver that ignored its input entirely.
//!
//! For the parallel solvers this is the part that could quietly not hold:
//! seeding only the outer RNG and letting rayon workers draw from
//! `thread_rng()` yields a run that looks reproducible in a single-threaded
//! test and is not. Each element's stream is therefore derived from
//! (seed, iteration, index), which does not depend on how rayon schedules
//! the work.

use ndarray::{array, Array1};
use samyama_optimization::algorithms::*;
use samyama_optimization::common::*;

struct SphereProblem;
impl Problem for SphereProblem {
    fn objective(&self, v: &Array1<f64>) -> f64 { v.iter().map(|&x| x * x).sum() }
    fn dim(&self) -> usize { 2 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) { (array![-10.0, -10.0], array![10.0, 10.0]) }
}

fn cfg() -> SolverConfig {
    SolverConfig { population_size: 30, max_iterations: 60 }
}

/// Same seed twice, then a different seed. Returns (a, b, c).
macro_rules! three_runs {
    ($build:expr) => {{
        let a = { let s = $build; s.with_seed(12345).solve(&SphereProblem) };
        let b = { let s = $build; s.with_seed(12345).solve(&SphereProblem) };
        let c = { let s = $build; s.with_seed(99999).solve(&SphereProblem) };
        (a, b, c)
    }};
}

fn assert_reproducible(name: &str, a: OptimizationResult, b: OptimizationResult, c: OptimizationResult) {
    assert_eq!(
        a.best_fitness.to_bits(),
        b.best_fitness.to_bits(),
        "{name}: same seed produced different results ({} vs {})",
        a.best_fitness, b.best_fitness
    );
    assert_eq!(
        a.best_variables, b.best_variables,
        "{name}: same seed produced a different solution vector"
    );
    // The full convergence history, not just the endpoint -- two runs can
    // agree on the best value while having taken different paths.
    assert_eq!(
        a.history.iter().map(|f| f.to_bits()).collect::<Vec<_>>(),
        b.history.iter().map(|f| f.to_bits()).collect::<Vec<_>>(),
        "{name}: same seed produced a different convergence history"
    );
    // A solver that ignored the seed would pass the checks above trivially, so
    // a different seed must produce a different run. Compared on the whole
    // trajectory rather than the endpoint: a solver that converges to the exact
    // optimum (QORao reaches 0.0 on Sphere under any seed) would otherwise look
    // like it was ignoring its input when it was not.
    let bits = |r: &OptimizationResult| {
        (r.best_fitness.to_bits(), r.history.iter().map(|f| f.to_bits()).collect::<Vec<_>>())
    };
    assert_ne!(
        bits(&a),
        bits(&c),
        "{name}: a different seed produced an identical run -- the seed is not being used"
    );
}

macro_rules! repro_test {
    ($fn_name:ident, $name:literal, $build:expr) => {
        #[test]
        fn $fn_name() {
            let (a, b, c) = three_runs!($build);
            assert_reproducible($name, a, b, c);
        }
    };
}

// Single-threaded solvers
repro_test!(hs_is_reproducible, "HS", HSSolver::new(cfg()));
repro_test!(gsa_is_reproducible, "GSA", GSASolver::new(cfg()));
repro_test!(ga_is_reproducible, "GA", GASolver::new(cfg()));
repro_test!(sa_is_reproducible, "SA", SASolver::new(cfg()));
repro_test!(bat_is_reproducible, "Bat", BatSolver::new(cfg()));
repro_test!(abc_is_reproducible, "ABC", ABCSolver::new(cfg()));
repro_test!(fpa_is_reproducible, "FPA", FPASolver::new(cfg()));
repro_test!(cuckoo_is_reproducible, "Cuckoo", CuckooSolver::new(cfg()));
repro_test!(firefly_is_reproducible, "Firefly", FireflySolver::new(cfg()));
repro_test!(gwo_is_reproducible, "GWO", GWOSolver::new(cfg()));

// Solvers whose population update runs under rayon -- the cases where naive
// seeding would look right and not be.
repro_test!(rao_is_reproducible, "Rao3", RaoSolver::new(cfg(), RaoVariant::Rao3));
repro_test!(de_is_reproducible, "DE", DESolver::new(cfg()));
repro_test!(jaya_is_reproducible, "Jaya", JayaSolver::new(cfg()));
repro_test!(qojaya_is_reproducible, "QOJaya", QOJayaSolver::new(cfg()));
repro_test!(tlbo_is_reproducible, "TLBO", TLBOSolver::new(cfg()));
repro_test!(itlbo_is_reproducible, "ITLBO", ITLBOSolver::new(cfg()));
repro_test!(gotlbo_is_reproducible, "GOTLBO", GOTLBOSolver::new(cfg()));
repro_test!(pso_is_reproducible, "PSO", PSOSolver::new(cfg()));
repro_test!(bmr_is_reproducible, "BMR", BMRSolver::new(cfg()));
repro_test!(bwr_is_reproducible, "BWR", BWRSolver::new(cfg()));
repro_test!(bmwr_is_reproducible, "BMWR", BMWRSolver::new(cfg()));
repro_test!(samp_jaya_is_reproducible, "SAMPJaya", SAMPJayaSolver::new(cfg()));
repro_test!(ehrjaya_is_reproducible, "EHRJaya", EHRJayaSolver::new(cfg()));
repro_test!(qorao_is_reproducible, "QORao", QORaoSolver::new(cfg(), RaoVariant::Rao1));

#[test]
fn an_unseeded_solver_still_varies() {
    // Seeding is opt-in. Without it the behaviour must be unchanged, or every
    // existing caller has silently acquired a fixed trajectory.
    let a = HSSolver::new(cfg()).solve(&SphereProblem);
    let b = HSSolver::new(cfg()).solve(&SphereProblem);
    assert_ne!(
        a.best_fitness.to_bits(),
        b.best_fitness.to_bits(),
        "an unseeded solver became deterministic; seeding must stay opt-in"
    );
}

#[test]
fn reproducibility_holds_across_thread_counts() {
    // The point of deriving each element's stream from (seed, iteration,
    // index): the answer must not depend on how rayon schedules the work.
    // Running the same seed under a 1-thread and a many-thread pool is the
    // direct test of that.
    let single = rayon::ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let many = rayon::ThreadPoolBuilder::new().num_threads(8).build().unwrap();

    let one = single.install(|| DESolver::new(cfg()).with_seed(4242).solve(&SphereProblem));
    let eight = many.install(|| DESolver::new(cfg()).with_seed(4242).solve(&SphereProblem));

    assert_eq!(
        one.best_fitness.to_bits(),
        eight.best_fitness.to_bits(),
        "DE gave {} on 1 thread and {} on 8 -- the result depends on scheduling",
        one.best_fitness, eight.best_fitness
    );
}
