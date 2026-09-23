//! Is every solver deterministic given a seed? (OPT-08)
//!
//! OPT-08 asks for runs that are "deterministic given a seed", with the seed recorded in
//! the result metadata. Every solver takes `with_seed`, and nothing checked that the seed
//! actually determines the answer — a single `thread_rng()` left anywhere inside an
//! operator makes the promise false while the API still looks right.
//!
//! So: run each solver twice with the same seed on the same problem and compare the
//! results bit for bit, then run it again with a different seed and require the answer to
//! *change* — a solver that ignores its seed entirely would otherwise pass the first test
//! by being constant.
//!
//! Single-objective solvers only. The multi-objective ones return a front rather than a
//! scalar and want their own comparison; they are listed as not covered rather than
//! quietly skipped.
//!
//!   cargo run --release --example optimization_determinism -- --json out.json

use ndarray::Array1;
use samyama_optimization::algorithms::{
    ABCSolver, BMRSolver, BMWRSolver, BWRSolver, BatSolver, CuckooSolver, DESolver,
    EHRJayaSolver, FPASolver, FireflySolver, GASolver, GOTLBOSolver, GSASolver, GWOSolver,
    HSSolver, ITLBOSolver, JayaSolver, PSOSolver, QOJayaSolver, QORaoSolver, RaoSolver,
    RaoVariant, SAMPJayaSolver, SAPHRSolver, SASolver, TLBOSolver,
};
use samyama_optimization::common::{OptimizationResult, Problem, SolverConfig};

/// A **shifted** sphere, 10-D: no plateaus, so any difference between two runs is the
/// solver's randomness and not the objective's.
///
/// Shifted deliberately. With the optimum at the origin and symmetric bounds, the
/// quasi-opposite of any point is its reflection through the midpoint — which *is* the
/// optimum — so opposition-based solvers land on the exact answer from any seed and look
/// seed-insensitive. `qo_rao` did exactly that on the unshifted version, and the fixture,
/// not the solver, was wrong.
struct Sphere {
    dim: usize,
}

/// Where the optimum sits. Irrational-ish and asymmetric so no reflection finds it.
const SHIFT: f64 = 1.2345678901;

impl Problem for Sphere {
    fn objective(&self, x: &Array1<f64>) -> f64 {
        x.iter().map(|v| (v - SHIFT) * (v - SHIFT)).sum()
    }
    fn dim(&self) -> usize {
        self.dim
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::from_elem(self.dim, -5.12), Array1::from_elem(self.dim, 5.12))
    }
}

fn same(a: &OptimizationResult, b: &OptimizationResult) -> bool {
    a.best_fitness.to_bits() == b.best_fitness.to_bits()
        && a.best_variables.len() == b.best_variables.len()
        && a.best_variables
            .iter()
            .zip(b.best_variables.iter())
            .all(|(x, y)| x.to_bits() == y.to_bits())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args.iter().position(|a| a == "--json").map(|i| args[i + 1].clone());

    let cfg = SolverConfig { population_size: 30, max_iterations: 40 };
    let problem = Sphere { dim: 10 };
    const SEED: u64 = 42;
    const OTHER: u64 = 43;

    // (name, run-with-seed). One closure per solver because `solve` is an inherent method
    // on each concrete type, not a trait object.
    type Run = Box<dyn Fn(u64) -> OptimizationResult>;
    let mut solvers: Vec<(&str, Run)> = Vec::new();
    macro_rules! add {
        ($name:literal, $ctor:expr) => {{
            let cfg = cfg.clone();
            solvers.push(($name, Box::new(move |s: u64| {
                let mk = $ctor;
                mk(cfg.clone(), s)
            })));
        }};
    }

    add!("jaya", |c, s| JayaSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("tlbo", |c, s| TLBOSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("bmr", |c, s| BMRSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("bwr", |c, s| BWRSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("bmwr", |c, s| BMWRSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("qojaya", |c, s| QOJayaSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("itlbo", |c, s| ITLBOSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("gotlbo", |c, s| GOTLBOSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("samp_jaya", |c, s| SAMPJayaSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("ehrjaya", |c, s| EHRJayaSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("saphr", |c, s| SAPHRSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("pso", |c, s| PSOSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("de", |c, s| DESolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("ga", |c, s| GASolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("sa", |c, s| SASolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("firefly", |c, s| FireflySolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("cuckoo", |c, s| CuckooSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("gwo", |c, s| GWOSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("bat", |c, s| BatSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("abc", |c, s| ABCSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("gsa", |c, s| GSASolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("hs", |c, s| HSSolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("fpa", |c, s| FPASolver::new(c).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("rao1", |c, s| RaoSolver::new(c, RaoVariant::Rao1).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("rao2", |c, s| RaoSolver::new(c, RaoVariant::Rao2).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("rao3", |c, s| RaoSolver::new(c, RaoVariant::Rao3).with_seed(s).solve(&Sphere { dim: 10 }));
    add!("qo_rao", |c, s| QORaoSolver::new(c, RaoVariant::Rao1).with_seed(s).solve(&Sphere { dim: 10 }));

    let _ = &problem;
    let mut rows = Vec::new();
    let (mut repeatable, mut seed_sensitive) = (0, 0);
    for (name, run) in &solvers {
        let a = run(SEED);
        let b = run(SEED);
        let c = run(OTHER);
        let rep = same(&a, &b);
        // A solver that ignores the seed is repeatable and useless; require the answer to
        // move when the seed does.
        let sens = !same(&a, &c);
        repeatable += rep as usize;
        seed_sensitive += sens as usize;
        rows.push(format!(
            "{{\"solver\":\"{name}\",\"repeatable\":{rep},\"seed_sensitive\":{sens},\
             \"fitness_seed_{SEED}\":{:.12},\"fitness_seed_{OTHER}\":{:.12}}}",
            a.best_fitness, c.best_fitness
        ));
        println!(
            "{name:12} repeatable={} seed-sensitive={}  f={:.6}",
            if rep { "yes" } else { "NO " },
            if sens { "yes" } else { "NO " },
            a.best_fitness
        );
    }

    let n = solvers.len();
    println!("\n{repeatable} of {n} repeatable with a fixed seed");
    println!("{seed_sensitive} of {n} change answer when the seed changes");
    println!("multi-objective solvers (NSGA2, MOTLBO, MOBMWR, MORaoDE) return a front and are not covered here");

    if let Some(path) = json_out {
        let body = format!(
            "{{\"problem\":\"sphere-10d\",\"population\":{},\"iterations\":{},\
             \"seeds\":[{SEED},{OTHER}],\"solvers\":{n},\"repeatable\":{repeatable},\
             \"seed_sensitive\":{seed_sensitive},\"rows\":[{}]}}",
            cfg.population_size, cfg.max_iterations, rows.join(",")
        );
        std::fs::write(&path, body).expect("write json");
        println!("wrote {path}");
    }
    if repeatable < n || seed_sensitive < n {
        std::process::exit(1);
    }
}
