//! What the optimizer ships, what a user can call, and how each solver does
//! on the functions we have (OPT-01, OPT-02, OPT-12, OPT-13).
//!
//! `CH-OPT-BENCH` was named in the spec and never written, so four
//! requirements sat `unmeasured`. Two of them can be answered today and two
//! cannot, and the difference is the point of this probe.
//!
//! **Reachable is measured by calling, not by counting files.** The crate has
//! twenty-nine solver modules; the question OPT-13 asks is how many a user can
//! invoke, which is a property of the dispatch rather than of the source tree.
//! Fourteen of them shipped unreachable until samyama-graph#1341, and a count
//! of modules would have reported twenty-nine throughout.
//!
//! **The statistics are real and the corpus is not CEC.** OPT-01 wants
//! CEC2017/CEC2022 with thirty seeds; `data/cec/` is empty and the loaders in
//! `benchmarks/cec_data.rs` say so themselves. What runs here is the ten
//! classic functions in `benchmarks/single_objective.rs`, with thirty seeds
//! each, and it is reported as what it is. Publishing these numbers against
//! OPT-01 would be a different benchmark wearing its name.
//!
//! ```text
//! cargo run --release --example optimization_survey -- --json opt.json
//! ```

use std::collections::BTreeMap;

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use samyama_optimization::algorithms::*;
use samyama_optimization::benchmarks::single_objective::so_suite;
use samyama_optimization::common::SolverConfig;

/// The names the Cypher surface is asked for. Written down rather than read
/// from the dispatch: a list derived from what the dispatch accepts would
/// report full coverage of whatever it already does, which is how fourteen
/// unreachable solvers went unnoticed.
const ASKED: &[&str] = &[
    "Rao1", "Rao2", "Rao3", "QORao", "TLBO", "ITLBO", "GOTLBO",
    "Jaya", "QOJaya", "SAMPJaya", "EHRJaya",
    "BMR", "BWR", "BMWR",
    "PSO", "DE", "GA", "SA", "ABC", "GSA", "HS", "FPA",
    "Firefly", "Cuckoo", "GWO", "Bat",
    // Multi-objective; reached with two cost properties.
    "NSGA2", "MOTLBO", "MOBMWR", "MORaoDE", "SAPHR",
];

/// The families OPT-13's H1 target names on top of what we ship.
const TARGET_FAMILIES: &[&str] = &["CMA-ES", "L-SHADE", "MOEA/D", "SPEA2", "Bayesian"];

const SEEDS: usize = 30;
const DIM: usize = 10;
const POP: usize = 30;
const ITERS: usize = 100;

fn median(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n == 0 {
        f64::NAN
    } else if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Which names the Cypher call actually runs.
///
/// A name that errors is not reachable. A name that is accepted and quietly
/// substituted would look reachable here, which is why samyama-graph#1341
/// removed the catch-all before this probe could mean anything.
fn reachable() -> BTreeMap<String, String> {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE (:OptVar {x: 0.0, c: 1.0, d: 2.0}), (:OptVar {x: 0.0, c: 2.0, d: 1.0})",
            &mut store,
            "default",
        )
        .expect("fixture");

    let mut out = BTreeMap::new();
    for name in ASKED {
        // Two cost properties for the multi-objective names, one otherwise:
        // asking a multi-objective solver a single-objective question routes
        // it elsewhere and would report it unreachable for the wrong reason.
        let multi = matches!(*name, "NSGA2" | "MOTLBO" | "MOBMWR" | "MORaoDE" | "SAPHR");
        let costs = if multi {
            "costProperties:['c','d']"
        } else {
            "costProperty:'c'"
        };
        let q = format!(
            "CALL algo.or.solve({{label:'OptVar', property:'x', algorithm:'{name}', {costs}, \
             maxIterations:5, populationSize:6}}) YIELD algorithm RETURN count(*)"
        );
        let verdict = match engine.execute_mut(&q, &mut store, "default") {
            Ok(_) => "reachable".to_string(),
            Err(e) => format!("refused: {e}"),
        };
        out.insert((*name).to_string(), verdict);
    }
    out
}

macro_rules! run_solver {
    ($name:expr, $seed:expr, $problem:expr, $( $label:literal => $ctor:expr ),* $(,)?) => {
        match $name {
            $( $label => Some($ctor.with_seed($seed).solve($problem).best_fitness), )*
            _ => None,
        }
    };
}

/// One solver, one function, one seed. `None` for a name this survey does not
/// drive directly -- the multi-objective ones, which do not produce a single
/// fitness to take statistics over.
fn one_run(name: &str, seed: u64, spec_idx: usize) -> Option<f64> {
    let spec = &so_suite(DIM)[spec_idx];
    let problem = spec.to_problem();
    let cfg = SolverConfig { population_size: POP, max_iterations: ITERS };
    // Rao takes its variant as an argument rather than having three types, so
    // it sits outside the macro rather than being left out of the survey --
    // which is what "not in the list" quietly means.
    let rao = |v| RaoSolver::new(SolverConfig { population_size: POP, max_iterations: ITERS }, v)
        .with_seed(seed).solve(&problem).best_fitness;
    match name {
        "Rao1" => return Some(rao(RaoVariant::Rao1)),
        "Rao2" => return Some(rao(RaoVariant::Rao2)),
        "Rao3" => return Some(rao(RaoVariant::Rao3)),
        "QORao" => {
            return Some(
                QORaoSolver::new(SolverConfig { population_size: POP, max_iterations: ITERS }, RaoVariant::Rao1)
                    .with_seed(seed)
                    .solve(&problem)
                    .best_fitness,
            )
        }
        _ => {}
    }
    run_solver!(name, seed, &problem,
        "Jaya" => JayaSolver::new(cfg),
        "QOJaya" => QOJayaSolver::new(cfg),
        "SAMPJaya" => SAMPJayaSolver::new(cfg),
        "EHRJaya" => EHRJayaSolver::new(cfg),
        "TLBO" => TLBOSolver::new(cfg),
        "ITLBO" => ITLBOSolver::new(cfg),
        "GOTLBO" => GOTLBOSolver::new(cfg),
        "BMR" => BMRSolver::new(cfg),
        "BWR" => BWRSolver::new(cfg),
        "BMWR" => BMWRSolver::new(cfg),
        "PSO" => PSOSolver::new(cfg),
        "DE" => DESolver::new(cfg),
        "GA" => GASolver::new(cfg),
        "SA" => SASolver::new(cfg),
        "ABC" => ABCSolver::new(cfg),
        "GSA" => GSASolver::new(cfg),
        "HS" => HSSolver::new(cfg),
        "FPA" => FPASolver::new(cfg),
        "Firefly" => FireflySolver::new(cfg),
        "Cuckoo" => CuckooSolver::new(cfg),
        "GWO" => GWOSolver::new(cfg),
        "Bat" => BatSolver::new(cfg),
    )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1))
        .cloned();
    let seeds: usize = args
        .iter()
        .position(|a| a == "--seeds")
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(SEEDS);

    let reach = reachable();
    let n_reachable = reach.values().filter(|v| *v == "reachable").count();
    eprintln!("{n_reachable} of {} asked names are reachable", ASKED.len());

    let suite = so_suite(DIM);
    let mut stats = Vec::new();
    let mut per_solver_medians: BTreeMap<String, Vec<f64>> = BTreeMap::new();

    for name in ASKED {
        if one_run(name, 0, 0).is_none() {
            continue; // not driven directly here; see the doc comment
        }
        for (fi, spec) in suite.iter().enumerate() {
            let mut fits: Vec<f64> = (0..seeds)
                .filter_map(|s| one_run(name, s as u64, fi))
                .collect();
            if fits.is_empty() {
                continue;
            }
            let n = fits.len() as f64;
            let mean = fits.iter().sum::<f64>() / n;
            let var = fits.iter().map(|f| (f - mean).powi(2)).sum::<f64>() / n;
            let best = fits.iter().cloned().fold(f64::INFINITY, f64::min);
            let worst = fits.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let med = median(&mut fits);
            per_solver_medians
                .entry((*name).to_string())
                .or_default()
                .push(med - spec.global_minimum);
            stats.push(serde_json::json!({
                "solver": name,
                "function": spec.name,
                "dim": DIM,
                "seeds": fits.len(),
                "best": best,
                "worst": worst,
                "mean": mean,
                "median": med,
                "std": var.sqrt(),
                "global_minimum": spec.global_minimum,
            }));
        }
        eprintln!("  {name} done");
    }

    // Friedman ranks across solvers, per function: the rank of each solver's
    // median on each function, averaged. Reported because OPT-02 asks for a
    // rank across algorithms; the significance test itself is not computed
    // here and is named as absent rather than implied by the ranks.
    let solvers: Vec<String> = per_solver_medians.keys().cloned().collect();
    let mut avg_rank: BTreeMap<String, f64> = BTreeMap::new();
    if !solvers.is_empty() {
        let n_funcs = per_solver_medians[&solvers[0]].len();
        for fi in 0..n_funcs {
            let mut col: Vec<(String, f64)> = solvers
                .iter()
                .map(|s| (s.clone(), per_solver_medians[s][fi]))
                .collect();
            col.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            for (rank, (s, _)) in col.iter().enumerate() {
                *avg_rank.entry(s.clone()).or_insert(0.0) += (rank + 1) as f64;
            }
        }
        for v in avg_rank.values_mut() {
            *v /= n_funcs as f64;
        }
    }

    let cec_present = std::path::Path::new("data/cec/cec2017").is_dir();

    let doc = serde_json::json!({
        "asked": ASKED,
        "reachable": reach,
        "reachable_count": n_reachable,
        "target_families_not_shipped": TARGET_FAMILIES,
        "corpus": {
            "name": "classic single-objective suite (benchmarks/single_objective.rs)",
            "functions": suite.iter().map(|s| s.name).collect::<Vec<_>>(),
            "dim": DIM, "seeds": seeds, "population": POP, "iterations": ITERS,
            "is_cec": false,
        },
        "cec_data_present": cec_present,
        "statistics": stats,
        "friedman_average_rank": avg_rank,
        "significance_tests_computed": false,
    });

    if let Some(path) = json_out {
        std::fs::write(&path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
        eprintln!("wrote {path}");
    } else {
        println!("{}", serde_json::to_string_pretty(&doc).unwrap());
    }
}
