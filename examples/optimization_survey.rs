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
use samyama_optimization::stats::{friedman, holm_adjust, wilcoxon_signed_rank, WilcoxonMethod};

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

    // Friedman across solvers, blocked by function, and Wilcoxon signed-rank
    // on every pair (OPT-02). This used to be ranks alone, with the tests
    // named as absent so the ordering could not be read as a result.
    //
    // Two things changed besides adding the tests. Ties now share the average
    // of the positions they span: the old code sorted and assigned
    // `position + 1`, so two solvers that both reached the global minimum were
    // separated by float comparison order. And the pairwise table carries a
    // Holm-adjusted column, because 31 solvers is 465 comparisons and a raw
    // table at 0.05 rejects about 23 of them from noise alone.
    let solvers: Vec<String> = per_solver_medians.keys().cloned().collect();
    let mut avg_rank: BTreeMap<String, f64> = BTreeMap::new();
    let mut friedman_json = serde_json::Value::Null;
    let mut pairwise = Vec::new();

    if solvers.len() >= 2 {
        let n_funcs = per_solver_medians[&solvers[0]].len();
        // Every solver is measured on every function, so a ragged row here is
        // a bug upstream rather than a missing datum to skip over.
        let complete = solvers
            .iter()
            .all(|s| per_solver_medians[s].len() == n_funcs);

        if complete && n_funcs >= 2 {
            let blocks: Vec<Vec<f64>> = (0..n_funcs)
                .map(|fi| solvers.iter().map(|s| per_solver_medians[s][fi]).collect())
                .collect();

            if let Some(f) = friedman(&blocks) {
                for (s, r) in solvers.iter().zip(&f.average_ranks) {
                    avg_rank.insert(s.clone(), *r);
                }
                friedman_json = serde_json::json!({
                    "k_solvers": f.k,
                    "n_functions": f.n,
                    "chi_square": f.chi_square,
                    "chi_square_df": f.chi_square_df,
                    "chi_square_p": f.chi_square_p,
                    "iman_davenport_f": f.iman_davenport_f,
                    "f_df1": f.f_df1,
                    "f_df2": f.f_df2,
                    "iman_davenport_p": f.iman_davenport_p,
                    "note": "Tie-corrected chi-square with the Iman-Davenport F \
                             beside it. Both are approximations and they disagree \
                             at this shape, so neither is presented alone.",
                });
            }

            // Pairwise, then Holm over the whole family at once -- adjusting
            // within a subset would give a different answer for the same pair
            // depending on which table it was printed in.
            let mut raw_p = Vec::new();
            for i in 0..solvers.len() {
                for j in (i + 1)..solvers.len() {
                    let a = &per_solver_medians[&solvers[i]];
                    let b = &per_solver_medians[&solvers[j]];
                    match wilcoxon_signed_rank(a, b) {
                        Some(w) => {
                            raw_p.push(w.p_two_sided);
                            pairwise.push(serde_json::json!({
                                "a": solvers[i], "b": solvers[j],
                                "n": w.n,
                                "w_plus": w.w_plus, "w_minus": w.w_minus,
                                "statistic": w.statistic,
                                "p_two_sided": w.p_two_sided,
                                "method": match w.method {
                                    WilcoxonMethod::Exact => "exact",
                                    WilcoxonMethod::Normal => "normal_approximation",
                                },
                            }));
                        }
                        // Identical on every function. Recorded as untestable
                        // rather than as p = 1: "no difference was detected"
                        // and "there was nothing to test" are different facts.
                        None => pairwise.push(serde_json::json!({
                            "a": solvers[i], "b": solvers[j],
                            "n": 0,
                            "p_two_sided": serde_json::Value::Null,
                            "method": "not_applicable",
                            "why": "every paired difference is zero",
                        })),
                    }
                }
            }
            let adjusted = holm_adjust(&raw_p);
            let mut k = 0;
            for entry in pairwise.iter_mut() {
                if entry["method"] == "not_applicable" {
                    entry["p_holm"] = serde_json::Value::Null;
                } else {
                    entry["p_holm"] = serde_json::json!(adjusted[k]);
                    k += 1;
                }
            }
        }
    }

    let tests_computed = !friedman_json.is_null() && !pairwise.is_empty();

    // Whether the pairwise half can reject anything *at all* on this corpus,
    // which is a property of the sizes and not of the results.
    //
    // With no ties the exact two-sided Wilcoxon p-value is a count over 2^n
    // sign assignments, so the smallest one obtainable from n functions is
    // 2/2^n -- reached only when one solver wins on every single function.
    // Holm's first threshold is alpha/m over m comparisons. If the smallest
    // achievable p exceeds it, no pair can be declared different however the
    // data comes out, and every "not significant" in the table above is a
    // statement about the corpus rather than about the solvers.
    let n_funcs_used = per_solver_medians
        .values()
        .next()
        .map(|v| v.len())
        .unwrap_or(0);
    let m = pairwise.len().max(1) as f64;
    let smallest_achievable_p = if n_funcs_used > 0 && n_funcs_used < 63 {
        2.0 / 2f64.powi(n_funcs_used as i32)
    } else {
        f64::NAN
    };
    let holm_first_threshold = 0.05 / m;
    let power_json = serde_json::json!({
        "alpha": 0.05,
        "comparisons": pairwise.len(),
        "functions": n_funcs_used,
        "smallest_achievable_exact_p": smallest_achievable_p,
        "holm_first_threshold": holm_first_threshold,
        "pairwise_can_reject": smallest_achievable_p <= holm_first_threshold,
        "note": "The exact two-sided Wilcoxon p-value on n blocks is a count over \
                 2^n sign assignments, so 2/2^n is the smallest value obtainable \
                 -- the case where one solver wins on every function. When that \
                 exceeds Holm's first threshold the pairwise comparison cannot \
                 reject for any data, and reporting 'no significant differences' \
                 from it would describe the corpus while appearing to describe \
                 the solvers. More functions is the fix; more seeds is not, \
                 because the seeds are inside each median and n here counts \
                 functions.",
    });

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
        "friedman_test": friedman_json,
        "wilcoxon_pairwise": pairwise,
        "wilcoxon_pairwise_count": pairwise.len(),
        "multiple_comparison_correction": "holm",
        "pairwise_power": power_json,
        "significance_tests_computed": tests_computed,
    });

    if let Some(path) = json_out {
        std::fs::write(&path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
        eprintln!("wrote {path}");
    } else {
        println!("{}", serde_json::to_string_pretty(&doc).unwrap());
    }
}
