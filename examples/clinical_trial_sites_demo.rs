//! Paper 8 problem 2: clinical-trial site selection.
//!
//! Loads the public clinicaltrials snapshot (AACT bulk / ClinicalTrials.gov)
//! and poses:
//!
//!   maximize    Σ_{s ∈ SELECTED} trial_count(s)
//!               + diversity_weight × |distinct countries in SELECTED|
//!   subject to  |SELECTED| ≤ k
//!
//! The graph is the data source: candidate sites, per-site trial counts, and
//! per-site countries are all fetched via Cypher *once at startup*. Per-
//! evaluation cost is then pure-Rust arithmetic over the precomputed maps —
//! microseconds, not 600 ms. For sub-problems where the relevant aggregates
//! depend on the candidate vector dynamically (e.g. problems 4, 5, 6), the
//! per-eval Cypher path via CypherProblem is the right choice. Both patterns
//! coexist in the paper.
//!
//! Usage:
//!   cargo run --release --example clinical_trial_sites_demo -- \
//!       [--snapshot PATH] [--candidates 50] [--k 5] [--seeds 3] \
//!       [--diversity-weight 5.0]

use ndarray::Array1;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;
use samyama::query::executor::record::Value;
use samyama_optimization::algorithms::{
    BMWRSolver, EHRJayaSolver, JayaSolver, RaoSolver, RaoVariant, SAMPJayaSolver,
};
use samyama_optimization::common::{OptimizationResult, Problem, SolverConfig};
use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
struct Args {
    snapshot: PathBuf,
    candidates: usize,
    k: usize,
    seeds: usize,
    diversity_weight: f64,
    out: PathBuf,
    export_spec: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            snapshot: PathBuf::from("../clinicaltrials-kg/data/clinical-trials.sgsnap"),
            candidates: 50,
            k: 5,
            seeds: 3,
            diversity_weight: 5.0,
            out: PathBuf::from("/tmp/p8-clinical-sites"),
            export_spec: None,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--snapshot" => { a.snapshot = PathBuf::from(&argv[i + 1]); i += 2; }
            "--candidates" => { a.candidates = argv[i + 1].parse().unwrap(); i += 2; }
            "--k" => { a.k = argv[i + 1].parse().unwrap(); i += 2; }
            "--seeds" => { a.seeds = argv[i + 1].parse().unwrap(); i += 2; }
            "--diversity-weight" => { a.diversity_weight = argv[i + 1].parse().unwrap(); i += 2; }
            "--out" => { a.out = PathBuf::from(&argv[i + 1]); i += 2; }
            "--export-spec" => { a.export_spec = Some(PathBuf::from(&argv[i + 1])); i += 2; }
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
}

/// Problem with KG-derived data pre-materialized into Rust vectors.
struct SiteSelectionProblem {
    dim: usize,
    trial_counts: Vec<f64>,
    countries: Vec<String>,
    diversity_weight: f64,
    k: usize,
    eval_count: std::sync::atomic::AtomicU64,
}

impl Problem for SiteSelectionProblem {
    fn dim(&self) -> usize { self.dim }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim), Array1::ones(self.dim))
    }
    fn objective(&self, x: &Array1<f64>) -> f64 {
        self.eval_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut idxs: Vec<(usize, f64)> = x.iter().enumerate()
            .filter(|(_, &v)| v > 0.5).map(|(i, &v)| (i, v)).collect();
        idxs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let take = self.k.min(idxs.len());
        let mut trials = 0.0;
        let mut distinct: HashSet<&str> = HashSet::new();
        for (i, _) in idxs.iter().take(take) {
            trials += self.trial_counts[*i];
            distinct.insert(self.countries[*i].as_str());
        }
        // Minimize negative of (trials + diversity_weight × |distinct|).
        -(trials + self.diversity_weight * distinct.len() as f64)
    }
}

fn main() {
    let a = parse_args();
    std::fs::create_dir_all(&a.out).unwrap();

    eprintln!("loading {} ...", a.snapshot.display());
    let mut store = GraphStore::new();
    let t0 = Instant::now();
    let f = File::open(&a.snapshot).expect("snapshot open");
    let stats = samyama::snapshot::import_tenant(&mut store, f).expect("import");
    let load_ms = t0.elapsed().as_millis();
    eprintln!("loaded {} nodes, {} edges in {} ms",
        stats.node_count, stats.edge_count, load_ms);

    let engine = QueryEngine::new();

    // ONE Cypher query at startup: top-N sites + their host-trial-count + country.
    let cand_q = format!(
        "MATCH (t:ClinicalTrial)-[:CONDUCTED_AT]->(s:Site) \
         WITH s, count(t) AS deg \
         ORDER BY deg DESC \
         LIMIT {} \
         RETURN s.facility AS facility, s.country AS country, deg AS trials",
        a.candidates);
    let t1 = Instant::now();
    let batch = engine.execute(&cand_q, &store).expect("candidate query");
    let candq_ms = t1.elapsed().as_millis();
    eprintln!("candidate query: {} rows in {} ms", batch.records.len(), candq_ms);

    let mut facilities = Vec::new();
    let mut countries = Vec::new();
    let mut trial_counts = Vec::new();
    for r in &batch.records {
        let facility = match r.get("facility") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            _ => continue,
        };
        let country = match r.get("country") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            _ => String::from("Unknown"),
        };
        let trials = match r.get("trials") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n as f64,
            Some(Value::Property(PropertyValue::Float(f))) => *f,
            _ => 0.0,
        };
        facilities.push(facility);
        countries.push(country);
        trial_counts.push(trials);
    }
    let dim = facilities.len();
    let distinct_countries: HashSet<&str> = countries.iter().map(|s| s.as_str()).collect();
    eprintln!("dim = {}, distinct countries in pool = {}, total trials in pool = {}",
        dim, distinct_countries.len(), trial_counts.iter().sum::<f64>());
    assert!(dim > 0, "no candidate sites returned");

    // Optional: export the spec for an OR-tools baseline.
    if let Some(path) = &a.export_spec {
        use std::io::Write as _;
        let mut f = File::create(path).unwrap();
        writeln!(f, r#"{{"k": {}, "diversity_weight": {}, "facilities": [{}], "countries": [{}], "trial_counts": [{}]}}"#,
            a.k, a.diversity_weight,
            facilities.iter().map(|s| format!(r#""{}""#, s.replace('"', r#"\""#))).collect::<Vec<_>>().join(","),
            countries.iter().map(|s| format!(r#""{}""#, s.replace('"', r#"\""#))).collect::<Vec<_>>().join(","),
            trial_counts.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(",")
        ).unwrap();
        eprintln!("spec -> {}", path.display());
    }

    let problem = Arc::new(SiteSelectionProblem {
        dim,
        trial_counts: trial_counts.clone(),
        countries: countries.clone(),
        diversity_weight: a.diversity_weight,
        k: a.k,
        eval_count: std::sync::atomic::AtomicU64::new(0),
    });

    let solvers: Vec<(&str, fn(SolverConfig, &SiteSelectionProblem) -> OptimizationResult)> = vec![
        ("BMWR",      |c, p| BMWRSolver::new(c).solve(p)),
        ("Jaya",      |c, p| JayaSolver::new(c).solve(p)),
        ("SAMP-Jaya", |c, p| SAMPJayaSolver::new(c).solve(p)),
        ("EHR-Jaya",  |c, p| EHRJayaSolver::new(c).solve(p)),
        ("Rao-1",     |c, p| RaoSolver::new(c, RaoVariant::Rao1).solve(p)),
    ];
    let cfg = SolverConfig { population_size: 30, max_iterations: 200 };

    let csv_path = a.out.join("results.csv");
    use std::io::Write;
    let mut csv = File::create(&csv_path).unwrap();
    writeln!(csv, "solver,seed,best_fitness,trial_count,distinct_countries,wall_ms,evals").unwrap();

    println!("\n=== Trial-site selection (k <= {}, candidates={}, diversity_weight={}) ===",
        a.k, a.candidates, a.diversity_weight);
    println!("{:<12} {:>5} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "solver", "seed", "fitness", "trials", "countries", "wall_ms", "evals");

    for (name, run) in &solvers {
        for seed in 0..a.seeds {
            problem.eval_count.store(0, std::sync::atomic::Ordering::Relaxed);
            let t0 = Instant::now();
            let r = run(cfg.clone(), &problem);
            let wall = t0.elapsed().as_millis();
            let evals = problem.eval_count.load(std::sync::atomic::Ordering::Relaxed);
            // Recover trial-count and country count from best vec.
            let mut idxs: Vec<(usize, f64)> = r.best_variables.iter().enumerate()
                .filter(|(_, &v)| v > 0.5).map(|(i, &v)| (i, v)).collect();
            idxs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let take = a.k.min(idxs.len());
            let mut trials = 0.0;
            let mut distinct: HashSet<&str> = HashSet::new();
            for (i, _) in idxs.iter().take(take) {
                trials += trial_counts[*i];
                distinct.insert(countries[*i].as_str());
            }
            println!("{:<12} {:>5} {:>12.2} {:>10.0} {:>10} {:>10} {:>10}",
                name, seed, r.best_fitness, trials, distinct.len(), wall, evals);
            writeln!(csv, "{},{},{:.4},{},{},{},{}",
                name, seed, r.best_fitness, trials, distinct.len(), wall, evals).unwrap();
        }
    }
    eprintln!("\nresults -> {}", csv_path.display());

    let manifest = format!(
        r#"{{"args": {{"snapshot": "{}", "candidates": {}, "k": {}, "seeds": {}, "diversity_weight": {}}}, "nodes_loaded": {}, "edges_loaded": {}, "load_ms": {}, "candidate_query_ms": {}, "dim": {}, "distinct_countries_pool": {}, "total_trials_pool": {}}}
"#,
        a.snapshot.display(), a.candidates, a.k, a.seeds, a.diversity_weight,
        stats.node_count, stats.edge_count, load_ms, candq_ms, dim,
        distinct_countries.len(), trial_counts.iter().sum::<f64>()
    );
    std::fs::write(a.out.join("manifest.json"), manifest).unwrap();
}
