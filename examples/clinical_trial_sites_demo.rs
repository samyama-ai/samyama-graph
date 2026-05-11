//! Paper 8 problem 2: clinical-trial site selection.
//!
//! Loads the public clinicaltrials snapshot (AACT / ClinicalTrials.gov bulk
//! data) and poses:
//!
//!   maximize    | { t in TRIALS : ∃ s in SELECTED, (t)-[:CONDUCTED_AT]->(s) } |
//!               + diversity_weight * |distinct countries in SELECTED|
//!   subject to  |SELECTED| <= k
//!
//! evaluated against samyama-graph via Cypher per candidate vector.
//!
//! Usage:
//!   cargo run --release --example clinical_trial_sites_demo -- \
//!       [--snapshot PATH] [--candidates 100] [--k 10] [--seeds 3] \
//!       [--diversity-weight 5.0]
//!
//! Default snapshot: ../clinicaltrials-kg/data/clinical-trials.sgsnap

use ndarray::Array1;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::optimization::CypherProblem;
use samyama::query::QueryEngine;
use samyama::query::executor::record::Value;
use samyama_optimization::algorithms::{
    BMWRSolver, EHRJayaSolver, JayaSolver, RaoSolver, RaoVariant, SAMPJayaSolver,
};
use samyama_optimization::common::{Problem, SolverConfig};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Instant;

#[derive(Debug)]
struct Args {
    snapshot: PathBuf,
    candidates: usize,
    k: usize,
    seeds: usize,
    diversity_weight: f64,
    out: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            snapshot: PathBuf::from("../clinicaltrials-kg/data/clinical-trials.sgsnap"),
            candidates: 100,
            k: 10,
            seeds: 3,
            diversity_weight: 5.0,
            out: PathBuf::from("/tmp/p8-clinical-sites"),
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
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
}

fn make_subs(
    x: &Array1<f64>,
    facilities: &[String],
    countries: &[String],
    diversity_weight: f64,
    k: usize,
) -> Vec<(String, String)> {
    let mut idxs: Vec<(usize, f64)> = x.iter().enumerate()
        .filter(|(_, &v)| v > 0.5).map(|(i, &v)| (i, v)).collect();
    idxs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let take = k.min(idxs.len());
    let selected: Vec<usize> = idxs.iter().take(take).map(|(i, _)| *i).collect();
    let list = selected.iter()
        .map(|i| format!("'{}'", facilities[*i].replace('\'', "\\'")))
        .collect::<Vec<_>>().join(",");
    let list = if list.is_empty() { "'__NONE__'".to_string() } else { list };
    let mut distinct: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for i in &selected { distinct.insert(countries[*i].as_str()); }
    // diversity_term ENTERS the objective as -count, so we *subtract* diversity_weight * |distinct|.
    let diversity_term = -diversity_weight * distinct.len() as f64;
    vec![
        ("$selected".to_string(), list),
        ("$diversity_term".to_string(), format!("{:.6}", diversity_term)),
    ]
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

    let engine = Arc::new(QueryEngine::new());

    // Pick top-N candidate sites by trial-host count. Also pull their country
    // for the Rust-side diversity term.
    let cand_q = format!(
        "MATCH (t:ClinicalTrial)-[:CONDUCTED_AT]->(s:Site) \
         WITH s, count(t) AS deg \
         ORDER BY deg DESC \
         LIMIT {} \
         RETURN s.facility AS facility, s.country AS country", a.candidates);
    let batch = engine.execute(&cand_q, &store).expect("candidate query");
    let mut facilities = Vec::new();
    let mut countries = Vec::new();
    for r in &batch.records {
        let facility = match r.get("facility") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            _ => continue,
        };
        let country = match r.get("country") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            _ => String::from("Unknown"),
        };
        facilities.push(facility);
        countries.push(country);
    }
    let dim = facilities.len();
    eprintln!("got {} candidate sites; {} distinct countries",
        dim, countries.iter().collect::<std::collections::HashSet<_>>().len());
    assert!(dim > 0, "no candidate sites returned");

    // Objective: -count of distinct trials hosted by selected sites + diversity_term.
    let objective = "MATCH (t:ClinicalTrial)-[:CONDUCTED_AT]->(s:Site) \
         WHERE s.facility IN [$selected] \
         RETURN (-toFloat(count(DISTINCT t)) + $diversity_term) AS f";

    let graph = Arc::new(RwLock::new(store));

    let solvers: Vec<(&str, fn(SolverConfig, &CypherProblem) -> samyama_optimization::common::OptimizationResult)> = vec![
        ("BMWR",      |c, p| BMWRSolver::new(c).solve(p)),
        ("Jaya",      |c, p| JayaSolver::new(c).solve(p)),
        ("SAMP-Jaya", |c, p| SAMPJayaSolver::new(c).solve(p)),
        ("EHR-Jaya",  |c, p| EHRJayaSolver::new(c).solve(p)),
        ("Rao-1",     |c, p| RaoSolver::new(c, RaoVariant::Rao1).solve(p)),
    ];
    let cfg = SolverConfig { population_size: 30, max_iterations: 100 };

    let csv_path = a.out.join("results.csv");
    use std::io::Write;
    let mut csv = std::fs::File::create(&csv_path).unwrap();
    writeln!(csv, "solver,seed,best_fitness,trial_count,distinct_countries,wall_ms,cache_hits,cache_misses,avg_eval_ms").unwrap();

    println!("\n=== Trial-site selection (k <= {}, candidates={}, diversity_weight={}) ===",
        a.k, a.candidates, a.diversity_weight);
    println!("{:<12} {:>5} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "solver", "seed", "fitness", "trials", "countries", "wall_ms", "misses", "avg_eval_ms");

    for (name, run) in &solvers {
        for seed in 0..a.seeds {
            let prob = CypherProblem::new(
                dim, Array1::zeros(dim), Array1::ones(dim),
                objective,
                graph.clone(), engine.clone(),
            ).with_subs({
                let f = facilities.clone();
                let c = countries.clone();
                let w = a.diversity_weight;
                let k = a.k;
                move |x: &Array1<f64>| make_subs(x, &f, &c, w, k)
            });
            let t0 = Instant::now();
            let r = run(cfg.clone(), &prob);
            let wall = t0.elapsed().as_millis();
            let st = prob.stats();
            let subs = make_subs(&r.best_variables, &facilities, &countries, a.diversity_weight, a.k);
            let diversity_term: f64 = subs.iter().find(|(p, _)| p == "$diversity_term")
                .map(|(_, v)| v.parse().unwrap_or(0.0)).unwrap_or(0.0);
            let trial_count = -(r.best_fitness - diversity_term);
            let countries_n = if a.diversity_weight > 0.0 { (-diversity_term / a.diversity_weight) as i64 } else { 0 };
            let avg_eval = st.total_eval_ms as f64 / st.misses.max(1) as f64;
            println!("{:<12} {:>5} {:>10.2} {:>10.0} {:>10} {:>10} {:>10} {:>12.2}",
                name, seed, r.best_fitness, trial_count, countries_n, wall, st.misses, avg_eval);
            writeln!(csv, "{},{},{:.4},{},{},{},{},{},{:.3}",
                name, seed, r.best_fitness, trial_count, countries_n, wall, st.hits, st.misses, avg_eval).unwrap();
        }
    }
    eprintln!("\nresults -> {}", csv_path.display());

    let manifest = format!(
        r#"{{"args": {{"snapshot": "{}", "candidates": {}, "k": {}, "seeds": {}, "diversity_weight": {}}}, "nodes_loaded": {}, "edges_loaded": {}, "load_ms": {}, "dim": {}, "distinct_countries_pool": {}}}
"#,
        a.snapshot.display(), a.candidates, a.k, a.seeds, a.diversity_weight,
        stats.node_count, stats.edge_count, load_ms, dim,
        countries.iter().collect::<std::collections::HashSet<_>>().len()
    );
    std::fs::write(a.out.join("manifest.json"), manifest).unwrap();
}
