//! Paper 8 problem 4: healthcare resource allocation under equity constraints.
//!
//! Loads the public health-systems snapshot (WHO SPAR + NHWA + GAVI + Global
//! Fund + IHME GHDx) and poses a health-equity investment problem:
//!
//!   maximize    Σ_{c ∈ SELECTED} deficit(c) × population_proxy(c)
//!               + region_weight × |distinct WHO regions in SELECTED|
//!   subject to  |SELECTED| ≤ k
//!
//! where deficit(c) = max(0, WHO_THRESHOLD - physician_density_per_10k(c))
//! captures the marginal impact of investment in country c (countries with
//! the LOWEST physician density have the HIGHEST deficit). WHO threshold for
//! adequate primary-care physician density is 23 / 10k population.
//!
//! Pre-materialised pattern (per-country aggregates fetched once at startup).
//!
//! Usage:
//!   cargo run --release --example healthcare_allocation_demo -- \
//!       [--snapshot PATH] [--candidates 50] [--k 10] [--seeds 3] \
//!       [--region-weight 5.0] [--threshold 23.0]

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
    region_weight: f64,
    threshold: f64,
    out: PathBuf,
    export_spec: Option<PathBuf>,
}
impl Default for Args {
    fn default() -> Self {
        Self {
            snapshot: PathBuf::from("../health-systems-kg/data/health-systems.sgsnap"),
            candidates: 50, k: 10, seeds: 3,
            region_weight: 5.0, threshold: 23.0,
            out: PathBuf::from("/tmp/p8-healthcare-allocation"),
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
            "--region-weight" => { a.region_weight = argv[i + 1].parse().unwrap(); i += 2; }
            "--threshold" => { a.threshold = argv[i + 1].parse().unwrap(); i += 2; }
            "--out" => { a.out = PathBuf::from(&argv[i + 1]); i += 2; }
            "--export-spec" => { a.export_spec = Some(PathBuf::from(&argv[i + 1])); i += 2; }
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
}

struct HealthEquityProblem {
    dim: usize,
    deficits: Vec<f64>,
    regions: Vec<String>,
    region_weight: f64,
    k: usize,
    eval_count: std::sync::atomic::AtomicU64,
}

impl Problem for HealthEquityProblem {
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
        let mut deficit_sum = 0.0;
        let mut distinct: HashSet<&str> = HashSet::new();
        for (i, _) in idxs.iter().take(take) {
            deficit_sum += self.deficits[*i];
            distinct.insert(self.regions[*i].as_str());
        }
        -(deficit_sum + self.region_weight * distinct.len() as f64)
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

    // ONE Cypher query: per-country aggregate of physician density (max over years),
    // alongside who_region. Filter to physicians; aggregate by country.
    let cand_q = format!(
        "MATCH (hw:HealthWorkforce)-[:SERVES]->(c:Country) \
         WHERE hw.profession = 'physicians' \
         WITH c, max(hw.density_per_10k) AS dens \
         ORDER BY dens ASC \
         LIMIT {} \
         RETURN c.name AS name, c.who_region AS region, dens AS density",
        a.candidates);
    let t1 = Instant::now();
    let batch = engine.execute(&cand_q, &store).expect("candidate query");
    let candq_ms = t1.elapsed().as_millis();
    eprintln!("candidate query: {} rows in {} ms", batch.records.len(), candq_ms);

    let mut names = Vec::new();
    let mut regions = Vec::new();
    let mut densities = Vec::new();
    let mut deficits = Vec::new();
    for r in &batch.records {
        let name = match r.get("name") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            _ => continue,
        };
        let region = match r.get("region") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            _ => String::from("Unknown"),
        };
        let dens = match r.get("density") {
            Some(Value::Property(PropertyValue::Float(f))) => *f,
            Some(Value::Property(PropertyValue::Integer(n))) => *n as f64,
            _ => 0.0,
        };
        let deficit = (a.threshold - dens).max(0.0);
        names.push(name);
        regions.push(region);
        densities.push(dens);
        deficits.push(deficit);
    }
    let dim = names.len();
    let distinct_regions: HashSet<&str> = regions.iter().map(|s| s.as_str()).collect();
    eprintln!("dim = {}, distinct regions in pool = {}, total deficit in pool = {:.1}",
        dim, distinct_regions.len(), deficits.iter().sum::<f64>());
    eprintln!("density range: [{:.2}, {:.2}] per 10k",
        densities.first().copied().unwrap_or(0.0),
        densities.last().copied().unwrap_or(0.0));
    assert!(dim > 0, "no candidate countries returned");

    if let Some(path) = &a.export_spec {
        use std::io::Write as _;
        let mut f = File::create(path).unwrap();
        writeln!(f, r#"{{"k": {}, "region_weight": {}, "threshold": {}, "names": [{}], "regions": [{}], "densities": [{}], "deficits": [{}]}}"#,
            a.k, a.region_weight, a.threshold,
            names.iter().map(|s| format!(r#""{}""#, s.replace('"', r#"\""#))).collect::<Vec<_>>().join(","),
            regions.iter().map(|s| format!(r#""{}""#, s.replace('"', r#"\""#))).collect::<Vec<_>>().join(","),
            densities.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(","),
            deficits.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(",")
        ).unwrap();
        eprintln!("spec -> {}", path.display());
    }

    let problem = Arc::new(HealthEquityProblem {
        dim, deficits: deficits.clone(), regions: regions.clone(),
        region_weight: a.region_weight, k: a.k,
        eval_count: std::sync::atomic::AtomicU64::new(0),
    });

    let solvers: Vec<(&str, fn(SolverConfig, &HealthEquityProblem) -> OptimizationResult)> = vec![
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
    writeln!(csv, "solver,seed,best_fitness,deficit_sum,distinct_regions,wall_ms,evals").unwrap();

    println!("\n=== Healthcare allocation (k <= {}, candidates={}, region_weight={}, threshold={}) ===",
        a.k, a.candidates, a.region_weight, a.threshold);
    println!("{:<12} {:>5} {:>12} {:>12} {:>10} {:>10} {:>10}",
        "solver", "seed", "fitness", "deficit_sum", "regions", "wall_ms", "evals");

    for (name, run) in &solvers {
        for seed in 0..a.seeds {
            problem.eval_count.store(0, std::sync::atomic::Ordering::Relaxed);
            let t0 = Instant::now();
            let r = run(cfg.clone(), &problem);
            let wall = t0.elapsed().as_millis();
            let evals = problem.eval_count.load(std::sync::atomic::Ordering::Relaxed);
            let mut idxs: Vec<(usize, f64)> = r.best_variables.iter().enumerate()
                .filter(|(_, &v)| v > 0.5).map(|(i, &v)| (i, v)).collect();
            idxs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let take = a.k.min(idxs.len());
            let mut def_sum = 0.0;
            let mut distinct: HashSet<&str> = HashSet::new();
            for (i, _) in idxs.iter().take(take) {
                def_sum += deficits[*i];
                distinct.insert(regions[*i].as_str());
            }
            println!("{:<12} {:>5} {:>12.2} {:>12.2} {:>10} {:>10} {:>10}",
                name, seed, r.best_fitness, def_sum, distinct.len(), wall, evals);
            writeln!(csv, "{},{},{:.4},{:.4},{},{},{}",
                name, seed, r.best_fitness, def_sum, distinct.len(), wall, evals).unwrap();
        }
    }
    eprintln!("\nresults -> {}", csv_path.display());

    let manifest = format!(
        r#"{{"args": {{"snapshot": "{}", "candidates": {}, "k": {}, "seeds": {}, "region_weight": {}, "threshold": {}}}, "nodes_loaded": {}, "edges_loaded": {}, "load_ms": {}, "candidate_query_ms": {}, "dim": {}, "distinct_regions_pool": {}, "total_deficit_pool": {:.2}}}
"#,
        a.snapshot.display(), a.candidates, a.k, a.seeds, a.region_weight, a.threshold,
        stats.node_count, stats.edge_count, load_ms, candq_ms, dim,
        distinct_regions.len(), deficits.iter().sum::<f64>()
    );
    std::fs::write(a.out.join("manifest.json"), manifest).unwrap();
}
