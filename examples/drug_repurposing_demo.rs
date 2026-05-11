//! Paper 8 problem 1: drug-repurposing portfolio selection.
//!
//! Loads the public druginteractions snapshot (DrugBank + DGIdb + SIDER +
//! ChEMBL + TTD + OpenFDA; 245K nodes, 388K edges), picks the top-N
//! candidate drugs by gene-interaction degree and a target-gene set, then
//! poses:
//!
//!   maximize    | { g in TARGETS : exists d in SELECTED, (d)-[:INTERACTS_WITH_GENE]->(g) } |
//!   subject to  |SELECTED| <= k
//!
//! evaluated against samyama-graph via Cypher per candidate vector.
//!
//! Usage:
//!   cargo run --release --example drug_repurposing_demo -- \
//!       [--snapshot PATH] [--candidates 50] [--targets 10] [--k 5] \
//!       [--seeds 10] [--out DIR]
//!
//! Default snapshot: ../druginteractions-kg/data/druginteractions.sgsnap

use ndarray::Array1;
use samyama::graph::GraphStore;
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
    targets: usize,
    k: usize,
    seeds: usize,
    sider_weight: f64,
    out: PathBuf,
    export_edges: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            snapshot: PathBuf::from("../druginteractions-kg/data/druginteractions.sgsnap"),
            candidates: 50,
            targets: 10,
            k: 5,
            seeds: 5,
            sider_weight: 0.0,
            out: PathBuf::from("/tmp/p8-drug-repurposing"),
            export_edges: None,
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
            "--targets" => { a.targets = argv[i + 1].parse().unwrap(); i += 2; }
            "--k" => { a.k = argv[i + 1].parse().unwrap(); i += 2; }
            "--seeds" => { a.seeds = argv[i + 1].parse().unwrap(); i += 2; }
            "--out" => { a.out = PathBuf::from(&argv[i + 1]); i += 2; }
            "--sider-weight" => { a.sider_weight = argv[i + 1].parse().unwrap(); i += 2; }
            "--export-edges" => { a.export_edges = Some(PathBuf::from(&argv[i + 1])); i += 2; }
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
}

/// Map a decision vector to (selected drug-id list, sider cost) Cypher substitutions.
fn make_subs(
    x: &Array1<f64>,
    candidates: &[String],
    sider_counts: &[f64],
    sider_weight: f64,
    k: usize,
) -> Vec<(String, String)> {
    let mut idxs: Vec<(usize, f64)> = x.iter().enumerate()
        .filter(|(_, &v)| v > 0.5)
        .map(|(i, &v)| (i, v)).collect();
    idxs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let take = k.min(idxs.len());
    let selected: Vec<usize> = idxs.iter().take(take).map(|(i, _)| *i).collect();
    let list = selected.iter()
        .map(|i| format!("'{}'", candidates[*i].replace('\'', "\\'")))
        .collect::<Vec<_>>().join(",");
    let list = if list.is_empty() { "'__NONE__'".to_string() } else { list };
    let sider_cost: f64 = selected.iter().map(|i| sider_counts[*i]).sum::<f64>() * sider_weight;
    vec![
        ("$selected".to_string(), list),
        ("$sider_cost".to_string(), format!("{:.6}", sider_cost)),
    ]
}

/// Pull a list of strings from the first column of a query result.
fn query_strings(engine: &QueryEngine, store: &GraphStore, q: &str) -> Vec<String> {
    let batch = engine.execute(q, store).expect("query failed");
    let col = batch.columns.first().expect("no columns").clone();
    batch.records.iter().filter_map(|r| {
        match r.get(&col) {
            Some(Value::Property(samyama::graph::PropertyValue::String(s))) => Some(s.clone()),
            _ => None,
        }
    }).collect()
}

fn main() {
    let a = parse_args();
    std::fs::create_dir_all(&a.out).unwrap();

    // 1. Load snapshot.
    eprintln!("loading {} ...", a.snapshot.display());
    let mut store = GraphStore::new();
    let t0 = Instant::now();
    let f = File::open(&a.snapshot).expect("snapshot open");
    let stats = samyama::snapshot::import_tenant(&mut store, f).expect("import");
    let load_ms = t0.elapsed().as_millis();
    eprintln!("loaded {} nodes, {} edges in {} ms",
        stats.node_count, stats.edge_count, load_ms);

    let engine = Arc::new(QueryEngine::new());

    // 2. Pick top-N candidate drugs by gene-interaction degree.
    let cand_q = format!(
        "MATCH (d:Drug)-[:INTERACTS_WITH_GENE]->(g:Gene) \
         WITH d, count(g) AS deg \
         ORDER BY deg DESC \
         LIMIT {} \
         RETURN d.drugbank_id AS id", a.candidates);
    let candidates = query_strings(&engine, &store, &cand_q);
    eprintln!("got {} candidate drugs", candidates.len());
    assert!(!candidates.is_empty(), "no candidates returned");

    // 3. Pick top-M target genes by drug-interaction degree.
    let tgt_q = format!(
        "MATCH (d:Drug)-[:INTERACTS_WITH_GENE]->(g:Gene) \
         WITH g, count(d) AS deg \
         ORDER BY deg DESC \
         LIMIT {} \
         RETURN g.gene_name AS name", a.targets);
    let targets = query_strings(&engine, &store, &tgt_q);
    eprintln!("got {} target genes: {:?}", targets.len(), targets);
    assert!(!targets.is_empty(), "no targets returned");

    // 4. Side-effect counts per candidate (Vec aligned with `candidates`).
    let mut sider_counts: Vec<f64> = vec![0.0; candidates.len()];
    let mut id_to_idx = std::collections::HashMap::new();
    for (i, id) in candidates.iter().enumerate() {
        id_to_idx.insert(id.clone(), i);
    }
    let se_q = "MATCH (d:Drug)-[:HAS_SIDE_EFFECT]->(s) \
                RETURN d.drugbank_id AS id, count(s) AS n";
    let se_batch = engine.execute(se_q, &store).expect("side-effect query");
    for r in &se_batch.records {
        if let (Some(Value::Property(samyama::graph::PropertyValue::String(id))),
                Some(Value::Property(samyama::graph::PropertyValue::Integer(n)))) =
            (r.get("id"), r.get("n"))
        {
            if let Some(&idx) = id_to_idx.get(id) { sider_counts[idx] = *n as f64; }
        }
    }
    let mean_se: f64 = sider_counts.iter().sum::<f64>() / sider_counts.len() as f64;
    eprintln!("side-effect counts per candidate: mean={:.1}, max={:.0}",
        mean_se, sider_counts.iter().cloned().fold(0.0f64, f64::max));

    // 5. Optional edge-export for the OR-tools baseline.
    if let Some(path) = &a.export_edges {
        let edge_q = format!(
            "MATCH (d:Drug)-[:INTERACTS_WITH_GENE]->(g:Gene) \
             WHERE d.drugbank_id IN [{}] AND g.gene_name IN [{}] \
             RETURN d.drugbank_id AS drug, g.gene_name AS gene",
            candidates.iter().map(|c| format!("'{}'", c.replace('\'', "\\'")))
                .collect::<Vec<_>>().join(","),
            targets.iter().map(|t| format!("'{}'", t.replace('\'', "\\'")))
                .collect::<Vec<_>>().join(","));
        let edge_batch = engine.execute(&edge_q, &store).expect("edge export");
        use std::io::Write as _;
        let mut f = std::fs::File::create(path).unwrap();
        writeln!(f, r#"{{"k": {}, "sider_weight": {}, "candidates": [{}], "targets": [{}], "side_effects": [{}], "edges": ["#,
            a.k, a.sider_weight,
            candidates.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(","),
            targets.iter().map(|t| format!("\"{}\"", t)).collect::<Vec<_>>().join(","),
            sider_counts.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(",")).unwrap();
        let mut first = true;
        for r in &edge_batch.records {
            if let (Some(Value::Property(samyama::graph::PropertyValue::String(d))),
                    Some(Value::Property(samyama::graph::PropertyValue::String(g)))) =
                (r.get("drug"), r.get("gene")) {
                if !first { writeln!(f, ",").unwrap(); }
                write!(f, r#"  {{"drug": "{}", "gene": "{}"}}"#, d, g).unwrap();
                first = false;
            }
        }
        writeln!(f, "\n]}}").unwrap();
        eprintln!("edges -> {} ({} drug-gene edges)", path.display(), edge_batch.records.len());
    }

    // 6. Build objective template. Targets are fixed; $selected and $sider_cost
    //    vary per candidate. $sider_cost is the Rust-computed penalty injected
    //    as a numeric literal so the Cypher query stays simple (no sum-over-CASE).
    let targets_list = targets.iter()
        .map(|g| format!("'{}'", g.replace('\'', "\\'")))
        .collect::<Vec<_>>().join(",");
    let objective = format!(
        "MATCH (d:Drug)-[:INTERACTS_WITH_GENE]->(g:Gene) \
         WHERE d.drugbank_id IN [$selected] AND g.gene_name IN [{}] \
         RETURN (-toFloat(count(DISTINCT g)) + $sider_cost) AS f", targets_list);

    let graph = Arc::new(RwLock::new(store));
    let candidates_for_sub = candidates.clone();
    let k_for_sub = a.k;
    let dim = candidates.len();

    // 5. Construct CypherProblem with custom $selected substitution.
    //    Map decision vars > 0.5 to selected; if more than k are selected,
    //    keep only the top-k by decision value (helps the solver discover
    //    the cardinality limit smoothly; hard penalty applied in wrapper).
    let problem = CypherProblem::new(
        dim,
        Array1::zeros(dim),
        Array1::ones(dim),
        objective,
        graph.clone(),
        engine.clone(),
    ).with_subs({
        let candidates_for_sub = candidates_for_sub.clone();
        let sider_counts = sider_counts.clone();
        let sider_weight = a.sider_weight;
        let k = k_for_sub;
        move |x: &Array1<f64>| make_subs(x, &candidates_for_sub, &sider_counts, sider_weight, k)
    });

    // 6. Run a small panel of Rao-family solvers.
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
    writeln!(csv, "solver,seed,best_fitness,coverage_only,sider_cost,wall_ms,cache_hits,cache_misses,avg_eval_ms").unwrap();

    println!("\n=== Drug repurposing (k <= {}; candidates={}, targets={}, sider_weight={}) ===",
        a.k, a.candidates, a.targets, a.sider_weight);
    println!("targets: {:?}", targets);
    println!("");
    println!("{:<12} {:>5} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "solver", "seed", "fitness", "coverage", "sider", "wall_ms", "misses", "avg_eval_ms");

    for (name, run) in &solvers {
        for seed in 0..a.seeds {
            // Fresh problem per run so cache stats are per-run.
            let prob = CypherProblem::new(
                dim, Array1::zeros(dim), Array1::ones(dim),
                problem.objective_template.clone(),
                graph.clone(), engine.clone(),
            ).with_subs({
                let cands = candidates.clone();
                let se = sider_counts.clone();
                let w = a.sider_weight;
                let k = a.k;
                move |x: &Array1<f64>| make_subs(x, &cands, &se, w, k)
            });
            let t0 = Instant::now();
            let r = run(cfg.clone(), &prob);
            let wall = t0.elapsed().as_millis();
            let stats = prob.stats();
            // Recover coverage and SIDER components from the best variables.
            let subs = make_subs(&r.best_variables, &candidates, &sider_counts, a.sider_weight, a.k);
            let sider_cost: f64 = subs.iter().find(|(p, _)| p == "$sider_cost")
                .map(|(_, v)| v.parse().unwrap_or(0.0)).unwrap_or(0.0);
            let coverage = -(r.best_fitness - sider_cost);
            let avg_eval = stats.total_eval_ms as f64 / stats.misses.max(1) as f64;
            println!("{:<12} {:>5} {:>10.2} {:>10.0} {:>10.2} {:>10} {:>10} {:>12.2}",
                name, seed, r.best_fitness, coverage, sider_cost, wall, stats.misses, avg_eval);
            writeln!(csv, "{},{},{:.4},{},{:.4},{},{},{},{:.3}",
                name, seed, r.best_fitness, coverage, sider_cost, wall, stats.hits, stats.misses, avg_eval).unwrap();
        }
    }
    eprintln!("\nresults -> {}", csv_path.display());

    let manifest = format!(
        r#"{{
  "args": {{ "snapshot": "{}", "candidates": {}, "targets": {}, "k": {}, "seeds": {} }},
  "nodes_loaded": {}, "edges_loaded": {},
  "load_ms": {},
  "target_genes": [{}]
}}
"#,
        a.snapshot.display(), a.candidates, a.targets, a.k, a.seeds,
        stats.node_count, stats.edge_count, load_ms,
        targets.iter().map(|t| format!("\"{}\"", t)).collect::<Vec<_>>().join(",")
    );
    std::fs::write(a.out.join("manifest.json"), manifest).unwrap();
}
