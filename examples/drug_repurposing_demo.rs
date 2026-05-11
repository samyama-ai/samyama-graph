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
    out: PathBuf,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            snapshot: PathBuf::from("../druginteractions-kg/data/druginteractions.sgsnap"),
            candidates: 50,
            targets: 10,
            k: 5,
            seeds: 5,
            out: PathBuf::from("/tmp/p8-drug-repurposing"),
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
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
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

    // 4. Build objective template with two placeholders: $selected and $targets.
    //    Targets are fixed across the run; selected varies per candidate vector.
    let targets_list = targets.iter()
        .map(|g| format!("'{}'", g.replace('\'', "\\'")))
        .collect::<Vec<_>>().join(",");
    let objective = format!(
        "MATCH (d:Drug)-[:INTERACTS_WITH_GENE]->(g:Gene) \
         WHERE d.drugbank_id IN [$selected] AND g.gene_name IN [{}] \
         RETURN -toFloat(count(DISTINCT g)) AS f", targets_list);

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
    ).with_subs(move |x: &Array1<f64>| {
        // Selected indices, by descending decision value, up to k.
        let mut idxs: Vec<(usize, f64)> = x.iter().enumerate()
            .filter(|(_, &v)| v > 0.5)
            .map(|(i, &v)| (i, v))
            .collect();
        idxs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let take = k_for_sub.min(idxs.len());
        let list = idxs.iter().take(take)
            .map(|(i, _)| format!("'{}'", candidates_for_sub[*i].replace('\'', "\\'")))
            .collect::<Vec<_>>()
            .join(",");
        // If nothing selected, IN [] would be invalid; emit a sentinel that matches nothing.
        let list = if list.is_empty() { "'__NONE__'".to_string() } else { list };
        vec![("$selected".to_string(), list)]
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
    writeln!(csv, "solver,seed,best_coverage,wall_ms,cache_hits,cache_misses,avg_eval_ms").unwrap();

    println!("\n=== Drug repurposing (k <= {}; candidates={}, targets={}) ===",
        a.k, a.candidates, a.targets);
    println!("targets: {:?}", targets);
    println!("");
    println!("{:<12} {:>5} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "solver", "seed", "coverage", "wall_ms", "hits", "misses", "avg_eval_ms");

    for (name, run) in &solvers {
        for seed in 0..a.seeds {
            // Fresh problem per run so cache stats are per-run.
            let prob = CypherProblem::new(
                dim, Array1::zeros(dim), Array1::ones(dim),
                problem.objective_template.clone(),
                graph.clone(), engine.clone(),
            ).with_subs({
                let candidates_for_sub = candidates.clone();
                let k_for_sub = a.k;
                move |x: &Array1<f64>| {
                    let mut idxs: Vec<(usize, f64)> = x.iter().enumerate()
                        .filter(|(_, &v)| v > 0.5)
                        .map(|(i, &v)| (i, v)).collect();
                    idxs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                    let take = k_for_sub.min(idxs.len());
                    let list = idxs.iter().take(take)
                        .map(|(i, _)| format!("'{}'", candidates_for_sub[*i].replace('\'', "\\'")))
                        .collect::<Vec<_>>().join(",");
                    let list = if list.is_empty() { "'__NONE__'".to_string() } else { list };
                    vec![("$selected".to_string(), list)]
                }
            });
            let t0 = Instant::now();
            let r = run(cfg.clone(), &prob);
            let wall = t0.elapsed().as_millis();
            let stats = prob.stats();
            let coverage = -r.best_fitness;  // we minimized -count
            let avg_eval = stats.total_eval_ms as f64 / stats.misses.max(1) as f64;
            println!("{:<12} {:>5} {:>10.0} {:>10} {:>10} {:>10} {:>12.2}",
                name, seed, coverage, wall, stats.hits, stats.misses, avg_eval);
            writeln!(csv, "{},{},{},{},{},{},{:.3}",
                name, seed, coverage, wall, stats.hits, stats.misses, avg_eval).unwrap();
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
