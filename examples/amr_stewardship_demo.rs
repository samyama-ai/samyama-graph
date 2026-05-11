//! Paper 8 problem 6: AMR antibiotic-stewardship — regimen selection.
//!
//! Loads NCBI AMRFinderPlus ReferenceGeneCatalog.txt (public domain) — every
//! curated antimicrobial-resistance gene with its drug class, drug subclass,
//! and whitelisted_taxa (the organism(s) it is known to confer resistance in).
//!
//! Builds an in-memory KG:
//!   :ResistanceGene {gene_family, scope, type, subtype}
//!   :DrugClass {name}
//!   :DrugSubclass {name}
//!   :Pathogen {name}
//!   (g)-[:CONFERS_RESISTANCE_TO]->(:DrugSubclass)
//!   (g)-[:RELEVANT_IN]->(:Pathogen)
//!   (:DrugSubclass)-[:BELONGS_TO]->(:DrugClass)
//!
//! Poses the regimen-selection problem:
//!
//!   maximize    Σ_{p ∈ PATHOGENS} max_{s ∈ SELECTED} efficacy(s, p)
//!             - resistance_weight × Σ_{s ∈ SELECTED} resistance_burden(s)
//!   subject to  |SELECTED| ≤ k
//!
//! where efficacy(s, p) = 1 / (1 + |ARGs targeting s found in p|) (fewer
//! documented resistance genes ⇒ higher expected efficacy) and
//! resistance_burden(s) = total distinct ARGs targeting subclass s
//! (more-prevalent resistance ⇒ riskier choice).
//!
//! Usage:
//!   cargo run --release --example amr_stewardship_demo -- \
//!       [--catalog PATH] [--candidates 50] [--pathogens 20] [--k 5] \
//!       [--seeds 3] [--resistance-weight 0.001]

use ndarray::Array1;
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;
use samyama::query::executor::record::Value;
use samyama_optimization::algorithms::{
    BMWRSolver, EHRJayaSolver, JayaSolver, RaoSolver, RaoVariant, SAMPJayaSolver,
};
use samyama_optimization::common::{OptimizationResult, Problem, SolverConfig};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
struct Args {
    catalog: PathBuf,
    candidates: usize,
    pathogens: usize,
    k: usize,
    seeds: usize,
    resistance_weight: f64,
    out: PathBuf,
    export_spec: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            catalog: PathBuf::from("../amr-kg/data/ncbi/ReferenceGeneCatalog.txt"),
            candidates: 50,
            pathogens: 20,
            k: 5,
            seeds: 3,
            resistance_weight: 0.001,
            out: PathBuf::from("/tmp/p8-amr-stewardship"),
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
            "--catalog" => { a.catalog = PathBuf::from(&argv[i + 1]); i += 2; }
            "--candidates" => { a.candidates = argv[i + 1].parse().unwrap(); i += 2; }
            "--pathogens" => { a.pathogens = argv[i + 1].parse().unwrap(); i += 2; }
            "--k" => { a.k = argv[i + 1].parse().unwrap(); i += 2; }
            "--seeds" => { a.seeds = argv[i + 1].parse().unwrap(); i += 2; }
            "--resistance-weight" => { a.resistance_weight = argv[i + 1].parse().unwrap(); i += 2; }
            "--out" => { a.out = PathBuf::from(&argv[i + 1]); i += 2; }
            "--export-spec" => { a.export_spec = Some(PathBuf::from(&argv[i + 1])); i += 2; }
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
}

/// Parse NCBI ReferenceGeneCatalog.txt into a KG and return (store, gene-rows).
fn build_amr_kg(catalog: &std::path::Path)
    -> (GraphStore, Vec<(String, String, String, String)>) // (gene_family, class, subclass, taxa)
{
    let mut store = GraphStore::new();
    let f = File::open(catalog).expect("catalog");
    let mut header: Vec<String> = Vec::new();
    let mut rows = Vec::new();

    let mut subclass_nodes: HashMap<String, samyama::graph::NodeId> = HashMap::new();
    let mut class_nodes: HashMap<String, samyama::graph::NodeId> = HashMap::new();
    let mut pathogen_nodes: HashMap<String, samyama::graph::NodeId> = HashMap::new();

    for (i, line) in BufReader::new(f).lines().enumerate() {
        let line = line.unwrap();
        let cols: Vec<&str> = line.split('\t').collect();
        if i == 0 { header = cols.iter().map(|s| s.to_string()).collect(); continue; }
        if cols.len() < 9 { continue; }
        let gene_family = cols[1].to_string();
        let taxa = cols[2].to_string();
        let class_str = cols[7].to_string();
        let subclass_str = cols[8].to_string();
        if subclass_str.is_empty() || class_str.is_empty() { continue; }

        // Create gene node.
        let g = store.create_node(Label::new("ResistanceGene"));
        let gm = store.get_node_mut(g).unwrap();
        gm.set_property("gene_family", PropertyValue::String(gene_family.clone()));
        gm.set_property("class", PropertyValue::String(class_str.clone()));
        gm.set_property("subclass", PropertyValue::String(subclass_str.clone()));
        gm.set_property("taxa", PropertyValue::String(taxa.clone()));

        // Create/lookup subclass node.
        let _sub_node = *subclass_nodes.entry(subclass_str.clone()).or_insert_with(|| {
            let n = store.create_node(Label::new("DrugSubclass"));
            store.get_node_mut(n).unwrap().set_property("name", PropertyValue::String(subclass_str.clone()));
            n
        });
        let _class_node = *class_nodes.entry(class_str.clone()).or_insert_with(|| {
            let n = store.create_node(Label::new("DrugClass"));
            store.get_node_mut(n).unwrap().set_property("name", PropertyValue::String(class_str.clone()));
            n
        });
        if !taxa.is_empty() {
            let _p_node = *pathogen_nodes.entry(taxa.clone()).or_insert_with(|| {
                let n = store.create_node(Label::new("Pathogen"));
                store.get_node_mut(n).unwrap().set_property("name", PropertyValue::String(taxa.clone()));
                n
            });
        }
        rows.push((gene_family, class_str, subclass_str, taxa));
        if i > 50_000 { break; } // safety cap; catalog is ~11K so won't trigger
    }
    let _ = header;
    (store, rows)
}

struct AmrProblem {
    dim: usize,
    /// efficacy[s][p] in [0,1].
    efficacy: Vec<Vec<f64>>,
    /// resistance_burden[s] = total ARGs targeting subclass s (NCBI count).
    resistance: Vec<f64>,
    /// drug class index per subclass (for diversity tracking).
    class_idx: Vec<usize>,
    n_pathogens: usize,
    resistance_weight: f64,
    k: usize,
    eval_count: std::sync::atomic::AtomicU64,
}

impl Problem for AmrProblem {
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
        if take == 0 { return 0.0; }
        let selected: Vec<usize> = idxs.iter().take(take).map(|(i, _)| *i).collect();
        let mut coverage = 0.0;
        for p in 0..self.n_pathogens {
            let mut best = 0.0_f64;
            for &s in &selected {
                if self.efficacy[s][p] > best { best = self.efficacy[s][p]; }
            }
            coverage += best;
        }
        let resistance_burden: f64 = selected.iter().map(|&s| self.resistance[s]).sum();
        -(coverage - self.resistance_weight * resistance_burden)
    }
}

fn main() {
    let a = parse_args();
    std::fs::create_dir_all(&a.out).unwrap();

    eprintln!("loading {} ...", a.catalog.display());
    let t0 = Instant::now();
    let (store, rows) = build_amr_kg(&a.catalog);
    let build_ms = t0.elapsed().as_millis();
    eprintln!("KG built: {} gene rows in {} ms", rows.len(), build_ms);

    // Cypher sanity queries.
    let engine = QueryEngine::new();
    let q1 = "MATCH (s:DrugSubclass) RETURN count(s) AS n";
    let q2 = "MATCH (p:Pathogen) RETURN count(p) AS n";
    let q3 = "MATCH (g:ResistanceGene) RETURN count(g) AS n";
    for (label, q) in [("subclasses", q1), ("pathogens", q2), ("resistance_genes", q3)] {
        let b = engine.execute(q, &store).expect(q);
        let n = match b.records.first().and_then(|r| r.get("n")) {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            _ => -1,
        };
        eprintln!("KG sanity — {} = {}", label, n);
    }

    // Pre-materialise: count of genes per (subclass, pathogen).
    let mut subclass_to_idx: HashMap<String, usize> = HashMap::new();
    let mut subclass_resistance: HashMap<String, f64> = HashMap::new();
    let mut subclass_class: HashMap<String, String> = HashMap::new();
    let mut pathogen_argcount: HashMap<String, f64> = HashMap::new();
    let mut pair_count: HashMap<(String, String), f64> = HashMap::new();
    for (_g, cls, sub, taxa) in &rows {
        *subclass_resistance.entry(sub.clone()).or_insert(0.0) += 1.0;
        subclass_class.insert(sub.clone(), cls.clone());
        if !taxa.is_empty() {
            *pathogen_argcount.entry(taxa.clone()).or_insert(0.0) += 1.0;
            *pair_count.entry((sub.clone(), taxa.clone())).or_insert(0.0) += 1.0;
        }
    }

    // Top-N candidate subclasses by total ARG count (most-studied = most clinically relevant).
    let mut sub_ranking: Vec<(String, f64)> = subclass_resistance.iter()
        .map(|(s, n)| (s.clone(), *n)).collect();
    sub_ranking.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let candidates: Vec<(String, f64)> = sub_ranking.iter().take(a.candidates).cloned().collect();
    for (i, (s, _)) in candidates.iter().enumerate() { subclass_to_idx.insert(s.clone(), i); }

    // Top-M pathogens by total ARG count.
    let mut path_ranking: Vec<(String, f64)> = pathogen_argcount.iter()
        .map(|(p, n)| (p.clone(), *n)).collect();
    path_ranking.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let pathogens: Vec<String> = path_ranking.iter().take(a.pathogens).map(|(p, _)| p.clone()).collect();
    let dim = candidates.len();
    let n_pathogens = pathogens.len();

    eprintln!("candidates (top {}): dim = {}, n_pathogens = {}",
        a.candidates, dim, n_pathogens);
    eprintln!("top-5 subclasses: {:?}",
        candidates.iter().take(5).map(|(s, n)| format!("{} ({})", s, n)).collect::<Vec<_>>());
    eprintln!("top-5 pathogens : {:?}", pathogens.iter().take(5).collect::<Vec<_>>());

    // Build efficacy matrix.
    let mut efficacy = vec![vec![1.0_f64; n_pathogens]; dim];
    for (i, (sub, _)) in candidates.iter().enumerate() {
        for (j, p) in pathogens.iter().enumerate() {
            let c = pair_count.get(&(sub.clone(), p.clone())).copied().unwrap_or(0.0);
            efficacy[i][j] = 1.0 / (1.0 + c);
        }
    }
    let resistance: Vec<f64> = candidates.iter().map(|(_, n)| *n).collect();

    // Class index per subclass (for reporting and diversity).
    let mut class_ids: HashMap<String, usize> = HashMap::new();
    let class_idx: Vec<usize> = candidates.iter().map(|(sub, _)| {
        let cls = subclass_class.get(sub).cloned().unwrap_or_default();
        let next = class_ids.len();
        *class_ids.entry(cls).or_insert(next)
    }).collect();

    if let Some(path) = &a.export_spec {
        let mut f = File::create(path).unwrap();
        writeln!(f, r#"{{"k": {}, "resistance_weight": {},"#, a.k, a.resistance_weight).unwrap();
        writeln!(f, r#""subclasses": [{}],"#,
            candidates.iter().map(|(s, _)| format!(r#""{}""#, s.replace('"', r#"\""#))).collect::<Vec<_>>().join(",")).unwrap();
        writeln!(f, r#""classes": [{}],"#,
            candidates.iter().map(|(s, _)| {
                let c = subclass_class.get(s).cloned().unwrap_or_default();
                format!(r#""{}""#, c.replace('"', r#"\""#))
            }).collect::<Vec<_>>().join(",")).unwrap();
        writeln!(f, r#""pathogens": [{}],"#,
            pathogens.iter().map(|p| format!(r#""{}""#, p.replace('"', r#"\""#))).collect::<Vec<_>>().join(",")).unwrap();
        writeln!(f, r#""resistance_burden": [{}],"#,
            resistance.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(",")).unwrap();
        write!(f, r#""efficacy": [["#).unwrap();
        for (i, row) in efficacy.iter().enumerate() {
            if i > 0 { write!(f, "],[").unwrap(); }
            write!(f, "{}", row.iter().map(|v| format!("{:.6}", v)).collect::<Vec<_>>().join(",")).unwrap();
        }
        writeln!(f, "]]}}").unwrap();
        eprintln!("spec -> {}", path.display());
    }

    let problem = Arc::new(AmrProblem {
        dim, efficacy: efficacy.clone(), resistance: resistance.clone(),
        class_idx: class_idx.clone(), n_pathogens,
        resistance_weight: a.resistance_weight,
        k: a.k,
        eval_count: std::sync::atomic::AtomicU64::new(0),
    });

    let solvers: Vec<(&str, fn(SolverConfig, &AmrProblem) -> OptimizationResult)> = vec![
        ("BMWR",      |c, p| BMWRSolver::new(c).solve(p)),
        ("Jaya",      |c, p| JayaSolver::new(c).solve(p)),
        ("SAMP-Jaya", |c, p| SAMPJayaSolver::new(c).solve(p)),
        ("EHR-Jaya",  |c, p| EHRJayaSolver::new(c).solve(p)),
        ("Rao-1",     |c, p| RaoSolver::new(c, RaoVariant::Rao1).solve(p)),
    ];
    let cfg = SolverConfig { population_size: 30, max_iterations: 200 };

    let csv_path = a.out.join("results.csv");
    let mut csv = File::create(&csv_path).unwrap();
    writeln!(csv, "solver,seed,best_fitness,coverage,resistance_burden,distinct_classes,wall_ms,evals").unwrap();

    println!("\n=== AMR stewardship (k <= {}, candidates={}, pathogens={}, ε={}) ===",
        a.k, a.candidates, a.pathogens, a.resistance_weight);
    println!("{:<12} {:>5} {:>12} {:>10} {:>14} {:>10} {:>10} {:>10}",
        "solver", "seed", "fitness", "coverage", "resistance", "classes", "wall_ms", "evals");

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
            let selected: Vec<usize> = idxs.iter().take(take).map(|(i, _)| *i).collect();
            let mut coverage = 0.0;
            for p in 0..n_pathogens {
                let mut best = 0.0_f64;
                for &s in &selected {
                    if efficacy[s][p] > best { best = efficacy[s][p]; }
                }
                coverage += best;
            }
            let resistance_burden: f64 = selected.iter().map(|&s| resistance[s]).sum();
            let distinct_classes: HashSet<usize> = selected.iter().map(|&s| class_idx[s]).collect();
            println!("{:<12} {:>5} {:>12.4} {:>10.4} {:>14.0} {:>10} {:>10} {:>10}",
                name, seed, r.best_fitness, coverage, resistance_burden,
                distinct_classes.len(), wall, evals);
            writeln!(csv, "{},{},{:.6},{:.6},{},{},{},{}",
                name, seed, r.best_fitness, coverage, resistance_burden,
                distinct_classes.len(), wall, evals).unwrap();
        }
    }
    eprintln!("\nresults -> {}", csv_path.display());

    let manifest = format!(
        r#"{{"args": {{"catalog": "{}", "candidates": {}, "pathogens": {}, "k": {}, "seeds": {}, "resistance_weight": {}}}, "gene_rows": {}, "dim": {}, "n_pathogens": {}, "build_ms": {}}}
"#,
        a.catalog.display(), a.candidates, a.pathogens, a.k, a.seeds, a.resistance_weight,
        rows.len(), dim, n_pathogens, build_ms);
    std::fs::write(a.out.join("manifest.json"), manifest).unwrap();
}
