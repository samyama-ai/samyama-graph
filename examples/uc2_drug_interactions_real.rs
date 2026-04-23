//! UC2-real — Drug-Combination Dosing on **live Drug Interactions KG**
//!
//! Counterpart to `examples/uc2_combo_dosing.rs` (synthetic) that targets
//! the real Drug Interactions KG (DrugBank + DGIdb + SIDER snapshot) on
//! a deployed Samyama instance. The KG carries drug-gene targets and
//! drug-side-effect edges but NO direct drug↔drug interaction edges, so
//! the "contraindicated pair" concept is re-cast as "dangerously overlapping
//! side-effect profiles":
//!
//!   risk(a, b) = side_effect_overlap(a, b) × dose_a × dose_b
//!
//! where `side_effect_overlap(a, b)` is the count of SideEffect nodes
//! shared via `HAS_SIDE_EFFECT` edges — a genuine population-level signal,
//! not a hand-tuned severity score.
//!
//! Indication = "Diabetes mellitus" → 99 drugs in the KG; we pick the top
//! 10 by gene-target count (proxy for pharmacological breadth). Three
//! objectives match synthetic UC2:
//!   - -efficacy  = -(Σ dose_i × gene_target_count_i)
//!   - risk       =  Σ_pairs overlap(a,b) × dose_a × dose_b
//!   - total_dose =  Σ dose_i
//!
//! Run:  SAMYAMA_URL=http://<host>:8080 cargo run --release --example uc2_drug_interactions_real
//!
//! [[Use-Case 2 — Drug-Combination Dosing]]
//! [[SGE + Optimization — Phase 2 Results]]

use samyama_sdk::{
    Array1, MultiObjectiveProblem, NSGA2Solver, RemoteClient, SamyamaClient, SolverConfig,
};
use std::collections::HashMap;
use std::sync::Arc;

const GRAPH: &str = "druginteractions";
const INDICATION: &str = "Diabetes mellitus";
const NUM_DRUGS: usize = 10;
// Pairs with side-effect overlap above this are flagged "dangerous" and
// incur a 1e6 penalty on every objective if both are simultaneously active
// (dose ≥ 5% of max), mirroring the contraindicated-pair pattern in
// synthetic UC2.
const DANGEROUS_OVERLAP_THRESHOLD: i64 = 80;

#[derive(Debug, Clone)]
struct Drug {
    name: String,
    gene_targets: f64,
}

struct ComboDosingProblem {
    drugs: Vec<Drug>,
    pair_overlap: HashMap<(usize, usize), f64>,
    dangerous_pairs: Vec<(usize, usize)>,
}

impl MultiObjectiveProblem for ComboDosingProblem {
    fn dim(&self) -> usize { self.drugs.len() }
    fn num_objectives(&self) -> usize { 3 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim()), Array1::ones(self.dim()))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let active: Vec<usize> = (0..self.drugs.len()).filter(|&i| x[i] >= 0.05).collect();
        if active.is_empty() { return vec![0.0, 0.0, 0.0]; }

        let efficacy: f64 = active.iter()
            .map(|&i| x[i] * self.drugs[i].gene_targets)
            .sum();

        let mut risk = 0.0_f64;
        for (a, b) in self.pair_overlap.keys() {
            risk += self.pair_overlap[&(*a, *b)] * x[*a] * x[*b];
        }

        let total_dose: f64 = (0..self.drugs.len()).map(|i| x[i]).sum();

        let bad = self.dangerous_pairs.iter().any(|(a, b)| x[*a] >= 0.05 && x[*b] >= 0.05);
        let penalty = if bad { 1e6 } else { 0.0 };
        vec![-efficacy + penalty, risk + penalty, total_dose + penalty]
    }
}

fn as_str(v: &serde_json::Value) -> String {
    v.as_str().map(|s| s.to_string()).unwrap_or_default()
}
fn as_f(v: &serde_json::Value) -> f64 {
    v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)).unwrap_or(0.0)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("SAMYAMA_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
    println!("UC2-real — Drug-Combination Dosing on live DI KG @ {url}");
    println!("=========================================================\n");
    let client = RemoteClient::new(&url);

    // 1. Top N drugs indicated for the target disease, ranked by gene-target
    // count (proxy for pharmacological breadth).
    let q = format!(
        "MATCH (d:Drug)-[:HAS_INDICATION]->(:Indication {{name: \"{INDICATION}\"}}) \
         OPTIONAL MATCH (d)-[:INTERACTS_WITH_GENE]->(g:Gene) \
         RETURN d.name AS drug, count(DISTINCT g) AS n_genes \
         ORDER BY n_genes DESC LIMIT {NUM_DRUGS}"
    );
    let r = client.query_readonly(GRAPH, &q).await?;
    let drugs: Vec<Drug> = r.records.iter().map(|row| Drug {
        name: as_str(&row[0]),
        gene_targets: as_f(&row[1]),
    }).collect();
    println!("[probe] {} drugs indicated for \"{INDICATION}\":", drugs.len());
    for (i, d) in drugs.iter().enumerate() {
        println!("  {:>3} {:<22} gene_targets={}", i, d.name, d.gene_targets);
    }

    // 2. Pairwise side-effect overlap — the "pharmacological risk" signal.
    let name_list = drugs.iter()
        .map(|d| format!("\"{}\"", d.name.replace('"', "\\\"")))
        .collect::<Vec<_>>().join(", ");
    let q = format!(
        "MATCH (a:Drug)-[:HAS_SIDE_EFFECT]->(s:SideEffect)<-[:HAS_SIDE_EFFECT]-(b:Drug) \
         WHERE a.name IN [{name_list}] AND b.name IN [{name_list}] AND a.name < b.name \
         RETURN a.name AS a, b.name AS b, count(DISTINCT s) AS overlap"
    );
    let r = client.query_readonly(GRAPH, &q).await?;
    let name_to_idx: HashMap<&str, usize> = drugs.iter().enumerate()
        .map(|(i, d)| (d.name.as_str(), i)).collect();
    let mut pair_overlap: HashMap<(usize, usize), f64> = HashMap::new();
    let mut dangerous_pairs: Vec<(usize, usize)> = Vec::new();
    for row in &r.records {
        let a = as_str(&row[0]);
        let b = as_str(&row[1]);
        let overlap = row[2].as_i64().unwrap_or(0);
        if let (Some(&ia), Some(&ib)) = (name_to_idx.get(a.as_str()), name_to_idx.get(b.as_str())) {
            pair_overlap.insert((ia, ib), overlap as f64);
            if overlap >= DANGEROUS_OVERLAP_THRESHOLD {
                dangerous_pairs.push((ia, ib));
            }
        }
    }
    println!("\n[probe] {} drug-pair side-effect overlaps", pair_overlap.len());
    println!("[probe] {} \"dangerous\" pairs (overlap ≥ {DANGEROUS_OVERLAP_THRESHOLD}):", dangerous_pairs.len());
    for (a, b) in &dangerous_pairs {
        let overlap = pair_overlap[&(*a, *b)];
        println!("  {} + {} → {} shared side effects",
            drugs[*a].name, drugs[*b].name, overlap as i64);
    }

    // 3. NSGA-II.
    let problem = Arc::new(ComboDosingProblem {
        drugs: drugs.clone(),
        pair_overlap,
        dangerous_pairs: dangerous_pairs.clone(),
    });
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 50,
        max_iterations: 60,
    });
    println!("\n[solve] NSGA-II pop=50 iter=60, 3 objectives (-efficacy, risk, total_dose)");
    let p = problem.clone();
    let (front, wall_ms) = tokio::task::spawn_blocking(move || {
        let t0 = std::time::Instant::now();
        let res = solver.solve(&*p);
        (res.pareto_front, t0.elapsed().as_millis())
    }).await?;
    println!("[done] {} plans, wall {wall_ms} ms", front.len());

    let mut rows: Vec<_> = front.iter().collect();
    rows.sort_by(|a, b| a.fitness[0].partial_cmp(&b.fitness[0]).unwrap());

    println!("\n[pareto] top 10 by efficacy:");
    println!("  {:>9}  {:>6}  {:>10}   active drugs (dose%)", "efficacy", "risk", "total_dose");
    for ind in rows.iter().take(10) {
        let active: Vec<String> = (0..drugs.len())
            .filter(|&i| ind.variables[i] >= 0.05)
            .map(|i| format!("{}={:.0}%", drugs[i].name, ind.variables[i] * 100.0))
            .collect();
        println!("  {:>9.2}  {:>6.1}  {:>10.2}   {}",
            -ind.fitness[0], ind.fitness[1], ind.fitness[2], active.join(" "));
    }

    // Sanity: no Pareto plan has any dangerous pair both active.
    let violations = rows.iter().filter(|ind| {
        dangerous_pairs.iter().any(|(a, b)|
            ind.variables[*a] >= 0.05 && ind.variables[*b] >= 0.05
        )
    }).count();
    println!("\n[check] Pareto plans with an active dangerous pair: {violations} (must be 0)");

    Ok(())
}
