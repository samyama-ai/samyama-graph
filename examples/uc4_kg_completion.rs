//! UC4 — Biomedical KG Edge-Completion Scoring via SGE + QO-Jaya
//!
//! Tunes the hyperparameters of a lightweight structural link-predictor on
//! a held-out split of a TREATS edge set. Each fitness evaluation issues
//! one Cypher query over the train subgraph to materialise structural
//! features for every (held-out gene, candidate disease) pair, then
//! computes Hits@5 against the held-out pairs.
//!
//! Hyperparameters (4-dim continuous; path_max_hops rounded):
//! - w_cn ∈ [0, 1]          — common-neighbor weight
//! - w_pa ∈ [0, 1]          — preferential-attachment weight
//! - w_path ∈ [0, 1]        — 3-hop path weight (gated by max_hops)
//! - path_max_hops ∈ [1, 3] — rounded to {1, 2, 3}
//!
//! Success: optimiser finds hyperparams that beat a uniform baseline
//! (w_cn = w_pa = w_path = 0.5, max_hops = 2) on Hits@5.
//!
//! Run:  cargo run --release --example uc4_kg_completion
//!
//! [[Use-Case 4 — Biomedical KG Edge-Completion Scoring]]

use samyama_sdk::{
    Array1, EmbeddedClient, Problem, QOJayaSolver, SamyamaClient, SolverConfig,
};
use std::collections::HashMap;
use std::sync::Arc;

// ── Fixture ────────────────────────────────────────────────────────────

const GENES: &[&str] = &[
    "G0","G1","G2","G3","G4","G5","G6","G7","G8","G9",
    "G10","G11","G12","G13","G14","G15","G16","G17","G18","G19",
];
const DISEASES: &[&str] = &[
    "D0","D1","D2","D3","D4","D5","D6","D7","D8","D9",
    "D10","D11","D12","D13","D14","D15","D16","D17","D18","D19",
];

/// All TREATS edges (gene → disease). Structured so there are clusters:
/// genes G0..G9 share diseases among D0..D9, genes G10..G19 share among
/// D10..D19. Held-out picks a few from each cluster; a good predictor
/// should recover them from cluster-mates.
const TREATS: &[(&str, &str)] = &[
    // Cluster A
    ("G0","D0"), ("G0","D1"), ("G0","D2"),
    ("G1","D0"), ("G1","D1"), ("G1","D3"),
    ("G2","D1"), ("G2","D2"), ("G2","D4"),
    ("G3","D0"), ("G3","D3"), ("G3","D5"),
    ("G4","D2"), ("G4","D4"), ("G4","D6"),
    ("G5","D1"), ("G5","D5"), ("G5","D7"),
    ("G6","D3"), ("G6","D6"), ("G6","D8"),
    ("G7","D4"), ("G7","D7"), ("G7","D9"),
    ("G8","D5"), ("G8","D8"),
    ("G9","D6"), ("G9","D9"),
    // Cluster B
    ("G10","D10"), ("G10","D11"), ("G10","D12"),
    ("G11","D10"), ("G11","D11"), ("G11","D13"),
    ("G12","D11"), ("G12","D12"), ("G12","D14"),
    ("G13","D10"), ("G13","D13"), ("G13","D15"),
    ("G14","D12"), ("G14","D14"), ("G14","D16"),
    ("G15","D11"), ("G15","D15"), ("G15","D17"),
    ("G16","D13"), ("G16","D16"), ("G16","D18"),
    ("G17","D14"), ("G17","D17"), ("G17","D19"),
    ("G18","D15"), ("G18","D18"),
    ("G19","D16"), ("G19","D19"),
];

/// (gene, disease) pairs held out from training; never materialised as
/// a :TREATS_TRAIN edge. Every gene here still has other train edges.
const HELD_OUT: &[(&str, &str)] = &[
    ("G0","D1"),   ("G2","D2"),   ("G4","D4"),   ("G6","D3"),   ("G8","D5"),
    ("G10","D11"), ("G12","D12"), ("G14","D14"), ("G16","D13"), ("G18","D15"),
];

const TOP_K: usize = 5;

// ── Problem ────────────────────────────────────────────────────────────

/// Structural features cached at setup time — none of them depend on the
/// hyperparameters, so materialising them per candidate is wasted Cypher.
struct Features {
    cn: HashMap<(String, String), f64>,
    path3: HashMap<(String, String), f64>,
    gene_deg: HashMap<String, f64>,
    disease_deg: HashMap<String, f64>,
    cn_max: f64,
    path_max: f64,
    gdeg_max: f64,
    ddeg_max: f64,
}

struct KgCompletionProblem {
    features: Arc<Features>,
    held_out: Vec<(&'static str, &'static str)>,
    call_count: std::sync::atomic::AtomicUsize,
}

impl Problem for KgCompletionProblem {
    fn dim(&self) -> usize { 4 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::from(vec![0.0, 0.0, 0.0, 1.0]),
         Array1::from(vec![1.0, 1.0, 1.0, 3.0]))
    }
    fn objective(&self, x: &Array1<f64>) -> f64 {
        self.call_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let w_cn = x[0];
        let w_pa = x[1];
        let w_path = x[2];
        let max_hops = x[3].round().clamp(1.0, 3.0) as usize;

        let cn = &self.features.cn;
        let path3 = &self.features.path3;
        let gene_deg = &self.features.gene_deg;
        let disease_deg = &self.features.disease_deg;

        // Rank each held-out gene's candidates and count hits.
        let mut hits = 0_usize;
        let cn_max = self.features.cn_max;
        let path_max = self.features.path_max;
        let gdeg_max = self.features.gdeg_max;
        let ddeg_max = self.features.ddeg_max;

        for (g, true_d) in &self.held_out {
            let mut scored: Vec<(&str, f64)> = Vec::with_capacity(DISEASES.len());
            for cand in DISEASES {
                let cn_v = cn.get(&((*g).to_string(), (*cand).to_string())).copied().unwrap_or(0.0);
                let gd = gene_deg.get(*g).copied().unwrap_or(0.0);
                let dd = disease_deg.get(*cand).copied().unwrap_or(0.0);
                let pa = (gd * dd) / (gdeg_max * ddeg_max).max(1.0);
                let p3 = if max_hops >= 3 {
                    path3.get(&((*g).to_string(), (*cand).to_string())).copied().unwrap_or(0.0) / path_max
                } else { 0.0 };
                let score = w_cn * (cn_v / cn_max) + w_pa * pa + w_path * p3;
                scored.push((cand, score));
            }
            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            if scored.iter().take(TOP_K).any(|(d, _)| *d == *true_d) {
                hits += 1;
            }
        }
        let hits_at_k = hits as f64 / self.held_out.len() as f64;
        // Minimise: negative Hits@K.
        -hits_at_k
    }
}

async fn materialise_features(
    client: &EmbeddedClient,
    held_out: &[(&str, &str)],
) -> Features {
    let gene_list = held_out.iter().map(|(g, _)| format!("\"{g}\""))
        .collect::<Vec<_>>().join(", ");

    let q_cn = format!(
        "MATCH (g:Gene)-[:TREATS_TRAIN]->(:Disease)<-[:TREATS_TRAIN]-(g2:Gene)-[:TREATS_TRAIN]->(d:Disease) \
         WHERE g.gid IN [{gene_list}] AND g <> g2 \
         RETURN g.gid AS src, d.did AS tgt, count(DISTINCT g2) AS cn"
    );
    let r_cn = client.query_readonly("default", &q_cn).await.expect("cn");
    let mut cn = HashMap::<(String, String), f64>::new();
    for row in &r_cn.records {
        let s = row[0].as_str().unwrap_or("").to_string();
        let t = row[1].as_str().unwrap_or("").to_string();
        let c = row[2].as_i64().unwrap_or(0) as f64;
        cn.insert((s, t), c);
    }

    let q_deg = "MATCH (g:Gene) OPTIONAL MATCH (g)-[t:TREATS_TRAIN]->() \
                 RETURN 'g' AS kind, g.gid AS id, count(t) AS deg \
                 UNION ALL \
                 MATCH (d:Disease) OPTIONAL MATCH ()-[t:TREATS_TRAIN]->(d) \
                 RETURN 'd' AS kind, d.did AS id, count(t) AS deg";
    let r_deg = client.query_readonly("default", q_deg).await.expect("deg");
    let mut gene_deg = HashMap::<String, f64>::new();
    let mut disease_deg = HashMap::<String, f64>::new();
    for row in &r_deg.records {
        let kind = row[0].as_str().unwrap_or("");
        let id = row[1].as_str().unwrap_or("").to_string();
        let d = row[2].as_i64().unwrap_or(0) as f64;
        if kind == "g" { gene_deg.insert(id, d); } else { disease_deg.insert(id, d); }
    }

    let q_p3 = format!(
        "MATCH (g:Gene)-[:TREATS_TRAIN]->(:Disease)<-[:TREATS_TRAIN]-(g2:Gene)\
         -[:TREATS_TRAIN]->(:Disease)<-[:TREATS_TRAIN]-(g3:Gene)-[:TREATS_TRAIN]->(d:Disease) \
         WHERE g.gid IN [{gene_list}] AND g <> g2 AND g2 <> g3 AND g <> g3 \
         RETURN g.gid AS src, d.did AS tgt, count(*) AS paths"
    );
    let r_p3 = client.query_readonly("default", &q_p3).await.expect("p3");
    let mut path3 = HashMap::<(String, String), f64>::new();
    for row in &r_p3.records {
        let s = row[0].as_str().unwrap_or("").to_string();
        let t = row[1].as_str().unwrap_or("").to_string();
        let c = row[2].as_i64().unwrap_or(0) as f64;
        path3.insert((s, t), c);
    }

    Features {
        cn_max: cn.values().copied().fold(1.0_f64, f64::max),
        path_max: path3.values().copied().fold(1.0_f64, f64::max),
        gdeg_max: gene_deg.values().copied().fold(1.0_f64, f64::max),
        ddeg_max: disease_deg.values().copied().fold(1.0_f64, f64::max),
        cn, path3, gene_deg, disease_deg,
    }
}

// ── Driver ─────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UC4 — Biomedical KG Edge-Completion Scoring via SGE + QO-Jaya");
    println!("==============================================================\n");

    let client = Arc::new(EmbeddedClient::new());
    let held_set: std::collections::HashSet<(&str, &str)> = HELD_OUT.iter().copied().collect();
    {
        let mut store = client.store_write().await;
        let mut gene_id = HashMap::<&str, samyama::graph::NodeId>::new();
        for g in GENES {
            let nid = store.create_node("Gene");
            if let Some(node) = store.get_node_mut(nid) {
                node.set_property("gid", *g);
            }
            gene_id.insert(*g, nid);
        }
        let mut disease_id = HashMap::<&str, samyama::graph::NodeId>::new();
        for d in DISEASES {
            let nid = store.create_node("Disease");
            if let Some(node) = store.get_node_mut(nid) {
                node.set_property("did", *d);
            }
            disease_id.insert(*d, nid);
        }
        for (g, d) in TREATS {
            let edge_type = if held_set.contains(&(*g, *d)) { "TREATS_HELD" } else { "TREATS_TRAIN" };
            store.create_edge(gene_id[g], disease_id[d], edge_type).unwrap();
        }
    }
    println!(
        "[load] {} Gene, {} Disease, {} train edges, {} held-out",
        GENES.len(),
        DISEASES.len(),
        TREATS.len() - HELD_OUT.len(),
        HELD_OUT.len()
    );

    let features = Arc::new(materialise_features(&client, HELD_OUT).await);
    println!(
        "[features] cn={} entries, path3={} entries, gdeg_max={}, ddeg_max={}",
        features.cn.len(),
        features.path3.len(),
        features.gdeg_max,
        features.ddeg_max,
    );

    let problem = Arc::new(KgCompletionProblem {
        features: features.clone(),
        held_out: HELD_OUT.to_vec(),
        call_count: std::sync::atomic::AtomicUsize::new(0),
    });

    // Baseline: uniform weights at max_hops=2.
    let baseline = Array1::from(vec![0.5, 0.5, 0.5, 2.0]);
    let baseline_fit = -problem.objective(&baseline);
    println!(
        "[baseline] w_cn=0.5 w_pa=0.5 w_path=0.5 max_hops=2 → Hits@{TOP_K}={baseline_fit:.3}"
    );

    let solver = QOJayaSolver::new(SolverConfig {
        population_size: 14,
        max_iterations: 30,
    });
    println!("\n[solve] QO-Jaya pop=14 iter=30");
    let p = problem.clone();
    let (best, calls, wall_ms) = tokio::task::spawn_blocking(move || {
        let t0 = std::time::Instant::now();
        let res = solver.solve(&*p);
        let calls = p.call_count.load(std::sync::atomic::Ordering::Relaxed);
        (res, calls, t0.elapsed().as_millis())
    })
    .await?;
    println!(
        "[done] {} cypher evaluations, wall {} ms ({:.2} ms/eval)",
        calls, wall_ms, wall_ms as f64 / calls.max(1) as f64
    );

    let best_fit = -best.best_fitness;
    println!(
        "\n[best] Hits@{TOP_K} = {:.3}   params: w_cn={:.3} w_pa={:.3} w_path={:.3} max_hops={}",
        best_fit,
        best.best_variables[0],
        best.best_variables[1],
        best.best_variables[2],
        best.best_variables[3].round() as i64
    );
    println!(
        "[check] best ≥ baseline: {} (baseline={:.3}, best={:.3})",
        best_fit >= baseline_fit - 1e-9,
        baseline_fit,
        best_fit
    );

    Ok(())
}
