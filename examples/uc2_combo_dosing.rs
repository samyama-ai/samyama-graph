//! UC2 — Drug-Combination Dosing via SGE + NSGA-II
//!
//! Same Cypher-driven-fitness pattern as UC1, applied to a continuous
//! 3-objective problem. The graph holds Drugs / Genes / Pathways / Disease
//! and INTERACTS_WITH edges (with severity_score). The optimizer searches
//! a dose vector d ∈ [0, 1]^k; for each candidate it issues two Cypher
//! queries — pathway coverage per drug and pairwise interactions over the
//! active set — and composes a 3-objective vector (efficacy, side-effect
//! risk, total dose).
//!
//! The contraindicated pair (simvastatin + clarithromycin) is emitted with
//! severity_score = 1.0, which dominates the side-effect objective if both
//! doses are non-zero — so NSGA-II should drive at least one of them to 0
//! on every Pareto-front plan.
//!
//! Run:  cargo run --release --example uc2_combo_dosing

use samyama_sdk::{
    Array1, EmbeddedClient, MultiObjectiveProblem, NSGA2Solver, SamyamaClient, SolverConfig,
};
use std::sync::Arc;
use tokio::runtime::Handle;

// ── Fixture ────────────────────────────────────────────────────────────

const DISEASE_ID: &str = "type2_diabetes";

/// (did, generic_name, max_dose_mg)
const DRUGS: &[(&str, &str, f64)] = &[
    ("D0", "metformin", 2000.0),
    ("D1", "sitagliptin", 100.0),
    ("D2", "empagliflozin", 25.0),
    ("D3", "simvastatin", 40.0),
    ("D4", "clarithromycin", 1000.0),
    ("D5", "warfarin", 10.0),
];

/// (drug_did, gene_id)
const TARGETS: &[(&str, &str)] = &[
    ("D0", "AMPK"),
    ("D0", "GLUT4"),
    ("D1", "DPP4"),
    ("D2", "SGLT2"),
    ("D3", "HMGCR"),
    ("D4", "50S"),
    ("D5", "VKOR"),
];

/// (gene_id, pathway_id)
const PART_OF: &[(&str, &str)] = &[
    ("AMPK", "glucose_homeostasis"),
    ("GLUT4", "glucose_homeostasis"),
    ("DPP4", "glucose_homeostasis"),
    ("SGLT2", "glucose_homeostasis"),
    ("HMGCR", "lipid_metabolism"),
    ("50S", "antibiotic"),
    ("VKOR", "coagulation"),
];

/// Pathways implicated in T2D (used to compute coverage).
const IMPLICATED_IN: &[(&str, &str)] = &[
    ("glucose_homeostasis", DISEASE_ID),
];

/// (drug_a, drug_b, severity_score in [0,1])
/// 0.1 = mild, 0.3 = moderate, 0.7 = severe, 1.0 = contraindicated.
const INTERACTIONS: &[(&str, &str, f64)] = &[
    ("D0", "D1", 0.1),
    ("D0", "D2", 0.1),
    ("D1", "D2", 0.1),
    ("D0", "D3", 0.3),
    ("D3", "D5", 0.7),
    ("D3", "D4", 1.0),  // simvastatin + clarithromycin — contraindicated (CYP3A4)
    ("D4", "D5", 0.3),
];

// ── Problem ────────────────────────────────────────────────────────────

struct ComboDosingProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
    drug_dids: Vec<&'static str>,
    call_count: std::sync::atomic::AtomicUsize,
}

impl MultiObjectiveProblem for ComboDosingProblem {
    fn dim(&self) -> usize {
        self.drug_dids.len()
    }
    fn num_objectives(&self) -> usize {
        3
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        // Normalised dose ∈ [0, 1] (fraction of max daily dose); decoded later.
        (Array1::zeros(self.dim()), Array1::ones(self.dim()))
    }

    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Active set: drugs with normalised dose >= 5% of max.
        let active_idx: Vec<usize> = (0..self.drug_dids.len()).filter(|&i| x[i] >= 0.05).collect();
        if active_idx.is_empty() {
            // No drug taken — zero efficacy, zero risk, zero dose. Strongly dominated.
            return vec![0.0, 0.0, 0.0];
        }
        let active_dids: Vec<&str> = active_idx.iter().map(|&i| self.drug_dids[i]).collect();
        let id_list = active_dids
            .iter()
            .map(|d| format!("\"{d}\""))
            .collect::<Vec<_>>()
            .join(", ");

        // ── Query 1: pathway coverage per active drug for the disease ──
        let q1 = format!(
            "MATCH (d:Drug)-[:TARGETS]->(:Gene)-[:PART_OF]->(p:Pathway)-[:IMPLICATED_IN]->(dz:Disease) \
             WHERE d.did IN [{id_list}] AND dz.did = \"{DISEASE_ID}\" \
             RETURN d.did AS did, count(DISTINCT p) AS coverage"
        );
        let r1 = self.run_cypher(&q1);
        let mut coverage = std::collections::HashMap::<String, f64>::new();
        for row in &r1.records {
            let did = row[0].as_str().unwrap_or("").to_string();
            let cov = row[1].as_i64().unwrap_or(0) as f64;
            coverage.insert(did, cov);
        }

        // ── Query 2: pairwise interactions over the active set ──
        let q2 = format!(
            "MATCH (a:Drug)-[r:INTERACTS_WITH]->(b:Drug) \
             WHERE a.did IN [{id_list}] AND b.did IN [{id_list}] \
             RETURN a.did AS a, b.did AS b, r.severity_score AS sev"
        );
        let r2 = self.run_cypher(&q2);

        // ── Compose objectives ──
        let efficacy: f64 = active_idx
            .iter()
            .map(|&i| {
                let cov = coverage.get(self.drug_dids[i]).copied().unwrap_or(0.0);
                x[i] * cov
            })
            .sum();

        let did_to_idx: std::collections::HashMap<&str, usize> = self
            .drug_dids
            .iter()
            .enumerate()
            .map(|(i, &d)| (d, i))
            .collect();
        let mut risk = 0.0_f64;
        let mut contraindicated_pair_active = false;
        for row in &r2.records {
            let a = row[0].as_str().unwrap_or("");
            let b = row[1].as_str().unwrap_or("");
            let sev = row[2].as_f64().unwrap_or(0.0);
            if let (Some(&ia), Some(&ib)) = (did_to_idx.get(a), did_to_idx.get(b)) {
                risk += sev * x[ia] * x[ib];
                if sev >= 0.999 && x[ia] >= 0.05 && x[ib] >= 0.05 {
                    contraindicated_pair_active = true;
                }
            }
        }

        let total_dose: f64 = (0..self.drug_dids.len()).map(|i| x[i]).sum();

        // Hard constraint: contraindicated-pair penalty pushes plan off the Pareto front.
        let penalty = if contraindicated_pair_active { 1e6 } else { 0.0 };

        // Minimise: -efficacy, +risk, +total_dose. All three add penalty equally
        // so the dominated plan is dominated on every objective.
        vec![-efficacy + penalty, risk + penalty, total_dose + penalty]
    }
}

impl ComboDosingProblem {
    fn run_cypher(&self, q: &str) -> samyama_sdk::QueryResult {
        let client = self.client.clone();
        let q_owned = q.to_string();
        self.handle
            .block_on(async move { client.query_readonly("default", &q_owned).await })
            .unwrap_or_else(|e| panic!("cypher: {e}\nquery: {q}"))
    }
}

// ── Driver ─────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UC2 — Drug-Combination Dosing via SGE + NSGA-II");
    println!("================================================\n");

    let client = Arc::new(EmbeddedClient::new());
    load_fixture(&client).await;

    let drug_dids: Vec<&'static str> = DRUGS.iter().map(|(d, _, _)| *d).collect();
    let problem = Arc::new(ComboDosingProblem {
        client: client.clone(),
        handle: Handle::current(),
        drug_dids,
        call_count: std::sync::atomic::AtomicUsize::new(0),
    });
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 50,
        max_iterations: 60,
    });

    println!(
        "[solve] NSGA-II pop=50 iter=60, dim={}, objectives=(-efficacy, risk, total_dose)",
        DRUGS.len()
    );
    let p = problem.clone();
    let (front, calls, wall_ms) = tokio::task::spawn_blocking(move || {
        let t0 = std::time::Instant::now();
        let res = solver.solve(&*p);
        let calls = p.call_count.load(std::sync::atomic::Ordering::Relaxed);
        (res.pareto_front, calls, t0.elapsed().as_millis())
    })
    .await?;

    println!(
        "[done] {} cypher evaluations, wall {} ms ({:.2} ms/eval)",
        calls,
        wall_ms,
        wall_ms as f64 / calls.max(1) as f64
    );

    // Sort by efficacy (ascending fitness[0] = ascending -efficacy = descending efficacy).
    let mut rows: Vec<_> = front.iter().collect();
    rows.sort_by(|a, b| a.fitness[0].partial_cmp(&b.fitness[0]).unwrap());

    println!("\n[pareto] {} non-dominated dose plans:", rows.len());
    println!(
        "  {:>9}  {:>6}  {:>10}   doses by drug",
        "efficacy", "risk", "total_dose"
    );
    for ind in rows.iter().take(12) {
        let plan = ind
            .variables
            .iter()
            .enumerate()
            .map(|(i, &d)| {
                let mg = d * DRUGS[i].2;
                format!("{}={:.0}mg", DRUGS[i].1.split_at(4).0, mg)
            })
            .collect::<Vec<_>>()
            .join(" ");
        println!(
            "  {:>9.2}  {:>6.2}  {:>10.2}   {plan}",
            -ind.fitness[0],
            ind.fitness[1],
            ind.fitness[2]
        );
    }
    if rows.len() > 12 {
        println!("  ... ({} more)", rows.len() - 12);
    }

    // Sanity: verify no Pareto plan has the contraindicated pair active.
    let bad = rows
        .iter()
        .filter(|ind| ind.variables[3] >= 0.05 && ind.variables[4] >= 0.05)
        .count();
    println!(
        "\n[check] Pareto plans with active contraindicated pair (D3+D4): {bad} (must be 0)"
    );

    Ok(())
}

async fn load_fixture(client: &EmbeddedClient) {
    use std::collections::HashMap;
    let mut store = client.store_write().await;

    let mut drug_id: HashMap<&str, samyama::graph::NodeId> = HashMap::new();
    for (did, name, max_dose) in DRUGS {
        let nid = store.create_node("Drug");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("did", *did);
            node.set_property("name", *name);
            node.set_property("max_dose_mg", *max_dose);
        }
        drug_id.insert(*did, nid);
    }
    let mut genes: Vec<&str> = TARGETS.iter().map(|(_, g)| *g).collect();
    genes.sort();
    genes.dedup();
    let mut gene_id: HashMap<&str, samyama::graph::NodeId> = HashMap::new();
    for g in &genes {
        let nid = store.create_node("Gene");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("gid", *g);
        }
        gene_id.insert(*g, nid);
    }
    let mut pathways: Vec<&str> = PART_OF.iter().map(|(_, p)| *p).collect();
    pathways.sort();
    pathways.dedup();
    let mut pathway_id: HashMap<&str, samyama::graph::NodeId> = HashMap::new();
    for p in &pathways {
        let nid = store.create_node("Pathway");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("pid", *p);
        }
        pathway_id.insert(*p, nid);
    }
    let mut disease_id: HashMap<&str, samyama::graph::NodeId> = HashMap::new();
    let did_node = store.create_node("Disease");
    if let Some(node) = store.get_node_mut(did_node) {
        node.set_property("did", DISEASE_ID);
        node.set_property("name", "Type 2 Diabetes");
    }
    disease_id.insert(DISEASE_ID, did_node);

    for (drug, gene) in TARGETS {
        store.create_edge(drug_id[drug], gene_id[gene], "TARGETS").unwrap();
    }
    for (gene, pathway) in PART_OF {
        store.create_edge(gene_id[gene], pathway_id[pathway], "PART_OF").unwrap();
    }
    for (pathway, disease) in IMPLICATED_IN {
        store.create_edge(pathway_id[pathway], disease_id[disease], "IMPLICATED_IN").unwrap();
    }
    for (a, b, sev) in INTERACTIONS {
        let eid = store.create_edge(drug_id[a], drug_id[b], "INTERACTS_WITH").unwrap();
        store.set_edge_property(eid, "severity_score", *sev).unwrap();
    }

    println!(
        "[load] {} drugs, {} genes, {} pathways, 1 disease, {} interactions",
        DRUGS.len(),
        genes.len(),
        pathways.len(),
        INTERACTIONS.len()
    );
}
