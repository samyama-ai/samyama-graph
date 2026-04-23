//! UC2 — Drug-Combination Dosing
//!
//! Locks in the contract for the SGE + NSGA-II Cypher-driven fitness pattern
//! applied to a continuous, 3-objective dose-vector problem
//! (samyama-cloud/wiki/use-cases/uc2-drug-combination-dosing.md).
//!
//! Verified properties:
//! - Pareto front is non-empty and diverse.
//! - The contraindicated pair (D3 simvastatin + D4 clarithromycin) is never
//!   simultaneously active on any Pareto plan — the 1e6 penalty is in fact
//!   pushing those candidates off the front.
//! - At least one plan reaches non-trivial efficacy (the optimizer is not
//!   stuck on the all-zero candidate).

use samyama_sdk::{
    Array1, EmbeddedClient, MultiObjectiveProblem, NSGA2Solver, SamyamaClient, SolverConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;

const DISEASE_ID: &str = "type2_diabetes";

const DRUGS: &[(&str, f64)] = &[
    ("D0", 2000.0),
    ("D1", 100.0),
    ("D2", 25.0),
    ("D3", 40.0),
    ("D4", 1000.0),
    ("D5", 10.0),
];
const TARGETS: &[(&str, &str)] = &[
    ("D0", "AMPK"), ("D0", "GLUT4"), ("D1", "DPP4"), ("D2", "SGLT2"),
    ("D3", "HMGCR"), ("D4", "50S"), ("D5", "VKOR"),
];
const PART_OF: &[(&str, &str)] = &[
    ("AMPK", "glucose_homeostasis"), ("GLUT4", "glucose_homeostasis"),
    ("DPP4", "glucose_homeostasis"), ("SGLT2", "glucose_homeostasis"),
    ("HMGCR", "lipid_metabolism"), ("50S", "antibiotic"), ("VKOR", "coagulation"),
];
const IMPLICATED_IN: &[(&str, &str)] = &[("glucose_homeostasis", DISEASE_ID)];
const INTERACTIONS: &[(&str, &str, f64)] = &[
    ("D0", "D1", 0.1), ("D0", "D2", 0.1), ("D1", "D2", 0.1),
    ("D0", "D3", 0.3), ("D3", "D5", 0.7),
    ("D3", "D4", 1.0), // contraindicated
    ("D4", "D5", 0.3),
];

struct ComboDosingProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
    drug_dids: Vec<&'static str>,
}

impl MultiObjectiveProblem for ComboDosingProblem {
    fn dim(&self) -> usize { self.drug_dids.len() }
    fn num_objectives(&self) -> usize { 3 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim()), Array1::ones(self.dim()))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let active_idx: Vec<usize> = (0..self.drug_dids.len()).filter(|&i| x[i] >= 0.05).collect();
        if active_idx.is_empty() {
            return vec![0.0, 0.0, 0.0];
        }
        let id_list = active_idx.iter()
            .map(|&i| format!("\"{}\"", self.drug_dids[i]))
            .collect::<Vec<_>>().join(", ");

        let q1 = format!(
            "MATCH (d:Drug)-[:TARGETS]->(:Gene)-[:PART_OF]->(p:Pathway)-[:IMPLICATED_IN]->(dz:Disease) \
             WHERE d.did IN [{id_list}] AND dz.did = \"{DISEASE_ID}\" \
             RETURN d.did AS did, count(DISTINCT p) AS coverage"
        );
        let r1 = self.run_cypher(&q1);
        let mut coverage = HashMap::<String, f64>::new();
        for row in &r1.records {
            let did = row[0].as_str().unwrap_or("").to_string();
            let cov = row[1].as_i64().unwrap_or(0) as f64;
            coverage.insert(did, cov);
        }

        let q2 = format!(
            "MATCH (a:Drug)-[r:INTERACTS_WITH]->(b:Drug) \
             WHERE a.did IN [{id_list}] AND b.did IN [{id_list}] \
             RETURN a.did AS a, b.did AS b, r.severity_score AS sev"
        );
        let r2 = self.run_cypher(&q2);

        let efficacy: f64 = active_idx.iter().map(|&i| {
            let cov = coverage.get(self.drug_dids[i]).copied().unwrap_or(0.0);
            x[i] * cov
        }).sum();

        let did_to_idx: HashMap<&str, usize> = self.drug_dids.iter().enumerate()
            .map(|(i, &d)| (d, i)).collect();
        let mut risk = 0.0_f64;
        let mut bad = false;
        for row in &r2.records {
            let a = row[0].as_str().unwrap_or("");
            let b = row[1].as_str().unwrap_or("");
            let sev = row[2].as_f64().unwrap_or(0.0);
            if let (Some(&ia), Some(&ib)) = (did_to_idx.get(a), did_to_idx.get(b)) {
                risk += sev * x[ia] * x[ib];
                if sev >= 0.999 && x[ia] >= 0.05 && x[ib] >= 0.05 {
                    bad = true;
                }
            }
        }
        let total_dose: f64 = (0..self.drug_dids.len()).map(|i| x[i]).sum();
        let penalty = if bad { 1e6 } else { 0.0 };
        vec![-efficacy + penalty, risk + penalty, total_dose + penalty]
    }
}

impl ComboDosingProblem {
    fn run_cypher(&self, q: &str) -> samyama_sdk::QueryResult {
        let client = self.client.clone();
        let q_owned = q.to_string();
        self.handle
            .block_on(async move { client.query_readonly("default", &q_owned).await })
            .expect("cypher")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uc2_pareto_avoids_contraindicated_pair() {
    let client = Arc::new(EmbeddedClient::new());
    {
        let mut store = client.store_write().await;
        let mut drug_id = HashMap::<&str, samyama::graph::NodeId>::new();
        for (did, max_dose) in DRUGS {
            let nid = store.create_node("Drug");
            if let Some(node) = store.get_node_mut(nid) {
                node.set_property("did", *did);
                node.set_property("max_dose_mg", *max_dose);
            }
            drug_id.insert(*did, nid);
        }
        let mut genes: Vec<&str> = TARGETS.iter().map(|(_, g)| *g).collect();
        genes.sort(); genes.dedup();
        let mut gene_id = HashMap::<&str, samyama::graph::NodeId>::new();
        for g in &genes {
            let nid = store.create_node("Gene");
            if let Some(node) = store.get_node_mut(nid) {
                node.set_property("gid", *g);
            }
            gene_id.insert(*g, nid);
        }
        let mut pathways: Vec<&str> = PART_OF.iter().map(|(_, p)| *p).collect();
        pathways.sort(); pathways.dedup();
        let mut pathway_id = HashMap::<&str, samyama::graph::NodeId>::new();
        for p in &pathways {
            let nid = store.create_node("Pathway");
            if let Some(node) = store.get_node_mut(nid) {
                node.set_property("pid", *p);
            }
            pathway_id.insert(*p, nid);
        }
        let dz_node = store.create_node("Disease");
        if let Some(node) = store.get_node_mut(dz_node) {
            node.set_property("did", DISEASE_ID);
        }

        for (drug, gene) in TARGETS {
            store.create_edge(drug_id[drug], gene_id[gene], "TARGETS").unwrap();
        }
        for (gene, pathway) in PART_OF {
            store.create_edge(gene_id[gene], pathway_id[pathway], "PART_OF").unwrap();
        }
        for (pathway, _disease) in IMPLICATED_IN {
            store.create_edge(pathway_id[pathway], dz_node, "IMPLICATED_IN").unwrap();
        }
        for (a, b, sev) in INTERACTIONS {
            let eid = store.create_edge(drug_id[a], drug_id[b], "INTERACTS_WITH").unwrap();
            store.set_edge_property(eid, "severity_score", *sev).unwrap();
        }
    }

    let drug_dids: Vec<&'static str> = DRUGS.iter().map(|(d, _)| *d).collect();
    let problem = Arc::new(ComboDosingProblem {
        client: client.clone(),
        handle: Handle::current(),
        drug_dids,
    });
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 30,
        max_iterations: 30,
    });

    let p = problem.clone();
    let front = tokio::task::spawn_blocking(move || solver.solve(&*p).pareto_front)
        .await
        .unwrap();

    assert!(!front.is_empty(), "Pareto front must be non-empty");
    assert!(front.len() >= 2, "Expected diverse front; got {} plans", front.len());

    // Critical: penalty (1e6) must keep contraindicated pair off the front.
    for ind in &front {
        let d3_active = ind.variables[3] >= 0.05;
        let d4_active = ind.variables[4] >= 0.05;
        assert!(
            !(d3_active && d4_active),
            "Pareto plan has both D3 and D4 active: vars={:?} fitness={:?}",
            ind.variables, ind.fitness,
        );
        for v in &ind.fitness {
            assert!(v.is_finite(), "non-finite fitness leaked: {:?}", ind.fitness);
            assert!(*v < 1e5, "penalty leaked into Pareto front: {:?}", ind.fitness);
        }
    }

    // Optimizer must reach non-trivial efficacy (i.e. not stuck at all-zero).
    let max_efficacy = front.iter().map(|ind| -ind.fitness[0]).fold(0.0_f64, f64::max);
    assert!(max_efficacy > 0.5, "best efficacy on front {} is too low", max_efficacy);
}
