//! Use-case Cypher-driven multi-objective problems, exposed through
//! `/optimize/solve` so they render on the same Pareto chart as the
//! synthetic ZDT/DTLZ1 benchmarks.
//!
//! Each UC owns its embedded GraphStore + QueryEngine; the fitness fn
//! queries its own graph (no shared state, no async locks). Runs inside
//! the existing spawn_blocking so QueryEngine's sync API is natural.

use crate::graph::GraphStore;
use crate::query::QueryEngine;
use ndarray::Array1;
use samyama_optimization::common::MultiObjectiveProblem;
use std::collections::HashMap;
use std::sync::RwLock;

/// UC2 — Drug-combination dosing. Mirrors `examples/uc2_combo_dosing.rs`:
/// 6 drugs, 7 genes, 4 pathways, 1 disease, 7 INTERACTS_WITH edges
/// including a contraindicated pair (D3+D4, severity 1.0). Fitness runs
/// two Cypher queries per candidate and composes a 3-vector of
/// (-efficacy, risk, total_dose), with a 1e6 penalty added to every
/// objective when the contraindicated pair is simultaneously active.
pub struct UC2DosingProblem {
    engine: QueryEngine,
    store: RwLock<GraphStore>,
    drug_dids: Vec<&'static str>,
}

const UC2_DISEASE: &str = "type2_diabetes";
const UC2_DRUGS: &[(&str, &str, f64)] = &[
    ("D0", "metformin", 2000.0),
    ("D1", "sitagliptin", 100.0),
    ("D2", "empagliflozin", 25.0),
    ("D3", "simvastatin", 40.0),
    ("D4", "clarithromycin", 1000.0),
    ("D5", "warfarin", 10.0),
];
const UC2_TARGETS: &[(&str, &str)] = &[
    ("D0", "AMPK"), ("D0", "GLUT4"), ("D1", "DPP4"), ("D2", "SGLT2"),
    ("D3", "HMGCR"), ("D4", "50S"), ("D5", "VKOR"),
];
const UC2_PART_OF: &[(&str, &str)] = &[
    ("AMPK", "glucose_homeostasis"), ("GLUT4", "glucose_homeostasis"),
    ("DPP4", "glucose_homeostasis"), ("SGLT2", "glucose_homeostasis"),
    ("HMGCR", "lipid_metabolism"), ("50S", "antibiotic"), ("VKOR", "coagulation"),
];
const UC2_IMPLICATED: &[(&str, &str)] = &[("glucose_homeostasis", UC2_DISEASE)];
const UC2_INTERACTIONS: &[(&str, &str, f64)] = &[
    ("D0", "D1", 0.1), ("D0", "D2", 0.1), ("D1", "D2", 0.1),
    ("D0", "D3", 0.3), ("D3", "D5", 0.7),
    ("D3", "D4", 1.0), // contraindicated
    ("D4", "D5", 0.3),
];

impl UC2DosingProblem {
    pub fn new() -> Self {
        let mut store = GraphStore::new();
        let mut drug_id = HashMap::<&str, crate::graph::NodeId>::new();
        for (did, name, max_dose) in UC2_DRUGS {
            let nid = store.create_node("Drug");
            if let Some(n) = store.get_node_mut(nid) {
                n.set_property("did", *did);
                n.set_property("name", *name);
                n.set_property("max_dose_mg", *max_dose);
            }
            drug_id.insert(*did, nid);
        }
        let mut genes: Vec<&str> = UC2_TARGETS.iter().map(|(_, g)| *g).collect();
        genes.sort(); genes.dedup();
        let mut gene_id = HashMap::<&str, crate::graph::NodeId>::new();
        for g in &genes {
            let nid = store.create_node("Gene");
            if let Some(n) = store.get_node_mut(nid) { n.set_property("gid", *g); }
            gene_id.insert(*g, nid);
        }
        let mut pathways: Vec<&str> = UC2_PART_OF.iter().map(|(_, p)| *p).collect();
        pathways.sort(); pathways.dedup();
        let mut pathway_id = HashMap::<&str, crate::graph::NodeId>::new();
        for p in &pathways {
            let nid = store.create_node("Pathway");
            if let Some(n) = store.get_node_mut(nid) { n.set_property("pid", *p); }
            pathway_id.insert(*p, nid);
        }
        let dz = store.create_node("Disease");
        if let Some(n) = store.get_node_mut(dz) { n.set_property("did", UC2_DISEASE); }

        for (d, g) in UC2_TARGETS {
            store.create_edge(drug_id[d], gene_id[g], "TARGETS").unwrap();
        }
        for (g, p) in UC2_PART_OF {
            store.create_edge(gene_id[g], pathway_id[p], "PART_OF").unwrap();
        }
        for (p, _) in UC2_IMPLICATED {
            store.create_edge(pathway_id[p], dz, "IMPLICATED_IN").unwrap();
        }
        for (a, b, sev) in UC2_INTERACTIONS {
            let eid = store.create_edge(drug_id[a], drug_id[b], "INTERACTS_WITH").unwrap();
            store.set_edge_property(eid, "severity_score", *sev).unwrap();
        }

        Self {
            engine: QueryEngine::new(),
            store: RwLock::new(store),
            drug_dids: UC2_DRUGS.iter().map(|(d, ..)| *d).collect(),
        }
    }

    fn run(&self, q: &str) -> crate::query::RecordBatch {
        let s = self.store.read().unwrap();
        self.engine.execute(q, &*s).unwrap_or_else(|e| panic!("uc2 cypher: {e}\n{q}"))
    }
}

impl Default for UC2DosingProblem {
    fn default() -> Self { Self::new() }
}

impl MultiObjectiveProblem for UC2DosingProblem {
    fn dim(&self) -> usize { self.drug_dids.len() }
    fn num_objectives(&self) -> usize { 3 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim()), Array1::ones(self.dim()))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        use crate::graph::PropertyValue as P;
        use crate::query::executor::record::Value as V;

        let active_idx: Vec<usize> = (0..self.drug_dids.len()).filter(|&i| x[i] >= 0.05).collect();
        if active_idx.is_empty() { return vec![0.0, 0.0, 0.0]; }
        let id_list = active_idx.iter().map(|&i| format!("\"{}\"", self.drug_dids[i]))
            .collect::<Vec<_>>().join(", ");

        let q1 = format!(
            "MATCH (d:Drug)-[:TARGETS]->(:Gene)-[:PART_OF]->(p:Pathway)-[:IMPLICATED_IN]->(dz:Disease) \
             WHERE d.did IN [{id_list}] AND dz.did = \"{UC2_DISEASE}\" \
             RETURN d.did AS did, count(DISTINCT p) AS coverage"
        );
        let r1 = self.run(&q1);
        let mut coverage = HashMap::<String, f64>::new();
        for rec in &r1.records {
            let did = match rec.get("did") { Some(V::Property(P::String(s))) => s.clone(), _ => continue };
            let cov = match rec.get("coverage") { Some(V::Property(P::Integer(i))) => *i as f64, _ => 0.0 };
            coverage.insert(did, cov);
        }

        let q2 = format!(
            "MATCH (a:Drug)-[r:INTERACTS_WITH]->(b:Drug) \
             WHERE a.did IN [{id_list}] AND b.did IN [{id_list}] \
             RETURN a.did AS a, b.did AS b, r.severity_score AS sev"
        );
        let r2 = self.run(&q2);

        let efficacy: f64 = active_idx.iter().map(|&i| {
            let cov = coverage.get(self.drug_dids[i]).copied().unwrap_or(0.0);
            x[i] * cov
        }).sum();

        let did_to_idx: HashMap<&str, usize> = self.drug_dids.iter().enumerate()
            .map(|(i, &d)| (d, i)).collect();
        let mut risk = 0.0_f64;
        let mut bad = false;
        for rec in &r2.records {
            let a = match rec.get("a") { Some(V::Property(P::String(s))) => s.as_str(), _ => continue };
            let b = match rec.get("b") { Some(V::Property(P::String(s))) => s.as_str(), _ => continue };
            let sev = match rec.get("sev") {
                Some(V::Property(P::Float(f))) => *f,
                Some(V::Property(P::Integer(i))) => *i as f64,
                _ => 0.0,
            };
            if let (Some(&ia), Some(&ib)) = (did_to_idx.get(a), did_to_idx.get(b)) {
                risk += sev * x[ia] * x[ib];
                if sev >= 0.999 && x[ia] >= 0.05 && x[ib] >= 0.05 { bad = true; }
            }
        }
        let total_dose: f64 = (0..self.drug_dids.len()).map(|i| x[i]).sum();
        let penalty = if bad { 1e6 } else { 0.0 };
        vec![-efficacy + penalty, risk + penalty, total_dose + penalty]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uc2_built_graph_has_expected_shape() {
        let p = UC2DosingProblem::new();
        // smoke-check via cypher: 6 drugs, 7 interactions.
        let g = p.store.read().unwrap();
        let drugs = g.get_nodes_by_label(&crate::graph::Label::new("Drug"));
        assert_eq!(drugs.len(), 6);
        let ints = g.get_edges_by_type(&crate::graph::EdgeType::new("INTERACTS_WITH"));
        assert_eq!(ints.len(), 7);
    }

    #[test]
    fn uc2_empty_plan_returns_zero_vector() {
        let p = UC2DosingProblem::new();
        let x = Array1::zeros(6);
        assert_eq!(p.objectives(&x), vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn uc2_contraindicated_pair_gets_penalty() {
        let p = UC2DosingProblem::new();
        // D3 + D4 both active at 0.5, others zero — penalty must apply.
        let x = Array1::from(vec![0.0, 0.0, 0.0, 0.5, 0.5, 0.0]);
        let obj = p.objectives(&x);
        // Every objective carries the 1e6 penalty.
        for v in &obj { assert!(*v >= 1e5, "expected penalty in {:?}", obj); }
    }
}
