//! UC1 — Clinical-Trial Site Selection
//!
//! Locks in the contract for the SGE + NSGA-II "Cypher-driven fitness
//! evaluator" pattern documented in
//! samyama-cloud/wiki/use-cases/uc1-clinical-trial-site-selection.md.
//!
//! Verified properties:
//! - Cypher aggregation over a dynamic IN-list returns finite numbers.
//! - The Pareto front has > 1 point (otherwise NSGA-II is degenerate).
//! - All Pareto plans satisfy the count + region constraints (no penalty
//!   has leaked through into the front).
//! - Selection is non-trivial: at least one plan picks ≤ 60 % of the
//!   facilities (i.e. the constraint isn't just "pick everything").

use samyama_sdk::{
    Array1, EmbeddedClient, MultiObjectiveProblem, NSGA2Solver, SamyamaClient, SolverConfig,
};
use std::sync::Arc;
use tokio::runtime::Handle;

const NUM_FACILITIES: usize = 30;
const TARGET_ENROLMENT: f64 = 200.0;
const TARGET_SITES_MIN: usize = 6;
const TARGET_SITES_MAX: usize = 12;
const REQUIRED_REGIONS: usize = 3;
const REGIONS: &[&str] = &["AMR", "EUR", "AFR", "SEAR", "WPR"];

fn synth_facility(fid: i64) -> (f64, f64, &'static str) {
    let f = fid as f64;
    let rate = 5.0 + ((f * 7.0) % 25.0);
    let cost = 50.0 + ((f * 13.0) % 250.0);
    let region = REGIONS[fid as usize % REGIONS.len()];
    (rate, cost, region)
}

struct TrialSiteProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
    n: usize,
}

impl MultiObjectiveProblem for TrialSiteProblem {
    fn dim(&self) -> usize {
        self.n
    }
    fn num_objectives(&self) -> usize {
        2
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.n), Array1::ones(self.n))
    }

    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let selected: Vec<i64> = (0..self.n as i64)
            .filter(|i| x[*i as usize] >= 0.5)
            .collect();
        if selected.is_empty() {
            return vec![1e9, 1e9];
        }
        let id_list = selected
            .iter()
            .map(|i| format!("\"F{i}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let q = format!(
            "MATCH (f:Facility) WHERE f.fid IN [{}] \
             RETURN sum(f.enrolment_rate) AS rate, \
                    sum(f.cost_index) AS cost, \
                    count(DISTINCT f.region) AS regions",
            id_list
        );
        let client = self.client.clone();
        let result = self
            .handle
            .block_on(async move { client.query_readonly("default", &q).await })
            .expect("cypher");
        let row = &result.records[0];
        let rate = row[0].as_f64().unwrap_or(0.0);
        let cost = row[1].as_f64().unwrap_or(0.0);
        let regions = row[2].as_i64().unwrap_or(0) as usize;
        let n = selected.len();
        let count_v = if n < TARGET_SITES_MIN {
            (TARGET_SITES_MIN - n) as f64
        } else if n > TARGET_SITES_MAX {
            (n - TARGET_SITES_MAX) as f64
        } else {
            0.0
        };
        let region_v = (REQUIRED_REGIONS.saturating_sub(regions)) as f64;
        let penalty = (count_v + region_v) * 1e5;
        vec![
            TARGET_ENROLMENT / rate.max(1e-6) + penalty,
            cost + penalty,
        ]
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uc1_pareto_front_satisfies_constraints() {
    let client = Arc::new(EmbeddedClient::new());
    {
        let mut store = client.store_write().await;
        for fid in 0..NUM_FACILITIES as i64 {
            let (rate, cost, region) = synth_facility(fid);
            let nid = store.create_node("Facility");
            if let Some(node) = store.get_node_mut(nid) {
                node.set_property("fid", format!("F{fid}"));
                node.set_property("enrolment_rate", rate);
                node.set_property("cost_index", cost);
                node.set_property("region", region);
            }
        }
    }

    let problem = Arc::new(TrialSiteProblem {
        client: client.clone(),
        handle: Handle::current(),
        n: NUM_FACILITIES,
    });
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 20,
        max_iterations: 20,
    });

    let p = problem.clone();
    let front = tokio::task::spawn_blocking(move || solver.solve(&*p).pareto_front)
        .await
        .unwrap();

    assert!(!front.is_empty(), "Pareto front must be non-empty");
    assert!(
        front.len() >= 2,
        "Expected diverse front; got {} plans",
        front.len()
    );

    // Every Pareto plan must satisfy the structural constraints (no penalty leaked).
    for ind in &front {
        let n_selected: usize = ind.variables.iter().filter(|&&v| v >= 0.5).count();
        assert!(
            n_selected >= TARGET_SITES_MIN && n_selected <= TARGET_SITES_MAX,
            "front plan picked {} sites — outside [{}, {}]",
            n_selected,
            TARGET_SITES_MIN,
            TARGET_SITES_MAX,
        );
        // Penalty makes objectives explode (>= 1e5) when constraint violated.
        assert!(
            ind.fitness[0] < 1e4 && ind.fitness[1] < 1e4,
            "front plan has penalty in fitness: {:?}",
            ind.fitness,
        );
    }

    // Sanity: non-trivial selection (at least one plan picks fewer than the
    // upper bound, proving the optimizer is exploring the cost end).
    let any_undersized = front
        .iter()
        .any(|ind| ind.variables.iter().filter(|&&v| v >= 0.5).count() < TARGET_SITES_MAX);
    assert!(
        any_undersized,
        "All Pareto plans pick the maximum {} sites — front is degenerate",
        TARGET_SITES_MAX
    );
}
