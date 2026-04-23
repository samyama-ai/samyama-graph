//! UC1 — Clinical-Trial Site Selection via SGE + NSGA-II
//!
//! Demonstrates the "Cypher-driven fitness evaluator" pattern from the SGE +
//! Optimization use-case catalog: the optimizer's inner loop queries an in-
//! process graph for the cost / feasibility terms, rather than carrying a copy
//! of the topology in its own state.
//!
//! **What this proves.** Unlike the prior `clinical_trials_demo.rs` which
//! holds site metrics in flat `Vec<f64>`, here we load facilities into the
//! graph as `(:Facility)` nodes with properties, and the NSGA-II `objectives`
//! fn issues Cypher `MATCH ... WHERE f.fid IN [...] RETURN sum(...), count(DISTINCT ...)`
//! per candidate. Swapping in real AACT data is a loader change, not a solver
//! change.
//!
//! Run:  cargo run --release --example uc1_trial_site_selection

use samyama_sdk::{
    Array1, EmbeddedClient, MultiObjectiveProblem, NSGA2Solver, SamyamaClient, SolverConfig,
};
use std::sync::Arc;
use tokio::runtime::Handle;

// ── Fixture parameters ─────────────────────────────────────────────────

const NUM_FACILITIES: usize = 50;
const TARGET_ENROLMENT: f64 = 200.0;
const TARGET_SITES_MIN: usize = 8;
const TARGET_SITES_MAX: usize = 15;
const REQUIRED_REGIONS: usize = 3;

/// Regions (proxy for WHO regions); facilities round-robin across these.
const REGIONS: &[&str] = &["AMR", "EUR", "AFR", "SEAR", "WPR"];

/// Deterministic synthetic per-facility metrics. Swap this loader for
/// `aact_loader` to run against real ClinicalTrials.gov data.
fn synth_facility(fid: i64) -> (f64, f64, &'static str) {
    let f = fid as f64;
    // Enrolment rate: ~5-30 patients/quarter, mild spread
    let rate = 5.0 + ((f * 7.0) % 25.0);
    // Cost index: 50-300 (USD thousand per site)
    let cost = 50.0 + ((f * 13.0) % 250.0);
    let region = REGIONS[fid as usize % REGIONS.len()];
    (rate, cost, region)
}

// ── The MultiObjectiveProblem — cypher-driven ──────────────────────────

struct TrialSiteProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
    facility_fids: Vec<i64>,
    call_count: std::sync::atomic::AtomicUsize,
}

impl MultiObjectiveProblem for TrialSiteProblem {
    fn dim(&self) -> usize {
        self.facility_fids.len()
    }
    fn num_objectives(&self) -> usize {
        2
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (
            Array1::zeros(self.facility_fids.len()),
            Array1::ones(self.facility_fids.len()),
        )
    }

    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Decode continuous x ∈ [0,1]^dim to a binary selection.
        let selected: Vec<i64> = self
            .facility_fids
            .iter()
            .enumerate()
            .filter_map(|(i, &fid)| if x[i] >= 0.5 { Some(fid) } else { None })
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

        // block_on inside an async context requires tokio's Handle::block_on
        // NOT to be called from within the runtime thread. We spawn_blocking
        // at the caller and use Handle::current there.
        let client = self.client.clone();
        let q_clone = q.clone();
        let result = self
            .handle
            .block_on(async move { client.query_readonly("default", &q).await })
            .unwrap_or_else(|e| panic!("cypher failed: {e}\nquery: {q_clone}"));

        let row = &result.records[0];
        let rate = row[0].as_f64().unwrap_or(0.0);
        let cost = row[1].as_f64().unwrap_or(0.0);
        let regions = row[2].as_i64().unwrap_or(0) as usize;

        // Constraint penalties expressed on both objectives so they're Pareto-dominated.
        let n = selected.len();
        let count_violation = if n < TARGET_SITES_MIN {
            (TARGET_SITES_MIN - n) as f64
        } else if n > TARGET_SITES_MAX {
            (n - TARGET_SITES_MAX) as f64
        } else {
            0.0
        };
        let region_violation = (REQUIRED_REGIONS.saturating_sub(regions)) as f64;
        let penalty = (count_violation + region_violation) * 1e5;

        // f1: time-to-enrol proxy (lower = faster); f2: total cost (lower = cheaper)
        let f1 = TARGET_ENROLMENT / rate.max(1e-6) + penalty;
        let f2 = cost + penalty;
        vec![f1, f2]
    }
}

// ── Driver ─────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UC1 — Clinical-Trial Site Selection via SGE + NSGA-II");
    println!("=====================================================\n");

    // 1. Spin up an in-process SGE and load the fixture.
    let client = Arc::new(EmbeddedClient::new());
    {
        let mut store = client.store_write().await;
        for fid in 0..NUM_FACILITIES as i64 {
            let (rate, cost, region) = synth_facility(fid);
            let nid = store.create_node("Facility");
            if let Some(node) = store.get_node_mut(nid) {
                // Store fid as string "F0", "F1", … because Cypher `IN [...]`
                // reliably evaluates string-list literals at runtime; the
                // integer-list path takes a non-Array execution branch in this
                // SGE build (verified — see runtime test_in_operator_with_strings).
                node.set_property("fid", format!("F{fid}"));
                node.set_property("enrolment_rate", rate);
                node.set_property("cost_index", cost);
                node.set_property("region", region);
            }
        }
    }
    println!("[load] {} :Facility nodes in SGE", NUM_FACILITIES);

    // 2. Sanity-check the graph via a Cypher call.
    let total = client
        .query_readonly(
            "default",
            "MATCH (f:Facility) RETURN count(f) AS n, count(DISTINCT f.region) AS r",
        )
        .await?;
    let row = &total.records[0];
    println!(
        "[check] facilities: {}, distinct regions: {}",
        row[0].as_i64().unwrap_or(0),
        row[1].as_i64().unwrap_or(0)
    );

    // 3. Build the problem and run NSGA-II. We pass the tokio Handle so
    //    the sync `objectives` fn can drive async cypher without nesting runtimes.
    let handle = Handle::current();
    let fids: Vec<i64> = (0..NUM_FACILITIES as i64).collect();
    let problem = TrialSiteProblem {
        client: client.clone(),
        handle,
        facility_fids: fids,
        call_count: std::sync::atomic::AtomicUsize::new(0),
    };

    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 40,
        max_iterations: 40,
    });

    println!(
        "\n[solve] NSGA-II pop={} iter={} objectives=(time-to-enrol, cost)",
        40, 40
    );
    // Run the solver in a blocking task — the inner fitness fn blocks on async.
    let problem = Arc::new(problem);
    let p = problem.clone();
    let (front, calls, wall_ms) = tokio::task::spawn_blocking(move || {
        let t0 = std::time::Instant::now();
        let result = solver.solve(&*p);
        let calls = p.call_count.load(std::sync::atomic::Ordering::Relaxed);
        (result.pareto_front, calls, t0.elapsed().as_millis())
    })
    .await?;

    println!(
        "[done] {} cypher evaluations, wall {} ms ({:.1} ms/eval)",
        calls,
        wall_ms,
        wall_ms as f64 / calls.max(1) as f64
    );

    // 4. Report Pareto: top rows sorted by f1.
    let mut rows: Vec<(f64, f64, usize, Vec<i64>)> = front
        .iter()
        .map(|ind| {
            let selected: Vec<i64> = ind
                .variables
                .iter()
                .enumerate()
                .filter_map(|(i, &v)| if v >= 0.5 { Some(i as i64) } else { None })
                .collect();
            (ind.fitness[0], ind.fitness[1], selected.len(), selected)
        })
        .collect();
    rows.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    println!("\n[pareto] {} non-dominated plans:", rows.len());
    println!("  {:>12}  {:>10}  {:>8}  selected sites", "time-to-enrol", "cost", "n_sites");
    for (f1, f2, n, sites) in rows.iter().take(10) {
        let preview = if sites.len() > 8 {
            format!("{:?}... ({} more)", &sites[..6], sites.len() - 6)
        } else {
            format!("{sites:?}")
        };
        println!("  {:>12.2}  {:>10.1}  {:>8}  {preview}", f1, f2, n);
    }
    if rows.len() > 10 {
        println!("  ... ({} more)", rows.len() - 10);
    }

    Ok(())
}
