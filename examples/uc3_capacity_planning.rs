//! UC3 — Hospital Network Capacity Planning via SGE + BMR
//!
//! Single-objective capacity allocation across 5 facilities. The decision
//! vector is `(Δbeds, Δnurses, Δdoctors)` per facility — 15 continuous
//! variables clamped to realistic ranges. The optimizer's fitness queries
//! SGE for current beds + last-period admissions per facility and combines
//! the candidate Δ with an analytical M/M/c wait-time model.
//!
//! Constraints (as external penalty):
//! - Total cost of changes ≤ budget.
//! - Nurses/bed ≥ 1.2, doctors/bed ≥ 0.15 after change.
//! - Post-change beds > 0.
//!
//! Success property (locked in `tests/uc3_capacity_planning_test.rs`):
//! increasing the budget strictly reduces best wait-time until the ratio
//! floors bind.
//!
//! Run:  cargo run --release --example uc3_capacity_planning
//!
//! [[Use-Case 3 — Hospital Network Capacity Planning]]

use samyama_sdk::{
    Array1, BMRSolver, EmbeddedClient, Problem, SamyamaClient, SolverConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;

// ── Fixture ────────────────────────────────────────────────────────────

/// (fid, current_beds, current_nurses, current_doctors, quarterly_admissions)
const FACILITIES: &[(&str, f64, f64, f64, f64)] = &[
    ("H0", 120.0, 150.0,  20.0, 2400.0),
    ("H1",  80.0,  96.0,  13.0, 1800.0),
    ("H2", 200.0, 260.0,  34.0, 4200.0),
    ("H3",  60.0,  75.0,  10.0, 1500.0),
    ("H4", 150.0, 180.0,  24.0, 3100.0),
];
const MEAN_SERVICE_DAYS: f64 = 4.0;           // avg length-of-stay
const QUARTER_DAYS: f64 = 90.0;
const DEMAND_SURGE: f64 = 1.20;               // 20 % growth forecast
const COST_BED: f64 = 80_000.0;
const COST_NURSE: f64 = 120_000.0;
const COST_DOCTOR: f64 = 280_000.0;
const BUDGET_DEFAULT: f64 = 15_000_000.0;     // 15 M USD
const NURSE_BED_FLOOR: f64 = 1.2;
const DOCTOR_BED_FLOOR: f64 = 0.15;

// Bounds on deltas per facility (beds, nurses, doctors).
const DBEDS_LO: f64 = -5.0;
const DBEDS_HI: f64 = 30.0;
const DNURSES_LO: f64 = -10.0;
const DNURSES_HI: f64 = 40.0;
const DDOCS_LO: f64 = -2.0;
const DDOCS_HI: f64 = 8.0;

// ── M/M/c wait-time ────────────────────────────────────────────────────

/// Approximate Erlang-C expected wait (in days) for an M/M/c queue.
/// `lambda` = arrival rate (patients/day), `mu` = 1/mean_service_days per
/// bed, `c` = number of beds. Returns 0 if rho >= 1 is handled by caller.
fn mmc_wait(lambda: f64, mu: f64, c: f64) -> f64 {
    if c <= 0.0 || mu <= 0.0 || lambda <= 0.0 {
        return 1e6;
    }
    let rho = lambda / (c * mu);
    if rho >= 1.0 {
        // Unstable queue — heavy penalty (proportional to overload).
        return 1000.0 * (rho - 1.0 + 0.01).max(0.01);
    }
    // Erlang-C probability of waiting.
    let a = lambda / mu;
    let mut sum = 0.0_f64;
    let mut term = 1.0_f64;
    for k in 0..(c as usize) {
        if k > 0 {
            term *= a / (k as f64);
        }
        sum += term;
    }
    let last = term * (a / c) / (1.0 - rho);
    let p_wait = last / (sum + last);
    // Expected wait in service-time units → convert to days.
    p_wait / (c * mu * (1.0 - rho))
}

// ── Problem ────────────────────────────────────────────────────────────

struct CapacityProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
    facility_fids: Vec<&'static str>,
    budget: f64,
    call_count: std::sync::atomic::AtomicUsize,
}

impl Problem for CapacityProblem {
    fn dim(&self) -> usize {
        3 * self.facility_fids.len()
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        let n = self.facility_fids.len();
        let mut lo = Array1::zeros(3 * n);
        let mut hi = Array1::zeros(3 * n);
        for i in 0..n {
            lo[3 * i] = DBEDS_LO;
            hi[3 * i] = DBEDS_HI;
            lo[3 * i + 1] = DNURSES_LO;
            hi[3 * i + 1] = DNURSES_HI;
            lo[3 * i + 2] = DDOCS_LO;
            hi[3 * i + 2] = DDOCS_HI;
        }
        (lo, hi)
    }

    fn objective(&self, x: &Array1<f64>) -> f64 {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Single Cypher query: pull current capacity + admissions per facility.
        let id_list = self
            .facility_fids
            .iter()
            .map(|f| format!("\"{f}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let q = format!(
            "MATCH (f:Facility) WHERE f.fid IN [{id_list}] \
             RETURN f.fid AS fid, f.beds AS beds, f.nurses AS nurses, \
                    f.doctors AS doctors, f.admissions AS admissions"
        );
        let r = self.run_cypher(&q);

        let mut state: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
        for row in &r.records {
            let fid = row[0].as_str().unwrap_or("").to_string();
            let beds = row[1].as_f64().unwrap_or(0.0);
            let nurses = row[2].as_f64().unwrap_or(0.0);
            let doctors = row[3].as_f64().unwrap_or(0.0);
            let adm = row[4].as_f64().unwrap_or(0.0);
            state.insert(fid, (beds, nurses, doctors, adm));
        }

        let mu = 1.0 / MEAN_SERVICE_DAYS;
        let mut total_wait_patient_days = 0.0_f64;
        let mut total_cost = 0.0_f64;
        let mut ratio_penalty = 0.0_f64;
        let mut nonneg_penalty = 0.0_f64;

        for (i, fid) in self.facility_fids.iter().enumerate() {
            let (beds, nurses, doctors, adm) = state.get(*fid).copied().unwrap_or((0.0, 0.0, 0.0, 0.0));
            let d_beds = x[3 * i];
            let d_nurses = x[3 * i + 1];
            let d_doctors = x[3 * i + 2];

            let new_beds = beds + d_beds;
            let new_nurses = nurses + d_nurses;
            let new_doctors = doctors + d_doctors;

            total_cost += COST_BED * d_beds.max(0.0)
                + COST_NURSE * d_nurses.max(0.0)
                + COST_DOCTOR * d_doctors.max(0.0);

            if new_beds <= 0.0 { nonneg_penalty += (-new_beds + 1.0) * 1e6; }
            if new_nurses < 0.0 { nonneg_penalty += (-new_nurses) * 1e6; }
            if new_doctors < 0.0 { nonneg_penalty += (-new_doctors) * 1e6; }

            // Post-change ratios.
            if new_beds > 0.0 {
                let nb = new_nurses / new_beds;
                if nb < NURSE_BED_FLOOR {
                    ratio_penalty += (NURSE_BED_FLOOR - nb) * 5e5 * new_beds.max(1.0);
                }
                let db = new_doctors / new_beds;
                if db < DOCTOR_BED_FLOOR {
                    ratio_penalty += (DOCTOR_BED_FLOOR - db) * 5e5 * new_beds.max(1.0);
                }
            }

            let lambda = adm * DEMAND_SURGE / QUARTER_DAYS;
            let wait_days = mmc_wait(lambda, mu, new_beds.max(0.1));
            total_wait_patient_days += wait_days * (adm * DEMAND_SURGE);
        }

        let budget_overrun = (total_cost - self.budget).max(0.0);
        let budget_penalty = budget_overrun * 10.0;

        total_wait_patient_days + budget_penalty + ratio_penalty + nonneg_penalty
    }
}

impl CapacityProblem {
    fn run_cypher(&self, q: &str) -> samyama_sdk::QueryResult {
        let client = self.client.clone();
        let q_owned = q.to_string();
        self.handle
            .block_on(async move { client.query_readonly("default", &q_owned).await })
            .unwrap_or_else(|e| panic!("cypher: {e}\nquery: {q}"))
    }
}

// ── Driver ─────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UC3 — Hospital Network Capacity Planning via SGE + BMR");
    println!("======================================================\n");

    let client = Arc::new(EmbeddedClient::new());
    {
        let mut store = client.store_write().await;
        for (fid, beds, nurses, doctors, adm) in FACILITIES {
            let nid = store.create_node("Facility");
            if let Some(node) = store.get_node_mut(nid) {
                node.set_property("fid", *fid);
                node.set_property("beds", *beds);
                node.set_property("nurses", *nurses);
                node.set_property("doctors", *doctors);
                node.set_property("admissions", *adm);
            }
        }
    }
    println!(
        "[load] {} :Facility nodes (beds, nurses, doctors, admissions)",
        FACILITIES.len()
    );

    let fids: Vec<&'static str> = FACILITIES.iter().map(|(f, ..)| *f).collect();

    // Solve at three budget levels to show monotonic wait-time improvement.
    let mut summary: Vec<(f64, f64, f64, Vec<f64>)> = Vec::new();
    for &budget in &[5_000_000.0_f64, BUDGET_DEFAULT, 40_000_000.0_f64] {
        let problem = Arc::new(CapacityProblem {
            client: client.clone(),
            handle: Handle::current(),
            facility_fids: fids.clone(),
            budget,
            call_count: std::sync::atomic::AtomicUsize::new(0),
        });
        let solver = BMRSolver::new(SolverConfig {
            population_size: 40,
            max_iterations: 60,
        });
        println!("\n[solve] BMR pop=40 iter=60, budget=${:.1}M", budget / 1e6);
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

        // Report cost / wait for the best plan.
        let plan: Vec<f64> = best.best_variables.iter().copied().collect();
        let mut total_cost = 0.0;
        for i in 0..fids.len() {
            total_cost += COST_BED * plan[3 * i].max(0.0)
                + COST_NURSE * plan[3 * i + 1].max(0.0)
                + COST_DOCTOR * plan[3 * i + 2].max(0.0);
        }
        println!(
            "[best] fitness={:.0} patient-days, cost=${:.2}M",
            best.best_fitness,
            total_cost / 1e6
        );
        for (i, fid) in fids.iter().enumerate() {
            println!(
                "   {fid}: Δbeds={:+.1} Δnurses={:+.1} Δdoctors={:+.1}",
                plan[3 * i], plan[3 * i + 1], plan[3 * i + 2]
            );
        }
        summary.push((budget, best.best_fitness, total_cost, plan));
    }

    // Sanity: higher budget → better (lower) fitness.
    println!("\n[sweep] budget → best fitness");
    for (b, f, c, _) in &summary {
        println!("  ${:>5.1}M budget → fitness {:>10.0} (spent ${:.2}M)",
            b / 1e6, f, c / 1e6);
    }
    let fits: Vec<f64> = summary.iter().map(|s| s.1).collect();
    println!(
        "[check] monotone improvement: {} (small→mid {}, mid→large {})",
        fits[0] >= fits[1] && fits[1] >= fits[2],
        fits[0] >= fits[1],
        fits[1] >= fits[2]
    );

    Ok(())
}
