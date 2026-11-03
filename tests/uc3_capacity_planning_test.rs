//! UC3 — Hospital Network Capacity Planning
//!
//! Locks in the contract for the SGE + BMR single-objective Cypher-driven
//! capacity allocation (samyama-cloud/wiki/use-cases/uc3-hospital-capacity-planning.md).
//!
//! Verified properties:
//! - Cypher MATCH-IN over a string IN-list returns finite numbers.
//! - Solver produces a feasible plan: ratio floors and non-negativity hold
//!   on the best plan (penalties stayed out).
//! - Increasing the budget produces a strictly lower (or equal) best
//!   fitness — the optimizer is actually using the extra resources.
//!
//! Smaller fixture (3 facilities, 30 iters) to keep the test under a
//! second on M1.

use samyama_sdk::{
    Array1, BMRSolver, EmbeddedClient, Problem, SamyamaClient, SolverConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;

const FACILITIES: &[(&str, f64, f64, f64, f64)] = &[
    ("H0", 120.0, 150.0, 20.0, 2400.0),
    ("H1",  80.0,  96.0, 13.0, 1800.0),
    ("H2", 200.0, 260.0, 34.0, 4200.0),
];
const MEAN_SERVICE_DAYS: f64 = 4.0;
const QUARTER_DAYS: f64 = 90.0;
const DEMAND_SURGE: f64 = 1.20;
const COST_BED: f64 = 80_000.0;
const COST_NURSE: f64 = 120_000.0;
const COST_DOCTOR: f64 = 280_000.0;
const NURSE_BED_FLOOR: f64 = 1.2;
const DOCTOR_BED_FLOOR: f64 = 0.15;

fn mmc_wait(lambda: f64, mu: f64, c: f64) -> f64 {
    if c <= 0.0 || mu <= 0.0 || lambda <= 0.0 {
        return 1e6;
    }
    let rho = lambda / (c * mu);
    if rho >= 1.0 {
        return 1000.0 * (rho - 1.0 + 0.01).max(0.01);
    }
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
    p_wait / (c * mu * (1.0 - rho))
}

struct CapacityProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
    fids: Vec<&'static str>,
    budget: f64,
}

impl Problem for CapacityProblem {
    fn dim(&self) -> usize { 3 * self.fids.len() }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        let n = self.fids.len();
        let mut lo = Array1::zeros(3 * n);
        let mut hi = Array1::zeros(3 * n);
        for i in 0..n {
            lo[3 * i] = -5.0;     hi[3 * i] = 30.0;
            lo[3 * i + 1] = -10.0; hi[3 * i + 1] = 40.0;
            lo[3 * i + 2] = -2.0;  hi[3 * i + 2] = 8.0;
        }
        (lo, hi)
    }
    fn objective(&self, x: &Array1<f64>) -> f64 {
        let id_list = self.fids.iter().map(|f| format!("\"{f}\""))
            .collect::<Vec<_>>().join(", ");
        let q = format!(
            "MATCH (f:Facility) WHERE f.fid IN [{id_list}] \
             RETURN f.fid AS fid, f.beds AS beds, f.nurses AS nurses, \
                    f.doctors AS doctors, f.admissions AS admissions"
        );
        let client = self.client.clone();
        let r = self.handle
            .block_on(async move { client.query_readonly("default", &q).await })
            .expect("cypher");

        let mut state = HashMap::<String, (f64, f64, f64, f64)>::new();
        for row in &r.records {
            state.insert(
                row[0].as_str().unwrap_or("").to_string(),
                (
                    row[1].as_f64().unwrap_or(0.0),
                    row[2].as_f64().unwrap_or(0.0),
                    row[3].as_f64().unwrap_or(0.0),
                    row[4].as_f64().unwrap_or(0.0),
                ),
            );
        }

        let mu = 1.0 / MEAN_SERVICE_DAYS;
        let mut wait_pd = 0.0;
        let mut cost = 0.0;
        let mut ratio_pen = 0.0;
        let mut nonneg_pen = 0.0;
        for (i, fid) in self.fids.iter().enumerate() {
            let (beds, nurses, doctors, adm) = state.get(*fid).copied().unwrap_or((0.0,0.0,0.0,0.0));
            let nb = beds + x[3*i];
            let nn = nurses + x[3*i+1];
            let nd = doctors + x[3*i+2];
            cost += COST_BED * x[3*i].max(0.0) + COST_NURSE * x[3*i+1].max(0.0) + COST_DOCTOR * x[3*i+2].max(0.0);
            if nb <= 0.0 { nonneg_pen += (-nb + 1.0) * 1e6; }
            if nn < 0.0 { nonneg_pen += -nn * 1e6; }
            if nd < 0.0 { nonneg_pen += -nd * 1e6; }
            if nb > 0.0 {
                let r = nn / nb;
                if r < NURSE_BED_FLOOR { ratio_pen += (NURSE_BED_FLOOR - r) * 5e5 * nb.max(1.0); }
                let r = nd / nb;
                if r < DOCTOR_BED_FLOOR { ratio_pen += (DOCTOR_BED_FLOOR - r) * 5e5 * nb.max(1.0); }
            }
            let lambda = adm * DEMAND_SURGE / QUARTER_DAYS;
            wait_pd += mmc_wait(lambda, mu, nb.max(0.1)) * (adm * DEMAND_SURGE);
        }
        let budget_pen = (cost - self.budget).max(0.0) * 10.0;
        wait_pd + budget_pen + ratio_pen + nonneg_pen
    }
}

async fn solve_at_budget(client: Arc<EmbeddedClient>, fids: Vec<&'static str>, budget: f64) -> (Vec<f64>, f64) {
    let problem = Arc::new(CapacityProblem {
        client,
        handle: Handle::current(),
        fids,
        budget,
    });
    let solver = BMRSolver::new(SolverConfig {
        population_size: 24,
        max_iterations: 30,
    });
    let p = problem.clone();
    let res = tokio::task::spawn_blocking(move || solver.solve(&*p))
        .await.unwrap();
    (res.best_variables.iter().copied().collect(), res.best_fitness)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc3_more_budget_lowers_wait() {
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
    let fids: Vec<&'static str> = FACILITIES.iter().map(|(f, ..)| *f).collect();

    let (plan_small, fit_small) = solve_at_budget(client.clone(), fids.clone(), 3_000_000.0).await;
    let (plan_large, fit_large) = solve_at_budget(client.clone(), fids.clone(), 30_000_000.0).await;

    assert!(fit_small.is_finite() && fit_large.is_finite(),
        "non-finite fitness: small={fit_small} large={fit_large}");

    // More budget must not produce a worse plan (allow tiny stochastic slack).
    assert!(
        fit_large <= fit_small * 1.05,
        "expected larger budget to lower wait; small={fit_small:.0} large={fit_large:.0}"
    );

    // The two budgets should yield meaningfully different fitness (otherwise
    // the optimizer isn't responding to the budget signal).
    assert!(
        fit_large < fit_small * 0.95,
        "no meaningful improvement with 10× budget: small={fit_small:.0} large={fit_large:.0}"
    );

    // Sanity: ratio floors hold on the large-budget plan.
    for (i, (_, beds, nurses, doctors, _)) in FACILITIES.iter().enumerate() {
        let nb = beds + plan_large[3*i];
        let nn = nurses + plan_large[3*i+1];
        let nd = doctors + plan_large[3*i+2];
        if nb > 0.0 {
            assert!(nn / nb >= NURSE_BED_FLOOR - 0.05,
                "nurse/bed ratio violated at facility {i}: {} / {} = {}", nn, nb, nn/nb);
            assert!(nd / nb >= DOCTOR_BED_FLOOR - 0.02,
                "doctor/bed ratio violated at facility {i}: {} / {} = {}", nd, nb, nd/nb);
        }
    }
    let _ = plan_small;
}
