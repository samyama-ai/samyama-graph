//! UC1-real — Clinical-Trial Site Selection on **live AACT data**
//!
//! Counterpart to `examples/uc1_trial_site_selection.rs` (synthetic fixture)
//! that targets a deployed Samyama instance holding the AACT KG.
//! Picks real ClinicalTrials.gov Sites, derives per-site enrolment rate
//! and a country-tier cost proxy from the graph, then runs NSGA-II with
//! the same constraints as the synthetic UC1 (site-count bounds + region
//! diversity floor).
//!
//! Because the optimizer inner loop is ~1600 Cypher calls per run, we
//! materialise per-site metrics ONCE via three HTTP round-trips and then
//! score plans in pure-sync Rust (same network-friendly pattern as
//! UC4-real).
//!
//! Run:  SAMYAMA_URL=http://<host>:8080 cargo run --release --example uc1_aact_real
//!
//! [[Use-Case 1 — Clinical-Trial Site Selection]]
//! [[SGE + Optimization — Phase 2 Results]]

use samyama_sdk::{
    Array1, MultiObjectiveProblem, NSGA2Solver, RemoteClient, SamyamaClient, SolverConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;

// ── Constraint shape (mirrors synthetic UC1) ──────────────────────────

const NUM_SITES: usize = 50;
const TARGET_ENROLMENT_PER_QUARTER: f64 = 2000.0;
const TARGET_SITES_MIN: usize = 8;
const TARGET_SITES_MAX: usize = 15;
const REQUIRED_REGIONS: usize = 3;

// ── Country → WHO-ish region map (keeps the region-diversity constraint
// ── meaningful without pulling a full ontology)

fn region_of(country: &str) -> &'static str {
    match country {
        // Americas
        "United States" | "Canada" | "Mexico" | "Brazil" | "Argentina" | "Chile"
        | "Colombia" | "Peru" | "Puerto Rico" | "Costa Rica" | "Guatemala" => "AMR",
        // Europe
        "United Kingdom" | "Germany" | "France" | "Spain" | "Italy" | "Netherlands"
        | "Belgium" | "Sweden" | "Norway" | "Denmark" | "Finland" | "Poland"
        | "Austria" | "Switzerland" | "Czechia" | "Hungary" | "Romania" | "Greece"
        | "Ireland" | "Portugal" | "Russia" | "Ukraine" | "Bulgaria" | "Serbia"
        | "Turkey (Türkiye)" | "Israel" => "EUR",
        // Africa
        "Egypt" | "South Africa" | "Nigeria" | "Kenya" | "Morocco" | "Tanzania"
        | "Ethiopia" | "Ghana" | "Uganda" => "AFR",
        // South-East Asia
        "India" | "Thailand" | "Indonesia" | "Bangladesh" | "Sri Lanka" | "Myanmar"
        | "Nepal" => "SEAR",
        // Western Pacific
        "China" | "Japan" | "Korea, Republic of" | "Australia" | "New Zealand"
        | "Singapore" | "Malaysia" | "Philippines" | "Vietnam" | "Taiwan" | "Hong Kong" => "WPR",
        // Everything else lumped for the purposes of the region-diversity constraint.
        _ => "OTH",
    }
}

// Country-tier cost proxy: AACT doesn't carry cost, and there's no public
// per-trial-site cost dataset, so we derive a tier from country GDP bracket
// (USD thousand/site per year; order-of-magnitude figures good enough for
// the optimizer to differentiate AMR/EUR sites from SEAR/AFR).
fn country_cost(country: &str) -> f64 {
    match country {
        "United States" | "Switzerland" => 320.0,
        "Canada" | "Germany" | "United Kingdom" | "Japan" | "France" | "Australia"
        | "Netherlands" | "Sweden" | "Norway" | "Denmark" | "Finland" | "Belgium"
        | "Austria" | "Ireland" | "Israel" | "Korea, Republic of" | "Singapore"
        | "New Zealand" => 220.0,
        "Italy" | "Spain" | "Portugal" | "Czechia" | "Greece" | "Taiwan" | "Hong Kong"
        | "Poland" | "Hungary" | "Slovenia" => 160.0,
        "Brazil" | "Mexico" | "Argentina" | "Chile" | "Russia" | "Turkey (Türkiye)"
        | "Malaysia" | "China" | "Thailand" | "South Africa" | "Romania" | "Bulgaria"
        | "Serbia" | "Ukraine" => 100.0,
        "India" | "Indonesia" | "Philippines" | "Vietnam" | "Egypt" | "Nigeria"
        | "Kenya" | "Morocco" | "Colombia" | "Peru" | "Bangladesh" | "Pakistan"
        | "Sri Lanka" | "Ghana" | "Tanzania" | "Ethiopia" => 60.0,
        _ => 120.0,
    }
}

// ── Problem ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Site {
    fid: usize,
    facility: String,
    country: String,
    region: &'static str,
    enrolment_per_quarter: f64,
    cost_index: f64,
}

struct TrialSiteProblem {
    sites: Vec<Site>,
}

impl MultiObjectiveProblem for TrialSiteProblem {
    fn dim(&self) -> usize { self.sites.len() }
    fn num_objectives(&self) -> usize { 2 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.sites.len()), Array1::ones(self.sites.len()))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let selected: Vec<&Site> = self.sites.iter().enumerate()
            .filter_map(|(i, s)| if x[i] >= 0.5 { Some(s) } else { None })
            .collect();
        if selected.is_empty() { return vec![1e9, 1e9]; }

        let rate: f64 = selected.iter().map(|s| s.enrolment_per_quarter).sum();
        let cost: f64 = selected.iter().map(|s| s.cost_index).sum();
        let mut regions = std::collections::HashSet::new();
        for s in &selected { regions.insert(s.region); }

        let n = selected.len();
        let count_v = if n < TARGET_SITES_MIN {
            (TARGET_SITES_MIN - n) as f64
        } else if n > TARGET_SITES_MAX {
            (n - TARGET_SITES_MAX) as f64
        } else { 0.0 };
        let region_v = (REQUIRED_REGIONS.saturating_sub(regions.len())) as f64;
        let penalty = (count_v + region_v) * 1e5;

        vec![
            TARGET_ENROLMENT_PER_QUARTER / rate.max(1e-6) + penalty,
            cost + penalty,
        ]
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
    println!("UC1-real — Trial Site Selection on live AACT @ {url}");
    println!("=====================================================\n");
    let client = RemoteClient::new(&url);

    // 1. Top sites by total trials conducted. Also pull facility / country
    // so we can derive region + cost_tier locally (avoids a second query).
    let q = format!(
        "MATCH (t:ClinicalTrial)-[:CONDUCTED_AT]->(s:Site) \
         RETURN s.facility AS facility, s.country AS country, count(DISTINCT t) AS trials \
         ORDER BY trials DESC LIMIT {}",
        NUM_SITES
    );
    let r = client.query_readonly("default", &q).await?;
    let site_rows: Vec<(String, String, i64)> = r.records.iter()
        .map(|row| (as_str(&row[0]), as_str(&row[1]), row[2].as_i64().unwrap_or(0)))
        .filter(|(f, _, _)| !f.is_empty())
        .collect();
    println!("[probe] {} sites by trial activity", site_rows.len());

    // 2. For each selected site, sum up enrolment across trials (it's a
    // per-trial target in AACT). One bulk query keyed on the facility name.
    let names_list = site_rows.iter()
        .map(|(f, _, _)| format!("\"{}\"", f.replace('"', "\\\"")))
        .collect::<Vec<_>>().join(", ");
    let q = format!(
        "MATCH (t:ClinicalTrial)-[:CONDUCTED_AT]->(s:Site) \
         WHERE s.facility IN [{names_list}] \
         RETURN s.facility AS facility, sum(t.enrollment) AS total_enrolment, \
                count(DISTINCT t) AS trials"
    );
    let r = client.query_readonly("default", &q).await?;
    let mut enrolment: HashMap<String, (f64, i64)> = HashMap::new();
    for row in &r.records {
        enrolment.insert(as_str(&row[0]), (as_f(&row[1]), row[2].as_i64().unwrap_or(1)));
    }

    // AACT trials span years → convert total_enrolment to "per-quarter" by
    // dividing by (trials × ~4 quarters/trial), a crude but consistent proxy.
    let sites: Vec<Site> = site_rows.iter().enumerate().map(|(i, (f, c, _))| {
        let (total, trials) = enrolment.get(f).copied().unwrap_or((0.0, 1));
        let epq = if trials > 0 { total / (trials as f64 * 4.0) } else { 0.0 };
        Site {
            fid: i,
            facility: f.clone(),
            country: c.clone(),
            region: region_of(c),
            enrolment_per_quarter: epq,
            cost_index: country_cost(c),
        }
    }).collect();

    println!("[probe] sample sites:");
    println!("  {:>4} {:>7} {:>5}  {:<22}  {:<50}", "idx", "enrol/q", "cost", "region", "facility");
    for s in sites.iter().take(8) {
        println!("  {:>4} {:>7.1} {:>5.0}  {:<22}  {:<50}",
            s.fid, s.enrolment_per_quarter, s.cost_index,
            format!("{} ({})", s.region, s.country),
            &s.facility[..s.facility.len().min(50)]);
    }
    let mut regions = std::collections::HashSet::new();
    for s in &sites { regions.insert(s.region); }
    println!("[probe] regions represented: {:?}", regions);

    // 3. NSGA-II, same shape as synthetic UC1.
    let problem = Arc::new(TrialSiteProblem { sites: sites.clone() });
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 40,
        max_iterations: 40,
    });
    println!("\n[solve] NSGA-II pop=40 iter=40, 2 objectives (time-to-enrol, cost)");
    let p = problem.clone();
    let (front, wall_ms) = tokio::task::spawn_blocking(move || {
        let t0 = std::time::Instant::now();
        let res = solver.solve(&*p);
        (res.pareto_front, t0.elapsed().as_millis())
    }).await?;
    println!("[done] {} plans, wall {wall_ms} ms", front.len());

    let mut rows: Vec<(f64, f64, usize, Vec<usize>, std::collections::HashSet<&str>)> = front.iter()
        .map(|ind| {
            let sel: Vec<usize> = ind.variables.iter().enumerate()
                .filter_map(|(i, &v)| if v >= 0.5 { Some(i) } else { None })
                .collect();
            let regs: std::collections::HashSet<&str> = sel.iter()
                .map(|&i| sites[i].region).collect();
            (ind.fitness[0], ind.fitness[1], sel.len(), sel, regs)
        })
        .collect();
    rows.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    println!("\n[pareto] top 10 by time-to-enrol:");
    println!("  {:>12}  {:>10}  {:>7}  {:>7}   regions", "time-to-enrol", "cost_k", "n_sites", "enrol/q");
    for (f1, f2, n, sel, regs) in rows.iter().take(10) {
        let sum_rate: f64 = sel.iter().map(|&i| sites[i].enrolment_per_quarter).sum();
        println!("  {:>12.2}  {:>10.0}  {:>7}  {:>7.1}   {:?}",
            f1, f2, n, sum_rate, regs);
    }

    // Sanity: every Pareto plan meets the constraints.
    let violations = rows.iter()
        .filter(|(_, _, n, _, regs)|
            *n < TARGET_SITES_MIN || *n > TARGET_SITES_MAX || regs.len() < REQUIRED_REGIONS)
        .count();
    println!("\n[check] front plans violating [{TARGET_SITES_MIN}..{TARGET_SITES_MAX}] sites \
              or <{REQUIRED_REGIONS} regions: {violations} (must be 0)");

    Ok(())
}
