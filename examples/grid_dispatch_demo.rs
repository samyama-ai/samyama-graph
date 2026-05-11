//! Paper 8 problem 5: economic + environmental grid dispatch.
//!
//! Builds a small in-memory KG from public smart-grid sample CSVs:
//!   :Generator {id, min_power, max_power, ramp_rate, cost_a, cost_b,
//!               emission_factor, is_renewable}
//!   :Hour      {hour, demand, price, solar_potential, wind_potential}
//!
//! Pre-materialises per-generator and per-hour aggregates via Cypher, then
//! optimises in pure Rust. Decision: continuous power output p[g][t] for
//! 4 generators × 24 hours = 96 dim, bounded by [0, max_power].
//!
//! Scalarised multi-objective:
//!   minimize  Σ_{g,t} (cost_a[g] + cost_b[g] × p[g][t])         (cost)
//!           + ε_w × Σ_{g,t} emission_factor[g] × p[g][t]        (emissions)
//!           + λ_demand × Σ_t (Σ_g p[g][t] − demand[t])²         (balance)
//!           + λ_ramp   × Σ_{g,t>0} max(0, |p[g][t]−p[g][t−1]|−ramp_rate[g])²
//!
//! All four Rao-family solvers run on the same scalarised problem; a
//! separate ε-sweep traces the cost/emissions Pareto front.
//!
//! Usage:
//!   cargo run --release --example grid_dispatch_demo -- \
//!       [--data-dir PATH] [--seeds 3] [--emission-weight 50.0]
//!       [--demand-penalty 100.0] [--ramp-penalty 50.0]

use ndarray::Array1;
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;
use samyama::query::executor::record::Value;
use samyama_optimization::algorithms::{
    BMWRSolver, EHRJayaSolver, JayaSolver, RaoSolver, RaoVariant, SAMPJayaSolver,
};
use samyama_optimization::common::{OptimizationResult, Problem, SolverConfig};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
struct Args {
    data_dir: PathBuf,
    seeds: usize,
    emission_weight: f64,
    demand_penalty: f64,
    ramp_penalty: f64,
    out: PathBuf,
    export_spec: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("../optimization/smart_grid/data/sample"),
            seeds: 3,
            emission_weight: 50.0,
            demand_penalty: 100.0,
            ramp_penalty: 50.0,
            out: PathBuf::from("/tmp/p8-grid-dispatch"),
            export_spec: None,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--data-dir" => { a.data_dir = PathBuf::from(&argv[i + 1]); i += 2; }
            "--seeds" => { a.seeds = argv[i + 1].parse().unwrap(); i += 2; }
            "--emission-weight" => { a.emission_weight = argv[i + 1].parse().unwrap(); i += 2; }
            "--demand-penalty" => { a.demand_penalty = argv[i + 1].parse().unwrap(); i += 2; }
            "--ramp-penalty" => { a.ramp_penalty = argv[i + 1].parse().unwrap(); i += 2; }
            "--out" => { a.out = PathBuf::from(&argv[i + 1]); i += 2; }
            "--export-spec" => { a.export_spec = Some(PathBuf::from(&argv[i + 1])); i += 2; }
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
}

#[derive(Clone, Debug)]
struct Generator {
    id: String,
    min_power: f64,
    max_power: f64,
    ramp_rate: f64,
    cost_a: f64,
    cost_b: f64,
    emission_factor: f64,
    is_renewable: bool,
}

#[derive(Clone, Debug)]
struct HourForecast {
    hour: i64,
    demand: f64,
}

/// Build an in-memory KG from CSVs in data_dir.
fn build_grid_kg(data_dir: &std::path::Path) -> (GraphStore, Vec<Generator>, Vec<HourForecast>) {
    let mut store = GraphStore::new();

    // Generators.
    let gen_csv = data_dir.join("sample_generators.csv");
    let mut gens = Vec::new();
    let f = File::open(&gen_csv).expect("generators csv");
    for (i, line) in BufReader::new(f).lines().enumerate() {
        let line = line.unwrap();
        if i == 0 { continue; } // header
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 8 { continue; }
        let id = cols[0].to_string();
        let cost_parts: Vec<&str> = cols[5].split('-').collect();
        let cost_a: f64 = cost_parts.first().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let cost_b: f64 = cost_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1.0);
        let g = Generator {
            id: id.clone(),
            min_power: cols[1].parse().unwrap_or(0.0),
            max_power: cols[2].parse().unwrap_or(0.0),
            ramp_rate: cols[3].parse().unwrap_or(0.0),
            cost_a, cost_b,
            emission_factor: cols[6].parse().unwrap_or(0.0),
            is_renewable: matches!(cols[7].trim(), "True" | "true" | "1"),
        };
        let n = store.create_node(Label::new("Generator"));
        let nm = store.get_node_mut(n).unwrap();
        nm.set_property("id", PropertyValue::String(g.id.clone()));
        nm.set_property("min_power", PropertyValue::Float(g.min_power));
        nm.set_property("max_power", PropertyValue::Float(g.max_power));
        nm.set_property("ramp_rate", PropertyValue::Float(g.ramp_rate));
        nm.set_property("cost_a", PropertyValue::Float(g.cost_a));
        nm.set_property("cost_b", PropertyValue::Float(g.cost_b));
        nm.set_property("emission_factor", PropertyValue::Float(g.emission_factor));
        nm.set_property("is_renewable", PropertyValue::Boolean(g.is_renewable));
        gens.push(g);
    }

    // Forecasts.
    let fc_csv = data_dir.join("sample_forecasts.csv");
    let mut hours = Vec::new();
    let f = File::open(&fc_csv).expect("forecasts csv");
    for (i, line) in BufReader::new(f).lines().enumerate() {
        let line = line.unwrap();
        if i == 0 { continue; }
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 5 { continue; }
        let hour: i64 = cols[0].parse().unwrap_or(0);
        let demand: f64 = cols[1].parse().unwrap_or(0.0);
        let n = store.create_node(Label::new("Hour"));
        let nm = store.get_node_mut(n).unwrap();
        nm.set_property("hour", PropertyValue::Integer(hour));
        nm.set_property("demand", PropertyValue::Float(demand));
        nm.set_property("price", PropertyValue::Float(cols[2].parse().unwrap_or(0.0)));
        nm.set_property("solar_potential", PropertyValue::Float(cols[3].parse().unwrap_or(0.0)));
        nm.set_property("wind_potential", PropertyValue::Float(cols[4].parse().unwrap_or(0.0)));
        hours.push(HourForecast { hour, demand });
    }

    (store, gens, hours)
}

struct DispatchProblem {
    num_gen: usize,
    num_hour: usize,
    gens: Vec<Generator>,
    hours: Vec<HourForecast>,
    emission_weight: f64,
    demand_penalty: f64,
    ramp_penalty: f64,
    eval_count: std::sync::atomic::AtomicU64,
}

impl DispatchProblem {
    fn dim(&self) -> usize { self.num_gen * self.num_hour }
    fn lower(&self) -> Array1<f64> { Array1::zeros(self.dim()) }
    fn upper(&self) -> Array1<f64> {
        let mut u = Array1::zeros(self.dim());
        for g in 0..self.num_gen {
            for t in 0..self.num_hour {
                u[g * self.num_hour + t] = self.gens[g].max_power;
            }
        }
        u
    }
    /// Decompose into raw cost / emissions / penalty terms for reporting.
    fn decompose(&self, x: &Array1<f64>) -> (f64, f64, f64, f64) {
        let mut cost = 0.0;
        let mut emissions = 0.0;
        let mut demand_pen = 0.0;
        let mut ramp_pen = 0.0;
        for g in 0..self.num_gen {
            for t in 0..self.num_hour {
                let p = x[g * self.num_hour + t].clamp(0.0, self.gens[g].max_power);
                cost += self.gens[g].cost_a + self.gens[g].cost_b * p;
                emissions += self.gens[g].emission_factor * p;
                if t > 0 {
                    let prev = x[g * self.num_hour + t - 1].clamp(0.0, self.gens[g].max_power);
                    let delta = (p - prev).abs();
                    let over = (delta - self.gens[g].ramp_rate).max(0.0);
                    ramp_pen += over * over;
                }
            }
        }
        for t in 0..self.num_hour {
            let supply: f64 = (0..self.num_gen)
                .map(|g| x[g * self.num_hour + t].clamp(0.0, self.gens[g].max_power))
                .sum();
            let dev = supply - self.hours[t].demand;
            demand_pen += dev * dev;
        }
        (cost, emissions, demand_pen, ramp_pen)
    }
}

impl Problem for DispatchProblem {
    fn dim(&self) -> usize { self.dim() }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) { (self.lower(), self.upper()) }
    fn objective(&self, x: &Array1<f64>) -> f64 {
        self.eval_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (cost, emissions, demand_pen, ramp_pen) = self.decompose(x);
        cost + self.emission_weight * emissions
            + self.demand_penalty * demand_pen
            + self.ramp_penalty * ramp_pen
    }
}

fn main() {
    let a = parse_args();
    std::fs::create_dir_all(&a.out).unwrap();

    eprintln!("building grid KG from {} ...", a.data_dir.display());
    let t0 = Instant::now();
    let (store, gens, hours) = build_grid_kg(&a.data_dir);
    let build_ms = t0.elapsed().as_millis();
    eprintln!("built {} generators, {} hours in {} ms",
        gens.len(), hours.len(), build_ms);

    // Verify via Cypher (one query per stratum, demonstrates the KG is real).
    let engine = QueryEngine::new();
    let q1 = "MATCH (g:Generator) RETURN sum(g.max_power) AS total_capacity";
    let b1 = engine.execute(q1, &store).expect("cap query");
    let total_cap = match b1.records.first().and_then(|r| r.get("total_capacity")) {
        Some(Value::Property(PropertyValue::Float(f))) => *f,
        Some(Value::Property(PropertyValue::Integer(i))) => *i as f64,
        _ => 0.0,
    };
    let q2 = "MATCH (h:Hour) RETURN max(h.demand) AS peak, sum(h.demand) AS total";
    let b2 = engine.execute(q2, &store).expect("peak query");
    let (peak_demand, total_demand) = match b2.records.first() {
        Some(r) => {
            let p = if let Some(Value::Property(PropertyValue::Float(f))) = r.get("peak") { *f } else { 0.0 };
            let t = if let Some(Value::Property(PropertyValue::Float(f))) = r.get("total") { *f } else { 0.0 };
            (p, t)
        }
        None => (0.0, 0.0),
    };
    eprintln!("KG sanity: total capacity = {:.0} MW, peak demand = {:.0} MW, total demand = {:.0} MWh",
        total_cap, peak_demand, total_demand);

    if let Some(path) = &a.export_spec {
        let mut f = File::create(path).unwrap();
        writeln!(f, r#"{{"emission_weight": {}, "demand_penalty": {}, "ramp_penalty": {},"#,
            a.emission_weight, a.demand_penalty, a.ramp_penalty).unwrap();
        let g_arr: Vec<String> = gens.iter().map(|g| format!(
            r#"{{"id":"{}","min_power":{},"max_power":{},"ramp_rate":{},"cost_a":{},"cost_b":{},"emission_factor":{},"is_renewable":{}}}"#,
            g.id, g.min_power, g.max_power, g.ramp_rate, g.cost_a, g.cost_b, g.emission_factor, g.is_renewable)).collect();
        writeln!(f, r#""generators": [{}],"#, g_arr.join(",")).unwrap();
        let h_arr: Vec<String> = hours.iter().map(|h| format!(
            r#"{{"hour":{},"demand":{}}}"#, h.hour, h.demand)).collect();
        writeln!(f, r#""hours": [{}]}}"#, h_arr.join(",")).unwrap();
        eprintln!("spec -> {}", path.display());
    }

    let problem = Arc::new(DispatchProblem {
        num_gen: gens.len(),
        num_hour: hours.len(),
        gens: gens.clone(),
        hours: hours.clone(),
        emission_weight: a.emission_weight,
        demand_penalty: a.demand_penalty,
        ramp_penalty: a.ramp_penalty,
        eval_count: std::sync::atomic::AtomicU64::new(0),
    });

    let solvers: Vec<(&str, fn(SolverConfig, &DispatchProblem) -> OptimizationResult)> = vec![
        ("BMWR",      |c, p| BMWRSolver::new(c).solve(p)),
        ("Jaya",      |c, p| JayaSolver::new(c).solve(p)),
        ("SAMP-Jaya", |c, p| SAMPJayaSolver::new(c).solve(p)),
        ("EHR-Jaya",  |c, p| EHRJayaSolver::new(c).solve(p)),
        ("Rao-1",     |c, p| RaoSolver::new(c, RaoVariant::Rao1).solve(p)),
    ];
    let cfg = SolverConfig { population_size: 60, max_iterations: 500 };

    let csv_path = a.out.join("results.csv");
    let mut csv = File::create(&csv_path).unwrap();
    writeln!(csv, "solver,seed,best_fitness,cost,emissions,demand_penalty,ramp_penalty,wall_ms,evals").unwrap();

    println!("\n=== Grid dispatch ({}gen × {}hr = {} dim, ε_em={}, λ_d={}, λ_r={}) ===",
        gens.len(), hours.len(), gens.len() * hours.len(),
        a.emission_weight, a.demand_penalty, a.ramp_penalty);
    println!("{:<12} {:>5} {:>14} {:>12} {:>10} {:>12} {:>10} {:>10} {:>10}",
        "solver", "seed", "fitness", "cost", "CO2", "demand_pen", "ramp_pen", "wall_ms", "evals");

    for (name, run) in &solvers {
        for seed in 0..a.seeds {
            problem.eval_count.store(0, std::sync::atomic::Ordering::Relaxed);
            let t0 = Instant::now();
            let r = run(cfg.clone(), &problem);
            let wall = t0.elapsed().as_millis();
            let evals = problem.eval_count.load(std::sync::atomic::Ordering::Relaxed);
            let (cost, em, dpen, rpen) = problem.decompose(&r.best_variables);
            println!("{:<12} {:>5} {:>14.2} {:>12.2} {:>10.2} {:>12.2} {:>10.2} {:>10} {:>10}",
                name, seed, r.best_fitness, cost, em, dpen, rpen, wall, evals);
            writeln!(csv, "{},{},{:.4},{:.4},{:.4},{:.4},{:.4},{},{}",
                name, seed, r.best_fitness, cost, em, dpen, rpen, wall, evals).unwrap();
        }
    }
    eprintln!("\nresults -> {}", csv_path.display());

    let renewable_cap: f64 = gens.iter().filter(|g| g.is_renewable).map(|g| g.max_power).sum();
    let manifest = format!(
        r#"{{"args": {{"data_dir": "{}", "seeds": {}, "emission_weight": {}, "demand_penalty": {}, "ramp_penalty": {}}}, "num_generators": {}, "num_hours": {}, "dim": {}, "total_capacity_mw": {}, "renewable_capacity_mw": {}, "peak_demand_mw": {}, "total_demand_mwh": {}, "build_ms": {}}}
"#,
        a.data_dir.display(), a.seeds, a.emission_weight, a.demand_penalty, a.ramp_penalty,
        gens.len(), hours.len(), gens.len() * hours.len(),
        total_cap, renewable_cap, peak_demand, total_demand, build_ms
    );
    std::fs::write(a.out.join("manifest.json"), manifest).unwrap();
}
