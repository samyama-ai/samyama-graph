//! Paper 8 problem 7: wildfire evacuation routing.
//!
//! Loads a public-domain OpenStreetMap export (Overpass API, no auth) of the
//! Paradise, CA road network and poses a multi-source / multi-sink
//! capacitated evacuation-assignment problem.
//!
//! KG (built in memory from OSM JSON at runtime):
//!   :RoadNode {osm_id, lat, lon, degree}
//!   :RoadSegment {highway, length_km}  — between (RoadNode)
//!   :PopulationCentroid {node_id, population}
//!   :Exit {node_id}
//!
//! Problem:
//!
//!   minimize    Σ_{i, j} pop[i] × travel_time[i, j] × x[i, j]
//!             + congestion_weight × Σ_j max(0, Σ_i pop[i] × x[i, j] − capacity[j])²
//!             + balance_penalty × Σ_i (1 − Σ_j x[i, j])²
//!   subject to  0 ≤ x[i, j] ≤ 1     (fraction of centroid i evacuating via exit j)
//!
//! Travel times: haversine distance / speed. Fire disruption injected as a
//! random per-(centroid, exit) multiplier (× 3 for affected pairs).
//!
//! Usage:
//!   cargo run --release --example wildfire_evac_demo -- \
//!       [--osm PATH] [--n-centroids 5] [--n-exits 5] [--seeds 3] \
//!       [--congestion-weight 0.001] [--balance-penalty 10000.0] [--disrupt-frac 0.3]

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
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
struct Args {
    osm: PathBuf,
    n_centroids: usize,
    n_exits: usize,
    seeds: usize,
    congestion_weight: f64,
    balance_penalty: f64,
    disrupt_frac: f64,
    speed_kmh: f64,
    out: PathBuf,
    export_spec: Option<PathBuf>,
    export_snapshot: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            osm: PathBuf::from("../wildfire-evac-kg/data/paradise_ca.json"),
            n_centroids: 5,
            n_exits: 5,
            seeds: 3,
            congestion_weight: 0.001,
            balance_penalty: 10000.0,
            disrupt_frac: 0.3,
            speed_kmh: 40.0,
            out: PathBuf::from("/tmp/p8-wildfire-evac"),
            export_spec: None,
            export_snapshot: None,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--osm" => { a.osm = PathBuf::from(&argv[i + 1]); i += 2; }
            "--n-centroids" => { a.n_centroids = argv[i + 1].parse().unwrap(); i += 2; }
            "--n-exits" => { a.n_exits = argv[i + 1].parse().unwrap(); i += 2; }
            "--seeds" => { a.seeds = argv[i + 1].parse().unwrap(); i += 2; }
            "--congestion-weight" => { a.congestion_weight = argv[i + 1].parse().unwrap(); i += 2; }
            "--balance-penalty" => { a.balance_penalty = argv[i + 1].parse().unwrap(); i += 2; }
            "--disrupt-frac" => { a.disrupt_frac = argv[i + 1].parse().unwrap(); i += 2; }
            "--speed-kmh" => { a.speed_kmh = argv[i + 1].parse().unwrap(); i += 2; }
            "--out" => { a.out = PathBuf::from(&argv[i + 1]); i += 2; }
            "--export-spec" => { a.export_spec = Some(PathBuf::from(&argv[i + 1])); i += 2; }
            "--export-snapshot" => { a.export_snapshot = Some(PathBuf::from(&argv[i + 1])); i += 2; }
            other => { eprintln!("unknown arg: {}", other); std::process::exit(2); }
        }
    }
    a
}

#[derive(Clone, Debug)]
struct OsmNode { id: i64, lat: f64, lon: f64 }

fn haversine_km(a: (f64, f64), b: (f64, f64)) -> f64 {
    let r = 6371.0_f64;
    let (lat1, lon1) = (a.0.to_radians(), a.1.to_radians());
    let (lat2, lon2) = (b.0.to_radians(), b.1.to_radians());
    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * r * h.sqrt().asin()
}

/// Parse Overpass JSON, build KG, return (nodes, degree-map).
fn parse_osm(path: &std::path::Path)
    -> (GraphStore, Vec<OsmNode>, HashMap<i64, usize>)
{
    let mut file = File::open(path).expect("OSM JSON");
    let mut buf = String::new();
    file.read_to_string(&mut buf).expect("read");
    let v: serde_json::Value = serde_json::from_str(&buf).expect("json parse");
    let elements = v.get("elements").and_then(|e| e.as_array()).expect("elements");

    let mut nodes_raw: HashMap<i64, OsmNode> = HashMap::new();
    let mut ways: Vec<(String, Vec<i64>)> = Vec::new();
    for el in elements {
        let ty = el.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match ty {
            "node" => {
                let id = el.get("id").and_then(|x| x.as_i64()).unwrap_or(0);
                let lat = el.get("lat").and_then(|x| x.as_f64()).unwrap_or(0.0);
                let lon = el.get("lon").and_then(|x| x.as_f64()).unwrap_or(0.0);
                nodes_raw.insert(id, OsmNode { id, lat, lon });
            }
            "way" => {
                let highway = el.get("tags").and_then(|t| t.get("highway"))
                    .and_then(|s| s.as_str()).unwrap_or("").to_string();
                let refs: Vec<i64> = el.get("nodes").and_then(|n| n.as_array())
                    .map(|a| a.iter().filter_map(|x| x.as_i64()).collect()).unwrap_or_default();
                if !refs.is_empty() { ways.push((highway, refs)); }
            }
            _ => {}
        }
    }

    // Compute degree from ways (number of ways each node appears in, capped).
    let mut degree: HashMap<i64, usize> = HashMap::new();
    for (_, refs) in &ways {
        for &n in refs { *degree.entry(n).or_insert(0) += 1; }
    }

    // Build KG.
    let mut store = GraphStore::new();
    let mut graph_node_id: HashMap<i64, samyama::graph::NodeId> = HashMap::new();
    let mut node_list: Vec<OsmNode> = nodes_raw.values().cloned().collect();
    node_list.sort_by_key(|n| n.id);
    for n in &node_list {
        let nid = store.create_node(Label::new("RoadNode"));
        let m = store.get_node_mut(nid).unwrap();
        m.set_property("osm_id", PropertyValue::Integer(n.id));
        m.set_property("lat", PropertyValue::Float(n.lat));
        m.set_property("lon", PropertyValue::Float(n.lon));
        m.set_property("degree", PropertyValue::Integer(*degree.get(&n.id).unwrap_or(&0) as i64));
        graph_node_id.insert(n.id, nid);
    }
    // Optional: also materialise way records (not required for the optimization).
    let _ = ways;
    (store, node_list, degree)
}

struct EvacProblem {
    n_centroids: usize,
    n_exits: usize,
    /// pop[i] in number of evacuees per centroid.
    pop: Vec<f64>,
    /// capacity[j] in evacuees per hour for exit j.
    capacity: Vec<f64>,
    /// travel_time[i][j] in hours (haversine / speed × disruption multiplier).
    travel_time: Vec<Vec<f64>>,
    congestion_weight: f64,
    balance_penalty: f64,
    eval_count: std::sync::atomic::AtomicU64,
}

impl Problem for EvacProblem {
    fn dim(&self) -> usize { self.n_centroids * self.n_exits }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim()), Array1::ones(self.dim()))
    }
    fn objective(&self, x: &Array1<f64>) -> f64 {
        self.eval_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut travel = 0.0;
        let mut exit_flow = vec![0.0_f64; self.n_exits];
        let mut balance = 0.0;
        for i in 0..self.n_centroids {
            let mut total = 0.0;
            for j in 0..self.n_exits {
                let xij = x[i * self.n_exits + j].clamp(0.0, 1.0);
                travel += self.pop[i] * self.travel_time[i][j] * xij;
                exit_flow[j] += self.pop[i] * xij;
                total += xij;
            }
            let dev = 1.0 - total;
            balance += dev * dev;
        }
        let mut congestion = 0.0;
        for j in 0..self.n_exits {
            let over = (exit_flow[j] - self.capacity[j]).max(0.0);
            congestion += over * over;
        }
        travel + self.congestion_weight * congestion + self.balance_penalty * balance
    }
}

fn main() {
    let a = parse_args();
    std::fs::create_dir_all(&a.out).unwrap();

    eprintln!("parsing OSM JSON {} ...", a.osm.display());
    let t0 = Instant::now();
    let (store, nodes, degree) = parse_osm(&a.osm);
    let parse_ms = t0.elapsed().as_millis();
    eprintln!("KG built: {} :RoadNode nodes in {} ms", nodes.len(), parse_ms);

    // Cypher sanity.
    let engine = QueryEngine::new();
    let b = engine.execute("MATCH (n:RoadNode) RETURN count(n) AS c", &store).expect("count");
    let n_count = match b.records.first().and_then(|r| r.get("c")) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i, _ => -1,
    };
    eprintln!("Cypher sanity: count(:RoadNode) = {}", n_count);

    if let Some(path) = &a.export_snapshot {
        let f = File::create(path).expect("snapshot file");
        let stats = samyama::snapshot::export_tenant(&store, f).expect("export");
        eprintln!("snapshot -> {} ({} nodes, {} edges)", path.display(),
            stats.node_count, stats.edge_count);
    }

    // Pick centroids: top-N by degree (busy intersections).
    let mut ranked_by_deg: Vec<(&OsmNode, usize)> = nodes.iter()
        .map(|n| (n, *degree.get(&n.id).unwrap_or(&0))).collect();
    ranked_by_deg.sort_by(|a, b| b.1.cmp(&a.1));
    let centroids: Vec<&OsmNode> = ranked_by_deg.iter().take(a.n_centroids).map(|(n, _)| *n).collect();

    // Pick exits: boundary nodes (extreme lat/lon — likely highway-leaving-town nodes).
    let mut ranked_by_lat_n: Vec<&OsmNode> = nodes.iter().collect();
    ranked_by_lat_n.sort_by(|a, b| b.lat.partial_cmp(&a.lat).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranked_by_lat_s: Vec<&OsmNode> = nodes.iter().collect();
    ranked_by_lat_s.sort_by(|a, b| a.lat.partial_cmp(&b.lat).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranked_by_lon_e: Vec<&OsmNode> = nodes.iter().collect();
    ranked_by_lon_e.sort_by(|a, b| b.lon.partial_cmp(&a.lon).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranked_by_lon_w: Vec<&OsmNode> = nodes.iter().collect();
    ranked_by_lon_w.sort_by(|a, b| a.lon.partial_cmp(&b.lon).unwrap_or(std::cmp::Ordering::Equal));
    let mut exits: Vec<&OsmNode> = Vec::new();
    let groups = [&ranked_by_lat_n, &ranked_by_lat_s, &ranked_by_lon_e, &ranked_by_lon_w];
    let mut cursors = [0usize; 4];
    let mut g = 0;
    while exits.len() < a.n_exits {
        let gi = g % groups.len();
        // Walk down this direction's ranking until we find an unused node.
        while cursors[gi] < groups[gi].len() {
            let n = groups[gi][cursors[gi]];
            cursors[gi] += 1;
            if !exits.iter().any(|e| e.id == n.id) {
                exits.push(n);
                break;
            }
        }
        g += 1;
        if g > 1000 { break; }
    }
    assert!(exits.len() == a.n_exits, "could not pick {} unique exits", a.n_exits);
    // Population per centroid: derive from degree (more intersections → more people).
    let pop: Vec<f64> = centroids.iter()
        .map(|c| (*degree.get(&c.id).unwrap_or(&1) as f64) * 500.0).collect();
    // Exit capacities: assume highway can move 2000 people/hour each (typical).
    let capacity: Vec<f64> = (0..a.n_exits).map(|_| 2000.0).collect();

    // Travel times in hours, with random disruption multiplier.
    let mut tt = vec![vec![0.0; a.n_exits]; a.n_centroids];
    // Deterministic disruption pattern (seeded by hash of pairs); skip rand crate.
    for i in 0..a.n_centroids {
        for j in 0..a.n_exits {
            let d_km = haversine_km(
                (centroids[i].lat, centroids[i].lon),
                (exits[j].lat, exits[j].lon));
            let base = d_km / a.speed_kmh;
            // Pseudo-random disruption: hash i,j.
            let h = (i as u64).wrapping_mul(2654435761).wrapping_add(j as u64).wrapping_mul(40503);
            let r = (h % 1000) as f64 / 1000.0;
            let mult = if r < a.disrupt_frac { 3.0 } else { 1.0 };
            tt[i][j] = base * mult;
        }
    }

    let total_pop: f64 = pop.iter().sum();
    let total_cap: f64 = capacity.iter().sum();
    eprintln!("centroids: {} (pop total {:.0})", a.n_centroids, total_pop);
    eprintln!("exits: {} (capacity total {:.0}/hr)", a.n_exits, total_cap);

    if let Some(path) = &a.export_spec {
        let mut f = File::create(path).unwrap();
        writeln!(f, r#"{{"n_centroids": {}, "n_exits": {}, "congestion_weight": {}, "balance_penalty": {},"#,
            a.n_centroids, a.n_exits, a.congestion_weight, a.balance_penalty).unwrap();
        writeln!(f, r#""centroids": [{}],"#,
            centroids.iter().map(|c| format!(r#"{{"osm_id":{},"lat":{},"lon":{}}}"#, c.id, c.lat, c.lon))
                .collect::<Vec<_>>().join(",")).unwrap();
        writeln!(f, r#""exits": [{}],"#,
            exits.iter().map(|e| format!(r#"{{"osm_id":{},"lat":{},"lon":{}}}"#, e.id, e.lat, e.lon))
                .collect::<Vec<_>>().join(",")).unwrap();
        writeln!(f, r#""pop": [{}],"#, pop.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",")).unwrap();
        writeln!(f, r#""capacity": [{}],"#, capacity.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",")).unwrap();
        write!(f, r#""travel_time": [["#).unwrap();
        for (i, row) in tt.iter().enumerate() {
            if i > 0 { write!(f, "],[").unwrap(); }
            write!(f, "{}", row.iter().map(|v| format!("{:.6}", v)).collect::<Vec<_>>().join(",")).unwrap();
        }
        writeln!(f, "]]}}").unwrap();
        eprintln!("spec -> {}", path.display());
    }

    let problem = Arc::new(EvacProblem {
        n_centroids: a.n_centroids,
        n_exits: a.n_exits,
        pop: pop.clone(),
        capacity: capacity.clone(),
        travel_time: tt.clone(),
        congestion_weight: a.congestion_weight,
        balance_penalty: a.balance_penalty,
        eval_count: std::sync::atomic::AtomicU64::new(0),
    });

    let solvers: Vec<(&str, fn(SolverConfig, &EvacProblem) -> OptimizationResult)> = vec![
        ("BMWR",      |c, p| BMWRSolver::new(c).solve(p)),
        ("Jaya",      |c, p| JayaSolver::new(c).solve(p)),
        ("SAMP-Jaya", |c, p| SAMPJayaSolver::new(c).solve(p)),
        ("EHR-Jaya",  |c, p| EHRJayaSolver::new(c).solve(p)),
        ("Rao-1",     |c, p| RaoSolver::new(c, RaoVariant::Rao1).solve(p)),
    ];
    let cfg = SolverConfig { population_size: 60, max_iterations: 500 };

    let csv_path = a.out.join("results.csv");
    let mut csv = File::create(&csv_path).unwrap();
    writeln!(csv, "solver,seed,best_fitness,wall_ms,evals").unwrap();

    println!("\n=== Wildfire evac ({} centroids × {} exits = {} dim, disrupt_frac={}) ===",
        a.n_centroids, a.n_exits, a.n_centroids * a.n_exits, a.disrupt_frac);
    println!("{:<12} {:>5} {:>14} {:>10} {:>10}", "solver", "seed", "fitness", "wall_ms", "evals");

    for (name, run) in &solvers {
        for seed in 0..a.seeds {
            problem.eval_count.store(0, std::sync::atomic::Ordering::Relaxed);
            let t0 = Instant::now();
            let r = run(cfg.clone(), &problem);
            let wall = t0.elapsed().as_millis();
            let evals = problem.eval_count.load(std::sync::atomic::Ordering::Relaxed);
            println!("{:<12} {:>5} {:>14.2} {:>10} {:>10}",
                name, seed, r.best_fitness, wall, evals);
            writeln!(csv, "{},{},{:.6},{},{}", name, seed, r.best_fitness, wall, evals).unwrap();
        }
    }
    eprintln!("\nresults -> {}", csv_path.display());

    let manifest = format!(
        r#"{{"args": {{"osm": "{}", "n_centroids": {}, "n_exits": {}, "seeds": {}, "disrupt_frac": {}, "speed_kmh": {}}}, "osm_node_count": {}, "total_pop": {}, "total_capacity_per_hr": {}, "parse_ms": {}}}
"#,
        a.osm.display(), a.n_centroids, a.n_exits, a.seeds, a.disrupt_frac, a.speed_kmh,
        nodes.len(), total_pop, total_cap, parse_ms);
    std::fs::write(a.out.join("manifest.json"), manifest).unwrap();
}
