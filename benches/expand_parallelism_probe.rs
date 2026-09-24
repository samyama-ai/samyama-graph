//! Temporary probe: does a FIXED-hop expand use more than one core? (#1457)
//!
//! `traversal_parallelism.rs`'s traversal arm is `[:NEXT*1..8]`, which the
//! planner sends to `VarLengthExpandOperator`. `ExpandOperator` -- the operator
//! step 2 of #1457 changes -- is not on that path at all. This arm asks the
//! same question of a fixed-hop pattern, which is.

use std::time::Instant;

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn cpu_seconds() -> f64 {
    let s = match std::fs::read_to_string("/proc/self/stat") {
        Ok(s) => s,
        Err(_) => return 0.0,
    };
    let rest = match s.rfind(')') {
        Some(i) => &s[i + 1..],
        None => return 0.0,
    };
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: f64 = f.get(11).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let stime: f64 = f.get(12).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    (utime + stime) / 100.0
}

const MIN_WALL: f64 = 3.0;

fn cores_used_for<F: FnMut()>(min_wall: f64, mut f: F) -> (f64, f64, u64) {
    let c0 = cpu_seconds();
    let t0 = Instant::now();
    let mut iters = 0u64;
    loop {
        f();
        iters += 1;
        if t0.elapsed().as_secs_f64() >= min_wall { break; }
    }
    let wall = t0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - c0;
    if wall <= 0.0 { (0.0, 0.0, iters) } else { (cpu / wall, wall, iters) }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let scale: usize = arg(&args, "--scale").and_then(|v| v.parse().ok()).unwrap_or(50_000);
    let fanout: usize = arg(&args, "--fanout").and_then(|v| v.parse().ok()).unwrap_or(1);
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);

    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let chain = scale / 2;
    let mut chain_ids = Vec::with_capacity(chain);
    for i in 0..chain {
        let id = store.create_node("Chain");
        if let Some(n) = store.get_node_mut(id) {
            n.set_property("id", i as i64);
        }
        chain_ids.push(id);
    }
    for (i, &a) in chain_ids.iter().enumerate() {
        for k in 1..=fanout {
            if let Some(&b) = chain_ids.get(i + k) {
                let _ = store.create_edge(a, b, "NEXT");
            }
        }
    }
    for i in 0..scale {
        let id = store.create_node("Wide");
        if let Some(n) = store.get_node_mut(id) {
            n.set_property("id", i as i64);
            n.set_property("name", format!("node-{i}"));
        }
    }

    let control_q = "MATCH (w:Wide) WHERE toUpper(w.name) CONTAINS 'NODE-9' \
                     AND size(w.name) > 3 RETURN count(w)";
    let (control_cores, _, _) = cores_used_for(MIN_WALL, || {
        let _ = engine.execute(control_q, &store);
    });

    // Fixed-hop: three ExpandOperators, no var-length.
    let fixed_q = "MATCH (a:Chain)-[:NEXT]->(b:Chain)-[:NEXT]->(c:Chain)-[:NEXT]->(d:Chain) \
                   RETURN count(d)";
    let (fixed_cores, fixed_wall, fixed_iters) = cores_used_for(MIN_WALL, || {
        let _ = engine.execute(fixed_q, &store);
    });

    // One hop, high degree: the per-source work is a real adjacency walk plus
    // one record per surviving edge, which is where cross-record parallelism
    // has something to divide.
    let hop1_q = "MATCH (a:Chain)-[:NEXT]->(b:Chain) RETURN count(b)";
    let (hop1_cores, hop1_wall, hop1_iters) = cores_used_for(MIN_WALL, || {
        let _ = engine.execute(hop1_q, &store);
    });

    println!("{{");
    println!("  \"available_cores\": {cores}, \"scale\": {scale}, \"fanout\": {fanout},");
    println!("  \"threshold_env\": {:?},", std::env::var("SAMYAMA_EXPAND_PARALLEL_ROWS").unwrap_or_else(|_| "unset".into()));
    println!("  \"control_mean_cores\": {control_cores:.3},");
    println!("  \"instrument_ok\": {},", control_cores > 1.2);
    println!("  \"fixed_hop_query\": {fixed_q:?},");
    println!("  \"fixed_hop_mean_cores\": {fixed_cores:.3},");
    println!("  \"fixed_hop_wall_s\": {fixed_wall:.3}, \"fixed_hop_iters\": {fixed_iters},");
    println!("  \"hop1_mean_cores\": {hop1_cores:.3},");
    println!("  \"hop1_wall_s\": {hop1_wall:.3}, \"hop1_iters\": {hop1_iters}");
    println!("}}");
}
