//! What a query costs in memory while it runs (#2084).
//!
//! `memory_footprint` measures the stored graph. This measures execution: how
//! high the heap goes while a query runs, what it leaves behind, how that
//! scales with concurrent queries, and whether a time limit bounds it.
//!
//!   cargo bench --bench query_memory
//!   cargo bench --bench query_memory -- --scale 200000 --json query_memory.json
//!
//! Exact live heap bytes from a counting allocator (`common/peak_alloc.rs`)
//! wrapping the shipped allocator. Not RSS: allocator slack and pages kept
//! after a free are not here, which is what makes a per-query figure
//! reproducible. Synthetic graph, so it runs anywhere; compare runs of this
//! harness with each other, not with another engine.

#[path = "common/peak_alloc.rs"]
mod peak_alloc;

use std::time::{Duration, Instant};

use peak_alloc::measure;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::{BoundParams, QueryEngine};

fn arg<T: std::str::FromStr>(name: &str, default: T) -> T {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == name)
        .and_then(|i| a.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn build(nodes: usize, fanout: usize) -> GraphStore {
    let mut g = GraphStore::new();
    let ids: Vec<_> = (0..nodes)
        .map(|i| {
            let id = g.create_node("P");
            g.set_node_property("default", id, "i", PropertyValue::Integer(i as i64)).unwrap();
            g.set_node_property("default", id, "name", PropertyValue::String(format!("name-{i}")))
                .unwrap();
            id
        })
        .collect();
    for k in 1..=fanout {
        for i in 0..nodes {
            g.create_edge(ids[i], ids[(i * (k * 7 + 1) + k) % nodes], "R").unwrap();
        }
    }
    g
}

const WORKLOADS: &[(&str, &str)] = &[
    ("1-hop count", "MATCH (a:P)-[:R]->(b) RETURN count(b) AS c"),
    ("2-hop count", "MATCH (a:P)-[:R]->()-[:R]->(c) RETURN count(c) AS c"),
    ("3-hop count", "MATCH (a:P)-[:R]->()-[:R]->()-[:R]->(d) RETURN count(d) AS c"),
    ("1-hop rows", "MATCH (a:P)-[:R]->(b) RETURN a.i AS a, b.i AS b"),
    ("group + collect", "MATCH (a:P)-[:R]->(b) RETURN a.i AS a, collect(b.i) AS bs"),
    ("sort all", "MATCH (a:P) RETURN a.name AS n ORDER BY n DESC"),
    ("top 10", "MATCH (a:P) RETURN a.name AS n ORDER BY n DESC LIMIT 10"),
    ("2-hop distinct", "MATCH (a:P)-[:R]->()-[:R]->(c) RETURN DISTINCT c.i AS i"),
    ("var-length 1..3 count", "MATCH (a:P {i: 1})-[:R*1..3]->(b) RETURN count(b) AS c"),
];

fn mb(b: isize) -> f64 {
    b as f64 / 1_000_000.0
}

fn main() {
    let nodes: usize = arg("--scale", 100_000);
    let fanout: usize = arg("--fanout", 3);
    let json_out: Option<String> = arg("--json", String::new()).into();
    let json_out = json_out.filter(|s| !s.is_empty());

    let t = Instant::now();
    let before = peak_alloc::live();
    let g = build(nodes, fanout);
    let graph_bytes = peak_alloc::live() - before;
    println!(
        "graph: {nodes} nodes, {} edges, {:.1} MB live heap, built in {:.1?}\n",
        nodes * fanout, mb(graph_bytes), t.elapsed()
    );

    let e = QueryEngine::new();
    let none = BoundParams::new();
    let mut report = serde_json::json!({
        "nodes": nodes, "edges": nodes * fanout, "graph_live_bytes": graph_bytes,
        "workloads": [], "repeat": [], "concurrency": [], "deadline": []
    });

    // ── single queries ──
    println!("{:<24} {:>10} {:>9} {:>11} {:>11} {:>12} {:>11}",
             "workload", "rows", "ms", "peak MB", "peak/graph", "B/row", "retained");
    for (name, q) in WORKLOADS {
        let _ = e.execute_with_params_within(q, &g, &none, None); // warm the parse cache
        let t = Instant::now();
        let (cost, rows) = measure(|| e.execute_with_params_within(q, &g, &none, None));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let rows = rows.unwrap_or(0);
        let per_row = if rows > 0 { cost.peak_bytes / rows as isize } else { 0 };
        println!("{:<24} {:>10} {:>9.1} {:>11.2} {:>10.2}x {:>12} {:>11}",
                 name, rows, ms, mb(cost.peak_bytes), cost.peak_bytes as f64 / graph_bytes as f64,
                 per_row, cost.retained_bytes);
        report["workloads"].as_array_mut().unwrap().push(serde_json::json!({
            "name": name, "query": q, "rows": rows, "ms": ms, "peak_bytes": cost.peak_bytes,
            "retained_bytes": cost.retained_bytes, "alloc_calls": cost.calls,
        }));
    }

    // ── repeated execution: does the heap creep? ──
    println!("\nrepeated 100x (heap growth across the runs)");
    for (name, q) in [WORKLOADS[1], WORKLOADS[4], WORKLOADS[7]] {
        let _ = e.execute_with_params_within(q, &g, &none, None);
        let start = peak_alloc::live();
        for _ in 0..100 {
            let _ = e.execute_with_params_within(q, &g, &none, None);
        }
        let grew = peak_alloc::live() - start;
        println!("  {name:<22} grew {grew} bytes");
        report["repeat"].as_array_mut().unwrap()
            .push(serde_json::json!({"name": name, "runs": 100, "grew_bytes": grew}));
    }

    // ── concurrency: the combined peak of k queries at once ──
    println!("\nconcurrent (combined peak of k identical queries started together)");
    let gref = &g;
    for (name, q) in [WORKLOADS[1], WORKLOADS[5]] {
        let (single, _) = measure(|| e.execute_with_params_within(q, gref, &none, None));
        for k in [1usize, 2, 4, 8] {
            let before = peak_alloc::live();
            peak_alloc::reset_peak();
            std::thread::scope(|s| {
                for _ in 0..k {
                    s.spawn(|| {
                        let e = QueryEngine::new();
                        let _ = e.execute_with_params_within(q, gref, &BoundParams::new(), None);
                    });
                }
            });
            let peak = peak_alloc::peak() - before;
            println!("  {name:<14} k={k}: peak {:>8.2} MB = {:.2}x one query",
                     mb(peak), peak as f64 / single.peak_bytes as f64);
            report["concurrency"].as_array_mut().unwrap().push(serde_json::json!({
                "name": name, "k": k, "peak_bytes": peak, "single_peak_bytes": single.peak_bytes,
            }));
        }
    }

    // ── a deadline: does it bound memory as well as time? ──
    println!("\ndeadline (peak with no limit vs. a 1 ms limit)");
    for (name, q) in [WORKLOADS[2], WORKLOADS[5]] {
        let (free, _) = measure(|| e.execute_with_params_within(q, &g, &none, None));
        let _ = e.execute_with_params_within(q, &g, &none, Some(Duration::from_millis(1)));
        let (cut, out) = measure(|| e.execute_with_params_within(q, &g, &none, Some(Duration::from_millis(1))));
        println!("  {name:<14} unbounded {:>8.2} MB | 1 ms limit {:>8.2} MB ({}), retained {}",
                 mb(free.peak_bytes), mb(cut.peak_bytes),
                 if out.is_err() { "stopped" } else { "finished inside 1 ms" }, cut.retained_bytes);
        report["deadline"].as_array_mut().unwrap().push(serde_json::json!({
            "name": name, "unbounded_peak_bytes": free.peak_bytes,
            "limited_peak_bytes": cut.peak_bytes, "stopped": out.is_err(),
            "retained_bytes": cut.retained_bytes,
        }));
    }

    if let Some(path) = json_out {
        std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap()).unwrap();
        println!("\nwrote {path}");
    }
}
