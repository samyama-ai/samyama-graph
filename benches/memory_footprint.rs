//! Measures resident memory per node and per edge (#477, PERF-10).
//!
//! `PERF-10` sets a target of ≤128 B/edge and records a current figure of
//! ~537 B/edge against FalkorDB's ~57. That figure had **no reproducer** — which
//! under spec 18 makes it unquotable, and in practice makes it unimprovable,
//! because nothing would tell us whether a change helped.
//!
//! Two independent measurements, deliberately:
//!
//!   * a **counting allocator** wrapping the system allocator, giving exact live
//!     heap bytes and letting each construction phase be attributed separately;
//!   * **RSS** from `/proc/self/statm`, which includes what the allocator cannot
//!     see — binary, stacks, allocator slack, fragmentation.
//!
//! They will not agree, and the gap is the point: allocator bytes are what a
//! layout change moves, RSS is what the machine actually has to have. Reporting
//! only one of them is how a "we cut memory 40%" claim survives an RSS that did
//! not move.
//!
//!   cargo bench --bench memory_footprint
//!   cargo bench --bench memory_footprint -- --json footprint.json
//!   cargo bench --bench memory_footprint -- --scale 200000

use samyama::graph::GraphStore;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

// ---------------------------------------------------------------- allocator

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicUsize = AtomicUsize::new(0);
static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Allocation-size histogram, bucketed by power of two (index = log2 of size,
/// capped). Knowing *what sizes* are being allocated is what turns "620 bytes
/// of overhead per node" into a specific structure to go and look at.
const BUCKETS: usize = 20;
static SIZE_HIST: [AtomicUsize; BUCKETS] = [const { AtomicUsize::new(0) }; BUCKETS];

fn bucket_of(size: usize) -> usize {
    if size == 0 {
        return 0;
    }
    let b = usize::BITS - size.leading_zeros();
    ((b as usize).saturating_sub(1)).min(BUCKETS - 1)
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        SIZE_HIST[bucket_of(layout.size())].fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size >= layout.size() {
            ALLOCATED.fetch_add(new_size - layout.size(), Ordering::Relaxed);
        } else {
            FREED.fetch_add(layout.size() - new_size, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Live heap bytes: everything allocated minus everything freed.
fn live_heap() -> usize {
    ALLOCATED.load(Ordering::Relaxed).saturating_sub(FREED.load(Ordering::Relaxed))
}

/// Number of allocation calls so far. Allocation *count* matters
/// independently of bytes: each one carries a header and rounding, and the
/// per-object overhead is what a "collapse these Vecs" change removes.
fn alloc_calls() -> usize {
    ALLOC_CALLS.load(Ordering::Relaxed)
}

fn hist_snapshot() -> [usize; BUCKETS] {
    let mut out = [0usize; BUCKETS];
    for (i, slot) in SIZE_HIST.iter().enumerate() {
        out[i] = slot.load(Ordering::Relaxed);
    }
    out
}

/// Resident set size in bytes, or `None` off Linux.
fn rss() -> Option<usize> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: usize = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(resident_pages * 4096)
}

// ---------------------------------------------------------------- fixture

/// A synthetic graph with a realistic shape: labelled nodes with a couple of
/// properties, typed edges, average degree ~4.
///
/// Synthetic rather than LDBC so the harness runs anywhere; the numbers are
/// comparable across runs of this harness, which is what a regression check
/// needs. Absolute comparison against a competitor needs the same dataset on
/// both, and that is a separate exercise.
fn build(nodes: usize, avg_degree: usize) -> (GraphStore, usize, usize) {
    let mut store = GraphStore::new();
    let labels = ["Person", "Post", "Forum", "Tag"];
    let edge_types = ["KNOWS", "LIKES", "MEMBER_OF", "HAS_TAG"];

    let mut node_ids = Vec::with_capacity(nodes);
    for i in 0..nodes {
        let id = store.create_node(labels[i % labels.len()]);
        // Two properties per node: one string, one integer -- the common shape.
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            samyama::graph::PropertyValue::String(format!("n{i}")),
        );
        let _ = store.set_node_property(
            "default",
            id,
            "value".to_string(),
            samyama::graph::PropertyValue::Integer(i as i64),
        );
        node_ids.push(id);
    }

    let mut edges = 0usize;
    for (i, &src) in node_ids.iter().enumerate() {
        for d in 0..avg_degree {
            // Deterministic spread so the adjacency is not degenerate.
            let tgt = node_ids[(i * 7 + d * 31 + 1) % nodes];
            if store
                .create_edge(src, tgt, edge_types[(i + d) % edge_types.len()])
                .is_ok()
            {
                edges += 1;
            }
        }
    }
    (store, nodes, edges)
}

// ---------------------------------------------------------------- report

struct Phase {
    name: &'static str,
    heap_delta: usize,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1).cloned())
    };
    let scale: usize = arg("--scale").and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let avg_degree: usize = arg("--degree").and_then(|s| s.parse().ok()).unwrap_or(4);

    println!("Memory footprint — {scale} nodes, target degree {avg_degree}");
    println!("{}", "-".repeat(78));

    let base_heap = live_heap();
    let base_calls = alloc_calls();
    let hist_base = hist_snapshot();
    let base_rss = rss();
    let t_start = std::time::Instant::now();

    // Phase 1: nodes only.
    let mut store = GraphStore::new();
    let labels = ["Person", "Post", "Forum", "Tag"];
    let mut node_ids = Vec::with_capacity(scale);
    for i in 0..scale {
        let id = store.create_node(labels[i % labels.len()]);
        node_ids.push(id);
    }
    let after_bare_nodes = live_heap();
    let calls_bare_nodes = alloc_calls();
    let hist_bare_nodes = hist_snapshot();
    let t_nodes = t_start.elapsed();

    // Phase 2: node properties.
    for (i, &id) in node_ids.iter().enumerate() {
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            samyama::graph::PropertyValue::String(format!("n{i}")),
        );
        let _ = store.set_node_property(
            "default",
            id,
            "value".to_string(),
            samyama::graph::PropertyValue::Integer(i as i64),
        );
    }
    let after_props = live_heap();
    let calls_props = alloc_calls();
    let t_props = t_start.elapsed();

    // Phase 3: edges.
    let edge_types = ["KNOWS", "LIKES", "MEMBER_OF", "HAS_TAG"];
    let mut edges = 0usize;
    for (i, &src) in node_ids.iter().enumerate() {
        for d in 0..avg_degree {
            let tgt = node_ids[(i * 7 + d * 31 + 1) % scale];
            if store.create_edge(src, tgt, edge_types[(i + d) % edge_types.len()]).is_ok() {
                edges += 1;
            }
        }
    }
    let after_edges = live_heap();
    let calls_edges = alloc_calls();
    let t_edges = t_start.elapsed();

    // Phase 4: statistics (the planner's view; built lazily elsewhere).
    let _stats = store.statistics();
    let after_stats = live_heap();

    let final_rss = rss();

    let phases = [
        Phase { name: "node structs", heap_delta: after_bare_nodes.saturating_sub(base_heap) },
        Phase { name: "node properties", heap_delta: after_props.saturating_sub(after_bare_nodes) },
        Phase { name: "edges + adjacency", heap_delta: after_edges.saturating_sub(after_props) },
        Phase { name: "statistics", heap_delta: after_stats.saturating_sub(after_edges) },
    ];

    let total_heap = after_stats.saturating_sub(base_heap);

    println!("{:<24} {:>14} {:>12}", "phase", "bytes", "share");
    for p in &phases {
        let share = if total_heap > 0 { 100.0 * p.heap_delta as f64 / total_heap as f64 } else { 0.0 };
        println!("{:<24} {:>14} {:>11.1}%", p.name, p.heap_delta, share);
    }
    println!("{:<24} {:>14}", "total (live heap)", total_heap);
    println!();

    println!(
        "allocations: {:>10} for nodes ({:.2}/node), {:>10} for properties ({:.2}/node), {:>10} for edges ({:.2}/edge)",
        calls_bare_nodes - base_calls,
        (calls_bare_nodes - base_calls) as f64 / scale as f64,
        calls_props - calls_bare_nodes,
        (calls_props - calls_bare_nodes) as f64 / scale as f64,
        calls_edges - calls_props,
        if edges > 0 { (calls_edges - calls_props) as f64 / edges as f64 } else { 0.0 },
    );
    // Throughput matters as much as bytes here: allocation count caps insert
    // rate independently of how much memory each object ends up occupying,
    // which is the PERF-14 half of this measurement.
    let edge_secs = (t_edges - t_props).as_secs_f64();
    println!(
        "insert rate: {:.0} nodes/s, {:.0} edges/s",
        scale as f64 / t_nodes.as_secs_f64().max(1e-9),
        edges as f64 / edge_secs.max(1e-9),
    );
    // Where the node-phase allocations went, by size class.
    println!();
    println!("node-phase allocations by size class:");
    let mut shown = false;
    for i in 0..BUCKETS {
        let n = hist_bare_nodes[i].saturating_sub(hist_base[i]);
        if n == 0 {
            continue;
        }
        let lo = if i == 0 { 0 } else { 1usize << i };
        let hi = (1usize << (i + 1)) - 1;
        println!(
            "  {:>6}..{:<6} B  {:>10} allocs  {:>6.2}/node",
            lo,
            hi,
            n,
            n as f64 / scale as f64
        );
        shown = true;
    }
    if !shown {
        println!("  (none)");
    }

    println!();
    println!("nodes: {scale}    edges: {edges}");
    println!("{:<28} {:>10.1}", "bytes/node (heap)", total_heap as f64 / scale as f64);
    println!(
        "{:<28} {:>10.1}",
        "bytes/edge (heap)",
        if edges > 0 { total_heap as f64 / edges as f64 } else { 0.0 }
    );

    // Edge-attributable cost on its own: the number PERF-10 is written against.
    let edge_only = phases[2].heap_delta;
    println!(
        "{:<28} {:>10.1}   <- edges+adjacency only",
        "bytes/edge (edge phase)",
        if edges > 0 { edge_only as f64 / edges as f64 } else { 0.0 }
    );

    if let (Some(b), Some(f)) = (base_rss, final_rss) {
        let rss_delta = f.saturating_sub(b);
        println!();
        println!("{:<28} {:>10}", "RSS delta (bytes)", rss_delta);
        println!(
            "{:<28} {:>10.1}",
            "bytes/edge (RSS)",
            if edges > 0 { rss_delta as f64 / edges as f64 } else { 0.0 }
        );
        let ratio = if total_heap > 0 { rss_delta as f64 / total_heap as f64 } else { 0.0 };
        println!("{:<28} {:>10.2}x", "RSS / live heap", ratio);
        println!();
        println!("The gap between the two is allocator slack, fragmentation and the");
        println!("binary itself. A layout change moves the heap number; only the RSS");
        println!("number decides whether a graph fits in a given machine.");
    } else {
        println!();
        println!("(RSS unavailable — /proc/self/statm is Linux-only)");
    }

    if let Some(path) = arg("--json") {
        let commit = std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".into());
        let rss_delta = match (base_rss, final_rss) {
            (Some(b), Some(f)) => f.saturating_sub(b) as i64,
            _ => -1,
        };
        let phase_json = phases
            .iter()
            .map(|p| format!("{{\"phase\": \"{}\", \"bytes\": {}}}", p.name, p.heap_delta))
            .collect::<Vec<_>>()
            .join(",\n      ");
        let envelope = format!(
            "{{
  \"suite\": \"memory-footprint\",
  \"requirement_ids\": [\"PERF-10\"],
  \"run_id\": \"footprint-{commit}-{scale}n-{avg_degree}d\",
  \"engine\": {{\"name\": \"samyama\", \"version\": \"{}\", \"commit\": \"{commit}\"}},
  \"hardware\": {{\"note\": \"single process; RSS is host-dependent, heap bytes are not\"}},
  \"dataset\": {{\"name\": \"synthetic\", \"nodes\": {scale}, \"edges\": {edges}, \"avg_degree\": {avg_degree}}},
  \"measurements\": {{
    \"live_heap_bytes\": {total_heap},
    \"rss_delta_bytes\": {rss_delta},
    \"bytes_per_node_heap\": {:.2},
    \"bytes_per_edge_heap\": {:.2},
    \"bytes_per_edge_edge_phase\": {:.2},
    \"phases\": [
      {phase_json}
    ]
  }},
  \"status\": \"measured\",
  \"artifacts\": [\"benches/memory_footprint.rs\"],
  \"caveat\": \"Synthetic dataset. Comparable across runs of this harness; not directly comparable to a competitor's figure on a different dataset.\"
}}
",
            env!("CARGO_PKG_VERSION"),
            total_heap as f64 / scale as f64,
            if edges > 0 { total_heap as f64 / edges as f64 } else { 0.0 },
            if edges > 0 { edge_only as f64 / edges as f64 } else { 0.0 },
        );
        match std::fs::write(&path, envelope) {
            Ok(()) => println!("\nwrote result envelope: {path}"),
            Err(e) => {
                eprintln!("could not write {path}: {e}");
                std::process::exit(1);
            }
        }
    }

    // Keep the store alive to here so nothing is freed before measurement.
    drop(store);
    let _ = build; // the shared fixture builder is kept for future LDBC wiring
}
