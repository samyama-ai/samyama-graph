//! #1807: what does the per-import tax track -- nodes, edges, or segments?
//!
//! Every snapshot import ends in `finish_bulk_load()`, which does four things:
//! compact the write buffer into a new CSR segment, rebuild the edge-type
//! index, recompute the catalog, and rebuild the vector index. Three of those
//! read the *whole accumulated store*, not the snapshot that was just
//! imported, which is why a 1,098-node import costs 201 s at 103M nodes.
//!
//! This probe times each of the four phases separately against a base store of
//! a chosen size, after staging a small "import" into the write buffer. Node
//! count and edge count vary independently so the two can be separated.
//!
//! The `sequence` mode then measures the fix end to end: the same small
//! snapshot imported N times into an identically-sized base store, once with
//! the eager `finish_bulk_load` and once inside
//! `begin_deferred_bulk_load` / `end_deferred_bulk_load`, with the resulting
//! node and edge counts compared.
//!
//! Run: cargo run --release --example import_tax_probe -- [nodes] [edges] [segments] [labels] [types]
//!      cargo run --release --example import_tax_probe -- sequence [nodes] [edges] [imports]

// Measure what ships: the server's allocator, not the system default (ADR-038).
#[global_allocator]
static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;

use samyama::graph::GraphStore;
use samyama::snapshot::{export_tenant, import_tenant};
use std::time::Instant;

/// CPU seconds (user + system) this process has burned, from /proc/self/stat.
///
/// Wall-clock alone cannot tell a phase that got faster from a phase that got
/// more cores. Reported as mean cores = cpu_delta / wall_delta, which is the
/// `%CPU` top shows, divided by 100 (#1461: a CPU figure without a throughput
/// floor is gameable, so both are printed).
fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // utime and stime are fields 14 and 15, after the parenthesised comm.
    let tail = match stat.rfind(')') { Some(i) => &stat[i + 1..], None => return 0.0 };
    let f: Vec<&str> = tail.split_whitespace().collect();
    let hz = 100.0; // CLK_TCK on Linux x86_64
    let utime: f64 = f.get(11).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let stime: f64 = f.get(12).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    (utime + stime) / hz
}

/// Run `f`, returning (wall seconds, mean cores used).
fn timed(f: impl FnOnce()) -> (f64, f64) {
    let c0 = cpu_seconds();
    let t = Instant::now();
    f();
    let wall = t.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - c0;
    (wall, if wall > 0.0 { cpu / wall } else { 0.0 })
}

fn build(nodes: usize, edges: usize, segments: usize) -> GraphStore {
    build_shaped(nodes, edges, segments, 2, 1)
}

/// `labels` distinct node labels and `types` distinct edge types, so the
/// catalog's triple count can be varied independently of graph size.
///
/// The default shape (2 labels, 1 type) is 4 triples, which is the *hardest*
/// case for a parallel catalog rebuild: all the degree entries land in four
/// maps, and merging one map is one thread's work. A federation of 22 KGs has
/// hundreds of triples. Both are worth measuring, and 2/1 is kept as the
/// default so the #1807 numbers stay comparable.
fn build_shaped(nodes: usize, edges: usize, segments: usize, labels: usize, types: usize) -> GraphStore {
    let mut g = GraphStore::new();
    let label_names: Vec<String> = (0..labels.max(1)).map(|i| format!("L{i}")).collect();
    let type_names: Vec<String> = (0..types.max(1)).map(|i| format!("REL{i}")).collect();
    let ids: Vec<_> = (0..nodes)
        .map(|i| g.create_node_stub(label_names[i % label_names.len()].as_str()))
        .collect();
    // Edges spread over the whole node range so every node has adjacency.
    let per_seg = edges / segments.max(1);
    for s in 0..segments.max(1) {
        for e in 0..per_seg {
            let i = (s * per_seg + e) % nodes;
            let j = (i * 7 + 1) % nodes;
            let _ = g.create_edge_stub(ids[i], ids[j], type_names[e % type_names.len()].as_str());
        }
        g.compact_adjacency();
    }
    g.rebuild_edge_type_index();
    g.rebuild_catalog();
    g
}

/// A small snapshot: the EdTech case, 1,000 nodes and 2,000 edges.
fn small_snapshot() -> Vec<u8> {
    let mut g = GraphStore::new();
    let ids: Vec<_> = (0..1_000).map(|_| g.create_node("Small")).collect();
    for i in 0..1_000 {
        let _ = g.create_edge(ids[i], ids[(i + 1) % 1_000], "SMALL");
        let _ = g.create_edge(ids[i], ids[(i + 13) % 1_000], "SMALL");
    }
    let mut buf = Vec::new();
    export_tenant(&g, &mut buf).expect("export");
    buf
}

fn sequence_mode(nodes: usize, edges: usize, imports: usize) {
    let snap = small_snapshot();

    for deferred in [false, true] {
        let mut g = build(nodes, edges, 1);
        let base_nodes = g.node_count();
        let t = Instant::now();
        if deferred {
            g.begin_deferred_bulk_load();
        }
        let mut each = Vec::new();
        for _ in 0..imports {
            let t0 = Instant::now();
            import_tenant(&mut g, &snap[..]).expect("import");
            each.push(t0.elapsed().as_secs_f64());
        }
        if deferred {
            g.end_deferred_bulk_load();
        }
        let total = t.elapsed().as_secs_f64();
        println!(
            "{:9} base_nodes={} imports={} total={:7.2} s  first={:5.2} last={:5.2}  finishes={} segments={} nodes={} edges={}",
            if deferred { "deferred" } else { "eager" },
            base_nodes,
            imports,
            total,
            each.first().copied().unwrap_or(0.0),
            each.last().copied().unwrap_or(0.0),
            g.bulk_load_finish_count(),
            g.adjacency_stats().frozen_segments,
            g.node_count(),
            g.edge_count(),
        );
    }
}

fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.first().map(|s| s.as_str()) == Some("sequence") {
        let nodes: usize = a.get(1).map(|s| s.parse().unwrap()).unwrap_or(4_000_000);
        let edges: usize = a.get(2).map(|s| s.parse().unwrap()).unwrap_or(8_000_000);
        let imports: usize = a.get(3).map(|s| s.parse().unwrap()).unwrap_or(8);
        sequence_mode(nodes, edges, imports);
        return;
    }
    let nodes: usize = a.first().map(|s| s.parse().unwrap()).unwrap_or(1_000_000);
    let edges: usize = a.get(1).map(|s| s.parse().unwrap()).unwrap_or(1_000_000);
    let segments: usize = a.get(2).map(|s| s.parse().unwrap()).unwrap_or(1);
    let labels: usize = a.get(3).map(|s| s.parse().unwrap()).unwrap_or(2);
    let types: usize = a.get(4).map(|s| s.parse().unwrap()).unwrap_or(1);

    let t = Instant::now();
    let mut g = build_shaped(nodes, edges, segments, labels, types);
    let build_s = t.elapsed().as_secs_f64();

    // Stage a small import: 1,000 nodes and 1,000 edges, the EdTech case.
    let small: Vec<_> = (0..1_000).map(|_| g.create_node_stub("C")).collect();
    for i in 0..1_000 {
        let _ = g.create_edge_stub(small[i], small[(i + 1) % 1_000], "SMALL");
    }

    let st = g.adjacency_stats();
    let (compact, compact_c) = timed(|| g.compact_adjacency());
    let (eti, eti_c) = timed(|| g.rebuild_edge_type_index());
    let (cat, cat_c) = timed(|| g.rebuild_catalog());
    let (vec_i, vec_c) = timed(|| g.rebuild_vector_index());

    println!("PROBE nodes={} edges={} segments_before={} labels={} types={} build_s={:.1} threads={}",
        g.node_count(), g.edge_count(), st.frozen_segments, labels, types, build_s, rayon::current_num_threads());
    println!("  compact_adjacency      {:8.3} s  {:5.2} cores", compact, compact_c);
    println!("  rebuild_edge_type_index{:8.3} s  {:5.2} cores", eti, eti_c);
    println!("  rebuild_catalog        {:8.3} s  {:5.2} cores", cat, cat_c);
    println!("  rebuild_vector_index   {:8.3} s  {:5.2} cores", vec_i, vec_c);
    println!("  TOTAL finish_bulk_load {:8.3} s", compact + eti + cat + vec_i);
}
