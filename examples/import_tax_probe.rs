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
//! Run: cargo run --release --example import_tax_probe -- [nodes] [edges] [segments]
//!      cargo run --release --example import_tax_probe -- sequence [nodes] [edges] [imports]

// Measure what ships: the server's allocator, not the system default (ADR-038).
#[global_allocator]
static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;

use samyama::graph::GraphStore;
use samyama::snapshot::{export_tenant, import_tenant};
use std::time::Instant;

fn build(nodes: usize, edges: usize, segments: usize) -> GraphStore {
    let mut g = GraphStore::new();
    let ids: Vec<_> = (0..nodes).map(|i| g.create_node_stub(if i % 2 == 0 { "A" } else { "B" })).collect();
    // Edges spread over the whole node range so every node has adjacency.
    let per_seg = edges / segments.max(1);
    for s in 0..segments.max(1) {
        for e in 0..per_seg {
            let i = (s * per_seg + e) % nodes;
            let j = (i * 7 + 1) % nodes;
            let _ = g.create_edge_stub(ids[i], ids[j], "REL");
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

    let t = Instant::now();
    let mut g = build(nodes, edges, segments);
    let build_s = t.elapsed().as_secs_f64();

    // Stage a small import: 1,000 nodes and 1,000 edges, the EdTech case.
    let small: Vec<_> = (0..1_000).map(|_| g.create_node_stub("C")).collect();
    for i in 0..1_000 {
        let _ = g.create_edge_stub(small[i], small[(i + 1) % 1_000], "SMALL");
    }

    let st = g.adjacency_stats();
    let t = Instant::now(); g.compact_adjacency();            let compact = t.elapsed().as_secs_f64();
    let t = Instant::now(); g.rebuild_edge_type_index();       let eti = t.elapsed().as_secs_f64();
    let t = Instant::now(); g.rebuild_catalog();               let cat = t.elapsed().as_secs_f64();
    let t = Instant::now(); g.rebuild_vector_index();          let vec_i = t.elapsed().as_secs_f64();

    println!("PROBE nodes={} edges={} segments_before={} build_s={:.1}", g.node_count(), g.edge_count(), st.frozen_segments, build_s);
    println!("  compact_adjacency      {:8.3} s", compact);
    println!("  rebuild_edge_type_index{:8.3} s", eti);
    println!("  rebuild_catalog        {:8.3} s", cat);
    println!("  rebuild_vector_index   {:8.3} s", vec_i);
    println!("  TOTAL finish_bulk_load {:8.3} s", compact + eti + cat + vec_i);
}
