//! CPU/GPU parity tests (workstream §4): the same `GraphView` through both paths,
//! matched to LDBC ~6-dp tolerance (explicitly not bit-exact). Only compiled under
//! `--features gpu` in test builds.
//!
//! Run: `cargo test -p samyama-graph-algorithms --features gpu`
//!
//! This is a single serial test because it toggles the process-global `SAMYAMA_GPU`
//! kill-switch to obtain a CPU reference and a GPU result over one graph. No other
//! test in this crate reads that env var or dispatches to the GPU (all use tiny graphs
//! below `MIN_GPU_NODES`), so the toggle does not race them.

use crate::common::{GraphView, NodeId};
use crate::{cdlp, count_triangles, local_clustering_coefficient, page_rank, CdlpConfig, PageRankConfig};
use std::collections::HashMap;

/// Deterministic random graph (LCG — no rng dependency, reproducible across runs).
fn build_graph(n: usize, avg_degree: usize) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..n).map(|i| i as NodeId).collect();
    let mut node_to_index = HashMap::new();
    for (i, &id) in index_to_node.iter().enumerate() {
        node_to_index.insert(id, i);
    }
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as usize
    };
    let mut outgoing = vec![Vec::new(); n];
    for u in 0..n {
        for _ in 0..avg_degree {
            let v = next() % n;
            if v != u {
                outgoing[u].push(v);
            }
        }
    }
    let mut incoming = vec![Vec::new(); n];
    for u in 0..n {
        for &v in &outgoing[u] {
            incoming[v].push(u);
        }
    }
    GraphView::from_adjacency_list(n, index_to_node, node_to_index, outgoing, incoming, None)
}

/// True if a GPU is available; panics instead of skipping when `SAMYAMA_REQUIRE_GPU=1`.
fn gpu_available_or_skip() -> bool {
    if samyama_gpu::GpuContext::is_available() {
        return true;
    }
    let require = std::env::var("SAMYAMA_REQUIRE_GPU").as_deref() == Ok("1");
    assert!(
        !require,
        "SAMYAMA_REQUIRE_GPU=1 but no GPU available — failing parity test instead of skipping."
    );
    eprintln!("[gpu-parity] no GPU — skipping.");
    false
}

fn pr_config() -> PageRankConfig {
    // tolerance 0.0 => run all iterations (deterministic iteration count for parity).
    PageRankConfig {
        damping_factor: 0.85,
        iterations: 50,
        tolerance: 0.0,
        dangling_redistribution: false,
    }
}

#[test]
fn cpu_gpu_parity_all_ops() {
    if !gpu_available_or_skip() {
        return;
    }

    // n > MIN_GPU_NODES (1000) so the dispatch routes to the GPU.
    let view = build_graph(2000, 10);

    // Force the CPU path with the kill-switch and capture references.
    std::env::set_var("SAMYAMA_GPU", "off");
    let pr_cpu = page_rank(&view, pr_config());
    let lcc_cpu = local_clustering_coefficient(&view);
    let tri_cpu = count_triangles(&view);
    let cdlp_cpu = cdlp(&view, &CdlpConfig { max_iterations: 20 });

    // Enable the GPU and re-run. Assert the context actually initialized, otherwise
    // we would be silently comparing CPU against CPU and the test would be worthless.
    std::env::remove_var("SAMYAMA_GPU");
    assert!(
        samyama_gpu::GpuContext::try_global().is_some(),
        "GPU context failed to initialize; parity test would be CPU-vs-CPU"
    );
    let pr_gpu = page_rank(&view, pr_config());
    let lcc_gpu = local_clustering_coefficient(&view);
    let tri_gpu = count_triangles(&view);
    let cdlp_gpu = cdlp(&view, &CdlpConfig { max_iterations: 20 });

    // PageRank — LDBC 6-dp tolerance (parallel reduction order differs; not bit-exact).
    for (id, &c) in &pr_cpu {
        let g = pr_gpu[id];
        assert!(
            (c - g).abs() < 1e-6,
            "PageRank mismatch @{id}: cpu={c} gpu={g}"
        );
    }

    // LCC — per-node coefficients + average, 6-dp.
    for (id, &c) in &lcc_cpu.coefficients {
        let g = lcc_gpu.coefficients[id];
        assert!((c - g).abs() < 1e-6, "LCC mismatch @{id}: cpu={c} gpu={g}");
    }
    assert!(
        (lcc_cpu.average - lcc_gpu.average).abs() < 1e-6,
        "LCC average mismatch: cpu={} gpu={}",
        lcc_cpu.average,
        lcc_gpu.average
    );

    // Triangle count — exact integer parity.
    assert_eq!(tri_cpu, tri_gpu, "triangle count mismatch");

    // CDLP — smoke only (both label every node). Full partition parity is validated
    // against the LDBC reference separately: CPU vs GPU tie-breaking can legitimately
    // differ, so label-equality is not asserted here.
    assert_eq!(cdlp_cpu.labels.len(), view.node_count);
    assert_eq!(cdlp_gpu.labels.len(), view.node_count);
}
