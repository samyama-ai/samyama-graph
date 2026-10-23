//! CPU/GPU parity tests (workstream §4, HW-09 #1682): every kernel in
//! `samyama-gpu` against a CPU reference, within the tolerance
//! `samyama_gpu::tolerance` declares for it (explicitly not bit-exact). Only
//! compiled under `--features gpu` in test builds.
//!
//! The four graph algorithms go through both dispatch paths over one
//! `GraphView`. The other five kernels are not behind an algorithm yet, so each
//! is called directly and compared with the computation it stands in for.
//!
//! Run: `cargo test -p samyama-graph-algorithms --features gpu`
//!
//! This is a single serial test because it toggles the process-global `SAMYAMA_GPU`
//! kill-switch to obtain a CPU reference and a GPU result over one graph. No other
//! test in this crate reads that env var or dispatches to the GPU (all use tiny graphs
//! below 1000 nodes), so the toggle does not race them.

use crate::common::{GraphView, NodeId};
use crate::{
    cdlp, count_triangles, local_clustering_coefficient, page_rank, CdlpConfig, PageRankConfig,
};
use samyama_gpu::tolerance::GRAPH_SCORE_ABS;
use std::collections::HashMap;

/// Knuth's MMIX linear congruential generator: no rng dependency, and the same
/// numbers on every run and every platform.
const LCG_MULTIPLIER: u64 = 6364136223846793005;
const LCG_INCREMENT: u64 = 1442695040888963407;

/// The seed `build_graph` has always used.
const GRAPH_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

fn lcg_step(state: u64) -> u64 {
    state
        .wrapping_mul(LCG_MULTIPLIER)
        .wrapping_add(LCG_INCREMENT)
}

/// Deterministic random graph (LCG — no rng dependency, reproducible across runs).
fn build_graph(n: usize, avg_degree: usize) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..n).map(|i| i as NodeId).collect();
    let mut node_to_index = HashMap::new();
    for (i, &id) in index_to_node.iter().enumerate() {
        node_to_index.insert(id, i);
    }
    let mut state: u64 = GRAPH_SEED;
    let mut next = move || {
        state = lcg_step(state);
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

    // n = 2000 is below the Metal default threshold (1M, #1402), so pin the threshold
    // to 1000 for this test; otherwise a Metal host would compare CPU against CPU.
    // Other tests in this crate use graphs well under 1000 nodes, so this is race-free.
    std::env::set_var("SAMYAMA_GPU_MIN_NODES", "1000");
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
    std::env::remove_var("SAMYAMA_GPU_MIN_NODES");

    // PageRank — LDBC 6-dp tolerance (parallel reduction order differs; not bit-exact).
    for (id, &c) in &pr_cpu {
        let g = pr_gpu[id];
        assert!(
            (c - g).abs() < GRAPH_SCORE_ABS,
            "PageRank mismatch @{id}: cpu={c} gpu={g}"
        );
    }

    // LCC — per-node coefficients + average, 6-dp.
    for (id, &c) in &lcc_cpu.coefficients {
        let g = lcc_gpu.coefficients[id];
        assert!(
            (c - g).abs() < GRAPH_SCORE_ABS,
            "LCC mismatch @{id}: cpu={c} gpu={g}"
        );
    }
    assert!(
        (lcc_cpu.average - lcc_gpu.average).abs() < GRAPH_SCORE_ABS,
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

// ── The five kernels with no algorithm in front of them (HW-09, #1682) ──

/// Bits of a draw that fill an `f64` mantissa exactly.
const MANTISSA_BITS: u32 = 53;

/// Seeds for the kernel inputs, one per test so no two share data.
const SUM_SEED: u64 = 1;
const SORT_SEED: u64 = 2;
const VECTOR_SEED: u64 = 3;
const PCA_SEED: u64 = 4;

/// Not a multiple of the 256-wide workgroup, so the last group is partial.
const SUM_LEN: usize = 100_003;

/// Not a power of two, so the bitonic sort pads its input.
const SORT_LEN: usize = 5_000;
/// Sort keys span `[-SORT_SPAN / 2, SORT_SPAN / 2)` ...
const SORT_SPAN: f64 = 1_000.0;
/// ... rounded to this many steps per unit, so keys repeat and the sort sees ties.
const SORT_STEPS_PER_UNIT: f64 = 10.0;

/// The dimension `DISTANCE_ABS` is stated for, and a candidate count that
/// leaves the last workgroup partial.
const VECTOR_DIM: usize = 128;
const VECTOR_CANDIDATES: usize = 1_000;

const PCA_SAMPLES: usize = 2_000;
/// Each feature's standard deviation. They are distinct, so the eigenvalues are
/// well separated and power iteration has one answer to converge to.
const PCA_FEATURE_SD: [f64; 6] = [5.0, 3.0, 2.0, 1.0, 0.5, 0.25];
/// Each feature's mean, so centring is exercised.
const PCA_FEATURE_MEAN: [f64; 6] = [1.0, -2.0, 0.5, 3.0, -1.0, 0.0];
/// How much of feature 0 is added to feature 1, so the components are not the
/// coordinate axes.
const PCA_MIXING: f64 = 0.5;
const PCA_COMPONENTS: usize = 3;
const PCA_MAX_ITERATIONS: usize = 1_000;
const PCA_CONVERGENCE: f64 = 1e-7;
/// How often the GPU solver downloads its vector to test convergence.
const PCA_CHECK_INTERVAL: usize = 10;

/// `n` deterministic values in `[0, 1)`.
fn unit_values(seed: u64, n: usize) -> Vec<f64> {
    let mut state = seed;
    (0..n)
        .map(|_| {
            state = lcg_step(state);
            (state >> (u64::BITS - MANTISSA_BITS)) as f64 / (1u64 << MANTISSA_BITS) as f64
        })
        .collect()
}

/// `n` deterministic values in `[-1, 1)`, as the `f32` the vector kernels take.
fn signed_f32(seed: u64, n: usize) -> Vec<f32> {
    unit_values(seed, n)
        .into_iter()
        .map(|v| (2.0 * v - 1.0) as f32)
        .collect()
}

/// A device of the kernel tests' own, or `None` to skip.
///
/// Not `GpuContext::try_global()`: the graph test above sets `SAMYAMA_GPU=off`
/// while it takes its CPU reference, and `try_global` honours that switch, so a
/// kernel test running alongside it would skip at random rather than run.
fn kernel_context() -> Option<samyama_gpu::GpuContext> {
    if !gpu_available_or_skip() {
        return None;
    }
    Some(samyama_gpu::GpuContext::new().expect("an adapter was found, so a context opens"))
}

/// Record the margin, not only the pass: a run's log shows how close each
/// kernel came to its tolerance.
fn report(kernel: &str, measured: f64, tolerance: f64) {
    eprintln!("[gpu-parity] {kernel}: max error {measured:.3e}, tolerance {tolerance:.0e}");
}

#[test]
fn gpu_sum_matches_the_cpu() {
    use samyama_gpu::tolerance::SUM_REL;
    let Some(ctx) = kernel_context() else {
        return;
    };
    // Centred on zero, so positive and negative values cancel.
    let values: Vec<f64> = unit_values(SUM_SEED, SUM_LEN)
        .into_iter()
        .map(|v| v - 0.5)
        .collect();
    let cpu: f64 = values.iter().sum();
    let scale: f64 = values.iter().map(|v| v.abs()).sum();

    let gpu = samyama_gpu::gpu_sum_f64(&ctx, &values).expect("gpu sum");
    let error = (gpu - cpu).abs() / scale;
    report("gpu_sum_f64", error, SUM_REL);
    assert!(
        error <= SUM_REL,
        "sum: cpu={cpu} gpu={gpu}, error {error:e} of sum(|x|)"
    );

    assert_eq!(samyama_gpu::gpu_sum_f64(&ctx, &[]).expect("empty sum"), 0.0);
}

#[test]
fn gpu_argsort_matches_the_cpu() {
    let Some(ctx) = kernel_context() else {
        return;
    };
    let values: Vec<f64> = unit_values(SORT_SEED, SORT_LEN)
        .into_iter()
        .map(|v| ((v - 0.5) * SORT_SPAN * SORT_STEPS_PER_UNIT).round() / SORT_STEPS_PER_UNIT)
        .collect();

    let order = samyama_gpu::gpu_argsort_f64(&ctx, &values).expect("gpu argsort");

    // Every input index exactly once: no padding slot leaks out, none is lost.
    let mut seen = vec![false; values.len()];
    for &i in &order {
        let i = i as usize;
        assert!(i < values.len(), "index {i} is padding, not input");
        assert!(!seen[i], "index {i} returned twice");
        seen[i] = true;
    }
    assert!(seen.iter().all(|&s| s), "an index is missing");

    // The kernel sorts `f32` keys and does not keep ties in input order, so the
    // keys are compared, not the indices.
    let gpu_keys: Vec<f32> = order.iter().map(|&i| values[i as usize] as f32).collect();
    let mut cpu_keys: Vec<f32> = values.iter().map(|&v| v as f32).collect();
    cpu_keys.sort_by(f32::total_cmp);
    assert_eq!(
        gpu_keys, cpu_keys,
        "argsort order differs from the CPU sort"
    );

    assert!(samyama_gpu::gpu_argsort_f64(&ctx, &[])
        .expect("empty argsort")
        .is_empty());
}

/// The query, then candidates: the query itself, the zero vector, and random
/// vectors. Row-major, `VECTOR_DIM` wide.
fn distance_inputs(normalise: bool) -> (Vec<f32>, Vec<f32>) {
    let mut rows = signed_f32(VECTOR_SEED, (VECTOR_CANDIDATES + 1) * VECTOR_DIM);
    if normalise {
        for row in rows.chunks_mut(VECTOR_DIM) {
            let norm = row.iter().map(|x| x * x).sum::<f32>().sqrt();
            row.iter_mut().for_each(|x| *x /= norm);
        }
    }
    let query = rows[..VECTOR_DIM].to_vec();
    let mut candidates = rows[VECTOR_DIM..].to_vec();
    candidates[..VECTOR_DIM].copy_from_slice(&query);
    candidates[VECTOR_DIM..2 * VECTOR_DIM].fill(0.0);
    (query, candidates)
}

fn dot(a: &[f32], b: &[f32]) -> f64 {
    a.iter().zip(b).map(|(&x, &y)| x as f64 * y as f64).sum()
}

/// Cosine distance as the engine's CPU index defines it (`CosineDistance` in
/// `src/vector/index.rs`): 1 for a zero vector, similarity clamped to `[-1, 1]`.
fn cpu_cosine(a: &[f32], b: &[f32]) -> f64 {
    let (na, nb) = (dot(a, a), dot(b, b));
    if na <= 0.0 || nb <= 0.0 {
        return 1.0;
    }
    (1.0 - (dot(a, b) / (na.sqrt() * nb.sqrt())).clamp(-1.0, 1.0)).max(0.0)
}

/// Inner-product distance as the engine's CPU index defines it
/// (`InnerProductDistance` in `src/vector/index.rs`).
fn cpu_inner_product(a: &[f32], b: &[f32]) -> f64 {
    1.0 - dot(a, b)
}

/// The largest absolute difference between the GPU distances and the CPU ones.
fn distance_error(
    query: &[f32],
    candidates: &[f32],
    gpu: &[f32],
    cpu: fn(&[f32], &[f32]) -> f64,
) -> f64 {
    assert_eq!(
        gpu.len(),
        candidates.len() / VECTOR_DIM,
        "one distance per candidate"
    );
    candidates
        .chunks(VECTOR_DIM)
        .zip(gpu)
        .map(|(c, &g)| (g as f64 - cpu(query, c)).abs())
        .fold(0.0, f64::max)
}

#[test]
fn gpu_batch_cosine_matches_the_cpu() {
    use samyama_gpu::tolerance::DISTANCE_ABS;
    let Some(ctx) = kernel_context() else {
        return;
    };
    let (query, candidates) = distance_inputs(false);
    let gpu =
        samyama_gpu::gpu_batch_cosine(&ctx, &query, &candidates, VECTOR_DIM).expect("gpu cosine");
    let error = distance_error(&query, &candidates, &gpu, cpu_cosine);
    report("gpu_batch_cosine", error, DISTANCE_ABS);
    assert!(error <= DISTANCE_ABS, "cosine: max error {error:e}");
}

#[test]
fn gpu_batch_inner_product_matches_the_cpu() {
    use samyama_gpu::tolerance::DISTANCE_ABS;
    let Some(ctx) = kernel_context() else {
        return;
    };
    // Unit vectors: the metric the engine uses inner product for.
    let (query, candidates) = distance_inputs(true);
    let gpu = samyama_gpu::gpu_batch_inner_product(&ctx, &query, &candidates, VECTOR_DIM)
        .expect("gpu inner product");
    let error = distance_error(&query, &candidates, &gpu, cpu_inner_product);
    report("gpu_batch_inner_product", error, DISTANCE_ABS);
    assert!(error <= DISTANCE_ABS, "inner product: max error {error:e}");
}

/// Rows of correlated features with distinct variances, rounded to `f32` once
/// so the CPU and the GPU see the same numbers.
fn pca_rows() -> Vec<Vec<f32>> {
    let features = PCA_FEATURE_SD.len();
    let draws = unit_values(PCA_SEED, PCA_SAMPLES * features);
    // A uniform draw on [-0.5, 0.5) has variance 1/12; scale it to unit variance.
    let unit_variance = 12f64.sqrt();
    draws
        .chunks(features)
        .map(|draw| {
            let mut row: Vec<f64> = draw
                .iter()
                .zip(PCA_FEATURE_SD)
                .map(|(&u, sd)| (u - 0.5) * unit_variance * sd)
                .collect();
            row[1] += PCA_MIXING * row[0];
            row.iter()
                .zip(PCA_FEATURE_MEAN)
                .map(|(&x, mean)| (x + mean) as f32)
                .collect()
        })
        .collect()
}

#[test]
fn gpu_pca_matches_the_cpu() {
    use crate::{pca, PcaConfig, PcaSolver};
    use samyama_gpu::pca::GpuPcaConfig;
    use samyama_gpu::tolerance::{PCA_COMPONENT_ALIGNMENT, PCA_EIGENVALUE_REL, PCA_MEAN_ABS};
    let Some(ctx) = kernel_context() else {
        return;
    };
    let rows = pca_rows();
    let features = PCA_FEATURE_SD.len();

    // The same algorithm on both sides: power iteration with deflation.
    let cpu = pca(
        &rows
            .iter()
            .map(|r| r.iter().map(|&x| x as f64).collect())
            .collect::<Vec<Vec<f64>>>(),
        PcaConfig {
            n_components: PCA_COMPONENTS,
            max_iterations: PCA_MAX_ITERATIONS,
            tolerance: PCA_CONVERGENCE,
            solver: PcaSolver::PowerIteration,
            ..PcaConfig::default()
        },
    );
    let flat: Vec<f32> = rows.concat();
    let gpu = samyama_gpu::gpu_pca(
        &ctx,
        &flat,
        PCA_SAMPLES,
        features,
        &GpuPcaConfig {
            n_components: PCA_COMPONENTS,
            max_iterations: PCA_MAX_ITERATIONS,
            tolerance: PCA_CONVERGENCE,
            check_interval: PCA_CHECK_INTERVAL,
        },
    )
    .expect("gpu pca");

    let mean_error = cpu
        .mean
        .iter()
        .zip(&gpu.mean)
        .map(|(&c, &g)| (c - g as f64).abs())
        .fold(0.0, f64::max);
    report("gpu_pca mean", mean_error, PCA_MEAN_ABS);
    assert!(
        mean_error <= PCA_MEAN_ABS,
        "pca mean: max error {mean_error:e}"
    );

    assert_eq!(gpu.eigenvalues.len(), PCA_COMPONENTS);
    assert_eq!(gpu.components.len(), PCA_COMPONENTS);
    let mut eigen_error: f64 = 0.0;
    let mut alignment_error: f64 = 0.0;
    for i in 0..PCA_COMPONENTS {
        let c = cpu.explained_variance[i];
        eigen_error = eigen_error.max((gpu.eigenvalues[i] as f64 - c).abs() / c);

        let (cv, gv) = (&cpu.components[i], &gpu.components[i]);
        let cos = cv.iter().zip(gv).map(|(&a, &b)| a * b as f64).sum::<f64>()
            / (cv.iter().map(|a| a * a).sum::<f64>().sqrt()
                * gv.iter().map(|&b| b as f64 * b as f64).sum::<f64>().sqrt());
        alignment_error = alignment_error.max(1.0 - cos.abs());
    }
    report("gpu_pca eigenvalues", eigen_error, PCA_EIGENVALUE_REL);
    report(
        "gpu_pca components",
        alignment_error,
        PCA_COMPONENT_ALIGNMENT,
    );
    assert!(
        eigen_error <= PCA_EIGENVALUE_REL,
        "pca eigenvalues: max relative error {eigen_error:e}"
    );
    assert!(
        alignment_error <= PCA_COMPONENT_ALIGNMENT,
        "pca components: max 1 - |cos| = {alignment_error:e}"
    );
}
