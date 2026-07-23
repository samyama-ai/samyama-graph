//! E3a — unified memory vs. explicit copy, all GPU operators (paper 22).
//!
//! Same engine, same kernels; the only difference is how the CSR reaches the GPU:
//!   - copy path (`SAMYAMA_GPU_UM=off`): `cuMemcpyHtoD` per run — the copy barrier.
//!   - UM path   (`SAMYAMA_GPU_UM=on`):  `cuMemAllocManaged`, kernel reads it directly.
//!
//! Because UM vs copy is the *same kernel* fed the *same data*, results must be identical
//! (checked first). On this box (discrete RTX 4050) UM is software unified memory — pages
//! migrate over PCIe — so this is the E3a control. On a GH200 the identical code is coherent
//! (E3b) and the copy barrier vanishes for a live graph. Requires the CUDA backend.
//!
//! Run: cargo run --release -p samyama-graph-algorithms --features cuda --example e3a_um_vs_copy

use samyama_graph_algorithms::common::{GraphView, NodeId};
use samyama_graph_algorithms::gpu_dispatch;
use samyama_graph_algorithms::{
    cdlp, count_triangles, local_clustering_coefficient, page_rank, CdlpConfig, PageRankConfig,
};
use std::collections::HashMap;
use std::time::Instant;

fn build_graph(n: usize, avg_degree: usize) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..n).map(|i| i as NodeId).collect();
    let mut node_to_index = HashMap::with_capacity(n);
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

fn pr_cfg() -> PageRankConfig {
    PageRankConfig {
        damping_factor: 0.85,
        iterations: 20,
        tolerance: 0.0,
        dangling_redistribution: false,
    }
}

/// Run one operator (for timing). Names match the table below.
fn run_op(op: &str, v: &GraphView) {
    match op {
        "PageRank" => {
            let _ = page_rank(v, pr_cfg());
        }
        "CDLP" => {
            let _ = cdlp(v, &CdlpConfig { max_iterations: 10 });
        }
        "LCC" => {
            let _ = local_clustering_coefficient(v);
        }
        "TriangleCount" => {
            let _ = count_triangles(v);
        }
        _ => unreachable!(),
    }
}

fn median_ms<F: FnMut()>(mut f: F, reps: usize) -> f64 {
    let mut t = Vec::with_capacity(reps);
    for _ in 0..reps {
        let s = Instant::now();
        f();
        t.push(s.elapsed().as_secs_f64() * 1000.0);
    }
    t.sort_by(|a, b| a.partial_cmp(b).unwrap());
    t[t.len() / 2]
}

fn um(on: bool) {
    if on {
        std::env::set_var("SAMYAMA_GPU_UM", "on");
    } else {
        std::env::set_var("SAMYAMA_GPU_UM", "off");
    }
}

/// UM vs copy must produce identical results (same kernel, same data).
fn correctness_check() {
    let v = build_graph(50_000, 10);
    println!("Correctness (50k nodes, UM must equal copy):");

    um(false);
    let pr_a = page_rank(&v, pr_cfg());
    um(true);
    let pr_b = page_rank(&v, pr_cfg());
    let d = pr_a
        .iter()
        .map(|(k, &a)| (a - pr_b[k]).abs())
        .fold(0.0f64, f64::max);
    println!("  PageRank       max|UM-copy| = {d:.2e}");
    assert!(d < 1e-9, "PageRank UM diverged");

    um(false);
    let c_a = cdlp(&v, &CdlpConfig { max_iterations: 10 }).labels;
    um(true);
    let c_b = cdlp(&v, &CdlpConfig { max_iterations: 10 }).labels;
    let cdlp_eq = c_a.len() == c_b.len() && c_a.iter().all(|(k, val)| c_b.get(k) == Some(val));
    println!("  CDLP           labels identical = {cdlp_eq}");
    assert!(cdlp_eq, "CDLP UM diverged");

    um(false);
    let l_a = local_clustering_coefficient(&v);
    um(true);
    let l_b = local_clustering_coefficient(&v);
    let ld = l_a
        .coefficients
        .iter()
        .map(|(k, &a)| (a - l_b.coefficients[k]).abs())
        .fold(0.0f64, f64::max);
    println!("  LCC            max|UM-copy| = {ld:.2e}");
    assert!(ld < 1e-9, "LCC UM diverged");

    um(false);
    let t_a = count_triangles(&v);
    um(true);
    let t_b = count_triangles(&v);
    println!("  TriangleCount  copy={t_a} um={t_b} equal={}", t_a == t_b);
    assert_eq!(t_a, t_b, "TriangleCount UM diverged");
}

fn main() {
    let backend = gpu_dispatch::init_runtime();
    println!("E3a — unified memory vs. explicit copy (all operators), paper 22");
    println!("Backend: {backend}\n");
    if backend != "CUDA" {
        eprintln!("Requires the CUDA backend (UM is CUDA-only). Got '{backend}'.");
        return;
    }

    correctness_check();
    println!();

    let reps = 5;
    println!("  {:<14} {:>8}  {:>12}  {:>12}   {}", "Operator", "Size", "copy (ms)", "UM (ms)", "UM/copy");
    println!("  {}", "-".repeat(62));

    let plan: [(&str, &[usize]); 4] = [
        ("PageRank", &[250_000, 500_000, 1_000_000]),
        ("CDLP", &[250_000, 500_000, 1_000_000]),
        ("LCC", &[250_000, 500_000, 1_000_000]),
        ("TriangleCount", &[50_000, 100_000]),
    ];

    for (op, sizes) in plan {
        for &n in sizes {
            let v = build_graph(n, 10);
            um(false);
            run_op(op, &v); // warm
            let copy = median_ms(|| run_op(op, &v), reps);
            um(true);
            run_op(op, &v); // warm (first-touch migration)
            let umt = median_ms(|| run_op(op, &v), reps);
            let sizelbl = if n >= 1_000_000 {
                format!("{}M", n / 1_000_000)
            } else {
                format!("{}k", n / 1000)
            };
            println!(
                "  {:<14} {:>8}  {:>12.2}  {:>12.2}   {:.2}x",
                op, sizelbl, copy, umt, umt / copy
            );
            drop(v);
        }
    }
    std::env::remove_var("SAMYAMA_GPU_UM");

    println!("\nUM/copy < 1.0 = unified memory faster; > 1.0 = software-UM migration overhead");
    println!("(expected on a discrete GPU). GH200 coherence should push this below 1.0 (E3b).");
}
