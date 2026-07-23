//! E1 — per-operator CPU-vs-GPU ablation on Samyama-Graph (OSS), paper 22.
//!
//! Same engine, GPU on/off via the `SAMYAMA_GPU` kill-switch. Reports CPU ms, GPU ms,
//! and speedup per operator across graph sizes. Preliminary (RTX 4050, 6 GB laptop) —
//! NOT the paper's headline (that needs a GH200); this validates the E1 harness on the
//! OSS build and produces the first E1 numbers from the paper's actual vehicle.
//!
//! Run: cargo run --release -p samyama-graph-algorithms --features gpu --example e1_ablation

use samyama_graph_algorithms::common::{GraphView, NodeId};
use samyama_graph_algorithms::{
    cdlp, count_triangles, local_clustering_coefficient, page_rank, CdlpConfig, PageRankConfig,
};
use std::collections::HashMap;
use std::time::Instant;

/// Deterministic random graph (LCG — reproducible, no rng dependency).
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

fn median_ms<F: FnMut()>(mut f: F, reps: usize) -> f64 {
    let mut times = Vec::with_capacity(reps);
    for _ in 0..reps {
        let t = Instant::now();
        f();
        times.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}

fn set_gpu(on: bool) {
    if on {
        std::env::remove_var("SAMYAMA_GPU");
    } else {
        std::env::set_var("SAMYAMA_GPU", "off");
    }
}

fn label(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}M nodes", n / 1_000_000)
    } else {
        format!("{}k nodes", n / 1000)
    }
}

fn row(op: &str, size: &str, cpu: f64, gpu: f64) {
    let spd = cpu / gpu;
    let tag = if spd >= 1.0 {
        format!("{spd:.2}x faster")
    } else {
        format!("{spd:.2}x (slower)")
    };
    println!("  {op:<14} {size:>12}  {cpu:>10.2}  {gpu:>10.2}   {tag}");
}

fn main() {
    let reps = 3;
    println!("E1 — per-operator CPU-vs-GPU ablation on Samyama-Graph (OSS), paper 22");
    println!(
        "GPU available: {}",
        samyama_graph_algorithms::gpu_dispatch::gpu_available()
    );
    println!();

    // Warm up the GPU (shader compile + buffer alloc) so it is not charged to the first timing.
    set_gpu(true);
    let warm = build_graph(50_000, 10);
    let _ = page_rank(
        &warm,
        PageRankConfig {
            damping_factor: 0.85,
            iterations: 5,
            tolerance: 0.0,
            dangling_redistribution: false,
        },
    );
    let _ = local_clustering_coefficient(&warm);
    drop(warm);

    println!(
        "  {:<14} {:>12}  {:>10}  {:>10}   {}",
        "Algorithm", "Size", "CPU (ms)", "GPU (ms)", "Speedup"
    );
    println!("  {}", "-".repeat(64));

    let pr = |v: &GraphView| {
        let _ = page_rank(
            v,
            PageRankConfig {
                damping_factor: 0.85,
                iterations: 20,
                tolerance: 0.0,
                dangling_redistribution: false,
            },
        );
    };
    let lc = |v: &GraphView| {
        let _ = local_clustering_coefficient(v);
    };
    let cd = |v: &GraphView| {
        let _ = cdlp(v, &CdlpConfig { max_iterations: 10 });
    };

    for &n in &[100_000usize, 250_000, 500_000, 1_000_000] {
        let v = build_graph(n, 10);
        let ops: [(&str, &dyn Fn(&GraphView)); 3] = [
            ("PageRank", &pr),
            ("LCC", &lc),
            ("CDLP", &cd),
        ];
        for (name, f) in ops {
            set_gpu(false);
            let cpu = median_ms(|| f(&v), reps);
            set_gpu(true);
            let gpu = median_ms(|| f(&v), reps);
            row(name, &label(n), cpu, gpu);
        }
        drop(v);
    }

    // Triangle count at smaller sizes (super-linear in degree).
    for &n in &[20_000usize, 50_000, 100_000] {
        let v = build_graph(n, 10);
        set_gpu(false);
        let cpu = median_ms(|| { let _ = count_triangles(&v); }, reps);
        set_gpu(true);
        let gpu = median_ms(|| { let _ = count_triangles(&v); }, reps);
        row("TriangleCount", &label(n), cpu, gpu);
        drop(v);
    }

    println!();
    println!("Note: RTX 4050 (6 GB laptop) vs 16-core rayon. Preliminary — paper headline needs a GH200.");
}
