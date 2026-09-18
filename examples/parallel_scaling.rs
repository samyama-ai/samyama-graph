//! ALGO-09: how far does each frontier-based algorithm scale with cores?
//!
//! The requirement asks for **≥0.6 efficiency at 16 cores for all frontier-based
//! algorithms**, and it has been `unmeasured` since it was written — "Rayon,
//! unmeasured" was the whole baseline. Rayon appears in four algorithm files;
//! whether the work actually spreads, and what happens to the six that never
//! call it, is a different question and this is it.
//!
//! Efficiency is `t(1) / (t(k) * k)`: 1.0 is perfect scaling, and a sequential
//! algorithm measured at 16 threads scores about 1/16. That is the number the
//! requirement wants, and it is deliberately unkind — a algorithm that is 3×
//! faster on 16 cores looks good until you notice it is using sixteen of them.
//!
//! Timing needs a quiet host. Run it on an idle machine and pass `--json` so the
//! harness can read it; a laptop with a browser open will produce numbers that
//! say more about the browser.
//!
//! ```text
//! cargo run --release --example parallel_scaling -- --json scaling.json
//! ```

use std::collections::HashMap;
use std::time::Instant;

use samyama_graph_algorithms::{
    betweenness_centrality, cdlp, closeness_centrality, count_triangles,
    local_clustering_coefficient, page_rank, weakly_connected_components, CdlpConfig, GraphView,
    NodeId, PageRankConfig,
};

/// The same LCG the parity exporter uses, so a graph can be rebuilt from a seed
/// rather than shipped.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
}

/// A connected undirected graph, stored once per direction so every algorithm
/// sees the same neighbourhood without a `bidirectional` flag.
fn build(n: usize, m: usize, seed: u64) -> GraphView {
    let mut rng = Lcg(seed);
    let mut out: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut inc: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut seen: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    let mut add = |a: usize, b: usize, out: &mut Vec<Vec<usize>>, inc: &mut Vec<Vec<usize>>| {
        out[a].push(b);
        inc[b].push(a);
        out[b].push(a);
        inc[a].push(b);
    };
    // A spanning chain first: a disconnected graph would make WCC trivially
    // cheap and the comparison meaningless.
    for i in 1..n {
        seen.insert((i - 1, i));
        add(i - 1, i, &mut out, &mut inc);
    }
    let mut edges = n - 1;
    while edges < m {
        let a = rng.below(n as u64) as usize;
        let b = rng.below(n as u64) as usize;
        if a == b {
            continue;
        }
        let key = if a < b { (a, b) } else { (b, a) };
        if !seen.insert(key) {
            continue;
        }
        add(key.0, key.1, &mut out, &mut inc);
        edges += 1;
    }
    let index_to_node: Vec<NodeId> = (0..n as u64).collect();
    let mut node_to_index = HashMap::new();
    for (i, id) in index_to_node.iter().enumerate() {
        node_to_index.insert(*id, i);
    }
    GraphView::from_adjacency_list(n, index_to_node, node_to_index, out, inc, None)
}

/// Median of `reps` runs. The median rather than the mean because one scheduling
/// hiccup on a shared box moves a mean and does not move a median, and the
/// question here is how the work spreads rather than how unlucky one run was.
fn time_ms(reps: usize, mut f: impl FnMut()) -> f64 {
    let mut times: Vec<f64> = (0..reps)
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed().as_secs_f64() * 1000.0
        })
        .collect();
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}

struct Case {
    name: &'static str,
    /// Frontier-based in the sense ALGO-09 means: the work is a traversal or a
    /// per-node sweep that could in principle be split across cores.
    frontier: bool,
    /// Whether the implementation calls into rayon at all, read from the source
    /// rather than inferred from the timings.
    uses_rayon: bool,
    small: bool,
    run: fn(&GraphView),
}

const CASES: &[Case] = &[
    Case { name: "pageRank", frontier: true, uses_rayon: true, small: false,
           run: |v| { page_rank(v, PageRankConfig::default()); } },
    Case { name: "cdlp", frontier: true, uses_rayon: true, small: false,
           run: |v| { cdlp(v, &CdlpConfig::default()); } },
    Case { name: "lcc", frontier: true, uses_rayon: true, small: false,
           run: |v| { local_clustering_coefficient(v); } },
    Case { name: "triangleCount", frontier: true, uses_rayon: true, small: false,
           run: |v| { count_triangles(v); } },
    Case { name: "wcc", frontier: true, uses_rayon: false, small: false,
           run: |v| { weakly_connected_components(v); } },
    // O(n*m): a graph big enough for the others would run for hours.
    Case { name: "betweenness", frontier: true, uses_rayon: false, small: true,
           run: |v| { betweenness_centrality(v, false); } },
    Case { name: "closeness", frontier: true, uses_rayon: false, small: true,
           run: |v| { closeness_centrality(v, false); } },
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
    };
    let json_out = arg("--json");
    let reps: usize = arg("--reps").and_then(|v| v.parse().ok()).unwrap_or(3);
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let mut threads: Vec<usize> = vec![1, 2, 4, 8, 16];
    threads.retain(|t| *t <= cores);
    if !threads.contains(&cores) {
        threads.push(cores);
    }

    let big = build(200_000, 2_000_000, 12345);
    let small = build(3_000, 30_000, 777);
    eprintln!(
        "cores={cores} threads={threads:?} reps={reps} big=200k/2M small=3k/30k"
    );

    let mut runs = Vec::new();
    for case in CASES {
        let view = if case.small { &small } else { &big };
        let mut baseline = 0.0_f64;
        for &t in &threads {
            let pool = rayon::ThreadPoolBuilder::new().num_threads(t).build().unwrap();
            let ms = pool.install(|| time_ms(reps, || (case.run)(view)));
            if t == 1 {
                baseline = ms;
            }
            let speedup = if ms > 0.0 { baseline / ms } else { 0.0 };
            eprintln!(
                "{:>14} t={:>2} {:>9.1} ms  speedup {:>5.2}x  efficiency {:>4.2}",
                case.name, t, ms, speedup, speedup / t as f64
            );
            runs.push(serde_json::json!({
                "algorithm": case.name,
                "threads": t,
                "median_ms": (ms * 10.0).round() / 10.0,
                "speedup": (speedup * 1000.0).round() / 1000.0,
                "efficiency": (speedup / t as f64 * 1000.0).round() / 1000.0,
                "frontier": case.frontier,
                "uses_rayon": case.uses_rayon,
                "graph": if case.small { "small" } else { "big" },
            }));
        }
    }

    let doc = serde_json::json!({
        "cores_available": cores,
        "thread_counts": threads,
        "repetitions": reps,
        "graphs": {"big": {"nodes": 200_000, "edges": 2_000_000},
                   "small": {"nodes": 3_000, "edges": 30_000}},
        "runs": runs,
    });
    if let Some(path) = json_out {
        std::fs::write(&path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
        eprintln!("wrote {path}");
    } else {
        println!("{}", serde_json::to_string_pretty(&doc).unwrap());
    }
}
