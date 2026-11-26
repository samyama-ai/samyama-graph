//! LDBC Graphalytics Benchmark Runner for Samyama Graph Database
//!
//! Implements the 6 core LDBC Graphalytics algorithms:
//!   1. BFS  — Breadth-First Search (single-source, all distances)
//!   2. PR   — PageRank
//!   3. WCC  — Weakly Connected Components
//!   4. CDLP — Community Detection via Label Propagation
//!   5. LCC  — Local Clustering Coefficient
//!   6. SSSP — Single-Source Shortest Path (Dijkstra)
//!
//! Loads graphs from standard Graphalytics edge-list format and builds a
//! `GraphView` directly for pure algorithm benchmarking.
//!
//! When validation output files are present (downloaded by the script),
//! results are compared against the official LDBC expected values.
//!
//! Usage:
//!   cargo bench --release --bench graphalytics_benchmark -- --all
//!   cargo bench --release --bench graphalytics_benchmark -- --dataset example-directed --algo BFS
//!   cargo bench --release --bench graphalytics_benchmark -- --data-dir data/graphalytics/ --all

use std::collections::{HashMap, VecDeque, BinaryHeap};
use std::cmp::Ordering;
use std::path::Path;
use std::time::Instant;

use samyama_graph_algorithms::{
    GraphView,
    page_rank, PageRankConfig,
    weakly_connected_components,
    cdlp, CdlpConfig,
    local_clustering_coefficient_with, DirectedLcc,
};

#[path = "common/bench_setup.rs"]
mod bench_setup;

mod graphalytics_common;
use graphalytics_common::{
    load_graphalytics_dataset, find_dataset_files, find_properties_file,
    parse_properties, load_validation, DatasetProperties,
    format_num, format_duration, scale, size_class, class_rank, scale_covered,
};

// ============================================================================
// ALGORITHM IMPLEMENTATIONS (Graphalytics-specific single-source variants)
// ============================================================================

/// BFS from a single source, returning distances to all reachable nodes.
/// Unreachable nodes get `u64::MAX` (matches Graphalytics convention of Long.MAX_VALUE).
fn bfs_single_source(view: &GraphView, source_id: u64) -> HashMap<u64, u64> {
    let mut result: HashMap<u64, u64> = HashMap::with_capacity(view.node_count);

    // Initialize all nodes as unreachable
    for &nid in &view.index_to_node {
        result.insert(nid, u64::MAX);
    }

    let Some(&source_idx) = view.node_to_index.get(&source_id) else {
        return result;
    };

    let mut distances: Vec<Option<u64>> = vec![None; view.node_count];
    distances[source_idx] = Some(0);
    let mut queue = VecDeque::new();
    queue.push_back(source_idx);

    while let Some(current) = queue.pop_front() {
        let current_dist = distances[current].unwrap();
        for &neighbor in view.successors(current) {
            if distances[neighbor].is_none() {
                distances[neighbor] = Some(current_dist + 1);
                queue.push_back(neighbor);
            }
        }
    }

    for (idx, dist) in distances.into_iter().enumerate() {
        if let Some(d) = dist {
            result.insert(view.index_to_node[idx], d);
        }
    }
    result
}

/// Dijkstra SSSP from a single source, returning distances to all reachable nodes.
/// Uses edge weights if available, otherwise assumes weight 1.0.
/// Unreachable nodes get `f64::INFINITY` (matches Graphalytics convention).
fn sssp_single_source(view: &GraphView, source_id: u64) -> HashMap<u64, f64> {
    let mut result: HashMap<u64, f64> = HashMap::with_capacity(view.node_count);

    // Initialize all nodes as unreachable
    for &nid in &view.index_to_node {
        result.insert(nid, f64::INFINITY);
    }

    let Some(&source_idx) = view.node_to_index.get(&source_id) else {
        return result;
    };

    #[derive(Copy, Clone, PartialEq)]
    struct State {
        cost: f64,
        idx: usize,
    }
    impl Eq for State {}
    impl Ord for State {
        fn cmp(&self, other: &Self) -> Ordering {
            other.cost.partial_cmp(&self.cost).unwrap_or(Ordering::Equal)
        }
    }
    impl PartialOrd for State {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut dist: Vec<f64> = vec![f64::INFINITY; view.node_count];
    dist[source_idx] = 0.0;
    let mut heap = BinaryHeap::new();
    heap.push(State { cost: 0.0, idx: source_idx });

    while let Some(State { cost, idx }) = heap.pop() {
        if cost > dist[idx] {
            continue;
        }

        let edges = view.successors(idx);
        let weights = view.weights(idx);

        for (i, &next_idx) in edges.iter().enumerate() {
            let w = if let Some(ws) = weights { ws[i] } else { 1.0 };
            if w < 0.0 {
                continue;
            }
            let next_cost = cost + w;
            if next_cost < dist[next_idx] {
                dist[next_idx] = next_cost;
                heap.push(State { cost: next_cost, idx: next_idx });
            }
        }
    }

    for (idx, &d) in dist.iter().enumerate() {
        result.insert(view.index_to_node[idx], d);
    }
    result
}

// ============================================================================
// BENCHMARK RUNNER
// ============================================================================

const ALL_ALGOS: &[&str] = &["BFS", "PR", "WCC", "CDLP", "LCC", "SSSP"];

/// Default datasets to run when --all is specified without --dataset or
/// --size: the two LDBC example graphs, the correctness set. They are far
/// below 2XS, so they cover no size class.
const DEFAULT_DATASETS: &[&str] = &["example-directed", "example-undirected"];

/// Every dataset under `data_dir` that has a properties file stating its
/// vertex and edge counts, with its scale and size class (ALGO-08, #1687).
fn discover_datasets(data_dir: &Path) -> Vec<(String, f64, Option<&'static str>)> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(data_dir) else { return found };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(props_path) = find_properties_file(data_dir, &name) else { continue };
        let props = parse_properties(&props_path);
        if let (Some(v), Some(e)) = (props.vertices, props.edges) {
            let s = scale(v, e);
            found.push((name, s, size_class(s)));
        }
    }
    found.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal).then(a.0.cmp(&b.0)));
    found
}

fn main() {
    bench_setup::init();

    let args: Vec<String> = std::env::args().collect();

    let data_dir = get_arg(&args, "--data-dir")
        .unwrap_or_else(|| "data/graphalytics".to_string());
    let dataset_arg = get_arg(&args, "--dataset");
    let algo_arg = get_arg(&args, "--algo");
    let size_arg = get_arg(&args, "--size");
    let run_all = args.iter().any(|a| a == "--all");
    let validate = !args.iter().any(|a| a == "--no-validate");
    let json_out = get_arg(&args, "--json");

    if !run_all && dataset_arg.is_none() && algo_arg.is_none() {
        print_usage();
        return;
    }

    // Determine which datasets to run based on --dataset and/or --size
    let datasets: Vec<String> = if let Some(ds) = dataset_arg {
        vec![ds]
    } else if let Some(ref size) = size_arg {
        // A size is a Graphalytics class, computed from each dataset's own
        // vertex and edge counts (ALGO-08, #1687), or `all`.
        let want = size.to_uppercase();
        if want != "ALL" && class_rank(&want).is_none() {
            eprintln!("Unknown size '{}'. Valid: 2XS, XS, S, M, L, XL, 2XL, all", size);
            std::process::exit(2);
        }
        let picked: Vec<String> = discover_datasets(Path::new(&data_dir))
            .into_iter()
            .filter(|(_, _, class)| match class {
                Some(c) => want == "ALL" || c.eq_ignore_ascii_case(&want),
                None => false,
            })
            .map(|(name, _, _)| name)
            .collect();
        if picked.is_empty() {
            // Not a silent success: asking for S and running nothing used to
            // exit 0, which a harness reads as "S passed".
            eprintln!("No {} datasets found in {}", want, data_dir);
            eprintln!("Run: scripts/download_graphalytics.sh --size {}", want);
            std::process::exit(2);
        }
        picked
    } else {
        DEFAULT_DATASETS.iter().map(|s| s.to_string()).collect()
    };

    // Determine which algorithms to run
    let algos: Vec<String> = if let Some(algo) = algo_arg {
        vec![algo.to_uppercase()]
    } else {
        ALL_ALGOS.iter().map(|s| s.to_string()).collect()
    };

    println!();
    println!("================================================================");
    println!("  LDBC Graphalytics Benchmark -- Samyama Graph Database");
    println!("================================================================");
    println!();
    println!("  Data directory: {}", data_dir);
    println!("  Datasets:       {:?}", datasets);
    println!("  Algorithms:     {:?}", algos);
    println!("  Validation:     {}", if validate { "enabled" } else { "disabled" });
    println!();

    let data_path = Path::new(&data_dir);

    let mut results: Vec<BenchmarkResult> = Vec::new();
    // Per dataset: its class and whether every algorithm it defines ran and
    // matched the reference (ALGO-08, #1687).
    let mut dataset_runs: Vec<DatasetRun> = Vec::new();

    for dataset in &datasets {
        println!("----------------------------------------------------------------");
        println!("  Dataset: {}", dataset);
        println!("----------------------------------------------------------------");

        // Find dataset files
        let (vertex_path, edge_path) = match find_dataset_files(data_path, dataset) {
            Ok(paths) => paths,
            Err(e) => {
                eprintln!("  ERROR: {}", e);
                eprintln!("  Run scripts/download_graphalytics.sh to download datasets.");
                eprintln!();
                dataset_runs.push(DatasetRun::failed(dataset, "dataset files not found"));
                continue;
            }
        };

        // Parse properties file for algorithm parameters
        let props = if let Some(props_path) = find_properties_file(data_path, dataset) {
            let p = parse_properties(&props_path);
            println!("  Properties loaded from {}", props_path.display());
            p
        } else {
            DatasetProperties::default()
        };

        let directed = props.directed.unwrap_or_else(|| {
            graphalytics_common::is_directed(dataset)
        });
        let (ds_scale, ds_class) = match (props.vertices, props.edges) {
            (Some(v), Some(e)) => {
                let s = scale(v, e);
                (Some(s), size_class(s))
            }
            _ => (None, None),
        };
        if let Some(s) = ds_scale {
            println!("  Scale {:.1}: class {}", s, ds_class.unwrap_or("below 2XS"));
        }

        // Load the graph
        println!("  Loading {} (directed={})...", dataset, directed);
        let load_start = Instant::now();
        let loaded = match load_graphalytics_dataset(&vertex_path, &edge_path, directed, dataset) {
            Ok(g) => g,
            Err(e) => {
                eprintln!("  ERROR loading dataset: {}", e);
                let mut run = DatasetRun::failed(dataset, "dataset did not load");
                run.class = ds_class;
                run.scale = ds_scale;
                dataset_runs.push(run);
                continue;
            }
        };
        let load_time = load_start.elapsed();

        println!(
            "  Loaded {} ({}): {} vertices, {} edges in {}",
            loaded.info.name,
            if loaded.info.directed { "directed" } else { "undirected" },
            format_num(loaded.info.vertex_count),
            format_num(loaded.info.edge_count),
            format_duration(load_time),
        );
        println!();

        let view = &loaded.view;

        let mut run = DatasetRun {
            dataset: dataset.clone(),
            class: ds_class,
            scale: ds_scale,
            vertices: loaded.info.vertex_count,
            edges: loaded.info.edge_count,
            load_ms: load_time.as_secs_f64() * 1000.0,
            all_passed: true,
            note: None,
        };

        // Run each requested algorithm the dataset defines. A dataset's
        // properties list its algorithms; an unweighted graph lists no SSSP and
        // ships no SSSP reference, which is not a failure. Without a list,
        // every requested algorithm counts.
        for algo in &algos {
            if let Some(listed) = &props.algorithms {
                if !listed.iter().any(|a| a == algo) {
                    println!("  {:>6}  {:>12}  not defined for this dataset", algo, "-");
                    continue;
                }
            }
            let result = run_algorithm(algo, view, dataset, &props, data_path, validate, directed);
            let Some(mut r) = result else {
                run.all_passed = false;
                continue;
            };
            // An algorithm the dataset defines with no reference output to
            // check it against has not been validated, and is reported as a
            // failure, not left blank: a blank is how a size class could look
            // covered without one answer having been compared.
            if validate && !r.validated {
                r.validated = true;
                r.validation_passed = false;
                r.summary.push_str(" (no reference output)");
            }
            if !(r.validated && r.validation_passed) {
                run.all_passed = false;
            }
            let status = if r.validated {
                if r.validation_passed { "  PASS" } else { "  FAIL" }
            } else {
                ""
            };
            println!(
                "  {:>6}  {:>12}  {}{}",
                r.algorithm,
                format_duration(r.duration),
                r.summary,
                status,
            );
            results.push(r);
        }
        if !validate {
            run.all_passed = false;
            run.note = Some("validation disabled");
        }
        dataset_runs.push(run);
        println!();
    }

    // ── Final Summary Table ─────────────────────────────────────────────
    if results.len() > 1 {
        println!("================================================================");
        println!("  SUMMARY");
        println!("================================================================");
        println!();
        let row = ["Dataset", "Algo", "Vertices", "Time", "Valid", "Result"];
        println!(
            "  {:>20} {:>6} {:>10} {:>12}  {:>6}  {}",
            row[0], row[1], row[2], row[3], row[4], row[5]
        );
        let row = ["-------", "----", "--------", "----", "-----", "------"];
        println!(
            "  {:>20} {:>6} {:>10} {:>12}  {:>6}  {}",
            row[0], row[1], row[2], row[3], row[4], row[5]
        );
        for r in &results {
            let valid_str = if r.validated {
                if r.validation_passed { "PASS" } else { "FAIL" }
            } else {
                "-"
            };
            println!(
                "  {:>20} {:>6} {:>10} {:>12}  {:>6}  {}",
                r.dataset,
                r.algorithm,
                format_num(r.vertex_count),
                format_duration(r.duration),
                valid_str,
                r.summary,
            );
        }
        println!();
    }

    // The largest size class every dataset of which validated (ALGO-08).
    let covered = scale_covered(
        &dataset_runs.iter().map(|r| (r.class, r.all_passed)).collect::<Vec<_>>(),
    );
    println!("  Datasets by class:");
    for r in &dataset_runs {
        println!(
            "    {:>20}  {:>9}  {}{}",
            r.dataset,
            r.class.unwrap_or("below 2XS"),
            if r.all_passed { "every algorithm validated" } else { "NOT validated" },
            r.note.map(|n| format!(" ({n})")).unwrap_or_default(),
        );
    }
    println!(
        "  scale_covered: {}",
        covered.unwrap_or("none (no size class has every dataset validated)")
    );
    println!();

    if let Some(path) = &json_out {
        let summary = serde_json::json!({
            "scale_covered": covered,
            "validation": validate,
            "datasets": dataset_runs.iter().map(|d| serde_json::json!({
                "dataset": d.dataset,
                "class": d.class,
                "scale": d.scale,
                "vertices": d.vertices,
                "edges": d.edges,
                "load_ms": d.load_ms,
                "all_passed": d.all_passed,
                "note": d.note,
                "algorithms": results.iter().filter(|r| r.dataset == d.dataset).map(|r| serde_json::json!({
                    "algorithm": r.algorithm,
                    "ms": r.duration.as_secs_f64() * 1000.0,
                    "validated": r.validated,
                    "passed": r.validated && r.validation_passed,
                    "summary": r.summary,
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        });
        match std::fs::write(path, serde_json::to_string_pretty(&summary).unwrap()) {
            Ok(()) => println!("  Wrote {}", path),
            Err(e) => {
                eprintln!("  ERROR writing {}: {}", path, e);
                std::process::exit(1);
            }
        }
    }

    // Exit with non-zero status if any validations failed
    let failures = results.iter().filter(|r| r.validated && !r.validation_passed).count();
    if failures > 0 {
        eprintln!("  {} validation(s) FAILED", failures);
        std::process::exit(1);
    }
}

// ============================================================================
// ALGORITHM DISPATCH
// ============================================================================

/// What one dataset's run established (ALGO-08, #1687).
struct DatasetRun {
    dataset: String,
    class: Option<&'static str>,
    scale: Option<f64>,
    vertices: usize,
    edges: usize,
    load_ms: f64,
    /// Every algorithm the dataset defines ran and matched its reference.
    all_passed: bool,
    note: Option<&'static str>,
}

impl DatasetRun {
    fn failed(dataset: &str, note: &'static str) -> Self {
        Self {
            dataset: dataset.to_string(),
            class: None,
            scale: None,
            vertices: 0,
            edges: 0,
            load_ms: 0.0,
            all_passed: false,
            note: Some(note),
        }
    }
}

struct BenchmarkResult {
    dataset: String,
    algorithm: String,
    vertex_count: usize,
    duration: std::time::Duration,
    summary: String,
    validated: bool,
    validation_passed: bool,
}

fn run_algorithm(
    algo: &str,
    view: &GraphView,
    dataset: &str,
    props: &DatasetProperties,
    data_dir: &Path,
    validate: bool,
    directed: bool,
) -> Option<BenchmarkResult> {
    match algo {
        "BFS" => {
            let source_id = props.bfs_source.unwrap_or_else(|| {
                if view.node_count > 0 { view.index_to_node[0] } else { 0 }
            });

            let start = Instant::now();
            let distances = bfs_single_source(view, source_id);
            let duration = start.elapsed();

            let reachable = distances.values().filter(|&&d| d < u64::MAX).count();
            let max_dist = distances.values().filter(|&&d| d < u64::MAX).max().copied().unwrap_or(0);

            // Validation
            let (validated, passed) = if validate {
                validate_bfs(&distances, data_dir, dataset)
            } else {
                (false, false)
            };

            Some(BenchmarkResult {
                dataset: dataset.to_string(),
                algorithm: "BFS".to_string(),
                vertex_count: view.node_count,
                duration,
                summary: format!(
                    "source={}, reachable={}, max_depth={}",
                    source_id, format_num(reachable), max_dist
                ),
                validated,
                validation_passed: passed,
            })
        }

        "PR" | "PAGERANK" => {
            let damping = props.pr_damping.unwrap_or(0.85);
            let iterations = props.pr_iterations.unwrap_or(20);

            let config = PageRankConfig {
                damping_factor: damping,
                iterations, // Use exact iteration count from LDBC properties
                tolerance: 0.0, // No early termination — run exactly num-iterations
                dangling_redistribution: true, // LDBC reference outputs include dangling mass redistribution
            };

            let start = Instant::now();
            let scores = page_rank(view, config);
            let duration = start.elapsed();

            let max_score = scores.values().cloned().fold(0.0f64, f64::max);
            let min_score = scores.values().cloned().fold(f64::MAX, f64::min);

            let (validated, passed) = if validate {
                validate_float_map(&scores, data_dir, dataset, "PR", 1e-6)
            } else {
                (false, false)
            };

            Some(BenchmarkResult {
                dataset: dataset.to_string(),
                algorithm: "PR".to_string(),
                vertex_count: view.node_count,
                duration,
                summary: format!(
                    "iters={}, d={}, min={:.6}, max={:.6}",
                    iterations, damping, min_score, max_score
                ),
                validated,
                validation_passed: passed,
            })
        }

        "WCC" => {
            let start = Instant::now();
            let wcc = weakly_connected_components(view);
            let duration = start.elapsed();

            let num_components = wcc.components.len();
            let largest = wcc.components.values().map(|v| v.len()).max().unwrap_or(0);

            // WCC validation: compare component IDs (two nodes in same component
            // should have the same component label in expected output)
            let (validated, passed) = if validate {
                validate_wcc(&wcc.node_component, data_dir, dataset)
            } else {
                (false, false)
            };

            Some(BenchmarkResult {
                dataset: dataset.to_string(),
                algorithm: "WCC".to_string(),
                vertex_count: view.node_count,
                duration,
                summary: format!(
                    "components={}, largest={}",
                    format_num(num_components), format_num(largest)
                ),
                validated,
                validation_passed: passed,
            })
        }

        "CDLP" => {
            let max_iters = props.cdlp_max_iterations.unwrap_or(100);
            let config = CdlpConfig {
                max_iterations: max_iters,
            };

            let start = Instant::now();
            let result = cdlp(view, &config);
            let duration = start.elapsed();

            // Count distinct communities
            let mut community_counts: HashMap<u64, usize> = HashMap::new();
            for &label in result.labels.values() {
                *community_counts.entry(label).or_insert(0) += 1;
            }
            let num_communities = community_counts.len();
            let largest = community_counts.values().max().copied().unwrap_or(0);

            // CDLP validation: same as WCC, compare community partitions
            let (validated, passed) = if validate {
                validate_cdlp(&result.labels, data_dir, dataset)
            } else {
                (false, false)
            };

            Some(BenchmarkResult {
                dataset: dataset.to_string(),
                algorithm: "CDLP".to_string(),
                vertex_count: view.node_count,
                duration,
                summary: format!(
                    "communities={}, largest={}, iters={}",
                    format_num(num_communities), format_num(largest), result.iterations,
                ),
                validated,
                validation_passed: passed,
            })
        }

        "LCC" => {
            let start = Instant::now();
            // LDBC's definition, because this bench is judged against LDBC's
            // expected-output files. The default (Fagiolo) is what
            // CH-ALGO-PARITY compares to NetworkX -- a different question with
            // a different right answer (#916).
            let definition = if directed { DirectedLcc::Ldbc } else { DirectedLcc::default() };
            let result = local_clustering_coefficient_with(view, directed, definition);
            let duration = start.elapsed();

            let non_zero = result.coefficients.values().filter(|&&cc| cc > 0.0).count();

            let (validated, passed) = if validate {
                validate_float_map(&result.coefficients, data_dir, dataset, "LCC", 1e-6)
            } else {
                (false, false)
            };

            Some(BenchmarkResult {
                dataset: dataset.to_string(),
                algorithm: "LCC".to_string(),
                vertex_count: view.node_count,
                duration,
                summary: format!(
                    "avg_cc={:.6}, non_zero={}",
                    result.average, format_num(non_zero),
                ),
                validated,
                validation_passed: passed,
            })
        }

        "SSSP" => {
            let source_id = props.sssp_source.unwrap_or_else(|| {
                if view.node_count > 0 { view.index_to_node[0] } else { 0 }
            });

            let start = Instant::now();
            let distances = sssp_single_source(view, source_id);
            let duration = start.elapsed();

            let reachable = distances.values().filter(|&&d| d < f64::INFINITY).count();
            let max_dist = distances
                .values()
                .filter(|&&d| d < f64::INFINITY)
                .cloned()
                .fold(0.0f64, f64::max);

            let (validated, passed) = if validate {
                validate_sssp(&distances, data_dir, dataset)
            } else {
                (false, false)
            };

            Some(BenchmarkResult {
                dataset: dataset.to_string(),
                algorithm: "SSSP".to_string(),
                vertex_count: view.node_count,
                duration,
                summary: format!(
                    "source={}, reachable={}, max_dist={:.4}",
                    source_id, format_num(reachable), max_dist,
                ),
                validated,
                validation_passed: passed,
            })
        }

        _ => {
            eprintln!(
                "  WARNING: Unknown algorithm '{}'. Valid: {:?}",
                algo, ALL_ALGOS
            );
            None
        }
    }
}

// ============================================================================
// VALIDATION FUNCTIONS
// ============================================================================

/// Validate BFS distances against expected output.
fn validate_bfs(
    actual: &HashMap<u64, u64>,
    data_dir: &Path,
    dataset: &str,
) -> (bool, bool) {
    let expected = match load_validation(data_dir, dataset, "BFS") {
        Some(e) => e,
        None => return (false, false),
    };

    let mut all_match = true;
    let mut mismatches = 0;

    for (node_id, expected_str) in &expected {
        let expected_val: u64 = if expected_str == "9223372036854775807" {
            u64::MAX
        } else {
            match expected_str.parse() {
                Ok(v) => v,
                Err(_) => { mismatches += 1; continue; }
            }
        };

        let actual_val = actual.get(node_id).copied().unwrap_or(u64::MAX);
        if actual_val != expected_val {
            if mismatches < 3 {
                eprintln!(
                    "    BFS mismatch: node {} expected {} got {}",
                    node_id, expected_val, actual_val
                );
            }
            mismatches += 1;
            all_match = false;
        }
    }

    if mismatches > 3 {
        eprintln!("    ... and {} more mismatches", mismatches - 3);
    }

    (true, all_match)
}

/// Validate float-valued results (PR, LCC) against expected output.
fn validate_float_map(
    actual: &HashMap<u64, f64>,
    data_dir: &Path,
    dataset: &str,
    algo: &str,
    tolerance: f64,
) -> (bool, bool) {
    let expected = match load_validation(data_dir, dataset, algo) {
        Some(e) => e,
        None => return (false, false),
    };

    let mut all_match = true;
    let mut mismatches = 0;

    for (node_id, expected_str) in &expected {
        let expected_val: f64 = match expected_str.parse() {
            Ok(v) => v,
            Err(_) => { mismatches += 1; continue; }
        };

        let actual_val = actual.get(node_id).copied().unwrap_or(0.0);
        let diff = (actual_val - expected_val).abs();
        let rel_diff = if expected_val.abs() > 1e-12 {
            diff / expected_val.abs()
        } else {
            diff
        };

        if diff > tolerance && rel_diff > tolerance {
            if mismatches < 3 {
                eprintln!(
                    "    {} mismatch: node {} expected {:.15e} got {:.15e} (diff={:.2e})",
                    algo, node_id, expected_val, actual_val, diff
                );
            }
            mismatches += 1;
            all_match = false;
        }
    }

    if mismatches > 3 {
        eprintln!("    ... and {} more mismatches", mismatches - 3);
    }

    (true, all_match)
}

/// Validate WCC results: two nodes with the same expected label should be
/// in the same component, and vice versa.
fn validate_wcc(
    actual: &HashMap<u64, usize>,
    data_dir: &Path,
    dataset: &str,
) -> (bool, bool) {
    let expected = match load_validation(data_dir, dataset, "WCC") {
        Some(e) => e,
        None => return (false, false),
    };

    // Build expected partition: group nodes by their expected label
    let mut expected_groups: HashMap<String, Vec<u64>> = HashMap::new();
    for (&node_id, label) in &expected {
        expected_groups.entry(label.clone()).or_default().push(node_id);
    }

    // Check that nodes in the same expected group share the same actual component
    let mut all_match = true;
    let mut mismatches = 0;

    for group in expected_groups.values() {
        if group.len() < 2 {
            continue;
        }
        let first_comp = actual.get(&group[0]);
        for &node_id in &group[1..] {
            let comp = actual.get(&node_id);
            if comp != first_comp {
                if mismatches < 3 {
                    eprintln!(
                        "    WCC mismatch: nodes {} and {} should be in same component",
                        group[0], node_id
                    );
                }
                mismatches += 1;
                all_match = false;
            }
        }
    }

    if mismatches > 3 {
        eprintln!("    ... and {} more mismatches", mismatches - 3);
    }

    (true, all_match)
}

/// Validate CDLP results by exact label comparison.
fn validate_cdlp(
    actual: &HashMap<u64, u64>,
    data_dir: &Path,
    dataset: &str,
) -> (bool, bool) {
    let expected = match load_validation(data_dir, dataset, "CDLP") {
        Some(e) => e,
        None => return (false, false),
    };

    let mut all_match = true;
    let mut mismatches = 0;

    for (node_id, expected_str) in &expected {
        let expected_label: u64 = match expected_str.parse() {
            Ok(v) => v,
            Err(_) => { mismatches += 1; continue; }
        };
        let actual_label = actual.get(node_id).copied().unwrap_or(0);
        if actual_label != expected_label {
            if mismatches < 3 {
                eprintln!(
                    "    CDLP mismatch: node {} expected label {} got {}",
                    node_id, expected_label, actual_label
                );
            }
            mismatches += 1;
            all_match = false;
        }
    }

    if mismatches > 3 {
        eprintln!("    ... and {} more mismatches", mismatches - 3);
    }

    (true, all_match)
}

/// Validate SSSP distances against expected output.
fn validate_sssp(
    actual: &HashMap<u64, f64>,
    data_dir: &Path,
    dataset: &str,
) -> (bool, bool) {
    let expected = match load_validation(data_dir, dataset, "SSSP") {
        Some(e) => e,
        None => return (false, false),
    };

    let mut all_match = true;
    let mut mismatches = 0;

    for (node_id, expected_str) in &expected {
        let expected_val: f64 = if expected_str == "Infinity" || expected_str == "infinity" {
            f64::INFINITY
        } else {
            match expected_str.parse() {
                Ok(v) => v,
                Err(_) => { mismatches += 1; continue; }
            }
        };

        let actual_val = actual.get(node_id).copied().unwrap_or(f64::INFINITY);

        if expected_val.is_infinite() && actual_val.is_infinite() {
            continue; // both unreachable
        }

        let diff = (actual_val - expected_val).abs();
        if diff > 1e-6 {
            if mismatches < 3 {
                eprintln!(
                    "    SSSP mismatch: node {} expected {:.15e} got {:.15e} (diff={:.2e})",
                    node_id, expected_val, actual_val, diff
                );
            }
            mismatches += 1;
            all_match = false;
        }
    }

    if mismatches > 3 {
        eprintln!("    ... and {} more mismatches", mismatches - 3);
    }

    (true, all_match)
}

// ============================================================================
// CLI HELPERS
// ============================================================================

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn print_usage() {
    println!("LDBC Graphalytics Benchmark for Samyama Graph Database");
    println!();
    println!("Usage:");
    println!("  cargo bench --release --bench graphalytics_benchmark -- --all");
    println!("  cargo bench --release --bench graphalytics_benchmark -- --dataset example-directed --algo BFS");
    println!("  cargo bench --release --bench graphalytics_benchmark -- --size S --all");
    println!("  cargo bench --release --bench graphalytics_benchmark -- --data-dir data/graphalytics/ --all");
    println!();
    println!("Options:");
    println!("  --all              Run all algorithms on all default datasets");
    println!("  --dataset <name>   Specify a dataset (e.g., example-directed, wiki-Talk)");
    println!("  --algo <name>      Run a specific algorithm: BFS, PR, WCC, CDLP, LCC, SSSP");
    println!("  --size <class|all> Every dataset in --data-dir of that LDBC size class");
    println!("                     (2XS, XS, S, M, L, XL, 2XL), computed from its");
    println!("                     .properties vertex and edge counts; or all of them");
    println!("  --json <path>      Also write the results and scale_covered as JSON");
    println!("  --data-dir <path>  Dataset directory (default: data/graphalytics/)");
    println!("  --no-validate      Skip result validation against expected outputs");
    println!();
    println!("Datasets are expected in Graphalytics edge-list format:");
    println!("  <data-dir>/<dataset>/<dataset>.v   -- one vertex ID per line");
    println!("  <data-dir>/<dataset>/<dataset>.e   -- source|target[|weight] per line");
    println!();
    println!("Download datasets:");
    println!("  scripts/download_graphalytics.sh                 # XS datasets");
    println!("  scripts/download_graphalytics.sh --size S        # S-size datasets");
}
