//! Resource and cost profile of the vector index (#1989).
//!
//! For each quantization the index offers, on one base set:
//!
//! - **bytes per vector**: the index's own accounting (`vector_bytes`,
//!   `graph_bytes`, `resident_bytes`), against the theoretical
//!   `dim·bytes + M·2·4`, so the overhead above the floor is attributable;
//! - **process RSS**: the growth over the phase and its peak, sampled on a
//!   background thread (1 Hz by default), not read once at the end;
//! - **build time**: an empty index filled one vector at a time;
//! - **dump / restart**: the index written with `VectorIndex::dump`, its size
//!   on disk, then `load_with_params` — which rebuilds the graph, as the server
//!   does at startup today — and the first query after it. Restart-to-first-
//!   query is the load plus that query;
//! - **recall and QPS** over an ef sweep, and the fastest point reaching the
//!   recall target (`--recall-target`, default 0.95). Memory is never shown
//!   without the recall it was measured at;
//! - **cost**: dollars per million queries at that point, and dollars of host
//!   time for the build, from `--dollars-per-hour`.
//!
//! ```text
//! # Default: 10,000 clustered synthetic vectors of 96 dims, 500 queries.
//! cargo bench --bench vector_cost_profile
//!
//! # The issue's grid, one size per run (RSS is cleanest one quantization per process):
//! cargo bench --bench vector_cost_profile -- --synthetic 1000000x768 --quantization fp16
//!
//! # A dataset directory as scripts/download_ann_datasets.py writes it:
//! cargo bench --bench vector_cost_profile -- --dataset data/ann/sift-128-euclidean \
//!     --dollars-per-hour 0.0786 --instance "r6i.xlarge spot" --json sift-cost.json
//! ```
//!
//! Options:
//!
//! | flag | default | |
//! |---|---|---|
//! | `--synthetic NxDIM` | `10000x96` | clustered synthetic vectors, brute-force ground truth |
//! | `--dataset DIR` | none | `base.fvecs`, `query.fvecs`, optional `groundtruth.ivecs`, `metric.txt` |
//! | `--limit N`, `--queries N` | all / 500 | base vectors and queries used |
//! | `--metric` | l2, or `metric.txt` | l2, cosine or dot |
//! | `--k` | 10 | recall@k |
//! | `--ef` | `10,20,40,80,160,320,640` | search widths swept |
//! | `--threads` | cores | client threads; QPS is throughput of the whole host |
//! | `--m`, `--ef-construction` | 16, 200 | HNSW build parameters |
//! | `--quantization` | all | comma list of names from the table below |
//! | `--recall-target` | 0.95 | recall the cost is quoted at |
//! | `--dollars-per-hour` | 0.252 | host price; see `DEFAULT_DOLLARS_PER_HOUR` |
//! | `--instance` | the default's name | what the price is for, echoed in the report |
//! | `--rss-interval-ms` | 1000 | RSS sampling period |
//! | `--json FILE` | none | write every row as JSON |
//!
//! Quantizations profiled: `vector_cost_common::QUANTIZATIONS`. A new one is
//! one line there.
//!
//! Caveats, stated rather than hidden:
//!
//! - RSS is read from `/proc/self/status` and is unknown off Linux. A freed
//!   index does not always return its pages to the OS, so when several
//!   quantizations run in one process the later rows' RSS includes what the
//!   earlier ones left behind; pass one `--quantization` per run to compare.
//! - The dollar figures are host time only — no storage, network or
//!   replication — and are only meaningful when the price is for the host
//!   that ran the bench.
//! - In-process, no network: the numbers are this engine's ceiling and are
//!   not comparable to a system measured over its API. Cross-system rows
//!   belong in `samyama-graph-competitor-benchmarks`.
//!
//! Run with `--release` on a quiet host and compare the calibration line
//! before comparing timings (#529).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[path = "common/bench_setup.rs"]
mod bench_setup;

#[path = "ann_common/mod.rs"]
mod ann_common;

#[path = "vector_cost_common/mod.rs"]
mod vector_cost_common;

use ann_common::{Metric, Neighbours, Vectors};
use samyama::graph::NodeId;
use samyama::vector::index::Quantization;
use samyama::vector::{HnswParams, VectorIndex};
use vector_cost_common::{Sweep, QUANTIZATIONS};

struct Args {
    synthetic: Option<(usize, usize)>,
    dataset: Option<PathBuf>,
    limit: Option<usize>,
    queries: Option<usize>,
    metric: Option<Metric>,
    k: usize,
    ef: Vec<usize>,
    threads: usize,
    m: usize,
    ef_construction: usize,
    quantizations: Vec<(&'static str, Quantization)>,
    recall_target: f64,
    dollars_per_hour: f64,
    instance: String,
    rss_interval: Duration,
    json: Option<PathBuf>,
}

fn list(s: &str, flag: &str) -> Result<Vec<usize>, String> {
    s.split(',')
        .map(|v| {
            v.trim()
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| format!("{flag}: `{v}` is not a positive integer"))
        })
        .collect()
}

fn one(s: &str, flag: &str) -> Result<usize, String> {
    match list(s, flag)?.as_slice() {
        [n] => Ok(*n),
        _ => Err(format!("{flag} takes one number")),
    }
}

fn positive_f64(s: &str, flag: &str) -> Result<f64, String> {
    s.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v > 0.0)
        .ok_or_else(|| format!("{flag}: `{s}` is not a positive number"))
}

fn quantizations(s: &str) -> Result<Vec<(&'static str, Quantization)>, String> {
    s.split(',')
        .map(|name| {
            let q = Quantization::parse(name.trim())
                .ok_or_else(|| format!("--quantization: unknown `{name}`"))?;
            QUANTIZATIONS
                .iter()
                .find(|(_, known)| *known == q)
                .copied()
                .ok_or_else(|| format!("--quantization: `{name}` is not profiled"))
        })
        .collect()
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        synthetic: None,
        dataset: None,
        limit: None,
        queries: None,
        metric: None,
        k: 10,
        ef: vec![10, 20, 40, 80, 160, 320, 640],
        threads: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        m: 16,
        ef_construction: 200,
        quantizations: QUANTIZATIONS.to_vec(),
        recall_target: 0.95,
        dollars_per_hour: vector_cost_common::DEFAULT_DOLLARS_PER_HOUR,
        instance: vector_cost_common::DEFAULT_INSTANCE.to_string(),
        rss_interval: Duration::from_secs(1),
        json: None,
    };
    let mut instance_given = false;
    // `cargo bench` passes `--bench` to every harness=false target.
    let mut it = std::env::args().skip(1).filter(|a| a != "--bench");
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--synthetic" => {
                let v = value()?;
                let (n, d) = v
                    .split_once('x')
                    .ok_or_else(|| format!("--synthetic takes NxDIM, got `{v}`"))?;
                a.synthetic = Some((one(n, "--synthetic")?, one(d, "--synthetic")?));
            }
            "--dataset" => a.dataset = Some(value()?.into()),
            "--limit" => a.limit = Some(one(&value()?, "--limit")?),
            "--queries" => a.queries = Some(one(&value()?, "--queries")?),
            "--metric" => a.metric = Some(Metric::parse(&value()?)?),
            "--k" => a.k = one(&value()?, "--k")?,
            "--ef" => a.ef = list(&value()?, "--ef")?,
            "--threads" => a.threads = one(&value()?, "--threads")?,
            "--m" => a.m = one(&value()?, "--m")?,
            "--ef-construction" => a.ef_construction = one(&value()?, "--ef-construction")?,
            "--quantization" => a.quantizations = quantizations(&value()?)?,
            "--recall-target" => {
                let v = value()?;
                a.recall_target = positive_f64(&v, "--recall-target")?;
                if a.recall_target > 1.0 {
                    return Err(format!("--recall-target is a fraction, got `{v}`"));
                }
            }
            "--dollars-per-hour" => {
                a.dollars_per_hour = positive_f64(&value()?, "--dollars-per-hour")?
            }
            "--instance" => {
                a.instance = value()?;
                instance_given = true;
            }
            "--rss-interval-ms" => {
                a.rss_interval = Duration::from_millis(one(&value()?, "--rss-interval-ms")? as u64)
            }
            "--json" => a.json = Some(value()?.into()),
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    if a.synthetic.is_some() && a.dataset.is_some() {
        return Err("--synthetic and --dataset are alternatives".into());
    }
    if a.synthetic.is_none() && a.dataset.is_none() {
        a.synthetic = Some((10_000, 96));
    }
    // A price passed without saying what it is for is no longer the default
    // instance's, and must not be labelled as if it were.
    if a.dollars_per_hour != vector_cost_common::DEFAULT_DOLLARS_PER_HOUR && !instance_given {
        a.instance = "host given by --dollars-per-hour".to_string();
    }
    HnswParams {
        m: a.m,
        ef_construction: a.ef_construction,
        ef: None,
    }
    .validate()?;
    Ok(a)
}

struct Dataset {
    name: String,
    metric: Metric,
    base: Vectors,
    queries: Vectors,
    truth: Neighbours,
    truth_source: &'static str,
}

fn load_dataset(a: &Args) -> Result<Dataset, String> {
    if let Some((n, dim)) = a.synthetic {
        let metric = a.metric.unwrap_or(Metric::L2);
        let base = ann_common::synthetic(n, dim, 0x5EED);
        let queries = ann_common::synthetic(a.queries.unwrap_or(500), dim, 0xC0FFEE);
        eprintln!(
            "Computing exact ground truth for {} queries over {n} vectors ...",
            queries.len()
        );
        let truth = ann_common::exact_ground_truth(&base, &queries, a.k, metric);
        return Ok(Dataset {
            name: format!("synthetic-{n}x{dim}"),
            metric,
            base,
            queries,
            truth,
            truth_source: "brute force",
        });
    }
    let dir = a.dataset.as_deref().expect("a source was chosen");
    let file_metric = match std::fs::read_to_string(dir.join("metric.txt")) {
        Ok(s) => Some(Metric::parse(s.trim())?),
        Err(_) => None,
    };
    let metric = match (a.metric, file_metric) {
        (Some(m), Some(f)) if m != f => {
            return Err(format!(
                "--metric {} disagrees with metric.txt ({})",
                m.name(),
                f.name()
            ))
        }
        (Some(m), _) | (None, Some(m)) => m,
        (None, None) => return Err("--metric is required: the dataset does not say".into()),
    };
    let base = ann_common::read_fvecs(&dir.join("base.fvecs"), a.limit)?;
    let queries = ann_common::read_fvecs(&dir.join("query.fvecs"), a.queries)?;
    if base.dim != queries.dim {
        return Err(format!(
            "base vectors have {} dimensions and queries have {}",
            base.dim, queries.dim
        ));
    }
    let gt = dir.join("groundtruth.ivecs");
    // Published ground truth is for the whole base set; after `--limit` its
    // neighbours may lie past the cut, so it is recomputed.
    let (truth, truth_source) = if gt.exists() && a.limit.is_none() {
        let t = ann_common::read_ivecs(&gt, a.queries)?;
        if t.len() < queries.len() || t.width < a.k {
            return Err(format!(
                "{} has {} rows of {} for {} queries at k={}",
                gt.display(),
                t.len(),
                t.width,
                queries.len(),
                a.k
            ));
        }
        (t, "file")
    } else {
        eprintln!(
            "Computing exact ground truth for {} queries over {} vectors ...",
            queries.len(),
            base.len()
        );
        (
            ann_common::exact_ground_truth(&base, &queries, a.k, metric),
            "brute force",
        )
    };
    Ok(Dataset {
        name: dir
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default(),
        metric,
        base,
        queries,
        truth,
        truth_source,
    })
}

fn rss_now() -> Option<u64> {
    ann_common::rss_bytes().0
}

/// One quantization's profile.
struct Row {
    name: &'static str,
    bytes_per_value: usize,
    vector_bytes_per_vec: f64,
    graph_bytes_per_vec: f64,
    resident_bytes_per_vec: f64,
    theoretical_bytes_per_vec: usize,
    rss_growth: Option<u64>,
    rss_peak: Option<u64>,
    build: Duration,
    dump: Duration,
    disk_bytes: u64,
    load: Duration,
    first_query: Duration,
    sweep: Vec<ann_common::Point>,
    at_target: Option<Sweep>,
    best_recall: f64,
}

impl Row {
    fn restart_to_first_query(&self) -> Duration {
        self.load + self.first_query
    }
}

fn search_with<'a>(
    index: &'a VectorIndex,
) -> impl Fn(&[f32], usize, usize) -> Result<Vec<u32>, String> + Sync + 'a {
    move |q: &[f32], k: usize, ef: usize| {
        index
            .search_ef(q, k, Some(ef))
            .map(|hits| hits.into_iter().map(|(id, _)| id.as_u64() as u32).collect())
            .map_err(|e| e.to_string())
    }
}

fn profile(
    a: &Args,
    ds: &Dataset,
    name: &'static str,
    quantization: Quantization,
    dir: &Path,
) -> Result<Row, String> {
    let n = ds.base.len();
    let dim = ds.base.dim;
    let metric = ann_common::samyama_metric(ds.metric);
    let params = HnswParams {
        m: a.m,
        ef_construction: a.ef_construction,
        ef: None,
    };
    let rss_before = rss_now();
    let sampler = vector_cost_common::RssSampler::start(a.rss_interval, rss_now);

    // Build: an empty index filled one vector at a time, ids = base positions.
    let started = Instant::now();
    let mut index = VectorIndex::with_params(dim, metric, quantization, params);
    for i in 0..n {
        index
            .add(NodeId::new(i as u64), &ds.base.row(i).to_vec())
            .map_err(|e| format!("add {i}: {e}"))?;
    }
    let build = started.elapsed();
    let rss_built = rss_now();
    let per = |bytes: usize| bytes as f64 / n.max(1) as f64;
    let vector_bytes_per_vec = per(index.vector_bytes());
    let graph_bytes_per_vec = per(index.graph_bytes());
    let resident_bytes_per_vec = per(index.resident_bytes());

    // Recall and QPS. One untimed pass first so the sweep does not pay for
    // cold caches.
    let search = search_with(&index);
    let _ = ann_common::run_point(&search, &ds.queries, &ds.truth, a.k, a.ef[0], 1);
    let sweep: Vec<ann_common::Point> =
        a.ef.iter()
            .map(|&ef| ann_common::run_point(&search, &ds.queries, &ds.truth, a.k, ef, a.threads))
            .collect();
    drop(search);
    let reduced: Vec<Sweep> = sweep
        .iter()
        .map(|p| Sweep {
            ef: p.ef,
            recall: p.recall,
            qps: p.qps,
        })
        .collect();
    let at_target = vector_cost_common::operating_point(&reduced, a.recall_target);
    let best_recall = reduced.iter().map(|p| p.recall).fold(0.0, f64::max);

    // Dump, then restart: load rebuilds the graph from the f32 file.
    let path = dir.join(format!("{name}.vec"));
    let started = Instant::now();
    index.dump(&path).map_err(|e| format!("dump: {e}"))?;
    let dump = started.elapsed();
    let disk_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    drop(index);

    let started = Instant::now();
    let reloaded = VectorIndex::load_with_params(&path, dim, metric, quantization, params)
        .map_err(|e| format!("load: {e}"))?;
    let load = started.elapsed();
    if reloaded.len() != n {
        return Err(format!("reloaded {} vectors of {n}", reloaded.len()));
    }
    let ef = at_target.map(|p| p.ef).unwrap_or(a.ef[0]);
    let started = Instant::now();
    reloaded
        .search_ef(ds.queries.row(0), a.k, Some(ef))
        .map_err(|e| format!("first query: {e}"))?;
    let first_query = started.elapsed();
    drop(reloaded);
    let _ = std::fs::remove_file(&path);

    let rss_peak = sampler.stop();
    let rss_growth = match (rss_before, rss_built) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };
    Ok(Row {
        name,
        bytes_per_value: quantization.bytes_per_value(),
        vector_bytes_per_vec,
        graph_bytes_per_vec,
        resident_bytes_per_vec,
        theoretical_bytes_per_vec: vector_cost_common::theoretical_bytes_per_vector(
            dim,
            quantization.bytes_per_value(),
            a.m,
        ),
        rss_growth,
        rss_peak,
        build,
        dump,
        disk_bytes,
        load,
        first_query,
        sweep,
        at_target,
        best_recall,
    })
}

fn mb(b: Option<u64>) -> String {
    b.map(|b| format!("{:.1}", b as f64 / 1e6))
        .unwrap_or_else(|| "?".into())
}

fn secs(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64())
}

fn dollars(v: f64) -> String {
    if v.is_finite() {
        format!("{v:.4}")
    } else {
        "-".into()
    }
}

fn print_rows(a: &Args, ds: &Dataset, rows: &[Row]) {
    let n = ds.base.len() as f64;
    println!("\nMemory (bytes per vector; theory = dim*bytes + M*2*4)");
    println!(
        "  {:<6} {:>8} {:>8} {:>9} {:>8} {:>9} {:>11} {:>11}",
        "quant", "vectors", "graph", "resident", "theory", "overhead", "RSS grow MB", "RSS peak MB"
    );
    for r in rows {
        println!(
            "  {:<6} {:>8.0} {:>8.0} {:>9.0} {:>8} {:>8.2}x {:>11} {:>11}",
            r.name,
            r.vector_bytes_per_vec,
            r.graph_bytes_per_vec,
            r.resident_bytes_per_vec,
            r.theoretical_bytes_per_vec,
            r.resident_bytes_per_vec / r.theoretical_bytes_per_vec.max(1) as f64,
            mb(r.rss_growth),
            mb(r.rss_peak),
        );
    }

    println!("\nTime (seconds); restart = load (graph rebuild) + first query");
    println!(
        "  {:<6} {:>9} {:>9} {:>11} {:>9} {:>12} {:>9}",
        "quant", "build", "dump", "disk MB", "load", "first query", "restart"
    );
    for r in rows {
        println!(
            "  {:<6} {:>9} {:>9} {:>11} {:>9} {:>12} {:>9}",
            r.name,
            secs(r.build),
            secs(r.dump),
            mb(Some(r.disk_bytes)),
            secs(r.load),
            format!("{:.6}", r.first_query.as_secs_f64()),
            secs(r.restart_to_first_query()),
        );
    }

    println!(
        "\nRecall@{} vs QPS ({} thread(s)), and cost at recall >= {}",
        a.k, a.threads, a.recall_target
    );
    println!(
        "  {:<6} {:>5} {:>8} {:>10} {:>11} {:>9} {:>10}",
        "quant", "ef", "recall", "QPS", "h per 1M q", "$ per 1M", "$ build"
    );
    for r in rows {
        let build_cost = vector_cost_common::dollars_for(r.build, a.dollars_per_hour);
        match r.at_target {
            Some(p) => println!(
                "  {:<6} {:>5} {:>8.4} {:>10.0} {:>11.4} {:>9} {:>10}",
                r.name,
                p.ef,
                p.recall,
                p.qps,
                vector_cost_common::hours_for(1e6, p.qps),
                dollars(vector_cost_common::dollars_per_million(
                    p.qps,
                    a.dollars_per_hour
                )),
                dollars(build_cost),
            ),
            None => println!(
                "  {:<6} {:>5} {:>8.4} {:>10} {:>11} {:>9} {:>10}   target not reached at any ef tried",
                r.name,
                "-",
                r.best_recall,
                "-",
                "-",
                "-",
                dollars(build_cost),
            ),
        }
    }
    println!(
        "\n  Price: ${}/h ({}). An assumption, not a measurement: the dollar columns",
        a.dollars_per_hour, a.instance
    );
    println!(
        "  hold only if that is the host this ran on. Host time only, {} vectors.",
        n
    );
}

fn json_rows(a: &Args, ds: &Dataset, rows: &[Row], calibration: Duration) -> serde_json::Value {
    use serde_json::json;
    let rows: Vec<_> = rows
        .iter()
        .map(|r| {
            json!({
                "quantization": r.name,
                "bytes_per_value": r.bytes_per_value,
                "vector_bytes_per_vector": r.vector_bytes_per_vec,
                "graph_bytes_per_vector": r.graph_bytes_per_vec,
                "resident_bytes_per_vector": r.resident_bytes_per_vec,
                "theoretical_bytes_per_vector": r.theoretical_bytes_per_vec,
                "rss_growth_bytes": r.rss_growth,
                "rss_peak_bytes": r.rss_peak,
                "build_s": r.build.as_secs_f64(),
                "dump_s": r.dump.as_secs_f64(),
                "disk_bytes": r.disk_bytes,
                "load_s": r.load.as_secs_f64(),
                "first_query_s": r.first_query.as_secs_f64(),
                "restart_to_first_query_s": r.restart_to_first_query().as_secs_f64(),
                "sweep": r.sweep.iter().map(|p| json!({
                    "ef": p.ef,
                    "recall": p.recall,
                    "qps": p.qps,
                    "p50_us": p.p50.as_secs_f64() * 1e6,
                    "p99_us": p.p99.as_secs_f64() * 1e6,
                    "errors": p.errors,
                })).collect::<Vec<_>>(),
                "at_target": r.at_target.map(|p| json!({
                    "ef": p.ef,
                    "recall": p.recall,
                    "qps": p.qps,
                    "hours_per_million_queries": vector_cost_common::hours_for(1e6, p.qps),
                    "dollars_per_million_queries":
                        vector_cost_common::dollars_per_million(p.qps, a.dollars_per_hour),
                })),
                "best_recall": r.best_recall,
                "build_dollars": vector_cost_common::dollars_for(r.build, a.dollars_per_hour),
            })
        })
        .collect();
    json!({
        "dataset": ds.name,
        "system": "samyama-graph",
        "metric": ds.metric.name(),
        "n": ds.base.len(),
        "dim": ds.base.dim,
        "queries": ds.queries.len(),
        "ground_truth": ds.truth_source,
        "k": a.k,
        "m": a.m,
        "ef_construction": a.ef_construction,
        "threads": a.threads,
        "recall_target": a.recall_target,
        "dollars_per_hour": a.dollars_per_hour,
        "instance": a.instance,
        "calibration_ms": calibration.as_secs_f64() * 1000.0,
        "rows": rows,
    })
}

fn main() {
    bench_setup::init();
    let a = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("vector_cost_profile: {e}");
            std::process::exit(2);
        }
    };
    let calibration = bench_setup::report_calibration();
    let ds = match load_dataset(&a) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("vector_cost_profile: {e}");
            std::process::exit(2);
        }
    };
    println!(
        "Dataset {}: {} base x {} dims, {} queries, metric {}, ground truth from {}; \
         m={}, ef_construction={}",
        ds.name,
        ds.base.len(),
        ds.base.dim,
        ds.queries.len(),
        ds.metric.name(),
        ds.truth_source,
        a.m,
        a.ef_construction
    );
    if rss_now().is_none() {
        println!("RSS: unknown on this platform (read from /proc); the RSS columns show `?`.");
    }
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("vector_cost_profile: tempdir: {e}");
            std::process::exit(1);
        }
    };

    let mut rows = Vec::new();
    for &(name, q) in &a.quantizations {
        eprintln!("Profiling {name} ...");
        match profile(&a, &ds, name, q, dir.path()) {
            Ok(r) => rows.push(r),
            Err(e) => {
                eprintln!("vector_cost_profile: {name}: {e}");
                std::process::exit(1);
            }
        }
    }
    print_rows(&a, &ds, &rows);
    if rows.len() > 1 {
        println!(
            "  Several quantizations ran in one process: RSS rows after the first include\n  \
             what earlier ones left resident. Run one --quantization per process to compare RSS."
        );
    }

    if let Some(path) = &a.json {
        let doc = json_rows(&a, &ds, &rows, calibration);
        let text = serde_json::to_string_pretty(&doc).expect("JSON of plain values");
        if let Err(e) = std::fs::write(path, text) {
            eprintln!("vector_cost_profile: {}: {e}", path.display());
            std::process::exit(1);
        }
        println!("\nWrote {}", path.display());
    }

    bench_setup::report_drift(calibration);
}
