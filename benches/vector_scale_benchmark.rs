//! Modern-embedding scale benchmark (#1984): VectorDBBench's Cohere and OpenAI
//! cases, 500K to 10M vectors of 768 and 1536 dimensions, and the NDS-05 line.
//!
//! One run, one dataset, one process:
//!
//! 1. checks the base set will fit in memory **before** reading it, and
//!    refuses a run that would not, rather than swap for an hour and be
//!    killed part way through the load;
//! 2. reads the dataset, loads it as `:Item` nodes and builds the HNSW index
//!    (load time and build time, separately);
//! 3. sweeps `ef` on one client thread, scoring recall@100 (VectorDBBench's
//!    figure) and recall@10 (NDS-05's) at each width;
//! 4. picks the operating point: the fastest width whose recall@100 reaches
//!    `--recall-target`;
//! 5. sweeps client threads at that width for the highest QPS;
//! 6. reports serial p99, max QPS, peak RSS, dollars per million queries at
//!    `--price-per-hour`, and an explicit NDS-05 line that names the vector
//!    count it was measured at.
//!
//! ```text
//! # Fetch and convert a case (pyarrow + numpy):
//! python3 scripts/download_vectordbbench_datasets.py cohere-1m
//!
//! cargo bench --bench vector_scale_benchmark -- --dataset data/vdbb/cohere-1m \
//!     --price-per-hour 0.40 --instance "r6i.4xlarge spot us-east-1" --json cohere-1m.json
//!
//! # Smoke run, no download:
//! cargo bench --bench vector_scale_benchmark -- --synthetic 20000x768 --queries 200
//! ```
//!
//! Options:
//!
//! | flag | default | |
//! |---|---|---|
//! | `--dataset DIR` | none | `base.fvecs`, `query.fvecs`, `groundtruth.ivecs`, `metric.txt` |
//! | `--synthetic NxDIM` | none | clustered synthetic vectors, brute-force ground truth |
//! | `--limit N` | all | first N base vectors; ground truth is then recomputed |
//! | `--queries N` | all (1000 synthetic) | first N queries |
//! | `--metric` | `metric.txt`, else cosine | l2, cosine or dot |
//! | `--ef` | `100,128,192,256,384,512` | widths of the serial sweep (each >= 100, the k) |
//! | `--threads` | `1,2,4,...,cores` | client threads of the concurrency sweep |
//! | `--operating-ef N` | chosen | measure concurrency and cost at this width instead |
//! | `--recall-target` | 0.95 | recall@100 the operating point must reach |
//! | `--m`, `--ef-construction` | 16, 200 | HNSW build parameters |
//! | `--path` | `direct` | `direct` (store search) or `cypher` (`queryNodes`) |
//! | `--price-per-hour` | none | host price in USD; no cost line without it |
//! | `--instance` | none | what the price is for, echoed in the report |
//! | `--memory-headroom` | 0.10 | share of available memory kept free |
//! | `--skip-memory-check` | off | load even when the estimate does not fit |
//! | `--name` | directory name | dataset name in the report |
//! | `--json FILE` | none | write the summary as JSON |
//!
//! What it is **not**:
//!
//! - Not a VectorDBBench result. It reads the same files and reports the same
//!   kinds of figure, in-process, without the network. The comparable run is
//!   `benches/vectordbbench/`, the client that runs samyama-graph inside
//!   VectorDBBench itself over HTTP.
//! - Not NDS-05 evidence below 10M vectors. The line says so when it is not.
//! - The cost is host time only, and only means something when the price is
//!   for the host the bench ran on.
//!
//! Run with `--release` on a quiet host and compare the calibration line
//! before comparing timings (#529).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

// Not every helper of the shared setup is used here.
#[allow(dead_code, unused_imports)]
#[path = "common/bench_setup.rs"]
mod bench_setup;

#[path = "ann_common/mod.rs"]
mod ann_common;

#[path = "vector_scale_common/mod.rs"]
mod vector_scale_common;

use ann_common::{Build, Metric, Neighbours, Point, SearchPath, Vectors};
use vector_scale_common::{Fit, SweepPoint};

/// VectorDBBench scores every case at k = 100.
const K: usize = 100;
/// NDS-05 is stated at k = 10.
const K_NDS: usize = 10;

struct Args {
    dataset: Option<PathBuf>,
    synthetic: Option<(usize, usize)>,
    limit: Option<usize>,
    queries: Option<usize>,
    metric: Option<Metric>,
    ef: Vec<usize>,
    threads: Vec<usize>,
    operating_ef: Option<usize>,
    recall_target: f64,
    build: Build,
    path: SearchPath,
    price_per_hour: Option<f64>,
    instance: Option<String>,
    headroom: f64,
    skip_memory_check: bool,
    name: Option<String>,
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

fn fraction(s: &str, flag: &str) -> Result<f64, String> {
    s.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| (0.0..=1.0).contains(v))
        .ok_or_else(|| format!("{flag}: `{s}` is not a fraction between 0 and 1"))
}

fn default_threads() -> Vec<usize> {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut t: Vec<usize> = std::iter::successors(Some(1usize), |t| Some(t * 2))
        .take_while(|t| *t < cores)
        .collect();
    t.push(cores);
    t
}

enum Invocation {
    Run(Box<Args>),
    /// No dataset named. `scripts/verify-sweep.sh` runs every bench with no
    /// arguments; as in `ann_benchmark`, that is a skip, not a failure.
    NoDataset,
}

fn parse_args() -> Result<Invocation, String> {
    let mut a = Args {
        dataset: None,
        synthetic: None,
        limit: None,
        queries: None,
        metric: None,
        ef: vec![100, 128, 192, 256, 384, 512],
        threads: default_threads(),
        operating_ef: None,
        recall_target: 0.95,
        build: Build {
            m: 16,
            ef_construction: 200,
        },
        path: SearchPath::Direct,
        price_per_hour: None,
        instance: None,
        headroom: 0.10,
        skip_memory_check: false,
        name: None,
        json: None,
    };
    // `cargo bench` passes `--bench` to every harness=false target.
    let mut it = std::env::args().skip(1).filter(|a| a != "--bench");
    while let Some(flag) = it.next() {
        if flag == "--skip-memory-check" {
            a.skip_memory_check = true;
            continue;
        }
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--dataset" => a.dataset = Some(value()?.into()),
            "--synthetic" => {
                let v = value()?;
                let (n, d) = v
                    .split_once('x')
                    .ok_or_else(|| format!("--synthetic takes NxDIM, got `{v}`"))?;
                a.synthetic = Some((one(n, "--synthetic")?, one(d, "--synthetic")?));
            }
            "--limit" => a.limit = Some(one(&value()?, "--limit")?),
            "--queries" => a.queries = Some(one(&value()?, "--queries")?),
            "--metric" => a.metric = Some(Metric::parse(&value()?)?),
            "--ef" => a.ef = list(&value()?, "--ef")?,
            "--threads" => a.threads = list(&value()?, "--threads")?,
            "--operating-ef" => a.operating_ef = Some(one(&value()?, "--operating-ef")?),
            "--recall-target" => a.recall_target = fraction(&value()?, "--recall-target")?,
            "--m" => a.build.m = one(&value()?, "--m")?,
            "--ef-construction" => a.build.ef_construction = one(&value()?, "--ef-construction")?,
            "--path" => a.path = SearchPath::parse(&value()?)?,
            "--price-per-hour" => {
                let v = value()?;
                a.price_per_hour = Some(
                    v.trim()
                        .parse::<f64>()
                        .ok()
                        .filter(|p| p.is_finite() && *p > 0.0)
                        .ok_or_else(|| {
                            format!("--price-per-hour: `{v}` is not a positive number")
                        })?,
                );
            }
            "--instance" => a.instance = Some(value()?),
            "--memory-headroom" => a.headroom = fraction(&value()?, "--memory-headroom")?,
            "--name" => a.name = Some(value()?),
            "--json" => a.json = Some(value()?.into()),
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    match (a.dataset.is_some(), a.synthetic.is_some()) {
        (false, false) => return Ok(Invocation::NoDataset),
        (true, true) => return Err("--dataset and --synthetic are alternatives".into()),
        _ => {}
    }
    // A width below k cannot return k answers; the store would widen it
    // silently and the row would be labelled with a width it did not use.
    if let Some(bad) = a.ef.iter().chain(a.operating_ef.iter()).find(|e| **e < K) {
        return Err(format!(
            "ef {bad} is below k = {K}; every width must be >= {K}"
        ));
    }
    Ok(Invocation::Run(Box::new(a)))
}

struct Dataset {
    name: String,
    metric: Metric,
    base: Vectors,
    queries: Vectors,
    truth: Neighbours,
    truth_source: &'static str,
    read: Duration,
}

fn read_metric_file(dir: &Path) -> Result<Option<Metric>, String> {
    match std::fs::read_to_string(dir.join("metric.txt")) {
        Ok(s) => Metric::parse(s.trim()).map(Some),
        Err(_) => Ok(None),
    }
}

/// The base set's size and dimension, without reading it.
fn shape(a: &Args) -> Result<(usize, usize), String> {
    if let Some((n, d)) = a.synthetic {
        return Ok((n, d));
    }
    let dir = a.dataset.as_ref().unwrap();
    let (n, d) = vector_scale_common::fvecs_shape(&dir.join("base.fvecs"))?;
    Ok((a.limit.map_or(n, |l| l.min(n)), d))
}

fn load_dataset(a: &Args) -> Result<Dataset, String> {
    let started = Instant::now();
    if let Some((n, dim)) = a.synthetic {
        let metric = a.metric.unwrap_or(Metric::Cosine);
        let base = ann_common::synthetic(n, dim, 0x5EED);
        let queries = ann_common::synthetic(a.queries.unwrap_or(1000), dim, 0xC0FFEE);
        let read = started.elapsed();
        eprintln!(
            "Computing exact top-{K} for {} queries over {n} vectors ...",
            queries.len()
        );
        let truth = ann_common::exact_ground_truth(&base, &queries, K, metric);
        return Ok(Dataset {
            name: a
                .name
                .clone()
                .unwrap_or_else(|| format!("synthetic-{n}x{dim}")),
            metric,
            base,
            queries,
            truth,
            truth_source: "brute force",
            read,
        });
    }

    let dir = a.dataset.as_ref().unwrap();
    let metric = match (a.metric, read_metric_file(dir)?) {
        (Some(m), Some(d)) if m != d => {
            return Err(format!(
                "--metric {} disagrees with metric.txt ({}); the ground truth is for {}",
                m.name(),
                d.name(),
                d.name()
            ))
        }
        (Some(m), _) | (None, Some(m)) => m,
        // Every VectorDBBench case this bench targets is cosine.
        (None, None) => Metric::Cosine,
    };
    let base = ann_common::read_fvecs(&dir.join("base.fvecs"), a.limit)?;
    let queries = ann_common::read_fvecs(&dir.join("query.fvecs"), a.queries)?;
    let read = started.elapsed();
    if base.dim != queries.dim {
        return Err(format!(
            "base vectors have {} dimensions and queries have {}",
            base.dim, queries.dim
        ));
    }
    let gt = dir.join("groundtruth.ivecs");
    // The published neighbours are for the whole base set; after `--limit`
    // they may lie past the cut, so they are recomputed.
    let (truth, truth_source) = if gt.exists() && a.limit.is_none() {
        let t = ann_common::read_ivecs(&gt, a.queries)?;
        if t.len() < queries.len() {
            return Err(format!(
                "{} has {} rows for {} queries",
                gt.display(),
                t.len(),
                queries.len()
            ));
        }
        if t.width < K {
            return Err(format!(
                "{} lists {} neighbours per query; recall@{K} needs {K}",
                gt.display(),
                t.width
            ));
        }
        if let Some(bad) = t.ids.iter().find(|id| **id as usize >= base.len()) {
            return Err(format!(
                "{} names base vector {bad}, but the base set has {}",
                gt.display(),
                base.len()
            ));
        }
        (t, "file")
    } else {
        eprintln!(
            "Computing exact top-{K} for {} queries over {} vectors ...",
            queries.len(),
            base.len()
        );
        (
            ann_common::exact_ground_truth(&base, &queries, K, metric),
            "brute force",
        )
    };
    let name = a.name.clone().unwrap_or_else(|| {
        dir.file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    Ok(Dataset {
        name,
        metric,
        base,
        queries,
        truth,
        truth_source,
        read,
    })
}

fn micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

fn mb(b: Option<u64>) -> String {
    b.map(|b| format!("{:.1} GiB", b as f64 / (1u64 << 30) as f64))
        .unwrap_or_else(|| "unknown (not Linux)".into())
}

fn fail(msg: impl std::fmt::Display, code: i32) -> ! {
    eprintln!("vector_scale_benchmark: {msg}");
    std::process::exit(code)
}

fn main() {
    bench_setup::init();
    let a = match parse_args() {
        Ok(Invocation::Run(a)) => a,
        Ok(Invocation::NoDataset) => {
            eprintln!("SKIP: vector_scale_benchmark needs a dataset.");
            eprintln!(
                "      Pass --dataset DIR (scripts/download_vectordbbench_datasets.py writes one) \
                 or --synthetic NxDIM."
            );
            eprintln!(
                "      Skipping rather than failing: the benchmark did not run, it did not break."
            );
            return;
        }
        Err(e) => fail(e, 2),
    };
    let calibration = bench_setup::report_calibration();

    // ---- memory check, before anything is read
    let (n, dim) = shape(&a).unwrap_or_else(|e| fail(e, 2));
    let estimate = vector_scale_common::estimate_bytes(
        n,
        dim,
        a.build.m,
        vector_scale_common::BENCH_VECTOR_COPIES,
    );
    let available = vector_scale_common::available_bytes();
    let fit = vector_scale_common::memory_fit(estimate, available, a.headroom);
    println!(
        "Memory: {n} x {dim}-d at m={} needs about {:.1} GiB ({} vector copies + links); available {}",
        a.build.m,
        vector_scale_common::gib(estimate),
        vector_scale_common::BENCH_VECTOR_COPIES,
        mb(available)
    );
    match fit {
        Fit::Fits => {}
        Fit::Unknown => println!("        cannot check the fit on this platform; loading anyway"),
        Fit::TooBig if a.skip_memory_check => {
            println!("        does NOT fit; loading anyway (--skip-memory-check)")
        }
        Fit::TooBig => fail(
            format!(
                "{n} vectors of {dim} dimensions need about {:.1} GiB and this host has {} available \
                 (keeping {:.0}% free). Use a larger host, --limit a subset, or pass \
                 --skip-memory-check to try anyway.",
                vector_scale_common::gib(estimate),
                mb(available),
                a.headroom * 100.0
            ),
            1,
        ),
    }

    // ---- read, load, build
    let ds = load_dataset(&a).unwrap_or_else(|e| fail(e, 2));
    println!(
        "Dataset {}: {} base x {} dims, {} queries, metric {}, ground truth from {}; read in {:.1} s",
        ds.name,
        ds.base.len(),
        ds.base.dim,
        ds.queries.len(),
        ds.metric.name(),
        ds.truth_source,
        ds.read.as_secs_f64()
    );
    let loaded = ann_common::load(&ds.base, ds.metric, a.build)
        .unwrap_or_else(|e| fail(format!("index build failed: {e}"), 1));
    let (rss_after_build, _) = ann_common::rss_bytes();
    println!(
        "Load {:.1} s; build {:.1} s (m={}, ef_construction={}); RSS after build {}",
        loaded.load.as_secs_f64(),
        loaded.build.as_secs_f64(),
        a.build.m,
        a.build.ef_construction,
        mb(rss_after_build)
    );

    let engine = samyama::query::QueryEngine::new();
    let search = |q: &[f32], k: usize, ef: usize| match a.path {
        SearchPath::Direct => ann_common::direct_search(&loaded, q, k, ef),
        SearchPath::Cypher => ann_common::cypher_search(&loaded, &engine, q, k, ef),
    };
    // One untimed pass: cold caches, and planning on the Cypher path.
    let _ = ann_common::run_point(&search, &ds.queries, &ds.truth, K, a.ef[0], 1);

    // ---- serial ef sweep
    println!(
        "\nSerial sweep ({} path):\n  {:>5} {:>10} {:>10} {:>9} {:>10} {:>10} {:>6}",
        a.path.name(),
        "ef",
        "recall@100",
        "recall@10",
        "QPS",
        "p50 µs",
        "p99 µs",
        "errors"
    );
    let mut serial: Vec<(Point, Point)> = Vec::new();
    for &ef in &a.ef {
        let p100 = ann_common::run_point(&search, &ds.queries, &ds.truth, K, ef, 1);
        let p10 = ann_common::run_point(&search, &ds.queries, &ds.truth, K_NDS, ef, 1);
        println!(
            "  {:>5} {:>10.4} {:>10.4} {:>9.0} {:>10.0} {:>10.0} {:>6}",
            ef,
            p100.recall,
            p10.recall,
            p100.qps,
            micros(p100.p50),
            micros(p100.p99),
            p100.errors + p10.errors
        );
        serial.push((p100, p10));
    }
    let sweep: Vec<SweepPoint> = serial
        .iter()
        .filter(|(p100, p10)| p100.errors == 0 && p10.errors == 0)
        .map(|(p100, p10)| SweepPoint {
            ef: p100.ef,
            recall_at_100: p100.recall,
            recall_at_10: p10.recall,
            qps: p100.qps,
        })
        .collect();

    // ---- operating point
    let (op_ef, op_serial, meets) = match a.operating_ef {
        Some(ef) => {
            let p = ann_common::run_point(&search, &ds.queries, &ds.truth, K, ef, 1);
            let meets = p.recall >= a.recall_target;
            (ef, p, meets)
        }
        None => {
            let op = vector_scale_common::operating_point(&sweep, a.recall_target)
                .unwrap_or_else(|| fail("every point of the sweep had failed queries", 1));
            let p = serial
                .iter()
                .find(|(p, _)| p.ef == op.point.ef)
                .map(|(p, _)| p.clone())
                .unwrap();
            (op.point.ef, p, op.meets_target)
        }
    };
    println!(
        "\nOperating point: ef={op_ef}, recall@100 {:.4} ({} the {} target), serial p99 {:.0} µs",
        op_serial.recall,
        if meets { "meets" } else { "MISSES" },
        a.recall_target,
        micros(op_serial.p99)
    );

    // ---- concurrency sweep
    println!(
        "\nConcurrency at ef={op_ef}, k={K}:\n  {:>7} {:>9} {:>10} {:>10} {:>6}",
        "threads", "QPS", "p50 µs", "p99 µs", "errors"
    );
    let mut conc = Vec::new();
    for &t in &a.threads {
        let p = ann_common::run_point(&search, &ds.queries, &ds.truth, K, op_ef, t);
        println!(
            "  {:>7} {:>9.0} {:>10.0} {:>10.0} {:>6}",
            t,
            p.qps,
            micros(p.p50),
            micros(p.p99),
            p.errors
        );
        conc.push(p);
    }
    let triples: Vec<(usize, f64, usize)> =
        conc.iter().map(|p| (p.threads, p.qps, p.errors)).collect();
    let max = vector_scale_common::max_qps(&triples);
    let (_, peak_rss) = ann_common::rss_bytes();

    // ---- summary
    println!(
        "\nSummary for {} ({} vectors, {}-d, {}):",
        ds.name,
        ds.base.len(),
        ds.base.dim,
        ds.metric.name()
    );
    println!(
        "  load {:.1} s, build {:.1} s",
        loaded.load.as_secs_f64(),
        loaded.build.as_secs_f64()
    );
    println!(
        "  recall@100 {:.4} at ef={op_ef}; serial p99 {:.0} µs",
        op_serial.recall,
        micros(op_serial.p99)
    );
    match max {
        Some((t, q)) => println!("  max QPS {q:.0} at {t} thread(s)"),
        None => println!("  max QPS: none -- every concurrency point had failed queries"),
    }
    println!("  peak RSS {}", mb(peak_rss));
    let cost = match (a.price_per_hour, max) {
        (Some(price), Some((_, q))) => {
            let c = vector_scale_common::dollars_per_million(q, price);
            println!(
                "  cost ${c:.4} per million queries at ${price}/h{}{}",
                a.instance
                    .as_deref()
                    .map(|i| format!(" ({i})"))
                    .unwrap_or_default(),
                if meets {
                    ""
                } else {
                    " -- at a recall BELOW the target"
                }
            );
            Some(c)
        }
        (None, _) => {
            println!("  cost: pass --price-per-hour with the price of this host");
            None
        }
        (Some(_), None) => None,
    };
    let best10 = sweep.iter().max_by(|a, b| {
        a.recall_at_10
            .total_cmp(&b.recall_at_10)
            .then(b.ef.cmp(&a.ef))
    });
    let nds = best10.map(|p| vector_scale_common::nds05_line(ds.base.len(), p.recall_at_10, p.ef));
    if let Some(line) = &nds {
        println!("\n{line}");
    }

    if let Some(path) = &a.json {
        let point = |p: &Point| {
            serde_json::json!({
                "ef": p.ef, "threads": p.threads, "recall": p.recall, "qps": p.qps,
                "p50_us": micros(p.p50), "p99_us": micros(p.p99), "errors": p.errors,
            })
        };
        let doc = serde_json::json!({
            "issue": 1984,
            "dataset": ds.name,
            "system": "samyama-graph",
            "path": a.path.name(),
            "metric": ds.metric.name(),
            "vectors": ds.base.len(),
            "dim": ds.base.dim,
            "queries": ds.queries.len(),
            "ground_truth": ds.truth_source,
            "m": a.build.m,
            "ef_construction": a.build.ef_construction,
            "memory_estimate_bytes": estimate.to_string(),
            "memory_available_bytes": available,
            "read_s": ds.read.as_secs_f64(),
            "load_s": loaded.load.as_secs_f64(),
            "build_s": loaded.build.as_secs_f64(),
            "serial_sweep": serial.iter().map(|(p100, p10)| serde_json::json!({
                "ef": p100.ef, "recall_at_100": p100.recall, "recall_at_10": p10.recall,
                "qps": p100.qps, "p50_us": micros(p100.p50), "p99_us": micros(p100.p99),
                "errors": p100.errors + p10.errors,
            })).collect::<Vec<_>>(),
            "recall_target": a.recall_target,
            "operating_ef": op_ef,
            "operating_meets_target": meets,
            "recall_at_100": op_serial.recall,
            "serial_p99_us": micros(op_serial.p99),
            "concurrency": conc.iter().map(point).collect::<Vec<_>>(),
            "max_qps": max.map(|m| m.1),
            "max_qps_threads": max.map(|m| m.0),
            "rss_after_build_bytes": rss_after_build,
            "peak_rss_bytes": peak_rss,
            "price_per_hour": a.price_per_hour,
            "instance": a.instance,
            "dollars_per_million_queries": cost,
            "nds05": nds,
            "calibration_ms": calibration.as_secs_f64() * 1000.0,
        });
        let text = serde_json::to_string_pretty(&doc).expect("json");
        if let Err(e) = std::fs::write(path, text) {
            fail(format!("{}: {e}", path.display()), 1);
        }
        println!("\nWrote {}", path.display());
    }

    bench_setup::report_drift(calibration);
}
