//! Recall–QPS sweep in the style of ann-benchmarks (#1983).
//!
//! Loads a base set, builds the HNSW index once, then runs every query at each
//! search width `ef` and each client-thread count, scoring the answers against
//! ground truth. Writes one CSV row per (ef, threads) point and prints the
//! Pareto frontier.
//!
//! ```text
//! # A dataset in TEXMEX layout (SIFT and GIST ship this way):
//! cargo bench --bench ann_benchmark -- \
//!     --base sift_base.fvecs --query sift_query.fvecs --gt sift_groundtruth.ivecs \
//!     --metric l2 --csv sift.csv
//!
//! # A directory holding base.fvecs, query.fvecs, groundtruth.ivecs and
//! # metric.txt, as scripts/ann_hdf5_to_vecs.py writes from an ann-benchmarks
//! # HDF5 file (GloVe, DEEP, GIST, ...):
//! cargo bench --bench ann_benchmark -- --dataset data/glove-100-angular
//!
//! # No dataset: clustered synthetic vectors, ground truth by brute force.
//! cargo bench --bench ann_benchmark -- --synthetic 100000x128
//! ```
//!
//! Options:
//!
//! | flag | default | |
//! |---|---|---|
//! | `--k` | 10 | recall@k |
//! | `--ef` | `10,20,40,80,160,320,640` | search widths to sweep |
//! | `--threads` | `1,<cores>` | client threads per point |
//! | `--m`, `--ef-construction` | 16, 200 | HNSW build parameters |
//! | `--limit N` | all | first N base vectors; ground truth is then recomputed |
//! | `--queries N` | all | first N queries |
//! | `--path` | `direct` | `direct` (store search) or `cypher` (`queryNodes` via `QueryEngine`) |
//! | `--name` | file stem | dataset name in the CSV |
//! | `--csv FILE` | none | append rows to FILE, writing a header if new |
//!
//! What it does **not** measure: the network. Both paths run in-process, so
//! the numbers are this engine's ceiling and are not comparable to a
//! competitor measured over HTTP or Bolt. The cross-system comparison lives in
//! `samyama-graph-competitor-benchmarks`, which runs every system through its
//! network API on the same host; it can read this CSV's columns.
//!
//! Run with `--release` on a quiet host and compare the calibration line
//! before comparing timings (#529).

use std::path::{Path, PathBuf};
use std::time::Duration;

#[path = "common/bench_setup.rs"]
mod bench_setup;

#[path = "ann_common/mod.rs"]
mod ann_common;

use ann_common::{Build, Metric, Neighbours, SearchPath, Vectors};

struct Args {
    base: Option<PathBuf>,
    query: Option<PathBuf>,
    gt: Option<PathBuf>,
    dataset: Option<PathBuf>,
    synthetic: Option<(usize, usize)>,
    metric: Option<Metric>,
    name: Option<String>,
    k: usize,
    ef: Vec<usize>,
    threads: Vec<usize>,
    build: Build,
    limit: Option<usize>,
    queries: Option<usize>,
    path: SearchPath,
    csv: Option<PathBuf>,
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
    let v = list(s, flag)?;
    match v.as_slice() {
        [n] => Ok(*n),
        _ => Err(format!("{flag} takes one number")),
    }
}

/// What the command line asked for.
enum Invocation {
    /// A dataset was named: run the sweep.
    Run(Args),
    /// No dataset was named at all.
    ///
    /// `scripts/verify-sweep.sh` runs every `benches/*.rs` with no arguments,
    /// so this is the normal way the nightly invokes this bench -- not a usage
    /// mistake, and it must not fail the sweep. Every other data-hungry bench
    /// already answers this case with `SKIP:` and exit 0: `ldbc_benchmark`,
    /// `ldbc_bi_benchmark`, `ingest_profile`, `cold_start_share`,
    /// `plan_share` and `result_cache_gain`. This one exited 2 instead, which
    /// is what failed the nightly on 2026-11-09 and 2026-11-10.
    ///
    /// A *wrong* dataset argument stays an error -- see the two checks below.
    /// Only the complete absence of one is a skip.
    NoDataset,
}

fn parse_args() -> Result<Invocation, String> {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut a = Args {
        base: None,
        query: None,
        gt: None,
        dataset: None,
        synthetic: None,
        metric: None,
        name: None,
        k: 10,
        ef: vec![10, 20, 40, 80, 160, 320, 640],
        threads: if cores > 1 { vec![1, cores] } else { vec![1] },
        build: Build {
            m: 16,
            ef_construction: 200,
        },
        limit: None,
        queries: None,
        path: SearchPath::Direct,
        csv: None,
    };
    // `cargo bench` passes `--bench` to every harness=false target.
    let mut it = std::env::args().skip(1).filter(|a| a != "--bench");
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--base" => a.base = Some(value()?.into()),
            "--query" => a.query = Some(value()?.into()),
            "--gt" => a.gt = Some(value()?.into()),
            "--dataset" => a.dataset = Some(value()?.into()),
            "--synthetic" => {
                let v = value()?;
                let (n, d) = v
                    .split_once('x')
                    .ok_or_else(|| format!("--synthetic takes NxDIM, got `{v}`"))?;
                a.synthetic = Some((one(n, "--synthetic")?, one(d, "--synthetic")?));
            }
            "--metric" => a.metric = Some(Metric::parse(&value()?)?),
            "--name" => a.name = Some(value()?),
            "--k" => a.k = one(&value()?, "--k")?,
            "--ef" => a.ef = list(&value()?, "--ef")?,
            "--threads" => a.threads = list(&value()?, "--threads")?,
            "--m" => a.build.m = one(&value()?, "--m")?,
            "--ef-construction" => a.build.ef_construction = one(&value()?, "--ef-construction")?,
            "--limit" => a.limit = Some(one(&value()?, "--limit")?),
            "--queries" => a.queries = Some(one(&value()?, "--queries")?),
            "--path" => a.path = SearchPath::parse(&value()?)?,
            "--csv" => a.csv = Some(value()?.into()),
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    let sources = [
        a.base.is_some() || a.query.is_some(),
        a.dataset.is_some(),
        a.synthetic.is_some(),
    ];
    match sources.iter().filter(|s| **s).count() {
        0 => return Ok(Invocation::NoDataset),
        1 => {}
        _ => return Err("--base/--query, --dataset and --synthetic are alternatives".into()),
    }
    if a.base.is_some() != a.query.is_some() {
        return Err("--base and --query go together".into());
    }
    Ok(Invocation::Run(a))
}

struct Dataset {
    name: String,
    metric: Metric,
    base: Vectors,
    queries: Vectors,
    truth: Neighbours,
    /// Where the ground truth came from, for the report.
    truth_source: &'static str,
}

fn read_metric_file(dir: &Path) -> Result<Option<Metric>, String> {
    match std::fs::read_to_string(dir.join("metric.txt")) {
        Ok(s) => Metric::parse(s.trim()).map(Some),
        Err(_) => Ok(None),
    }
}

fn load_dataset(a: &Args) -> Result<Dataset, String> {
    if let Some((n, dim)) = a.synthetic {
        let metric = a.metric.unwrap_or(Metric::L2);
        let base = ann_common::synthetic(n, dim, 0x5EED);
        let nq = a.queries.unwrap_or(1000);
        let queries = ann_common::synthetic(nq, dim, 0xC0FFEE);
        eprintln!("Computing exact ground truth for {nq} queries over {n} vectors ...");
        let truth = ann_common::exact_ground_truth(&base, &queries, a.k, metric);
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
        });
    }

    let (base_p, query_p, gt_p, dir_metric) = match &a.dataset {
        Some(dir) => (
            dir.join("base.fvecs"),
            dir.join("query.fvecs"),
            Some(dir.join("groundtruth.ivecs")).filter(|p| p.exists()),
            read_metric_file(dir)?,
        ),
        None => (
            a.base.clone().unwrap(),
            a.query.clone().unwrap(),
            a.gt.clone(),
            None,
        ),
    };
    let metric = match (a.metric, dir_metric) {
        (Some(m), Some(d)) if m != d => {
            return Err(format!(
                "--metric {} disagrees with metric.txt ({}); the ground truth is for {}",
                m.name(),
                d.name(),
                d.name()
            ))
        }
        (Some(m), _) | (None, Some(m)) => m,
        (None, None) => return Err("--metric is required: the files do not say".into()),
    };
    let name = a.name.clone().unwrap_or_else(|| {
        let p = a.dataset.as_ref().unwrap_or(&base_p);
        p.file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default()
    });

    let base = ann_common::read_fvecs(&base_p, a.limit)?;
    let queries = ann_common::read_fvecs(&query_p, a.queries)?;
    if base.dim != queries.dim {
        return Err(format!(
            "base vectors have {} dimensions and queries have {}",
            base.dim, queries.dim
        ));
    }
    // Published ground truth is for the whole base set: after `--limit` its
    // neighbours may lie past the cut, and scoring against it would charge
    // the index for vectors it was never given.
    let (truth, truth_source) = match (&gt_p, a.limit) {
        (Some(p), None) => {
            let t = ann_common::read_ivecs(p, a.queries)?;
            if t.len() < queries.len() {
                return Err(format!(
                    "{} has {} rows for {} queries",
                    p.display(),
                    t.len(),
                    queries.len()
                ));
            }
            if t.width < a.k {
                return Err(format!(
                    "{} lists {} neighbours per query; recall@{} needs {}",
                    p.display(),
                    t.width,
                    a.k,
                    a.k
                ));
            }
            if let Some(bad) = t.ids.iter().find(|id| **id as usize >= base.len()) {
                return Err(format!(
                    "{} names base vector {bad}, but the base set has {}",
                    p.display(),
                    base.len()
                ));
            }
            (t, "file")
        }
        _ => {
            eprintln!(
                "Computing exact ground truth for {} queries over {} vectors ...",
                queries.len(),
                base.len()
            );
            (
                ann_common::exact_ground_truth(&base, &queries, a.k, metric),
                "brute force",
            )
        }
    };
    Ok(Dataset {
        name,
        metric,
        base,
        queries,
        truth,
        truth_source,
    })
}

const CSV_HEADER: &str = "dataset,system,path,metric,n,dim,queries,k,m,ef_construction,ef,threads,\
recall,qps,p50_us,p99_us,mean_us,errors,load_s,build_s,rss_after_build_bytes,peak_rss_bytes,calibration_ms";

fn micros(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e6)
}

fn opt(v: Option<u64>) -> String {
    v.map(|b| b.to_string()).unwrap_or_default()
}

fn main() {
    bench_setup::init();
    let a = match parse_args() {
        Ok(Invocation::Run(a)) => a,
        Ok(Invocation::NoDataset) => {
            eprintln!("SKIP: ann_benchmark needs a dataset.");
            eprintln!(
                "      Pass --synthetic NxDIM for clustered synthetic vectors, --dataset DIR, \
                 or --base/--query[/--gt] for a TEXMEX layout."
            );
            eprintln!(
                "      Skipping rather than failing: the benchmark did not run, it did not break."
            );
            return;
        }
        Err(e) => {
            eprintln!("ann_benchmark: {e}");
            std::process::exit(2);
        }
    };
    let calibration = bench_setup::report_calibration();

    let ds = match load_dataset(&a) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("ann_benchmark: {e}");
            std::process::exit(2);
        }
    };
    println!(
        "Dataset {}: {} base x {} dims, {} queries, metric {}, ground truth from {}",
        ds.name,
        ds.base.len(),
        ds.base.dim,
        ds.queries.len(),
        ds.metric.name(),
        ds.truth_source
    );

    let loaded = match ann_common::load(&ds.base, ds.metric, a.build) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("ann_benchmark: index build failed: {e}");
            std::process::exit(1);
        }
    };
    let (rss_after_build, _) = ann_common::rss_bytes();
    println!(
        "Loaded in {:.2} s; index built in {:.2} s (m={}, ef_construction={}); RSS {}",
        loaded.load.as_secs_f64(),
        loaded.build.as_secs_f64(),
        a.build.m,
        a.build.ef_construction,
        rss_after_build
            .map(|b| format!("{:.1} MB", b as f64 / 1e6))
            .unwrap_or_else(|| "unknown (not Linux)".into())
    );

    let engine = samyama::query::QueryEngine::new();
    let search = |q: &[f32], k: usize, ef: usize| match a.path {
        SearchPath::Direct => ann_common::direct_search(&loaded, q, k, ef),
        SearchPath::Cypher => ann_common::cypher_search(&loaded, &engine, q, k, ef),
    };

    // One untimed pass so the first point does not pay for cold caches and,
    // on the Cypher path, for planning.
    let _ = ann_common::run_point(&search, &ds.queries, &ds.truth, a.k, a.ef[0], 1);

    println!(
        "\n  {:>6} {:>7} {:>9} {:>11} {:>10} {:>10} {:>6}",
        "ef", "threads", "recall", "QPS", "p50 µs", "p99 µs", "errors"
    );
    let mut points = Vec::new();
    for &threads in &a.threads {
        for &ef in &a.ef {
            let p = ann_common::run_point(&search, &ds.queries, &ds.truth, a.k, ef, threads);
            println!(
                "  {:>6} {:>7} {:>9.4} {:>11.0} {:>10} {:>10} {:>6}",
                p.ef,
                p.threads,
                p.recall,
                p.qps,
                micros(p.p50),
                micros(p.p99),
                p.errors
            );
            points.push(p);
        }
    }
    let (_, peak_rss) = ann_common::rss_bytes();

    for &threads in &a.threads {
        let at: Vec<_> = points
            .iter()
            .filter(|p| p.threads == threads)
            .cloned()
            .collect();
        let frontier = ann_common::pareto_frontier(&at);
        let shown: Vec<String> = frontier
            .iter()
            .map(|p| format!("ef={} {:.3}@{:.0}", p.ef, p.recall, p.qps))
            .collect();
        println!(
            "\nPareto frontier, {threads} thread(s), recall@QPS: {}",
            shown.join(", ")
        );
    }
    if points.iter().any(|p| p.errors > 0) {
        println!("\nSome queries failed and were scored as recall 0; see the errors column.");
    }

    if let Some(csv) = &a.csv {
        let new = !csv.exists();
        let mut out = String::new();
        if new {
            out.push_str(CSV_HEADER);
            out.push('\n');
        }
        for p in &points {
            out.push_str(&format!(
                "{},samyama-graph,{},{},{},{},{},{},{},{},{},{},{:.6},{:.1},{},{},{},{},{:.3},{:.3},{},{},{:.0}\n",
                ds.name,
                a.path.name(),
                ds.metric.name(),
                ds.base.len(),
                ds.base.dim,
                ds.queries.len(),
                a.k,
                a.build.m,
                a.build.ef_construction,
                p.ef,
                p.threads,
                p.recall,
                p.qps,
                micros(p.p50),
                micros(p.p99),
                micros(p.mean),
                p.errors,
                loaded.load.as_secs_f64(),
                loaded.build.as_secs_f64(),
                opt(rss_after_build),
                opt(peak_rss),
                calibration.as_secs_f64() * 1000.0,
            ));
        }
        use std::io::Write;
        let written = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(csv)
            .and_then(|mut f| f.write_all(out.as_bytes()));
        match written {
            Ok(()) => println!("\nWrote {} rows to {}", points.len(), csv.display()),
            Err(e) => {
                eprintln!("ann_benchmark: {}: {e}", csv.display());
                std::process::exit(1);
            }
        }
    }

    bench_setup::report_drift(calibration);
}
