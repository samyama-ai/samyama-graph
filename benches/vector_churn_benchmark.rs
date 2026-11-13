//! Streaming / churn benchmark: recall stability of the HNSW vector index
//! under insert, delete and update (#1986).
//!
//! Loads an index, applies a workload of inserts, deletes and in-place
//! updates, and after every search step scores the index against exact search
//! over the vectors then live. Next to that it scores an index built fresh
//! from the same live set, so a fall in recall is told apart from a live set
//! that is simply harder, and reports the dead entries still in the graph
//! (`VectorIndex::superseded`), the rebuilds the index did on its own, write
//! throughput, search latency both at the search step and between writes,
//! and memory.
//!
//! ```text
//! # Random churn: a tenth of the live set per cycle, half deleted and
//! # replaced by new vectors, half updated in place. The default, and what a
//! # run with no arguments does.
//! cargo bench --bench vector_churn_benchmark -- --synthetic 100000x128 --cycles 20
//!
//! # Drift: a sliding window over a moving distribution. Inserts follow the
//! # distribution as it moves and deletes remove the oldest vectors.
//! cargo bench --bench vector_churn_benchmark -- --mode drift --synthetic 100000x128
//!
//! # The same two over a real dataset (rows read in file order, so `drift` is
//! # a sliding window over the file rather than a moving distribution):
//! cargo bench --bench vector_churn_benchmark -- --dataset data/ann/sift-128-euclidean --limit 200000
//!
//! # A Big-ANN 2023 streaming-track runbook over that dataset's base and
//! # query vectors:
//! cargo bench --bench vector_churn_benchmark -- --runbook final_runbook.yaml \
//!     --runbook-name <dataset key in the file> --base base.fbin --query query.fbin --metric l2
//! ```
//!
//! Options:
//!
//! | flag | default | |
//! |---|---|---|
//! | `--mode` | `churn` | `churn` (random) or `drift` (sliding window); ignored with `--runbook` |
//! | `--synthetic NxDIM` | `10000x32` | N vectors live, clustered, generated |
//! | `--dataset DIR`, `--base`/`--query` | | real vectors, `.fvecs` or Big-ANN `.fbin` (`--base`/`--query` only) |
//! | `--limit N` | a share of the file | live set size over a real dataset |
//! | `--runbook FILE`, `--runbook-name` | | Big-ANN streaming runbook; needs a real dataset |
//! | `--cycles` | 10 | churn cycles, each followed by a search step |
//! | `--churn` | 0.1 | share of the live set touched per cycle |
//! | `--drift` | 2.0 | how far a synthetic drift distribution moves over the run |
//! | `--queries` | 200 | queries per search step |
//! | `--k` | 10 | recall@k |
//! | `--ef` | 64 | search width for every search |
//! | `--m`, `--ef-construction` | 16, 200 | HNSW build parameters |
//! | `--fresh-every` | 1 | build and score a fresh index every Nth search step; 0 never |
//! | `--probe-every` | 50 | one unscored search after every N writes; 0 never |
//! | `--seed` | 1986 | |
//! | `--name` | from the source | dataset name in the CSV |
//! | `--csv FILE` | none | append one row per search step, writing a header if new |
//!
//! Ground truth is exact search over the live set at every search step,
//! computed outside the timed region. A runbook's published per-step ground
//! truth files are not read.
//!
//! The index is driven directly (`VectorIndex::add`/`remove`/`search_ef`),
//! not through a `GraphStore` or Cypher: this measures the index's
//! maintenance, not the store's write path. Single client, in process; see
//! `docs/benchmarks/vector-churn.md` for what that does and does not compare
//! with. Run with `--release` on a quiet host and compare the calibration
//! line before comparing timings (#529).

use std::path::{Path, PathBuf};
use std::time::Duration;

#[path = "common/bench_setup.rs"]
mod bench_setup;

#[path = "ann_common/mod.rs"]
mod ann_common;

#[path = "churn_common/mod.rs"]
mod churn_common;

use ann_common::Metric;
use churn_common::{Config, Report, Workload};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Churn,
    Drift,
}

impl Mode {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "churn" => Ok(Mode::Churn),
            "drift" => Ok(Mode::Drift),
            other => Err(format!("unknown mode `{other}`; use churn or drift")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Mode::Churn => "churn",
            Mode::Drift => "drift",
        }
    }
}

struct Args {
    mode: Mode,
    synthetic: Option<(usize, usize)>,
    dataset: Option<PathBuf>,
    base: Option<PathBuf>,
    query: Option<PathBuf>,
    metric: Option<Metric>,
    limit: Option<usize>,
    runbook: Option<PathBuf>,
    runbook_name: Option<String>,
    cycles: usize,
    churn: f64,
    drift: f32,
    queries: usize,
    k: usize,
    ef: usize,
    m: usize,
    ef_construction: usize,
    fresh_every: usize,
    probe_every: usize,
    seed: u64,
    name: Option<String>,
    csv: Option<PathBuf>,
}

fn count(s: &str, flag: &str) -> Result<usize, String> {
    s.trim()
        .parse::<usize>()
        .map_err(|_| format!("{flag}: `{s}` is not a count"))
}

fn positive(s: &str, flag: &str) -> Result<usize, String> {
    match count(s, flag)? {
        0 => Err(format!("{flag} must be positive")),
        n => Ok(n),
    }
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        mode: Mode::Churn,
        synthetic: None,
        dataset: None,
        base: None,
        query: None,
        metric: None,
        limit: None,
        runbook: None,
        runbook_name: None,
        cycles: 10,
        churn: 0.1,
        drift: 2.0,
        queries: 200,
        k: 10,
        ef: 64,
        m: 16,
        ef_construction: 200,
        fresh_every: 1,
        probe_every: 50,
        seed: 1986,
        name: None,
        csv: None,
    };
    // `cargo bench` passes `--bench` to every harness=false target.
    let mut it = std::env::args().skip(1).filter(|a| a != "--bench");
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--mode" => a.mode = Mode::parse(&value()?)?,
            "--synthetic" => {
                let v = value()?;
                let (n, d) = v
                    .split_once('x')
                    .ok_or_else(|| format!("--synthetic takes NxDIM, got `{v}`"))?;
                a.synthetic = Some((positive(n, "--synthetic")?, positive(d, "--synthetic")?));
            }
            "--dataset" => a.dataset = Some(value()?.into()),
            "--base" => a.base = Some(value()?.into()),
            "--query" => a.query = Some(value()?.into()),
            "--metric" => a.metric = Some(Metric::parse(&value()?)?),
            "--limit" => a.limit = Some(positive(&value()?, "--limit")?),
            "--runbook" => a.runbook = Some(value()?.into()),
            "--runbook-name" => a.runbook_name = Some(value()?),
            "--cycles" => a.cycles = count(&value()?, "--cycles")?,
            "--churn" => {
                let v = value()?;
                a.churn = v
                    .parse::<f64>()
                    .ok()
                    .filter(|c| *c > 0.0 && *c <= 1.0)
                    .ok_or_else(|| format!("--churn takes a share in (0, 1], got `{v}`"))?;
            }
            "--drift" => {
                let v = value()?;
                a.drift = v
                    .parse::<f32>()
                    .ok()
                    .filter(|d| d.is_finite() && *d >= 0.0)
                    .ok_or_else(|| format!("--drift takes a distance >= 0, got `{v}`"))?;
            }
            "--queries" => a.queries = positive(&value()?, "--queries")?,
            "--k" => a.k = positive(&value()?, "--k")?,
            "--ef" => a.ef = positive(&value()?, "--ef")?,
            "--m" => a.m = positive(&value()?, "--m")?,
            "--ef-construction" => a.ef_construction = positive(&value()?, "--ef-construction")?,
            "--fresh-every" => a.fresh_every = count(&value()?, "--fresh-every")?,
            "--probe-every" => a.probe_every = count(&value()?, "--probe-every")?,
            "--seed" => a.seed = count(&value()?, "--seed")? as u64,
            "--name" => a.name = Some(value()?),
            "--csv" => a.csv = Some(value()?.into()),
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    let real = a.dataset.is_some() || a.base.is_some() || a.query.is_some();
    if real && a.synthetic.is_some() {
        return Err("--synthetic and a real dataset are alternatives".into());
    }
    if a.dataset.is_some() && (a.base.is_some() || a.query.is_some()) {
        return Err("--dataset and --base/--query are alternatives".into());
    }
    if a.base.is_some() != a.query.is_some() {
        return Err("--base and --query go together".into());
    }
    if a.runbook.is_some() && !real {
        return Err("--runbook reads a real dataset: pass --dataset or --base/--query".into());
    }
    if a.runbook_name.is_some() && a.runbook.is_none() {
        return Err("--runbook-name goes with --runbook".into());
    }
    if !real && a.synthetic.is_none() {
        // The no-argument run, which is how `scripts/verify-sweep.sh` calls
        // every bench: small enough to finish in seconds, large enough that
        // searches go through the graph rather than a linear scan.
        a.synthetic = Some((10_000, 32));
    }
    let params = samyama::vector::HnswParams {
        m: a.m,
        ef_construction: a.ef_construction,
        ef: None,
    };
    params.validate()?;
    samyama::vector::index::check_ef(a.ef)?;
    Ok(a)
}

fn read_metric_file(dir: &Path) -> Result<Option<Metric>, String> {
    match std::fs::read_to_string(dir.join("metric.txt")) {
        Ok(s) => Metric::parse(s.trim()).map(Some),
        Err(_) => Ok(None),
    }
}

fn per_cycle(n: usize, churn: f64) -> usize {
    ((n as f64 * churn).round() as usize).clamp(1, n.max(1))
}

/// The workload the arguments describe, and the metric to score it with.
fn workload(a: &Args) -> Result<(Workload, Metric), String> {
    if let Some((n, dim)) = a.synthetic {
        let metric = a.metric.unwrap_or(Metric::L2);
        let per = per_cycle(n, a.churn);
        let mut w = match a.mode {
            Mode::Churn => churn_common::synthetic_churn(n, dim, a.cycles, per, a.queries, a.seed),
            Mode::Drift => {
                churn_common::synthetic_drift(n, dim, a.cycles, per, a.queries, a.drift, a.seed)
            }
        };
        if let Some(name) = &a.name {
            w.name = name.clone();
        }
        return Ok((w, metric));
    }

    let (base_p, query_p, dir_metric) = match &a.dataset {
        Some(dir) => (
            dir.join("base.fvecs"),
            dir.join("query.fvecs"),
            read_metric_file(dir)?,
        ),
        None => (a.base.clone().unwrap(), a.query.clone().unwrap(), None),
    };
    let metric = match (a.metric, dir_metric) {
        (Some(m), Some(d)) if m != d => {
            return Err(format!(
                "--metric {} disagrees with metric.txt ({})",
                m.name(),
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
    let queries = churn_common::read_vectors(&query_p, Some(a.queries))?;

    let (name, steps, data) = if let Some(rb) = &a.runbook {
        let text = std::fs::read_to_string(rb).map_err(|e| format!("{}: {e}", rb.display()))?;
        let (rb_name, steps) = churn_common::parse_runbook(&text, a.runbook_name.as_deref())?;
        let needed = churn_common::rows_needed(&steps);
        // Read only as far as the runbook reaches: a 30M-row file need not
        // be resident for a runbook that uses its first million.
        let data = churn_common::read_vectors(&base_p, Some(needed))?;
        (format!("{name}/{rb_name}"), steps, data)
    } else {
        // Without a runbook the file supplies the live set and, after it,
        // the vectors the churn draws on. `--limit` sizes the live set;
        // otherwise it is as large as the file allows.
        let rows_per_n = 1.0 + a.cycles as f64 * a.churn;
        let data = churn_common::read_vectors(&base_p, None)?;
        let fits = (data.len() as f64 / rows_per_n).floor() as usize;
        let n = a.limit.unwrap_or(fits);
        let per = per_cycle(n, a.churn);
        let steps = match a.mode {
            Mode::Churn => churn_common::churn_steps(n, a.cycles, per, a.seed),
            Mode::Drift => churn_common::window_steps(n, a.cycles, per, 1),
        };
        let needed = churn_common::rows_needed(&steps);
        if needed > data.len() {
            return Err(format!(
                "{} cycles at {} per cycle over {n} live vectors need {needed} rows; {} has {}",
                a.cycles,
                per,
                base_p.display(),
                data.len()
            ));
        }
        (format!("{name}-{}", a.mode.name()), steps, data)
    };
    if queries.dim != data.dim {
        return Err(format!(
            "base vectors have {} dimensions and queries have {}",
            data.dim, queries.dim
        ));
    }
    Ok((
        Workload {
            name,
            data,
            queries: vec![queries],
            steps,
        },
        metric,
    ))
}

const CSV_HEADER: &str =
    "dataset,system,metric,dim,k,m,ef_construction,ef,step,search,live,superseded,dead_share,\
compactions,path,recall,fresh_recall,recall_gap,search_qps,search_p50_us,search_p99_us,\
inserts,insert_ops_s,deletes,delete_ops_s,updates,update_ops_s,write_p99_us,write_max_us,\
probes,probe_p50_us,probe_p99_us,fresh_build_s,resident_bytes,rss_bytes,calibration_ms";

fn micros(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e6)
}

fn writes(r: &Report) -> churn_common::Ops {
    let mut all = churn_common::Ops::default();
    for o in [&r.inserts, &r.deletes, &r.updates] {
        all.latencies.extend_from_slice(&o.latencies);
    }
    all
}

fn path_name(r: &Report) -> &'static str {
    if r.scanned {
        "scan"
    } else {
        "graph"
    }
}

fn print_header() {
    println!(
        "\n  {:>4} {:>8} {:>8} {:>5} {:>5} {:>7} {:>7} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>9}",
        "step",
        "live",
        "dead",
        "comp",
        "path",
        "recall",
        "fresh",
        "gap",
        "QPS",
        "p99 µs",
        "ins/s",
        "del/s",
        "upd/s",
        "wr max µs",
        "probe p99",
        "MB"
    );
}

fn print_row(r: &Report) {
    let opt = |v: Option<f64>| v.map(|x| format!("{x:.4}")).unwrap_or_else(|| "-".into());
    let rate = |o: &churn_common::Ops| {
        if o.count() == 0 {
            "-".into()
        } else {
            format!("{:.0}", o.per_second())
        }
    };
    let w = writes(r);
    println!(
        "  {:>4} {:>8} {:>8} {:>5} {:>5} {:>7.4} {:>7} {:>7} {:>9.0} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>9.1}",
        r.search,
        r.live,
        r.superseded,
        r.compactions,
        path_name(r),
        r.recall,
        opt(r.fresh_recall),
        opt(r.fresh_recall.map(|f| f - r.recall)),
        r.search_qps(),
        micros(r.search_ops.percentile(99.0)),
        rate(&r.inserts),
        rate(&r.deletes),
        rate(&r.updates),
        if w.count() == 0 { "-".into() } else { micros(w.max()) },
        if r.probes.count() == 0 {
            "-".into()
        } else {
            micros(r.probes.percentile(99.0))
        },
        r.resident_bytes as f64 / 1e6
    );
}

fn csv_row(w: &Workload, metric: Metric, a: &Args, r: &Report, calibration: Duration) -> String {
    let opt_f = |v: Option<f64>| v.map(|x| format!("{x:.6}")).unwrap_or_default();
    let all = writes(r);
    let p = |o: &churn_common::Ops, pct: f64| {
        if o.count() == 0 {
            String::new()
        } else {
            micros(o.percentile(pct))
        }
    };
    format!(
        "{},samyama-graph,{},{},{},{},{},{},{},{},{},{},{:.6},{},{},{:.6},{},{},{:.1},{},{},\
         {},{:.1},{},{:.1},{},{:.1},{},{},{},{},{},{},{},{},{:.0}\n",
        w.name,
        metric.name(),
        w.data.dim,
        a.k,
        a.m,
        a.ef_construction,
        a.ef,
        r.step,
        r.search,
        r.live,
        r.superseded,
        r.dead_share(),
        r.compactions,
        path_name(r),
        r.recall,
        opt_f(r.fresh_recall),
        opt_f(r.fresh_recall.map(|f| f - r.recall)),
        r.search_qps(),
        p(&r.search_ops, 50.0),
        p(&r.search_ops, 99.0),
        r.inserts.count(),
        r.inserts.per_second(),
        r.deletes.count(),
        r.deletes.per_second(),
        r.updates.count(),
        r.updates.per_second(),
        p(&all, 99.0),
        if all.count() == 0 {
            String::new()
        } else {
            micros(all.max())
        },
        r.probes.count(),
        p(&r.probes, 50.0),
        p(&r.probes, 99.0),
        r.fresh_build
            .map(|d| format!("{:.3}", d.as_secs_f64()))
            .unwrap_or_default(),
        r.resident_bytes,
        r.rss_bytes.map(|b| b.to_string()).unwrap_or_default(),
        calibration.as_secs_f64() * 1000.0,
    )
}

fn main() {
    bench_setup::init();
    let a = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("vector_churn_benchmark: {e}");
            std::process::exit(2);
        }
    };
    let calibration = bench_setup::report_calibration();

    let (w, metric) = match workload(&a) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("vector_churn_benchmark: {e}");
            std::process::exit(2);
        }
    };
    let searches = w
        .steps
        .iter()
        .filter(|s| matches!(s, churn_common::Step::Search { .. }))
        .count();
    println!(
        "Workload {}: {} steps ({searches} searches) over {} rows x {} dims, metric {}, \
         recall@{} at ef={}, m={}, ef_construction={}",
        w.name,
        w.steps.len(),
        w.data.len(),
        w.data.dim,
        metric.name(),
        a.k,
        a.ef,
        a.m,
        a.ef_construction
    );
    println!(
        "Ground truth: exact search over the live set at each step. `fresh` is an index \
         built from scratch on the same live set; `gap` is fresh minus churned."
    );

    let config = Config {
        metric,
        params: samyama::vector::HnswParams {
            m: a.m,
            ef_construction: a.ef_construction,
            ef: None,
        },
        k: a.k,
        ef: a.ef,
        fresh_every: a.fresh_every,
        probe_every: a.probe_every,
    };
    print_header();
    let reports = match churn_common::run(&w, &config, print_row) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("vector_churn_benchmark: {e}");
            std::process::exit(1);
        }
    };

    if let Some(first) = reports.first() {
        let min = reports
            .iter()
            .min_by(|x, y| x.recall.total_cmp(&y.recall))
            .unwrap_or(first);
        let worst_gap = reports
            .iter()
            .filter_map(|r| r.fresh_recall.map(|f| (f - r.recall, r.search)))
            .max_by(|x, y| x.0.total_cmp(&y.0));
        let scanned = reports.iter().filter(|r| r.scanned).count();
        let last = reports.last().unwrap_or(first);
        println!(
            "\nRecall@{}: {:.4} after the load, {:.4} at the end, lowest {:.4} at step {}.",
            a.k, first.recall, last.recall, min.recall, min.search
        );
        if let Some((gap, at)) = worst_gap {
            println!("Largest shortfall against a fresh build: {gap:+.4} at step {at}.");
        }
        println!(
            "Dead entries at the end: {} ({:.1}% of the graph); {} rebuild(s) by the index.",
            last.superseded,
            last.dead_share() * 100.0,
            last.compactions
        );
        if scanned > 0 {
            println!(
                "{scanned} of {} search steps were answered by a linear scan, not the graph: \
                 their recall says nothing about the graph. Raise N or lower --ef.",
                reports.len()
            );
        }
    }

    if let Some(csv) = &a.csv {
        let new = !csv.exists();
        let mut out = String::new();
        if new {
            out.push_str(CSV_HEADER);
            out.push('\n');
        }
        for r in &reports {
            out.push_str(&csv_row(&w, metric, &a, r, calibration));
        }
        use std::io::Write;
        let written = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(csv)
            .and_then(|mut f| f.write_all(out.as_bytes()));
        match written {
            Ok(()) => println!("\nWrote {} rows to {}", reports.len(), csv.display()),
            Err(e) => {
                eprintln!("vector_churn_benchmark: {}: {e}", csv.display());
                std::process::exit(1);
            }
        }
    }

    bench_setup::report_drift(calibration);
}
