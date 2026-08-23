//! HIER benchmark runner — hierarchy-heavy complex queries (ADR-035).
//!
//! Runs the corpus in `benchmarks/hier/queries.json` twice: once against a store with the
//! four hierarchy indexes declared, once against an identical store with none. Every query
//! must return the **same answer** both ways; the difference in latency is the result.
//!
//! Correctness is the gate, not a footnote. The unindexed run is the ground truth — a
//! plain variable-length traversal, no index involved — so a disagreement means the index
//! is wrong and the runner exits non-zero. A benchmark that reported a speedup without
//! that check would be measuring how fast we can produce the wrong number.
//!
//! ```bash
//! cargo run --release --example hier_benchmark
//! cargo run --release --example hier_benchmark -- --reps 20 --out results.csv
//! ```

#[path = "../benches/hier_common/mod.rs"]
mod hier_common;

use std::collections::BTreeMap;
use std::time::Instant;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::{QueryEngine, RecordBatch};

use hier_common::{HierScale, HIER_DECLARATIONS, SETUP_DECLARATIONS};

/// One corpus entry.
#[derive(Debug)]
struct Query {
    id: String,
    class: String,
    name: String,
    /// The query as a user would write it with the index available.
    cypher: String,
    /// The same question written without index assistance. When absent, the runner uses
    /// `cypher` itself against the index-free store.
    baseline: Option<String>,
    /// Set when the query is specified but cannot run on this engine today. The corpus
    /// keeps it — a class that silently vanished from the table would read as "covered".
    skip: Option<String>,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let reps = arg_value(&args, "--reps").and_then(|v| v.parse().ok()).unwrap_or(5usize);
    let out = arg_value(&args, "--out").unwrap_or_else(|| "benchmarks/hier/results/results.csv".to_string());
    let corpus_path =
        arg_value(&args, "--corpus").unwrap_or_else(|| "benchmarks/hier/queries.json".to_string());
    let filter = arg_value(&args, "--class");

    let scale = HierScale::default();
    eprintln!("[hier] building dataset…");
    let t0 = Instant::now();
    let data = hier_common::build(&scale);
    eprintln!(
        "[hier] {} nodes, {} edges in {:.2}s",
        data.nodes,
        data.edges,
        t0.elapsed().as_secs_f64()
    );

    // Two stores from one build: the indexed store gets the declarations, the baseline
    // store is byte-identical except that it has none.
    let mut indexed = data.store;
    let mut baseline_store = hier_common::build(&scale).store;
    let engine = QueryEngine::new();

    // Fixture indexes go on both stores: they are not what is being measured.
    for decl in SETUP_DECLARATIONS {
        for store in [&mut indexed, &mut baseline_store] {
            engine
                .execute_mut(decl, store, "default")
                .unwrap_or_else(|e| panic!("setup failed: {decl}\n{e}"));
        }
    }

    eprintln!("[hier] declaring hierarchies…");
    let mut build_report = Vec::new();
    for decl in HIER_DECLARATIONS {
        let t = Instant::now();
        let r = engine
            .execute_mut(decl, &mut indexed, "default")
            .unwrap_or_else(|e| panic!("declaration failed: {decl}\n{e}"));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let name = cell_string(&r, 0, "name");
        let encoding = cell_string(&r, 0, "encoding");
        let nodes = cell_i64(&r, 0, "nodes");
        let bytes = cell_i64(&r, 0, "bytes");
        let structural = cell_i64(&r, 0, "structural_bytes");
        let rollup = cell_i64(&r, 0, "rollup_bytes");
        eprintln!(
            "[hier]   {name:8} {encoding:11} {nodes:>7} nodes  {:>6.1} B/node structural              + {:>7.1} roll-up  build {ms:>7.1} ms",
            structural as f64 / nodes.max(1) as f64,
            rollup as f64 / nodes.max(1) as f64
        );
        build_report.push((name, encoding, nodes, bytes, structural, rollup, ms));
    }

    let corpus = load_corpus(&corpus_path);
    let corpus: Vec<Query> = match &filter {
        Some(c) => corpus.into_iter().filter(|q| &q.class == c).collect(),
        None => corpus,
    };
    eprintln!("[hier] running {} queries × {reps} reps…", corpus.len());

    let mut rows: Vec<String> = vec![
        "id,class,name,indexed_ms,baseline_ms,speedup,rows,agree".to_string(),
    ];
    let mut by_class: BTreeMap<String, (usize, usize, f64, f64)> = BTreeMap::new();
    let mut mismatches: Vec<String> = Vec::new();

    let mut skipped: Vec<(&str, &str)> = Vec::new();
    for q in &corpus {
        if let Some(reason) = &q.skip {
            skipped.push((q.id.as_str(), reason.as_str()));
            rows.push(format!("{},{},\"{}\",,,,,skipped", q.id, q.class, q.name));
            continue;
        }
        let (indexed_ms, indexed_result) = timed(&engine, &q.cypher, &indexed, reps);
        let baseline_query = q.baseline.as_deref().unwrap_or(&q.cypher);
        let (baseline_ms, baseline_result) = timed(&engine, baseline_query, &baseline_store, reps);

        let (agree, rows_out) = match (&indexed_result, &baseline_result) {
            (Ok(a), Ok(b)) => {
                let ca = canonical(a);
                let cb = canonical(b);
                if ca != cb {
                    mismatches.push(format!(
                        "{}: indexed={} baseline={}",
                        q.id,
                        truncate(&ca),
                        truncate(&cb)
                    ));
                }
                (ca == cb, a.records.len())
            }
            (Err(e), _) => {
                mismatches.push(format!("{}: indexed query failed: {e}", q.id));
                (false, 0)
            }
            (_, Err(e)) => {
                mismatches.push(format!("{}: baseline query failed: {e}", q.id));
                (false, 0)
            }
        };

        let speedup = if indexed_ms > 0.0 { baseline_ms / indexed_ms } else { 0.0 };
        rows.push(format!(
            "{},{},\"{}\",{:.4},{:.4},{:.2},{},{}",
            q.id, q.class, q.name, indexed_ms, baseline_ms, speedup, rows_out, agree
        ));
        let e = by_class.entry(q.class.clone()).or_insert((0, 0, 0.0, 0.0));
        e.0 += 1;
        if agree {
            e.1 += 1;
        }
        e.2 += indexed_ms;
        e.3 += baseline_ms;
    }

    // --- report -------------------------------------------------------------
    println!();
    println!("HIER benchmark — {} queries, {reps} reps, median of each", corpus.len());
    println!("{}", "-".repeat(78));
    println!(
        "{:<6} {:>5} {:>7} {:>12} {:>12} {:>9}",
        "class", "n", "agree", "indexed ms", "baseline ms", "speedup"
    );
    for (class, (n, agree, ims, bms)) in &by_class {
        println!(
            "{:<6} {:>5} {:>7} {:>12.3} {:>12.3} {:>8.1}x",
            class,
            n,
            format!("{agree}/{n}"),
            ims / *n as f64,
            bms / *n as f64,
            if *ims > 0.0 { bms / ims } else { 0.0 }
        );
    }
    println!("{}", "-".repeat(78));
    let total: usize = by_class.values().map(|v| v.0).sum();
    let agreed: usize = by_class.values().map(|v| v.1).sum();
    let ti: f64 = by_class.values().map(|v| v.2).sum();
    let tb: f64 = by_class.values().map(|v| v.3).sum();
    println!(
        "{:<6} {:>5} {:>7} {:>12.3} {:>12.3} {:>8.1}x",
        "ALL",
        total,
        format!("{agreed}/{total}"),
        ti / total.max(1) as f64,
        tb / total.max(1) as f64,
        if ti > 0.0 { tb / ti } else { 0.0 }
    );

    if let Some(dir) = std::path::Path::new(&out).parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    std::fs::write(&out, rows.join("\n") + "\n").expect("write results");
    let index_csv = std::path::Path::new(&out)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("indexes.csv");
    let mut ix = vec![
        "name,encoding,nodes,bytes,structural_bytes,rollup_bytes,structural_bytes_per_node,build_ms"
            .to_string(),
    ];
    for (name, encoding, nodes, bytes, structural, rollup, ms) in &build_report {
        ix.push(format!(
            "{name},{encoding},{nodes},{bytes},{structural},{rollup},{:.2},{ms:.2}",
            *structural as f64 / (*nodes).max(1) as f64
        ));
    }
    std::fs::write(&index_csv, ix.join("\n") + "\n").expect("write index stats");

    // Who produced these numbers, written beside them.
    //
    // `results.csv` went a release out of step with `docs/BENCHMARKS.md` and
    // nothing in either said so: the CSV had H1 at 0.25x where the prose had
    // 1.1x, and a reader doing the right thing — going to the artifact rather
    // than trusting the prose — got the worse, older number (#476). A file
    // that cannot say when or where it came from cannot be checked against
    // anything.
    //
    // Deliberately not a claim that the numbers are good: a run on a laptop
    // writes a laptop here, which is exactly the fact a reader needs in order
    // to decide whether to quote it.
    let provenance = std::path::Path::new(&out)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("PROVENANCE.json");
    let git = |args: &[&str]| -> String {
        std::process::Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    };
    let field_after_colon = |path: &str, prefix: &str| -> String {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|t| {
                t.lines()
                    .find(|l| l.starts_with(prefix))
                    .and_then(|l| l.split(':').nth(1))
                    .map(|v| v.trim().to_string())
            })
            .unwrap_or_default()
    };
    let whole_file = |path: &str| -> String {
        std::fs::read_to_string(path).map(|t| t.trim().to_string()).unwrap_or_default()
    };
    let dirty = !git(&["status", "--porcelain"]).is_empty();
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    let load = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(|v| v.to_string()))
        .unwrap_or_default();
    std::fs::write(
        &provenance,
        format!(
            "{{\n  \"commit\": \"{}\",\n  \"dirty\": {},\n  \"host\": \"{}\",\n  \
             \"cpu\": \"{}\",\n  \"cores\": {},\n  \"load_average_1m\": \"{}\",\n  \
             \"reps\": {},\n  \"corpus\": \"{}\",\n  \"queries\": {},\n  \
             \"agreed\": {}\n}}\n",
            git(&["rev-parse", "--short=7", "HEAD"]),
            dirty,
            whole_file("/proc/sys/kernel/hostname"),
            field_after_colon("/proc/cpuinfo", "model name"),
            cores,
            load,
            reps,
            corpus_path,
            total,
            agreed,
        ),
    )
    .expect("write provenance");
    eprintln!(
        "[hier] wrote {out}, {} and {}",
        index_csv.display(),
        provenance.display()
    );

    if !skipped.is_empty() {
        println!();
        println!("{} corpus queries specified but not runnable on this engine:", skipped.len());
        for (id, reason) in &skipped {
            println!("  {id}: {reason}");
        }
    }

    if !mismatches.is_empty() {
        eprintln!();
        eprintln!("[hier] {} DISAGREEMENTS — the index is wrong, not fast:", mismatches.len());
        for m in mismatches.iter().take(20) {
            eprintln!("  {m}");
        }
        std::process::exit(1);
    }
    println!();
    println!("All {total} queries agree with the unindexed ground truth.");
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
}

/// Run `q` `reps` times and return the median wall time in ms plus one result.
fn timed(
    engine: &QueryEngine,
    q: &str,
    store: &GraphStore,
    reps: usize,
) -> (f64, Result<RecordBatch, String>) {
    let mut times = Vec::with_capacity(reps);
    let mut last: Result<RecordBatch, String> = Err("not run".to_string());
    for _ in 0..reps.max(1) {
        let t = Instant::now();
        let r = engine.execute(q, store);
        times.push(t.elapsed().as_secs_f64() * 1000.0);
        last = r.map_err(|e| e.to_string());
        if last.is_err() {
            break;
        }
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[times.len() / 2], last)
}

/// Canonical, order-independent rendering of a result set, for the agreement check.
fn canonical(batch: &RecordBatch) -> String {
    let mut rows: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            let mut cells: Vec<String> = r
                .bindings()
                .iter()
                .map(|(k, v)| format!("{k}={}", render(v)))
                .collect();
            cells.sort();
            cells.join("|")
        })
        .collect();
    rows.sort();
    rows.join(";")
}

fn render(v: &Value) -> String {
    match v {
        Value::Property(PropertyValue::Float(f)) => format!("{f:.6}"),
        Value::Property(p) => format!("{p:?}"),
        Value::Node(id, _) => format!("n{}", id.as_u64()),
        Value::NodeRef(id) => format!("n{}", id.as_u64()),
        other => format!("{other:?}"),
    }
}

fn truncate(s: &str) -> String {
    if s.len() > 120 {
        format!("{}…", &s[..120])
    } else {
        s.to_string()
    }
}

fn cell_string(b: &RecordBatch, row: usize, col: &str) -> String {
    match b.records[row].get(col) {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        other => format!("{other:?}"),
    }
}

fn cell_i64(b: &RecordBatch, row: usize, col: &str) -> i64 {
    match b.records[row].get(col) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        _ => 0,
    }
}

fn load_corpus(path: &str) -> Vec<Query> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("cannot read corpus {path}: {e}"));
    let json: serde_json::Value = serde_json::from_str(&text).expect("corpus is not valid JSON");
    json["queries"]
        .as_array()
        .expect("corpus needs a `queries` array")
        .iter()
        .map(|q| Query {
            id: q["id"].as_str().unwrap().to_string(),
            class: q["class"].as_str().unwrap().to_string(),
            name: q["name"].as_str().unwrap_or("").to_string(),
            cypher: q["cypher"].as_str().unwrap().to_string(),
            baseline: q["baseline"].as_str().map(|s| s.to_string()),
            skip: q["skip"].as_str().map(|s| s.to_string()),
        })
        .collect()
}
