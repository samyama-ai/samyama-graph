//! Time arbitrary Cypher against an LDBC extract, one line per query.
//!
//! `is7_probe` and `ic11_probe` are the same forty lines twice: load the
//! dataset, build the `id` indexes the bench builds, run a handful of variants
//! that each remove one thing, print medians, print plans. A third copy for
//! IS3 would have made it three, so this is the general form.
//!
//! Every variant is a separate `--q "<label>=<cypher>"`, and the difference
//! between two lines is the cost of whatever separates them. That is the whole
//! method: never one number, always a pair whose difference means something.
//!
//! ```bash
//! cargo run --release --example query_probe -- --data-dir <sf1> \
//!     --q 'full=MATCH (p:Person {id: 123})-[:KNOWS]-(f) RETURN f.id' \
//!     --q 'count=MATCH (p:Person {id: 123})-[:KNOWS]-(f) RETURN count(f) AS n'
//! ```
//!
//! `--explain` prints each plan as well. The host calibration and a busy-host
//! warning are printed first, because a probe sharing the machine with a build
//! measures the build (#715).

#[path = "../benches/ldbc_common/mod.rs"]
mod ldbc_common;

#[path = "../benches/common/bench_setup.rs"]
mod bench_setup;

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench_setup::init();
    let _calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let one = |flag: &str| args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1));
    let data_dir = one("--data-dir").map(PathBuf::from).expect("--data-dir <path> is required");
    let runs: usize = one("--runs").map(|s| s.parse()).transpose()?.unwrap_or(11);
    let explain = args.iter().any(|a| a == "--explain");

    // `label=cypher`, repeated. The label is everything before the first `=`,
    // so a query may contain as many as it likes.
    let variants: Vec<(String, String)> = args
        .iter()
        .enumerate()
        .filter(|(_, a)| *a == "--q")
        .filter_map(|(i, _)| args.get(i + 1))
        .map(|spec| match spec.split_once('=') {
            Some((label, cypher)) => (label.to_string(), cypher.to_string()),
            None => (spec.chars().take(28).collect(), spec.clone()),
        })
        .collect();
    if variants.is_empty() {
        eprintln!("nothing to run: pass at least one --q '<label>=<cypher>'");
        std::process::exit(2);
    }

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    let t = Instant::now();
    ldbc_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded in {:.1}s", t.elapsed().as_secs_f64());

    // The same `id` indexes the benchmark builds. Without them every
    // `MATCH (x:Label {id: ...})` is a full label scan, which drowns whatever
    // the variants are trying to separate.
    for label in ["Person", "Post", "Comment", "Forum", "Place", "Organisation", "Tag"] {
        let q = parse_query(&format!("CREATE INDEX ON :{label}(id)"))?;
        MutQueryExecutor::new(&mut graph, "default".to_string()).execute(&q)?;
    }

    let width = variants.iter().map(|(l, _)| l.len()).max().unwrap_or(10).max(10);
    for (label, cypher) in &variants {
        let q = match parse_query(cypher) {
            Ok(q) => q,
            Err(e) => {
                println!("{label:<width$}  does not parse: {e}");
                continue;
            }
        };
        let mut times = Vec::with_capacity(runs);
        let mut rows = 0usize;
        for _ in 0..runs {
            let started = Instant::now();
            match QueryExecutor::new(&graph).execute(&q) {
                Ok(out) => {
                    rows = out.records.len();
                    times.push(started.elapsed().as_secs_f64() * 1000.0);
                }
                Err(e) => {
                    println!("{label:<width$}  failed: {e}");
                    times.clear();
                    break;
                }
            }
        }
        if times.is_empty() {
            continue;
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "{label:<width$}  median {:>9.3} ms   rows {rows}",
            times[times.len() / 2]
        );
    }

    if explain {
        for (label, cypher) in &variants {
            println!("\n--- plan: {label} ---");
            if let Ok(q) = parse_query(&format!("EXPLAIN {cypher}")) {
                if let Ok(out) = QueryExecutor::new(&graph).execute(&q) {
                    for r in &out.records {
                        if let Some(v) = r.get("plan") {
                            println!("{}", format!("{v:?}").replace("\\n", "\n"));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
