//! What the client path costs on top of the executor, per returned row (#718).
//!
//! The LDBC bench times `client.query_readonly`; `query_probe` times
//! `QueryExecutor::execute`. On IS3 at SF10 they disagree by 3.3× — 1.50 ms
//! against 0.458 ms — and IS3 is the one short read that is not fastest, so
//! which of the two is being optimised matters.
//!
//! The difference cannot be a fixed per-query overhead: IS1 and IS4 return one
//! row and the bench clocks them at 0.09 and 0.02 ms, which leaves no room for
//! it. The hypothesis is that it scales with **rows returned**, because
//! `record_batch_to_query_result` converts every value of every row into
//! `serde_json::Value` — allocating a JSON object per entity and cloning every
//! property.
//!
//! This runs the same query through both paths **in one process**, over a row
//! count sweep, which is the comparison the two harnesses cannot make. Two
//! harnesses is how I got the earlier estimate wrong.
//!
//! ```bash
//! cargo run --release --example result_path_probe
//! ```

use std::time::Instant;

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use samyama_sdk::{EmbeddedClient, SamyamaClient};

#[path = "../benches/common/bench_setup.rs"]
mod bench_setup;

#[path = "../benches/ldbc_common/mod.rs"]
mod ldbc_common;

const ROWS: &[usize] = &[1, 10, 100, 1_000, 10_000];
const RUNS: usize = 9;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Three arms, so the two steps between them are separated:
///
/// 1. `QueryExecutor::execute` — what `query_probe` times.
/// 2. `QueryEngine::execute` — adds the cached parse **and a deadline**.
///    `query_timeout_secs` defaults to **120**, so every call through the
///    engine installs one, and `check_deadline()` calls `Instant::now()` at
///    thirteen sites inside operator materialisation loops. The direct
///    executor installs none.
/// 3. `client.query_readonly` — adds `record_batch_to_query_result`.
///
/// The LDBC bench uses (3) and `query_probe` uses (1), and on IS3 at SF10 they
/// disagree 3.3×. (2) is the arm that says which half of the gap is which.
async fn ldbc_mode(data_dir: &std::path::Path, queries: &[(String, String)], runs: usize)
    -> Result<(), Box<dyn std::error::Error>> {
    use samyama::query::QueryEngine;

    let mut direct = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    ldbc_common::load_dataset(&mut direct, data_dir)?;
    let client = EmbeddedClient::new();
    {
        let mut g = client.store_write().await;
        ldbc_common::load_dataset(&mut g, data_dir)?;
    }

    // The same `id` indexes both harnesses build. Without them every
    // `MATCH (x:Label {id: ...})` is a full label scan, which is 27x IS3's
    // whole runtime at SF1 and drowns the thing being compared. Leaving them
    // out is how the first version of this probe reported 1.083 ms for a query
    // the bench measures at 0.04 ms.
    for label in ["Person", "Post", "Comment", "Forum", "Place", "Organisation", "Tag", "TagClass"] {
        let stmt = format!("CREATE INDEX ON :{label}(id)");
        let q = parse_query(&stmt)?;
        samyama::query::executor::MutQueryExecutor::new(&mut direct, "default".to_string())
            .execute(&q)?;
        client.query("default", &stmt).await?;
    }

    let engine = QueryEngine::new();

    println!(
        "\n{:<22} {:>11} {:>11} {:>11}  {:>9} {:>9}",
        "query", "executor", "engine", "client", "eng/exec", "cli/eng"
    );
    println!("{:-<22} {:->11} {:->11} {:->11}  {:->9} {:->9}", "", "", "", "", "", "");

    for (label, cypher) in queries {
        let q = parse_query(cypher)?;
        let _ = QueryExecutor::new(&direct).execute(&q)?;
        let exec_ms = median((0..runs).map(|_| {
            let t = Instant::now();
            let _ = QueryExecutor::new(&direct).execute(&q).expect("runs");
            t.elapsed().as_secs_f64() * 1000.0
        }).collect());

        let _ = engine.execute(cypher, &direct)?;
        let engine_ms = median((0..runs).map(|_| {
            let t = Instant::now();
            let _ = engine.execute(cypher, &direct).expect("runs");
            t.elapsed().as_secs_f64() * 1000.0
        }).collect());

        let _ = client.query_readonly("default", cypher).await?;
        let mut cts = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t = Instant::now();
            let _ = client.query_readonly("default", cypher).await?;
            cts.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let client_ms = median(cts);

        println!(
            "{label:<22} {:>9.3}ms {:>9.3}ms {:>9.3}ms  {:>8.2}x {:>8.2}x",
            exec_ms, engine_ms, client_ms,
            engine_ms / exec_ms.max(1e-9),
            client_ms / engine_ms.max(1e-9),
        );
    }
    println!(
        "\nexecutor -> engine is the cached parse plus the deadline \
         (SAMYAMA_QUERY_TIMEOUT=0 removes the deadline);\n\
         engine -> client is record_batch_to_query_result (#718)."
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench_setup::init();
    let _ = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let one = |f: &str| args.iter().position(|a| a == f).and_then(|i| args.get(i + 1));
    if let Some(dir) = one("--data-dir") {
        let runs: usize = one("--runs").map(|s| s.parse()).transpose()?.unwrap_or(9);
        let queries: Vec<(String, String)> = args
            .iter()
            .enumerate()
            .filter(|(_, a)| a.as_str() == "--q")
            .map(|(i, _)| {
                let s = args.get(i + 1).expect("--q needs label=cypher");
                let (l, c) = s.split_once('=').expect("--q needs label=cypher");
                (l.to_string(), c.to_string())
            })
            .collect();
        return ldbc_mode(std::path::Path::new(dir), &queries, runs).await;
    }

    // One store, built once, shared by both paths. The client holds its own,
    // so it is loaded with the same CREATEs rather than the same object —
    // identical content, which is what the comparison needs.
    let biggest = *ROWS.last().unwrap();
    let mut direct = GraphStore::new();
    let client = EmbeddedClient::new();
    for i in 0..biggest {
        let cypher = format!(
            "CREATE (:P {{n: {i}, name: \"person{i}\", tag: \"t{}\"}})",
            i % 7
        );
        let q = parse_query(&cypher)?;
        samyama::query::executor::MutQueryExecutor::new(&mut direct, "default".to_string())
            .execute(&q)?;
        client.query("default", &cypher).await?;
    }

    println!(
        "\n{:>8}  {:>12}  {:>12}  {:>10}  {:>12}",
        "rows", "executor", "client", "client/exec", "delta/row"
    );
    println!("{:->8}  {:->12}  {:->12}  {:->10}  {:->12}", "", "", "", "", "");

    for &n in ROWS {
        // Properties, not just a count: a bare count returns one row and would
        // measure nothing about per-row materialisation.
        let cypher = format!("MATCH (p:P) RETURN p.n, p.name, p.tag LIMIT {n}");
        let q = parse_query(&cypher)?;

        let _ = QueryExecutor::new(&direct).execute(&q)?;
        let exec_ms = median(
            (0..RUNS)
                .map(|_| {
                    let t = Instant::now();
                    let out = QueryExecutor::new(&direct).execute(&q).expect("runs");
                    let ms = t.elapsed().as_secs_f64() * 1000.0;
                    assert_eq!(out.records.len(), n, "executor row count");
                    ms
                })
                .collect(),
        );

        let _ = client.query_readonly("default", &cypher).await?;
        let mut client_times = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let t = Instant::now();
            let out = client.query_readonly("default", &cypher).await?;
            client_times.push(t.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(out.records.len(), n, "client row count");
        }
        let client_ms = median(client_times);

        println!(
            "{n:>8}  {:>10.3}ms  {:>10.3}ms  {:>9.2}x  {:>10.2}us",
            exec_ms,
            client_ms,
            client_ms / exec_ms.max(1e-9),
            (client_ms - exec_ms) * 1000.0 / n as f64,
        );
    }

    println!(
        "\nThe last column is what `record_batch_to_query_result` costs per returned row.\n\
         If it is flat, the client path is a per-row cost and IS3 is dominated by it;\n\
         if it falls away with n, the 3.3x on IS3 is something else (#718)."
    );
    Ok(())
}
