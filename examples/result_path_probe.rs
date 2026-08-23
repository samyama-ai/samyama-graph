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

const ROWS: &[usize] = &[1, 10, 100, 1_000, 10_000];
const RUNS: usize = 9;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench_setup::init();
    let _ = bench_setup::report_calibration();

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
