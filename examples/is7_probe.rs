//! Where LDBC IS7's time goes (#618).
//!
//! IS7 is the one short read that is not sub-millisecond: 26.8 ms at SF10
//! against FalkorDB's 0.66 ms on the same host, and 0.54 ms at SF1 when every
//! other short read there is under 0.06 ms. It returns five rows. The profile
//! puts ~93% of it in `Project`, which is where the `EXISTS` subquery is
//! evaluated — so the question is whether the cost is the subquery, the
//! property reads beside it, or the plan feeding both.
//!
//! Each variant below removes exactly one thing from the query, so the
//! difference between two lines is the cost of that thing.
//!
//!   cargo run --release --example is7_probe -- --data-dir <ldbc-sf1-dir> --post-id <id>

#[path = "../benches/ldbc_common/mod.rs"]
mod ldbc_common;

#[path = "../benches/common/bench_setup.rs"]
mod bench_setup;

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A probe that shares the machine with a build measures the build: this one
    // reported IC11 at 16.9 ms next to a `cargo test --workspace` and 8.0 ms on
    // a quiet host (#715). The calibration line makes the host part of the
    // output rather than something the reader has to remember.
    bench_setup::init();
    let _calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1));
    let data_dir = arg("--data-dir").map(PathBuf::from).expect("--data-dir <path> is required");
    let post_id: i64 = arg("--post-id").expect("--post-id <id> is required").parse()?;
    let runs: usize = arg("--runs").map(|s| s.parse()).transpose()?.unwrap_or(11);

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    let t = Instant::now();
    ldbc_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded in {:.1}s", t.elapsed().as_secs_f64());

    // The same `id` indexes the benchmark builds. Without them every
    // `MATCH (m:Post {id: ...})` lowers to a full label scan over 1.19M posts,
    // which costs 150 ms and drowns everything this probe is trying to
    // separate — the first version of this file measured exactly that.
    for (label, prop) in [("Person", "id"), ("Post", "id"), ("Comment", "id")] {
        let stmt = format!("CREATE INDEX ON :{label}({prop})");
        let q = parse_query(&stmt)?;
        samyama::query::executor::MutQueryExecutor::new(&mut graph, "default".to_string())
            .execute(&q)?;
    }

    // The query as the benchmark runs it.
    let full = format!(
        "MATCH (m:Post {{id: {post_id}}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
MATCH (m)-[:HAS_CREATOR]->(op:Person)
RETURN c.id, c.content, c.creationDate, author.id, author.firstName, author.lastName, \
EXISTS {{ MATCH (op)-[:KNOWS]-(author) }} AS isKnows
ORDER BY c.creationDate DESC
LIMIT 20"
    );
    // Same plan, no EXISTS: isolates the subquery from everything else.
    let no_exists = format!(
        "MATCH (m:Post {{id: {post_id}}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
MATCH (m)-[:HAS_CREATOR]->(op:Person)
RETURN c.id, c.content, c.creationDate, author.id, author.firstName, author.lastName, op.id
ORDER BY c.creationDate DESC
LIMIT 20"
    );
    // EXISTS kept, the six property reads dropped.
    let only_exists = format!(
        "MATCH (m:Post {{id: {post_id}}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
MATCH (m)-[:HAS_CREATOR]->(op:Person)
RETURN EXISTS {{ MATCH (op)-[:KNOWS]-(author) }} AS isKnows
LIMIT 20"
    );
    // The second MATCH clause removed: is the HashJoin carrying `op` at all?
    let no_op = format!(
        "MATCH (m:Post {{id: {post_id}}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
RETURN c.id, c.content, c.creationDate, author.id, author.firstName, author.lastName
ORDER BY c.creationDate DESC
LIMIT 20"
    );
    // The subquery on its own, once per author, with `op` pinned by the outer
    // match — the shape `EXISTS` reduces to when its start variable is bound.
    let knows_only = format!(
        "MATCH (m:Post {{id: {post_id}}})-[:HAS_CREATOR]->(op:Person)
MATCH (op)-[:KNOWS]-(f:Person)
RETURN count(f) AS c"
    );

    for (label, cypher) in [
        ("IS7 as written", &full),
        ("  minus EXISTS", &no_exists),
        ("  EXISTS only (no property reads)", &only_exists),
        ("  minus the (m)-[:HAS_CREATOR]->(op) clause", &no_op),
        ("  op's KNOWS degree, counted", &knows_only),
    ] {
        let q = parse_query(cypher)?;
        let mut times: Vec<f64> = Vec::with_capacity(runs);
        let mut rows = 0usize;
        for _ in 0..runs {
            let t = Instant::now();
            match QueryExecutor::new(&graph).execute(&q) {
                Ok(out) => {
                    rows = out.records.len();
                    times.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                Err(e) => {
                    println!("{label:<44} failed: {e}");
                    times.clear();
                    break;
                }
            }
        }
        if times.is_empty() {
            continue;
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!("{label:<44} median {:>8.3} ms   rows {rows}", times[times.len() / 2]);
    }

    println!("\n--- plan of IS7 as written ---");
    let q = parse_query(&format!("EXPLAIN {full}"))?;
    for r in &QueryExecutor::new(&graph).execute(&q)?.records {
        if let Some(v) = r.get("plan") {
            println!("{}", format!("{v:?}").replace("\\n", "\n"));
        }
    }
    Ok(())
}
