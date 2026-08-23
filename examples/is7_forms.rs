//! IS7 the way we run it, and the way the competitors run it (#725).
//!
//! The SNB-I corpus gives Samyama `EXISTS { MATCH (op)-[:KNOWS]-(author) }`
//! and gives Neo4j and FalkorDB `OPTIONAL MATCH (op)-[k:KNOWS]-(author)` with
//! `(k IS NOT NULL)`. The two are semantically equivalent, and
//! `benches/ldbc_benchmark.rs` says in a comment why ours is the one it is:
//!
//!   Note: OPTIONAL MATCH version is semantically correct but triggers full
//!   Post scan in planner
//!
//! So the form was chosen to avoid a weakness of our own planner, and the
//! published ratio compares it against the form we avoided. This measures how
//! much that is worth, which is the only way to know whether the ratio
//! survives being made like-for-like.
//!
//!   cargo run --release --example is7_forms -- --data-dir <sf1> --post-id <id>

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
    let _c = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |f: &str| args.iter().position(|a| a == f).and_then(|i| args.get(i + 1));
    let data_dir = arg("--data-dir").map(PathBuf::from).expect("--data-dir <path>");
    let post_id: i64 = arg("--post-id").expect("--post-id <id>").parse()?;
    let runs: usize = arg("--runs").map(|s| s.parse()).transpose()?.unwrap_or(11);

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    ldbc_common::load_dataset(&mut graph, &data_dir)?;
    for (label, prop) in [("Person", "id"), ("Post", "id"), ("Comment", "id")] {
        let q = parse_query(&format!("CREATE INDEX ON :{label}({prop})"))?;
        MutQueryExecutor::new(&mut graph, "default".to_string()).execute(&q)?;
    }

    let head = format!(
        "MATCH (m:Post {{id: {post_id}}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(author:Person)
MATCH (m)-[:HAS_CREATOR]->(op:Person)"
    );
    // What the corpus gives Samyama.
    let ours = format!(
        "{head}
RETURN c.id, c.creationDate, author.id, \
EXISTS {{ MATCH (op)-[:KNOWS]-(author) }} AS isKnows
ORDER BY c.creationDate DESC
LIMIT 20"
    );
    // What the corpus gives Neo4j and FalkorDB, verbatim.
    let theirs = format!(
        "{head}
OPTIONAL MATCH (op)-[k:KNOWS]-(author)
RETURN c.id, c.creationDate, author.id, (k IS NOT NULL) AS isKnows
ORDER BY c.creationDate DESC
LIMIT 20"
    );

    for (label, cypher) in [("IS7, EXISTS (ours)", &ours), ("IS7, OPTIONAL MATCH (theirs)", &theirs)] {
        let q = match parse_query(cypher) {
            Ok(q) => q,
            Err(e) => {
                println!("{label:<32} does not parse: {e}");
                continue;
            }
        };
        let mut times = Vec::with_capacity(runs);
        let mut rows = 0usize;
        for _ in 0..runs {
            let t = Instant::now();
            match QueryExecutor::new(&graph).execute(&q) {
                Ok(o) => {
                    rows = o.records.len();
                    times.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                Err(e) => {
                    println!("{label:<32} failed: {e}");
                    times.clear();
                    break;
                }
            }
        }
        if times.is_empty() {
            continue;
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!("{label:<32} median {:>9.3} ms   rows {rows}", times[times.len() / 2]);
    }

    for (label, cypher) in [("ours", &ours), ("theirs", &theirs)] {
        println!("\n--- plan, {label} ---");
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
    Ok(())
}
