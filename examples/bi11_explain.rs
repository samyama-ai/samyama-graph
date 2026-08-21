//! EXPLAIN / PROFILE for LDBC BI-11 against a real SF1 extract (#681).
//!
//! BI-11 went from 5.1 s to not finishing at commit 4cbf36e, which changed
//! anchor selection. A plan is the shortest route to seeing what that did, but
//! the plan is cost-based, so it has to be taken against the real graph rather
//! than an empty store.
//!
//!   cargo run --release --example bi11_explain -- --data-dir <ldbc-sf1-dir>

#[path = "../benches/ldbc_bi_common/mod.rs"]
mod ldbc_bi_common;

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::path::PathBuf;

const BI11: &str = "\
MATCH (reply:Comment)-[:REPLY_OF]->(post:Post)
WHERE NOT EXISTS {
  MATCH (reply)-[:HAS_TAG]->(t:Tag)<-[:HAS_TAG]-(post)
}
RETURN count(reply) AS unrelatedReplies";

// Sub-shapes, to separate "the anti-join is slow" from "the outer match is slow".
const OUTER_ONLY: &str = "MATCH (reply:Comment)-[:REPLY_OF]->(post:Post) RETURN count(reply) AS c";
const INNER_ONLY: &str = "MATCH (reply:Comment)-[:HAS_TAG]->(t:Tag) RETURN count(reply) AS c";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .expect("--data-dir <path> is required");

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    let t = std::time::Instant::now();
    ldbc_bi_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded in {:.1}s", t.elapsed().as_secs_f64());

    for (label, cypher) in [("BI-11", BI11), ("outer only", OUTER_ONLY), ("inner only", INNER_ONLY)] {
        println!("\n================ {label} ================");
        println!("{cypher}\n");
        let q = parse_query(&format!("EXPLAIN {cypher}"))?;
        match QueryExecutor::new(&graph).execute(&q) {
            Ok(out) => {
                for r in &out.records {
                    if let Some(v) = r.get("plan") {
                        println!("{}", format!("{v:?}").replace("\\n", "\n"));
                    }
                }
            }
            Err(e) => println!("EXPLAIN failed: {e}"),
        }
    }
    Ok(())
}
