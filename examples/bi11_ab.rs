//! A/B for the pinned-endpoint lookup in `EXISTS` (#1071 follow-up).
//!
//! LDBC BI-11 asks, for every reply-to-post pair, whether the two share a tag:
//!
//!   MATCH (reply:Comment)-[:REPLY_OF]->(post:Post)
//!   WHERE NOT (reply)-[:HAS_TAG]->(:Tag)<-[:HAS_TAG]-(post)
//!
//! The inner pattern reaches the anonymous Tag with `post` already bound, so
//! the closing hop is an existence test between two known nodes. Walking the
//! Tag to run it costs that tag's whole popularity.
//!
//! Both arms run in one process against one loaded graph, so the only
//! difference between them is the flag. Each arm prints its answer as well as
//! its time: a faster arm that changed the count is not a faster arm.
//!
//!   SAMYAMA_EXISTS_PIN_LOOKUP=0 cargo run --release --example bi11_ab -- --data-dir <dir>
//!   SAMYAMA_EXISTS_PIN_LOOKUP=1 cargo run --release --example bi11_ab -- --data-dir <dir>

#[path = "../benches/ldbc_bi_common/mod.rs"]
mod ldbc_bi_common;

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::path::PathBuf;

/// Bounded so both arms terminate: unbounded BI-11 does not finish on the walk,
/// and "one arm never returned" is a worse measurement than a ratio over a
/// slice. The bound is on the outer scan only -- the inner anti-join, which is
/// what changed, runs in full for every row the slice keeps.
fn bounded(limit: u64) -> String {
    format!(
        "MATCH (reply:Comment)-[:REPLY_OF]->(post:Post)
         WHERE reply.id < {limit}
           AND NOT (reply)-[:HAS_TAG]->(:Tag)<-[:HAS_TAG]-(post)
         RETURN count(reply) AS unrelatedReplies"
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .expect("--data-dir <path> is required");

    let arm = std::env::var("SAMYAMA_EXISTS_PIN_LOOKUP").unwrap_or_else(|_| "1".into());

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    let t = std::time::Instant::now();
    ldbc_bi_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded in {:.1}s", t.elapsed().as_secs_f64());

    // Quantiles of the actual reply-to-post comment ids at SF1, not round
    // numbers: LDBC ids run from 557 to 2.199e12, so a plausible-looking
    // `reply.id < 1_000_000` keeps a handful of rows and measures nothing.
    // These are the 1st, 5th and 20th percentiles of the 1,011,420 REPLY_OF
    // edges into Posts, so each arm is doing a known amount of work.
    for limit in [137_440_184_271u64, 412_317_278_587, 824_634_888_495] {
        let cypher = bounded(limit);
        let q = parse_query(&cypher)?;
        let t = std::time::Instant::now();
        match QueryExecutor::new(&graph).execute(&q) {
            Ok(out) => {
                let v = out
                    .records
                    .first()
                    .and_then(|r| r.get("unrelatedReplies"))
                    .map(|v| format!("{v:?}"))
                    .unwrap_or_else(|| "<none>".into());
                println!(
                    "@@ pin={arm} reply.id<{limit:<9} {:>9.3}s  unrelatedReplies={v}",
                    t.elapsed().as_secs_f64()
                );
            }
            Err(e) => println!("@@ pin={arm} reply.id<{limit:<9} failed: {e}"),
        }
    }
    Ok(())
}
