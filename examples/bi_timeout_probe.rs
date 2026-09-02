//! Why BI-11 and BI-17 do not finish at SF10.
//!
//! Both hit the bench's 120 s per-query limit on the first SF10 SNB-BI run
//! (#1065), and both are the two slowest at SF1 — 1,321 ms and 7,745 ms — so
//! the growth is superlinear rather than a limit set slightly too tight. SF10
//! cannot be profiled inside the timeout, so this profiles SF1 and reads the
//! shape.
//!
//! BI-17 is the interesting one on paper: the pattern
//! `(a)-[:KNOWS]-(b)-[:KNOWS]-(c)-[:KNOWS]-(a)` enumerates every triangle six
//! times and `a.id < b.id AND b.id < c.id` keeps one. `a.id < b.id` needs only
//! `a` and `b`, so it can be decided the moment `b` is bound — before `c` is
//! expanded at all. If it is applied after the whole pattern instead, the walk
//! is doing roughly six times the work it needs to.
use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod ldbc_common;
use ldbc_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

const BI11: &str = "\
MATCH (reply:Comment)-[:REPLY_OF]->(post:Post)
WHERE NOT EXISTS {
  MATCH (reply)-[:HAS_TAG]->(t:Tag)<-[:HAS_TAG]-(post)
}
RETURN count(reply) AS unrelatedReplies";

const BI17: &str = "\
MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)
WHERE a.id < b.id AND b.id < c.id
RETURN count(a) AS triangleCount";

fn show(store: &GraphStore, id: &str, q: &str) {
    println!("\n================ {id} ================");
    // The plan first: where the predicates sit is the question, and PROFILE at
    // SF1 can take minutes.
    if let Ok(p) = parse_query(&format!("EXPLAIN {q}")) {
        if let Ok(b) = QueryExecutor::new(store).execute(&p) {
            if let Some(v) = b.records.first().and_then(|r| r.values().next()) {
                for line in format!("{v:?}").replace("\\n", "\n").lines().take(16) {
                    println!("  {}", line.trim_matches(|c| c == '"'));
                }
            }
        }
    }
    // PROFILE, not just EXPLAIN: the plan text does not show whether a closing
    // hop is pinned to its bound target, so two very different executions print
    // the same tree. Only the per-operator rows say which one ran.
    if let Ok(p) = parse_query(&format!("PROFILE {q}")) {
        if let Ok(b) = QueryExecutor::new(store).execute(&p) {
            if let Some(v) = b.records.first().and_then(|r| r.values().next()) {
                let text = format!("{v:?}").replace("\\n", "\n");
                let mut show = false;
                for line in text.lines() {
                    if line.contains("Profile (per operator)") { show = true; }
                    if show { println!("  {}", line.trim_matches(|c| c == '"')); }
                    if line.contains("Hottest") { break; }
                }
            }
        }
    }
    let t = Instant::now();
    match parse_query(q).map_err(|e| format!("{e:?}"))
        .and_then(|p| QueryExecutor::new(store).execute(&p).map_err(|e| format!("{e:?}")))
    {
        Ok(b) => println!("\n  answer: {:?} in {}", b.records.first(), format_duration(t.elapsed())),
        Err(e) => println!("\n  ERROR after {}: {e}", format_duration(t.elapsed())),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(
        args.iter().position(|a| a == "--data-dir").and_then(|i| args.get(i + 1))
            .ok_or("--data-dir <extract> required")?,
    );
    let client = EmbeddedClient::new();
    let t = Instant::now();
    let loaded = {
        let mut g = client.store_write().await;
        ldbc_common::load_dataset(&mut g, &dir)?
    };
    eprintln!("Loaded {} nodes, {} edges in {}",
        format_num(loaded.total_nodes), format_num(loaded.total_edges),
        format_duration(t.elapsed()));
    for (label, prop) in &[("Person", "id"), ("Post", "id"), ("Comment", "id"), ("Tag", "id")] {
        let _ = client.query("default", &format!("CREATE INDEX ON :{label}({prop})")).await;
    }
    let store = client.store_read().await;
    show(&store, "BI-17 Friend Triangles", BI17);
    show(&store, "BI-11 Unrelated Replies", BI11);
    Ok(())
}
