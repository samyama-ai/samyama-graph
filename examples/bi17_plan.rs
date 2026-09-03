//! Print the plan BI-17 gets, so the pushdown question has an answer rather
//! than an assumption (#1078).
//!
//! The row arithmetic in #1078 assumes `a.id < b.id` is applied *after* the
//! full triangle is built. That is worth ~6x if true and worth nothing if the
//! planner already applies it at the first expand -- and the last several
//! things assumed about this query were wrong, so this reads the plan.

#[path = "../benches/ldbc_bi_common/mod.rs"]
mod ldbc_bi_common;

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::path::PathBuf;

const QUERIES: &[(&str, &str)] = &[
    (
        "bi17-triangle",
        "EXPLAIN MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)
         WHERE a.id < b.id AND b.id < c.id
         RETURN count(a) AS triangles",
    ),
    (
        "two-hop-one-pred",
        "EXPLAIN MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)
         WHERE a.id < b.id
         RETURN count(a) AS n",
    ),
    (
        "one-hop",
        "EXPLAIN MATCH (a:Person)-[:KNOWS]-(b:Person)
         WHERE a.id < b.id
         RETURN count(a) AS n",
    ),
];

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
    ldbc_bi_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded");

    for (name, q) in QUERIES {
        println!("=== {name} ===");
        let parsed = parse_query(q)?;
        let out = QueryExecutor::new(&graph).execute(&parsed)?;
        for rec in &out.records {
            for v in rec.values() {
                println!("{v:?}");
            }
        }
        println!();
    }
    Ok(())
}
