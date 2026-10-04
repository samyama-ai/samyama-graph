//! The Q17 shape from issue #1812: group by a **property of the far endpoint**,
//! `ORDER BY count DESC LIMIT 10`, one edge type, one segment.
//!
//! ```text
//! MATCH (a:Article)-[:ANNOTATED_WITH]->(m:MeSHTerm)
//! RETURN m.name, count(a) AS n ORDER BY n DESC LIMIT 10
//! ```
//!
//! Three PubMed queries of this shape got 2.6x-6.2x slower between v1.7.0 and
//! v1.10.0 on byte-identical data. This probe reproduces the shape on a
//! synthetic graph so the two versions can be timed against each other: it
//! uses only `create_node`/`create_edge`/`set_node_property`, which carry the
//! same signatures on v1.7.0 and on main, so the same file compiles on both.
//!
//! ```bash
//! cargo run --release --example topn_property_group_regression -- --articles 400000 --terms 200000 --per-article 5
//! ```
//!
//! Reports the third of three runs per shape, plus `EXPLAIN` for each, so a
//! plan change between versions is visible next to the time change.

use std::time::Instant;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
}

/// `articles` :Article nodes, `terms` :MeSHTerm nodes each with a distinct
/// `name`, and `per_article` ANNOTATED_WITH edges per article. Term choice is
/// skewed so the top ten counts are ten different numbers rather than a tie:
/// every 64th edge lands on one of the first 4096 terms.
fn build(articles: usize, terms: usize, per_article: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let mut term_ids = Vec::with_capacity(terms);
    for i in 0..terms {
        let id = store.create_node("MeSHTerm");
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(format!("MeSH descriptor {i}")),
        );
        term_ids.push(id);
    }
    let mut e: usize = 0;
    for a in 0..articles {
        let aid = store.create_node("Article");
        let _ = store.set_node_property(
            "default",
            aid,
            "pmid".to_string(),
            PropertyValue::Integer(a as i64),
        );
        for k in 0..per_article {
            let t = if e % 64 == 0 {
                (e / 64) % 4096.min(terms)
            } else {
                (a * per_article + k * 7919) % terms
            };
            store.create_edge(aid, term_ids[t % terms], "ANNOTATED_WITH").unwrap();
            e += 1;
        }
    }
    store
}

fn run(store: &GraphStore, label: &str, cypher: &str, reps: usize) {
    let query = match parse_query(cypher) {
        Ok(q) => q,
        Err(err) => {
            println!("{label:<40} PARSE ERROR {err:?}");
            return;
        }
    };
    let mut last = f64::NAN;
    let mut rows = 0usize;
    let mut head = String::new();
    for _ in 0..reps {
        let t = Instant::now();
        match QueryExecutor::new(store).execute(&query) {
            Ok(batch) => {
                last = t.elapsed().as_secs_f64() * 1000.0;
                rows = batch.records.len();
                head = batch
                    .records
                    .iter()
                    .take(3)
                    .map(|r| format!("{:?}", r.get("n")))
                    .collect::<Vec<_>>()
                    .join(",");
            }
            Err(err) => {
                println!("{label:<40} ERROR {err:?}");
                return;
            }
        }
    }
    println!("{label:<40} {last:>10.1} ms  rows={rows:<6} head={head}");
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let q = match parse_query(&format!("EXPLAIN {cypher}")) {
        Ok(q) => q,
        Err(err) => return format!("PARSE ERROR {err:?}"),
    };
    match QueryExecutor::new(store).execute(&q) {
        Ok(b) => b
            .records
            .iter()
            .map(|r| format!("{:?}", r))
            .collect::<Vec<_>>()
            .join("\n"),
        Err(err) => format!("ERROR {err:?}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let articles: usize =
        arg(&args, "--articles").and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let terms: usize = arg(&args, "--terms").and_then(|v| v.parse().ok()).unwrap_or(100_000);
    let per_article: usize =
        arg(&args, "--per-article").and_then(|v| v.parse().ok()).unwrap_or(5);
    let reps: usize = arg(&args, "--reps").and_then(|v| v.parse().ok()).unwrap_or(3);

    let t = Instant::now();
    let store = build(articles, terms, per_article);
    println!(
        "built {} nodes / {} edges in {:.1} s",
        store.node_count(),
        store.edge_count(),
        t.elapsed().as_secs_f64()
    );

    let shapes: &[(&str, &str)] = &[
        (
            "Q17: m.name, count(a) DESC LIMIT 10",
            "MATCH (a:Article)-[:ANNOTATED_WITH]->(m:MeSHTerm) RETURN m.name, count(a) AS n ORDER BY n DESC LIMIT 10",
        ),
        (
            "Q17 with count(*)",
            "MATCH (a:Article)-[:ANNOTATED_WITH]->(m:MeSHTerm) RETURN m.name, count(*) AS n ORDER BY n DESC LIMIT 10",
        ),
        (
            "node key instead of property",
            "MATCH (a:Article)-[:ANNOTATED_WITH]->(m:MeSHTerm) RETURN m, count(a) AS n ORDER BY n DESC LIMIT 10",
        ),
        (
            "Q17 without ORDER BY / LIMIT",
            "MATCH (a:Article)-[:ANNOTATED_WITH]->(m:MeSHTerm) RETURN m.name, count(a) AS n",
        ),
    ];
    for (label, cypher) in shapes {
        run(&store, label, cypher, reps);
    }
    for (label, cypher) in shapes {
        println!("\n=== EXPLAIN {label}\n{}", plan(&store, cypher));
    }
}
