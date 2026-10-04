//! How `ORDER BY count(*) … LIMIT k` over a degree aggregate scales with the
//! **group count** (#304).
//!
//! `cargo run --release --example top_k_degree_scaling -- <articles> <degree>`
//!
//! Builds a synthetic citation graph of `articles` `:Article` nodes, each cited
//! by `degree` of the others, every node carrying a `title`, and every 1000th
//! node a hub with extra in-degree so the top ten are ten different numbers.
//! Uses `create_node_stub`/`create_edge_stub` + `finish_bulk_load`, so the
//! catalog's degree maps are rebuilt and exact.
//!
//! Then times the shapes that matter, third of three runs reported, so the
//! same binary answers all of them over identical data:
//!
//! | shape | what it isolates |
//! |---|---|
//! | `ORDER BY c DESC LIMIT 10` | the query #304 asks about |
//! | `ORDER BY c DESC` (no limit) | the full sort, which declines streaming |
//! | `LIMIT 10` (no sort) | the row source alone |
//! | `ORDER BY c DESC, t ASC LIMIT 10` | a declined ORDER BY, so the old path |
//! | grouped on `a.title` | the property group key, where the read is the grouping |
//!
//! Read it against the base branch's binary for a before/after: the same
//! example compiles on both.

// Measure what ships: the server's allocator, not the system default (ADR-038).
#[global_allocator]
static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;

use std::time::Instant;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn build(articles: usize, degree: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::with_capacity(articles);
    for i in 0..articles {
        let id = store.create_node_stub("Article");
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(format!("T{i}")),
        );
        ids.push(id);
    }
    for i in 0..articles {
        // article i is cited by the `degree` that follow it
        for d in 1..=degree {
            let citer = ids[(i + d) % articles];
            store.create_edge_stub(citer, ids[i], "CITES").unwrap();
        }
        // hubs, so the top ten are distinct numbers rather than a tie
        if i % 1000 == 0 {
            let extra = 1 + (i / 1000);
            for d in 0..extra {
                let citer = ids[(i + degree + 1 + d) % articles];
                store.create_edge_stub(citer, ids[i], "CITES").unwrap();
            }
        }
    }
    store.finish_bulk_load();
    store
}

fn time(store: &GraphStore, label: &str, cypher: &str) {
    let query = match parse_query(cypher) {
        Ok(q) => q,
        Err(e) => {
            println!("{label:<34} PARSE ERROR {e:?}");
            return;
        }
    };
    let mut last = f64::NAN;
    let mut rows = 0usize;
    let mut top = String::new();
    for _ in 0..3 {
        let t = Instant::now();
        match QueryExecutor::new(store).execute(&query) {
            Ok(batch) => {
                last = t.elapsed().as_secs_f64() * 1000.0;
                rows = batch.records.len();
                top = batch
                    .records
                    .iter()
                    .take(5)
                    .map(|r| format!("{:?}", r.get("c")))
                    .collect::<Vec<_>>()
                    .join(",");
            }
            Err(e) => {
                println!("{label:<34} ERROR {e:?}");
                return;
            }
        }
    }
    println!("{label:<34} {last:>10.1} ms  rows={rows:<9} head={top}");
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let b = QueryExecutor::new(store).execute(&q).unwrap();
    match b.records[0].get("plan") {
        Some(samyama::query::executor::Value::Property(PropertyValue::String(t))) => t.clone(),
        other => format!("{other:?}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let articles: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(100_000);
    let degree: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(20);

    let t = Instant::now();
    let store = build(articles, degree);
    println!(
        "built {} nodes / {} edges in {:.1} s",
        store.node_count(),
        store.edge_count(),
        t.elapsed().as_secs_f64()
    );

    let shapes: &[(&str, &str)] = &[
        (
            "node key, ORDER BY c DESC LIMIT 10",
            "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c DESC LIMIT 10",
        ),
        (
            "node key, ORDER BY c ASC LIMIT 10",
            "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c ASC LIMIT 10",
        ),
        (
            "node key, no LIMIT (declines)",
            "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c ORDER BY c DESC",
        ),
        (
            "node key, no sort (row source)",
            "MATCH (a:Article)<-[:CITES]-() RETURN a, count(*) AS c LIMIT 10",
        ),
        (
            "node key, two keys (declines)",
            "MATCH (a:Article)<-[:CITES]-() RETURN a AS t, count(*) AS c ORDER BY c DESC, t ASC LIMIT 10",
        ),
        (
            "title key, ORDER BY c DESC LIMIT 10",
            "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c DESC LIMIT 10",
        ),
        (
            "title key, two keys (declines)",
            "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c DESC, t ASC LIMIT 10",
        ),
    ];
    for (label, cypher) in shapes {
        time(&store, label, cypher);
    }
    println!("\n{}", plan(&store, shapes[0].1));
}
