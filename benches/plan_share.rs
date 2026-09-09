//! How much of a repeated query's wall clock is planning? (#1152)
//!
//! The planner declares a plan cache that nothing inserts into and nothing reads
//! (`plan_cache`, `cache_generation`, `PlanCacheEntry`). Before deciding whether to
//! wire it up or delete it, the question is whether planning is a material share of
//! end-to-end latency for a query asked more than once. This measures that.
//!
//! Planning is timed in isolation via `QueryPlanner::plan`, which is what a plan
//! cache would skip. End-to-end is timed via `QueryExecutor`, the same path the
//! HTTP surface takes. Both run on the same loaded store, in the same process,
//! after a warmup -- a cold first call would measure page faults, not planning.
//!
//! Usage: cargo bench --bench plan_share -- [--data-dir DIR] [--iters N]

use std::path::PathBuf;
use std::time::Instant;

use samyama::graph::GraphStore;
use samyama::query::{parse_query, QueryExecutor};
use samyama::query::executor::QueryPlanner;

#[path = "ldbc_common/mod.rs"]
mod ldbc_common;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = PathBuf::from(arg(&args, "--data-dir").unwrap_or_else(|| {
        format!("{}/sgwork/ldbc-data/social_network-sf1-CsvBasic-LongDateFormatter",
                std::env::var("HOME").unwrap_or_default())
    }));
    let iters: usize = arg(&args, "--iters").and_then(|v| v.parse().ok()).unwrap_or(200);

    if !data_dir.exists() {
        eprintln!("SKIP: dataset not present at {}", data_dir.display());
        return;
    }

    let mut store = GraphStore::new();
    let t = Instant::now();
    let loaded = ldbc_common::load_dataset(&mut store, &data_dir).expect("load");
    eprintln!("loaded {} nodes / {} edges in {:.1}s",
              loaded.total_nodes, loaded.total_edges, t.elapsed().as_secs_f64());

    // A real Person id from the loaded store. A literal that matches nothing
    // would make every query return zero rows and time the empty case (#probe
    // needs data).
    let person_id = {
        let label = samyama::Label::new("Person");
        let nodes = store.get_nodes_by_label(&label);
        let node = nodes.first().expect("no Person nodes loaded");
        node.properties.get("id").map(|v| format!("{v}"))
            .expect("Person has no id property")
    };
    eprintln!("using personId {person_id}");

    // An index on the lookup key, so the selective case is measured against the
    // execution time it *should* have. Without it a slow scan inflates the
    // denominator and makes planning look immaterial for the wrong reason.
    {
        let mut ex = samyama::query::MutQueryExecutor::new(&mut store, "default".to_string());
        if let Ok(q) = parse_query("CREATE INDEX ON :Person(id)") {
            let _ = ex.execute(&q);
        }
    }

    let queries: Vec<(&str, String)> = vec![
        // The floor: a query with no store access at all. Planning share here is
        // the upper bound on what any plan cache could ever buy.
        ("RETURN-1-floor", "RETURN 1".to_string()),
        ("IS1-selective", format!(
            "MATCH (p:Person {{id: {person_id}}}) \
             RETURN p.firstName, p.lastName, p.birthday, p.locationIP, p.browserUsed")),
        ("IC1-complex", format!(
            "MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..3]-(f:Person) \
             WHERE f.id <> {person_id} \
             RETURN DISTINCT f.id, f.lastName ORDER BY f.lastName LIMIT 20")),
    ];

    println!("\n{:<16} {:>12} {:>12} {:>10}  {}", "query", "plan p50 us", "e2e p50 us", "share", "verdict");
    println!("{}", "-".repeat(70));

    for (id, cypher) in &queries {
        let query = match parse_query(cypher) {
            Ok(q) => q,
            Err(e) => { println!("{id:<16} PARSE FAILED: {e}"); continue; }
        };
        let planner = QueryPlanner::new();

        // Warm up both paths before timing either.
        for _ in 0..10 {
            let _ = planner.plan(&query, &store);
            let _ = QueryExecutor::new(&store).execute(&query);
        }

        let mut plan_us = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let _ = planner.plan(&query, &store);
            plan_us.push(t.elapsed().as_secs_f64() * 1e6);
        }
        let mut e2e_us = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let _ = QueryExecutor::new(&store).execute(&query);
            e2e_us.push(t.elapsed().as_secs_f64() * 1e6);
        }

        let p50 = |v: &mut Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            v[v.len() / 2]
        };
        let p = p50(&mut plan_us);
        let e = p50(&mut e2e_us);
        let share = if e > 0.0 { p / e * 100.0 } else { f64::NAN };
        // The threshold the issue set: under ~1% and the cache is not worth wiring.
        let verdict = if share < 1.0 { "immaterial (<1%)" } else { "material" };
        println!("{id:<16} {p:>12.1} {e:>12.1} {share:>9.2}%  {verdict}");
    }
    println!("\nplan p50 is the cost a plan cache would remove; e2e p50 is what it \
              would remove it from.");
}
