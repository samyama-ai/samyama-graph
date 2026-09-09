//! What does the result cache actually buy, and what does it cost? (#1153)
//!
//! The issue requires this before any claim ships: hit rate, p50/p95 latency with
//! and without, and the memory the cache spends — PERF-10 is already at 521 B/edge
//! against a 256 B H1 target, so a cache is more resident memory and must be
//! budgeted rather than spent silently.
//!
//! Three things this measures that a naive "cache is faster" bench would not:
//!
//! - **The hit path is not free.** A hit clones a `RecordBatch`. For a query
//!   returning one row that is nothing; for one returning 100k rows the clone can
//!   cost more than the query. The corpus spans both on purpose, because a bench
//!   that only asked selective queries would report a speedup that disappears in
//!   production on the first dashboard aggregation.
//! - **The denominator must be honest.** `plan_share` (#1152) first reported
//!   planning at 0.47% because the fixture had no index on `Person.id`, inflating
//!   end-to-end 260x. The index is built here for the same reason.
//! - **Cost is measured, not modelled.** Resident memory is read from
//!   /proc/self/statm before and after filling the cache, so the number is what
//!   the process actually holds, not `size_of` arithmetic over the entries.
//!
//! Usage: cargo bench --bench result_cache_gain -- [--data-dir DIR] [--iters N]

use std::path::PathBuf;
use std::time::Instant;

use samyama::graph::GraphStore;
use samyama::query::{parse_query, QueryEngine};

#[path = "ldbc_common/mod.rs"]
mod ldbc_common;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

/// Resident set size in bytes, read from the kernel rather than estimated.
fn rss_bytes() -> u64 {
    let s = match std::fs::read_to_string("/proc/self/statm") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    // field 2 is resident pages
    let pages: u64 = s.split_whitespace().nth(1).and_then(|v| v.parse().ok()).unwrap_or(0);
    pages * 4096
}

fn pct(v: &mut Vec<f64>, p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[idx]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = PathBuf::from(arg(&args, "--data-dir").unwrap_or_else(|| {
        format!("{}/sgwork/ldbc-data/social_network-sf1-CsvBasic-LongDateFormatter",
                std::env::var("HOME").unwrap_or_default())
    }));
    let iters: usize = arg(&args, "--iters").and_then(|v| v.parse().ok()).unwrap_or(500);

    if !data_dir.exists() {
        eprintln!("SKIP: dataset not present at {}", data_dir.display());
        return;
    }

    let mut store = GraphStore::new();
    let t = Instant::now();
    let loaded = ldbc_common::load_dataset(&mut store, &data_dir).expect("load");
    eprintln!("loaded {} nodes / {} edges in {:.1}s",
              loaded.total_nodes, loaded.total_edges, t.elapsed().as_secs_f64());

    let person_id = {
        let label = samyama::Label::new("Person");
        let nodes = store.get_nodes_by_label(&label);
        let node = nodes.first().expect("no Person nodes loaded");
        node.properties.get("id").map(|v| format!("{v}"))
            .expect("Person has no id property")
    };

    // Same reason as plan_share: an unindexed lookup inflates the uncached side
    // and reports a speedup that is really a missing index.
    {
        let mut ex = samyama::query::MutQueryExecutor::new(&mut store, "default".to_string());
        if let Ok(q) = parse_query("CREATE INDEX ON :Person(id)") {
            let _ = ex.execute(&q);
        }
    }

    let queries: Vec<(&str, String)> = vec![
        ("RETURN-1-floor", "RETURN 1".to_string()),
        ("IS1-selective-1row", format!(
            "MATCH (p:Person {{id: {person_id}}}) \
             RETURN p.firstName, p.lastName, p.birthday, p.locationIP, p.browserUsed")),
        ("agg-small-result", "MATCH (p:Person) RETURN count(p) AS n".to_string()),
        // Wide results are where a hit stops being free: the answer is cloned.
        // Comment, not Person: SF1 has only 9,892 Person nodes, so LIMIT 10000
        // and LIMIT 100000 over Person return the same 9,892 rows and the two
        // lines measure one case twice. Comment has ~2M.
        ("scan-10k-rows", "MATCH (c:Comment) RETURN c.id, c.browserUsed LIMIT 10000".to_string()),
        ("scan-100k-rows", "MATCH (c:Comment) RETURN c.id, c.browserUsed LIMIT 100000".to_string()),
        ("scan-500k-rows", "MATCH (c:Comment) RETURN c.id, c.browserUsed LIMIT 500000".to_string()),
    ];

    println!("\n{:<22} {:>7} {:>11} {:>11} {:>11} {:>11} {:>9}",
             "query", "rows", "cold p50", "hit p50", "cold p95", "hit p95", "speedup");
    println!("{}", "-".repeat(92));

    let mut any_slower = Vec::new();

    for (id, cypher) in &queries {
        let engine = QueryEngine::new();

        // Row count, and a check that the query returns something. A query
        // matching nothing would time the empty case and report a huge speedup.
        let rows = match engine.execute(cypher, &store) {
            Ok(b) => b.records.len(),
            Err(e) => { println!("{id:<22} FAILED: {e}"); continue; }
        };

        // Uncached: `execute` never consults or fills the result cache, so this
        // is the engine's own latency, not a cache miss path.
        for _ in 0..20 { let _ = engine.execute(cypher, &store); }
        let mut cold = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let _ = engine.execute(cypher, &store);
            cold.push(t.elapsed().as_secs_f64() * 1e6);
        }

        // Cached: prime once, then every call must be a hit.
        let (_, was_cached) = engine.execute_cached(cypher, &store).expect("prime");
        assert!(!was_cached, "{id}: first call reported a hit");
        let hits_before = engine.result_cache_stats().hits();
        let mut warm = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let (_, hit) = engine.execute_cached(cypher, &store).expect("cached");
            warm.push(t.elapsed().as_secs_f64() * 1e6);
            assert!(hit, "{id}: a warm call missed; the epoch moved mid-run");
        }
        let hit_rate = (engine.result_cache_stats().hits() - hits_before) as f64 / iters as f64;
        assert!((hit_rate - 1.0).abs() < 1e-9, "{id}: hit rate {hit_rate}, expected 1.0");

        let c50 = pct(&mut cold, 0.50);
        let w50 = pct(&mut warm, 0.50);
        let c95 = pct(&mut cold, 0.95);
        let w95 = pct(&mut warm, 0.95);
        let speedup = if w50 > 0.0 { c50 / w50 } else { f64::NAN };
        if speedup < 1.0 { any_slower.push((*id, speedup)); }

        println!("{id:<22} {rows:>7} {c50:>11.1} {w50:>11.1} {c95:>11.1} {w95:>11.1} {speedup:>8.1}x");
    }

    // --- what it costs -----------------------------------------------------
    //
    // Fill one engine's cache with distinct entries and read resident memory
    // either side. Distinct queries, because caching the same text once would
    // measure one entry and call it a cap.
    println!("\nmemory at a stated cap");
    println!("{}", "-".repeat(92));
    let engine = QueryEngine::new();
    let cap_probe = 512usize;
    let before = rss_bytes();
    for i in 0..cap_probe {
        // LIMIT varies the text *and* the answer, so entries are genuinely distinct.
        let q = format!("MATCH (p:Person) RETURN p.id, p.firstName LIMIT {}", i + 1);
        let _ = engine.execute_cached(&q, &store);
    }
    let after = rss_bytes();
    let entries = engine.result_cache_len();
    let delta = after.saturating_sub(before);
    println!("entries held        : {entries}");
    println!("RSS delta           : {:.1} MB", delta as f64 / 1e6);
    if entries > 0 {
        println!("per entry (mean)    : {:.1} KB", delta as f64 / entries as f64 / 1e3);
    }
    println!("note: these entries average {} rows; a cache of selective queries costs \
              far less and one of dashboard aggregations far more. The cap is entries, \
              not bytes -- so the bound is workload-dependent and this is the number \
              to quote when budgeting against PERF-10.", cap_probe / 2);

    if !any_slower.is_empty() {
        println!("\nSLOWER WHEN CACHED (the clone cost exceeded the query): {any_slower:?}");
    }
}
