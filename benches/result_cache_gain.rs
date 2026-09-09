//! What does the result cache actually buy, and what does it cost? (#1153)
//!
//! The issue requires this before any claim ships: hit rate, p50/p95 latency with
//! and without, and the memory the cache spends — PERF-10 is already at 521 B/edge
//! against a 256 B H1 target, so a cache is more resident memory and must be
//! budgeted rather than spent silently.
//!
//! What this measures that a naive "cache is faster" bench would not:
//!
//! - **Whether the clone eats the gain.** A hit clones a `RecordBatch`, so the
//!   expectation going in was that the speedup would decay as results widen. The
//!   corpus spans 1 to 500,000 rows to find where that happens. (It does not, up
//!   to 500k — cloning a materialized batch stays far cheaper than re-running the
//!   scan. The measurement is here so the next person does not have to guess.)
//! - **An honest denominator.** `plan_share` (#1152) first reported planning at
//!   0.47% because the fixture had no index on `Person.id`, inflating end-to-end
//!   260x. The index is built here for the same reason.
//! - **What an entry costs, measured not modelled.** Resident memory is read from
//!   /proc/self/statm either side of filling the cache, for narrow and for wide
//!   results, because the cap is a count of *entries* and the whole question is
//!   what an entry weighs.
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
        // Wide results, where a hit stops being free because the answer is cloned.
        // Comment, not Person: SF1 has only 9,892 Person nodes, so LIMIT 10000 and
        // LIMIT 100000 over Person both return 9,892 rows and the two lines
        // measure one case twice. Comment has ~2M.
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

        // Row count, and a check the query returns something. A query matching
        // nothing would time the empty case and report a flattering speedup.
        let rows = match engine.execute(cypher, &store) {
            Ok(b) => b.records.len(),
            Err(e) => { println!("{id:<22} FAILED: {e}"); continue; }
        };

        // Uncached: `execute` never consults or fills the result cache, so this
        // is the engine's own latency and not a cache-miss path.
        for _ in 0..20 { let _ = engine.execute(cypher, &store); }
        let mut cold = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let _ = engine.execute(cypher, &store);
            cold.push(t.elapsed().as_secs_f64() * 1e6);
        }

        // Cached: prime once, then every call must be a hit.
        let (_, was_cached) = engine.execute_cached(cypher, &store).expect("prime");
        assert!(!was_cached, "{id}: first call reported a hit on an empty cache");
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
    println!("\nmemory: what an entry weighs");
    println!("{}", "-".repeat(92));

    // Narrow entries. Distinct queries, because caching one text repeatedly
    // would measure a single entry and call it a cap.
    let engine = QueryEngine::new();
    let cap_probe = 512usize;
    let before = rss_bytes();
    for i in 0..cap_probe {
        let q = format!("MATCH (p:Person) RETURN p.id, p.firstName LIMIT {}", i + 1);
        let _ = engine.execute_cached(&q, &store);
    }
    let after = rss_bytes();
    let entries = engine.result_cache_len();
    let delta = after.saturating_sub(before);
    let small_per_entry = if entries > 0 { delta as f64 / entries as f64 } else { 0.0 };
    println!("narrow entries held : {entries} (mean {} rows each)", cap_probe / 2);
    println!("RSS delta           : {:.1} MB", delta as f64 / 1e6);
    println!("per entry (mean)    : {:.1} KB", small_per_entry / 1e3);

    // The same probe over wide results.
    let engine_wide = QueryEngine::new();
    let wide_n = 16usize;
    let before_w = rss_bytes();
    for i in 0..wide_n {
        let q = format!("MATCH (c:Comment) RETURN c.id, c.browserUsed LIMIT {}", 100_000 + i);
        let _ = engine_wide.execute_cached(&q, &store);
    }
    let after_w = rss_bytes();
    let wide_entries = engine_wide.result_cache_len();
    let wide_delta = after_w.saturating_sub(before_w);
    let wide_per_entry = if wide_entries > 0 { wide_delta as f64 / wide_entries as f64 } else { 0.0 };
    println!("\nwide entries held   : {wide_entries} (100k rows each)");
    println!("RSS delta           : {:.1} MB", wide_delta as f64 / 1e6);
    println!("per entry (mean)    : {:.1} KB", wide_per_entry / 1e3);

    if small_per_entry > 0.0 && wide_per_entry > 0.0 {
        let ratio = wide_per_entry / small_per_entry;
        println!("\nentry size spread   : {ratio:.0}x between the two workloads");
        println!("At the 1024-entry default the cache holds {:.0} MB of the narrow \
                  results, or {:.1} GB of the wide ones.",
                 small_per_entry * 1024.0 / 1e6, wide_per_entry * 1024.0 / 1e9);
        println!("An entry count is therefore not a memory bound. PERF-10 is a \
                  bytes/edge budget; a cache capped in entries cannot be budgeted \
                  against it, and the cap should be bytes.");
    }

    if !any_slower.is_empty() {
        println!("\nSLOWER WHEN CACHED (the clone cost exceeded the query): {any_slower:?}");
    }
}
