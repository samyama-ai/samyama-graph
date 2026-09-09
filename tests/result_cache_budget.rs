//! The result cache is bounded in bytes, not entries (#1153).
//!
//! An entry count cannot bound it. Measured on LDBC SF1
//! (`benches/result_cache_gain.rs`), a 256-row entry costs 8.3 KB and a
//! 100,000-row entry 32.1 MB — a 3,867x spread — so the same 1024-entry setting
//! holds 8 MB of one workload or 32.8 GB of another. PERF-10 is a bytes/edge
//! budget at 521 B/edge against a 256 B target; a cache capped in entries cannot
//! be budgeted against it.

use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::query::QueryEngine;

/// A store whose rows are wide enough that a handful of them are worth real
/// bytes, so a budget in the tens of KB is reached by a few entries rather than
/// by thousands.
fn padded_store(nodes: usize, pad: usize) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..nodes {
        let mut props = PropertyMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("pad".to_string(), PropertyValue::String("x".repeat(pad)));
        store.create_node_with_properties("default", vec![Label::new("N")], props);
    }
    store
}

/// Distinct query texts that return distinct, non-trivial answers.
fn distinct_queries(n: usize) -> Vec<String> {
    (1..=n)
        .map(|i| format!("MATCH (n:N) RETURN n.id, n.pad ORDER BY n.id LIMIT {i}"))
        .collect()
}

#[test]
fn bytes_bound_the_cache_not_the_entry_count() {
    let store = padded_store(40, 512);
    // Entry cap far above what we insert, so only the byte budget can stop it.
    let budget = 64 * 1024;
    let engine = QueryEngine::with_capacity(4096).with_result_cache_budget(budget);

    for q in distinct_queries(60) {
        let _ = engine.execute_cached(&q, &store).expect(&q);
    }

    let held = engine.result_cache_bytes();
    assert!(
        held <= budget,
        "cache holds {held} bytes against a {budget}-byte budget"
    );
    assert!(
        engine.result_cache_len() < 60,
        "all 60 entries survived a {budget}-byte budget, so the budget did \
         nothing: {} entries, {held} bytes",
        engine.result_cache_len()
    );
    assert!(
        engine.result_cache_len() > 0,
        "the budget evicted everything; it should hold what fits"
    );
}

#[test]
fn an_answer_larger_than_the_whole_budget_is_not_cached() {
    let store = padded_store(200, 1024);
    // Small enough that one 200-row answer cannot fit.
    let engine = QueryEngine::with_capacity(64).with_result_cache_budget(4 * 1024);

    let big = "MATCH (n:N) RETURN n.id, n.pad ORDER BY n.id";
    let (rows, cached) = engine.execute_cached(big, &store).expect("big");
    assert!(!cached);
    assert_eq!(rows.records.len(), 200, "the query must actually return rows");

    let (_, cached_again) = engine.execute_cached(big, &store).expect("big again");
    assert!(
        !cached_again,
        "an answer bigger than the entire budget was admitted to the cache"
    );
    assert_eq!(engine.result_cache_bytes(), 0);
}

/// Admitting an oversized answer must not flush what is already warm.
///
/// Evicting a full cache to make room for something that cannot fit is worse
/// than refusing it: the memory spike happens and the hit rate drops too.
#[test]
fn an_oversized_answer_does_not_flush_the_warm_entries() {
    let store = padded_store(200, 1024);
    let engine = QueryEngine::with_capacity(64).with_result_cache_budget(32 * 1024);

    let small = "MATCH (n:N) RETURN n.id ORDER BY n.id LIMIT 3";
    let _ = engine.execute_cached(small, &store).unwrap();
    let (_, warm) = engine.execute_cached(small, &store).unwrap();
    assert!(warm, "the small answer did not stay cached");
    let entries_before = engine.result_cache_len();

    let big = "MATCH (n:N) RETURN n.id, n.pad ORDER BY n.id";
    let _ = engine.execute_cached(big, &store).unwrap();

    assert_eq!(
        engine.result_cache_len(), entries_before,
        "the oversized answer changed the cache contents"
    );
    let (_, still_warm) = engine.execute_cached(small, &store).unwrap();
    assert!(still_warm, "the oversized answer evicted a warm entry to hold nothing");
}

/// Re-inserting the same key charges the difference, not the whole entry.
///
/// Without this the tracked total climbs on every re-cache of the same query
/// until the budget evicts a cache that is nowhere near full.
#[test]
fn re_caching_the_same_query_does_not_drift_the_total() {
    let store = padded_store(20, 256);
    let engine = QueryEngine::with_capacity(64).with_result_cache_budget(8 * 1024 * 1024);
    let q = "MATCH (n:N) RETURN n.id, n.pad ORDER BY n.id LIMIT 5";

    let _ = engine.execute_cached(q, &store).unwrap();
    let after_first = engine.result_cache_bytes();
    assert!(after_first > 0, "nothing was charged for the first insert");

    // Clearing and re-inserting the same key many times must land on the same
    // total, not a multiple of it.
    for _ in 0..20 {
        engine.clear_result_cache();
        let _ = engine.execute_cached(q, &store).unwrap();
    }
    assert_eq!(
        engine.result_cache_bytes(), after_first,
        "the byte total drifted across repeated inserts of one key"
    );
}

#[test]
fn clearing_resets_the_accounting() {
    let store = padded_store(20, 256);
    let engine = QueryEngine::with_capacity(64).with_result_cache_budget(8 * 1024 * 1024);
    for q in distinct_queries(5) {
        let _ = engine.execute_cached(&q, &store).unwrap();
    }
    assert!(engine.result_cache_bytes() > 0);
    engine.clear_result_cache();
    assert_eq!(engine.result_cache_bytes(), 0, "clear left bytes charged");
    assert_eq!(engine.result_cache_len(), 0);
}

/// The estimate must track the shape of the answer, not just its row count.
///
/// A row-count proxy would call these two the same size. They differ by 1000x.
#[test]
fn the_estimate_follows_payload_not_row_count() {
    let narrow = padded_store(50, 4);
    let wide = padded_store(50, 4096);
    let q = "MATCH (n:N) RETURN n.id, n.pad ORDER BY n.id";

    let e_narrow = QueryEngine::new();
    let _ = e_narrow.execute_cached(q, &narrow).unwrap();
    let e_wide = QueryEngine::new();
    let _ = e_wide.execute_cached(q, &wide).unwrap();

    let (n, w) = (e_narrow.result_cache_bytes(), e_wide.result_cache_bytes());
    assert!(n > 0 && w > 0, "nothing charged: narrow {n}, wide {w}");
    assert!(
        w > n * 10,
        "same row count, 1000x the payload, but the estimate moved from {n} to \
         {w} — it is counting rows rather than bytes"
    );
}
