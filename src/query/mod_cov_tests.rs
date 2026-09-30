//! Additional unit tests for `QueryEngine`: streaming, the result cache's
//! parameter keying and byte budget, the slow-query log and error spans.

use super::*;
use crate::graph::{GraphStore, PropertyValue};

fn people(n: i64) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..n {
        let id = store.create_node("Person");
        store.set_column_property(id, "i", PropertyValue::Integer(i));
    }
    store
}

#[test]
fn canonical_params_are_order_independent() {
    assert_eq!(canonical_params(&BoundParams::new()), "");
    let mut a = BoundParams::new();
    a.insert("b".into(), PropertyValue::Integer(2));
    a.insert("a".into(), PropertyValue::Integer(1));
    let rendered = canonical_params(&a);
    assert!(rendered.starts_with("a="), "{rendered}");
    assert!(rendered.contains('\u{1f}'), "{rendered}");
    let mut b = BoundParams::new();
    b.insert("a".into(), PropertyValue::Integer(1));
    b.insert("b".into(), PropertyValue::Integer(2));
    assert_eq!(canonical_params(&b), rendered);
}

#[test]
fn cached_results_are_keyed_by_their_parameters() {
    let store = people(5);
    let engine = QueryEngine::new();
    let q = "MATCH (n:Person) WHERE n.i < $max RETURN count(n) AS c";
    let mut two = BoundParams::new();
    two.insert("max".into(), PropertyValue::Integer(2));
    let mut four = BoundParams::new();
    four.insert("max".into(), PropertyValue::Integer(4));

    let (r1, hit1) = engine.execute_cached_with_params(q, &store, &two).unwrap();
    let (r2, hit2) = engine.execute_cached_with_params(q, &store, &four).unwrap();
    let (r3, hit3) = engine.execute_cached_with_params(q, &store, &two).unwrap();
    assert!(
        !hit1 && !hit2,
        "different parameters are different questions"
    );
    assert!(hit3);
    assert_eq!(
        r1.records[0].get("c"),
        Some(&executor::Value::Property(PropertyValue::Integer(2)))
    );
    assert_eq!(
        r2.records[0].get("c"),
        Some(&executor::Value::Property(PropertyValue::Integer(4)))
    );
    assert_eq!(r3.records[0].get("c"), r1.records[0].get("c"));
    assert_eq!(engine.result_cache_stats().hits(), 1);
    assert_eq!(engine.result_cache_stats().misses(), 2);
    assert_eq!(engine.result_cache_len(), 2);
}

#[test]
fn the_result_cache_accounts_bytes_and_clears() {
    let store = people(3);
    let engine = QueryEngine::new();
    assert_eq!(engine.result_cache_bytes(), 0);
    engine
        .execute_cached("MATCH (n:Person) RETURN n.i", &store)
        .unwrap();
    let held = engine.result_cache_bytes();
    assert!(held > 0);
    assert!(held <= engine.result_cache_budget());
    engine.clear_result_cache();
    assert_eq!(engine.result_cache_bytes(), 0);
    assert_eq!(engine.result_cache_len(), 0);
}

#[test]
fn an_answer_larger_than_the_whole_budget_is_not_cached() {
    let store = people(3);
    let engine = QueryEngine::new().with_result_cache_budget(1);
    assert_eq!(engine.result_cache_budget(), 1);
    let (_, hit) = engine
        .execute_cached("MATCH (n:Person) RETURN n.i", &store)
        .unwrap();
    assert!(!hit);
    assert_eq!(engine.result_cache_len(), 0);
    assert_eq!(engine.result_cache_bytes(), 0);
    let (_, hit) = engine
        .execute_cached("MATCH (n:Person) RETURN n.i", &store)
        .unwrap();
    assert!(!hit, "nothing was remembered");
}

#[test]
fn the_byte_budget_evicts_least_recently_used_entries() {
    let store = people(3);
    let probe = QueryEngine::new();
    probe
        .execute_cached("MATCH (n:Person) RETURN n.i AS a", &store)
        .unwrap();
    let one = probe.result_cache_bytes();

    // Room for one answer of this size, not two.
    let engine = QueryEngine::new().with_result_cache_budget(one + one / 2);
    engine
        .execute_cached("MATCH (n:Person) RETURN n.i AS a", &store)
        .unwrap();
    engine
        .execute_cached("MATCH (n:Person) RETURN n.i AS b", &store)
        .unwrap();
    assert_eq!(engine.result_cache_len(), 1);
    assert!(engine.result_cache_bytes() <= engine.result_cache_budget());
    // The older answer was evicted, the newer one is still served.
    let (_, hit_b) = engine
        .execute_cached("MATCH (n:Person) RETURN n.i AS b", &store)
        .unwrap();
    assert!(hit_b);
    let (_, hit_a) = engine
        .execute_cached("MATCH (n:Person) RETURN n.i AS a", &store)
        .unwrap();
    assert!(!hit_a);
}

#[test]
fn reinserting_a_key_charges_only_the_difference() {
    let engine = QueryEngine::new();
    let key = || ResultKey {
        query: "q".into(),
        params: String::new(),
        epoch: 1,
    };
    let mut batch = RecordBatch::new(vec!["x".into()]);
    let mut r = executor::Record::new();
    r.bind(
        "x",
        executor::Value::Property(PropertyValue::String("abc".into())),
    );
    batch.push(r);
    engine.insert_with_budget(key(), batch.clone());
    let once = engine.result_cache_bytes();
    assert_eq!(once, batch.clone().approx_heap_bytes());
    engine.insert_with_budget(key(), batch.clone());
    assert_eq!(engine.result_cache_bytes(), once);
    assert_eq!(engine.result_cache_len(), 1);
}

#[test]
fn streaming_hands_rows_to_the_sink_in_chunks() {
    let store = people(5);
    let engine = QueryEngine::new().with_plan_hash(true);
    let mut chunks: Vec<usize> = Vec::new();
    let mut seen_columns = Vec::new();
    let result = engine
        .execute_streaming_with_params(
            "MATCH (n:Person) RETURN n.i AS i",
            &store,
            &BoundParams::new(),
            2,
            &mut |cols, recs| {
                seen_columns = cols.to_vec();
                chunks.push(recs.len());
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(result.rows, 5);
    assert_eq!(result.columns, vec!["i".to_string()]);
    assert_eq!(seen_columns, vec!["i".to_string()]);
    assert_eq!(chunks.iter().sum::<usize>(), 5);
    assert!(chunks.iter().all(|&c| c <= 2), "{chunks:?}");
    assert!(result.plan_hash.is_some());
}

#[test]
fn a_failing_sink_stops_the_stream_with_its_message() {
    let store = people(5);
    let engine = QueryEngine::new();
    let err = engine
        .execute_streaming_with_params(
            "MATCH (n:Person) RETURN n.i AS i",
            &store,
            &BoundParams::new(),
            1,
            &mut |_, _| Err("client went away".to_string()),
        )
        .unwrap_err();
    assert!(err.to_string().contains("client went away"), "{err}");
}

#[test]
fn streaming_a_query_that_does_not_parse_is_an_error() {
    let store = people(1);
    let engine = QueryEngine::new();
    assert!(engine
        .execute_streaming_with_params(
            "MATCH (n RETURN n",
            &store,
            &BoundParams::new(),
            10,
            &mut |_, _| Ok(()),
        )
        .is_err());
}

#[test]
fn slow_queries_are_counted_and_fast_ones_are_not() {
    let engine = QueryEngine::new().with_slow_query_ms(5);
    let before = crate::query::metrics::snapshot();
    engine.log_if_slow("RETURN 1", std::time::Duration::from_millis(50), 1, true);
    engine.log_if_slow("RETURN 1", std::time::Duration::from_millis(1), 1, false);
    let after = crate::query::metrics::snapshot();
    // Counters are process-wide; other tests may add to them, never subtract.
    assert!(after.slow > before.slow);
    assert!(after.failed > before.failed);
    assert!(after.total >= before.total + 2);
}

#[test]
fn an_execution_error_that_names_a_token_is_spanned() {
    let store = people(0);
    let engine = QueryEngine::new();
    let err = engine
        .execute("RETURN range(1, 5, 0) AS r", &store)
        .unwrap_err();
    let spanned = err
        .downcast_ref::<SpannedError>()
        .unwrap_or_else(|| panic!("expected a spanned error, got {err}"));
    let inner = spanned.inner().to_string();
    let shown = spanned.to_string();
    assert!(shown.starts_with(&inner), "{shown}");
    assert!(shown.len() > inner.len(), "the span adds a caret: {shown}");
    assert!(shown.contains('^'), "{shown}");
}

#[test]
fn with_span_leaves_an_unlocatable_error_alone() {
    let e: Box<dyn std::error::Error> = "Type error: Add requires numeric".into();
    let out = with_span(e, "RETURN 1 + 'a'");
    assert!(out.downcast_ref::<SpannedError>().is_none());
    assert_eq!(out.to_string(), "Type error: Add requires numeric");
}

#[test]
fn engine_builders_and_defaults() {
    let engine = QueryEngine::default().with_row_budget(7);
    assert_eq!(engine.row_budget(), 7);
    assert_eq!(engine.cache_stats().hits(), 0);
    let store = people(1);
    engine.execute("RETURN 1", &store).unwrap();
    engine.execute("RETURN 1", &store).unwrap();
    assert_eq!(engine.cache_stats().hits(), 1, "the second parse is cached");
}
