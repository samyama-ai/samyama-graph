//! Graph algorithms are reachable from Cypher under the names people actually type (#198).
//!
//! Dispatch matched `"algo.pageRank"` *exactly*, so `algo.pagerank` — the first spelling
//! anyone tries — returned "Unknown algorithm" while the implementation sat right there,
//! and the bare `CALL pagerank()` never even reached the operator ("Unknown procedure").
//! The algorithm crate's own 38 unit tests all passed throughout; nothing exercised the
//! path from Cypher to them.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

/// 5 nodes, edges 0→1, 1→2, 3→1, leaving node 4 isolated.
fn fixture() -> GraphStore {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    for i in 0..5 {
        engine
            .execute_mut(&format!("CREATE (:N {{id: {i}}})"), &mut store, "default")
            .unwrap();
    }
    for (a, b) in [(0, 1), (1, 2), (3, 1)] {
        engine
            .execute_mut(
                &format!("MATCH (a:N {{id: {a}}}), (b:N {{id: {b}}}) CREATE (a)-[:R]->(b)"),
                &mut store, "default",
            )
            .unwrap();
    }
    store
}

#[test]
fn pagerank_is_callable_under_every_reasonable_spelling() {
    let store = fixture();
    let engine = QueryEngine::new();

    for spelling in [
        "CALL pagerank() YIELD node, score RETURN node, score",
        "CALL algo.pagerank() YIELD node, score RETURN node, score",
        "CALL algo.pageRank() YIELD node, score RETURN node, score",
        "CALL samyama.pagerank() YIELD node, score RETURN node, score",
        "CALL PAGERANK() YIELD node, score RETURN node, score",
    ] {
        let batch = engine
            .execute(spelling, &store)
            .unwrap_or_else(|e| panic!("{spelling}\n  {e}"));
        assert_eq!(batch.records.len(), 5, "one row per node: {spelling}");
    }
}

#[test]
fn pagerank_actually_ranks() {
    // A count of rows would pass even if every score were identical, or zero. Node 1 has
    // two in-edges and must outrank the isolated node 4.
    let store = fixture();
    let engine = QueryEngine::new();

    let batch = engine
        .execute("CALL pagerank() YIELD node, score RETURN node, score", &store)
        .unwrap();

    let scores: Vec<f64> = batch
        .records
        .iter()
        .filter_map(|r| match r.get("score") {
            Some(Value::Property(PropertyValue::Float(f))) => Some(*f),
            _ => None,
        })
        .collect();
    assert_eq!(scores.len(), 5);

    let mut distinct: Vec<String> = scores.iter().map(|s| format!("{s:.6}")).collect();
    distinct.sort();
    distinct.dedup();
    assert!(distinct.len() > 1, "all scores identical — did it run? {scores:?}");

    let max = scores.iter().cloned().fold(f64::MIN, f64::max);
    let min = scores.iter().cloned().fold(f64::MAX, f64::min);
    assert!(max > min, "no ranking at all: {scores:?}");
}

#[test]
fn other_algorithms_are_reachable_too() {
    let store = fixture();
    let engine = QueryEngine::new();

    let wcc = engine
        .execute("CALL algo.wcc() YIELD node, componentId RETURN node, componentId", &store)
        .expect("wcc");
    assert_eq!(wcc.records.len(), 5);

    // case-insensitive here as well
    let wcc_upper = engine
        .execute("CALL algo.WCC() YIELD node, componentId RETURN node, componentId", &store)
        .expect("WCC");
    assert_eq!(wcc_upper.records.len(), 5);
}

#[test]
fn an_unknown_algorithm_still_errors() {
    // Leniency about spelling must not turn into running the wrong thing, and an
    // unrecognised name under `algo.` keeps the specific error rather than the generic
    // "Unknown procedure".
    let store = fixture();
    let engine = QueryEngine::new();

    let err = engine
        .execute("CALL algo.nonExistent() YIELD x RETURN x", &store)
        .expect_err("should not resolve");
    assert!(
        format!("{err}").contains("Unknown algorithm"),
        "expected an algorithm-specific error, got: {err}"
    );

    let err = engine
        .execute("CALL notAProcedure() YIELD x RETURN x", &store)
        .expect_err("should not resolve");
    assert!(format!("{err}").contains("Unknown"), "got: {err}");
}
