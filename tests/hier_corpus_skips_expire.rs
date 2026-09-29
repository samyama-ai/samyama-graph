//! A benchmark skip must expire when the engine gains the capability (samyama-graph#444).
//!
//! `benchmarks/hier/queries.json` once carried four H9 queries skipped with a static
//! "engine gap" string. #439 closed the gap; nothing noticed, and the benchmark reported
//! 108/108 while it was 108 of 112. A skip is now a probe — `{"reason", "error"}` — and
//! this test runs every skipped query against the engine under `cargo test`:
//!
//! - still failing with the stated error: the skip holds;
//! - running: the skip is stale, and this test fails until it is removed;
//! - failing differently: the stated reason is wrong, and this test fails.

#[allow(dead_code)]
#[path = "../benches/hier_common/mod.rs"]
mod hier_common;
#[path = "../benches/hier_common/skip.rs"]
mod skip;

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use serde_json::json;
use skip::{classify, describe_failure, parse_skip, Skip, SkipVerdict};

const CORPUS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/benchmarks/hier/queries.json");

/// Every skipped corpus query, parsed. Panics on a skip that cannot expire.
fn corpus_skips() -> Vec<(String, String, Skip)> {
    let text = std::fs::read_to_string(CORPUS).expect("read corpus");
    let json: serde_json::Value = serde_json::from_str(&text).expect("corpus is JSON");
    let mut out = Vec::new();
    for q in json["queries"].as_array().expect("queries array") {
        let id = q["id"].as_str().expect("id").to_string();
        match parse_skip(&id, &q["skip"]) {
            Ok(Some(s)) => out.push((id, q["cypher"].as_str().expect("cypher").to_string(), s)),
            Ok(None) => {}
            Err(e) => panic!("{e}"),
        }
    }
    out
}

/// The store the benchmark's indexed arm runs against: same dataset, same declarations.
fn indexed_store(engine: &QueryEngine) -> GraphStore {
    let mut store = hier_common::build(&hier_common::HierScale::default()).store;
    for decl in hier_common::SETUP_DECLARATIONS
        .iter()
        .chain(hier_common::HIER_DECLARATIONS)
    {
        engine
            .execute_mut(decl, &mut store, "default")
            .unwrap_or_else(|e| panic!("declaration failed: {decl}\n{e}"));
    }
    store
}

fn probe(engine: &QueryEngine, store: &GraphStore, cypher: &str) -> Result<(), String> {
    engine
        .execute(cypher, store)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[test]
fn every_skip_in_the_hier_corpus_still_holds() {
    let skips = corpus_skips();
    if skips.is_empty() {
        // Nothing is excluded, so the reported denominator is the specified one.
        return;
    }
    let engine = QueryEngine::new();
    let store = indexed_store(&engine);
    let failures: Vec<String> = skips
        .iter()
        .filter_map(|(id, cypher, s)| {
            describe_failure(id, s, &classify(s, probe(&engine, &store, cypher)))
        })
        .collect();
    assert!(
        failures.is_empty(),
        "corpus skips no longer hold:\n  {}",
        failures.join("\n  ")
    );
}

// ---- the checker itself -------------------------------------------------------------

fn gap(error: &str) -> Skip {
    Skip {
        reason: "engine gap under test".into(),
        error: error.into(),
    }
}

#[test]
fn a_bare_string_skip_is_rejected() {
    let e = parse_skip("H9-01", &json!("engine gap: YIELD not in scope")).unwrap_err();
    assert!(e.contains("bare string"), "{e}");
}

#[test]
fn a_skip_without_an_error_signature_is_rejected() {
    assert!(parse_skip("X", &json!({"reason": "r"})).is_err());
    assert!(parse_skip("X", &json!({"reason": "r", "error": "  "})).is_err());
    assert!(parse_skip("X", &json!({"error": "e"})).is_err());
    assert!(parse_skip("X", &json!(true)).is_err());
}

#[test]
fn a_well_formed_skip_parses() {
    assert_eq!(parse_skip("X", &serde_json::Value::Null).unwrap(), None);
    assert_eq!(
        parse_skip("X", &json!({"reason": "r", "error": "e"})).unwrap(),
        Some(Skip {
            reason: "r".into(),
            error: "e".into()
        })
    );
}

#[test]
fn classification_with_fake_probes() {
    let s = gap("Variable not found: node");
    assert_eq!(
        classify(&s, Err("Semantic: Variable not found: node".into())),
        SkipVerdict::StillBlocked
    );
    assert_eq!(classify(&s, Ok(())), SkipVerdict::Stale);
    assert_eq!(
        classify(&s, Err("Parse error at 1:5".into())),
        SkipVerdict::WrongReason("Parse error at 1:5".into())
    );
    assert!(describe_failure("X", &s, &SkipVerdict::StillBlocked).is_none());
    assert!(describe_failure("X", &s, &SkipVerdict::Stale)
        .unwrap()
        .contains("STALE"));
}

/// The case #444 is about, against the real engine: a skip written for a gap that does not
/// exist (the query runs) is reported stale, while a query that really fails holds its skip.
#[test]
fn a_skip_for_a_capability_the_engine_has_is_reported_stale() {
    let engine = QueryEngine::new();
    let store = GraphStore::new();

    let works = "RETURN 1 AS n";
    let s = gap("not supported");
    let v = classify(&s, probe(&engine, &store, works));
    assert_eq!(v, SkipVerdict::Stale);
    assert!(!v.holds());

    let broken = "RETURN no_such_function_444(1) AS n";
    let actual = probe(&engine, &store, broken).expect_err("an unknown function must fail");
    // Signature taken from the engine's own message, so this holds whatever the wording.
    let s = gap(&actual);
    assert!(classify(&s, probe(&engine, &store, broken)).holds());
    // ...and a signature that names a different failure does not.
    let s = gap("Variable not found");
    assert!(matches!(
        classify(&s, probe(&engine, &store, broken)),
        SkipVerdict::WrongReason(_)
    ));
}
