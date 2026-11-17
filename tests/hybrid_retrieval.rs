//! `db.hybrid.search` (#1279): BM25 and vector retrieval fused into seeds,
//! a bounded expansion around them, and a rerank.
//!
//! The corpus is small enough that every number is computed by hand: four
//! documents, one found only by its words, one only by its embedding, one by
//! both and one by neither list's top entries, and a citation chain hanging
//! off the seeds. A vector index this small is searched exactly, so nothing
//! depends on HNSW recall. No assertion reads a clock.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{Record, Value};
use samyama::query::QueryEngine;

fn write(store: &mut GraphStore, q: &str) {
    QueryEngine::new()
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn rows(store: &GraphStore, q: &str) -> Vec<Record> {
    QueryEngine::new()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .records
}

fn error(store: &GraphStore, q: &str) -> String {
    match QueryEngine::new().execute(q, store) {
        Ok(b) => panic!("{q}: expected an error, got {:?}", b.records),
        Err(e) => e.to_string(),
    }
}

fn string(r: &Record, col: &str) -> String {
    match r.get(col) {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        other => panic!("{col}: {other:?}"),
    }
}

fn float(r: &Record, col: &str) -> f64 {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Float(f))) => *f,
        other => panic!("{col}: {other:?}"),
    }
}

fn int_or_null(r: &Record, col: &str) -> Option<i64> {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Integer(i))) => Some(*i),
        Some(Value::Null) | Some(Value::Property(PropertyValue::Null)) | None => None,
        other => panic!("{col}: {other:?}"),
    }
}

fn strings(r: &Record, col: &str) -> Vec<String> {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Array(items))) => items
            .iter()
            .map(|i| match i {
                PropertyValue::String(s) => s.clone(),
                other => panic!("{col}: {other:?}"),
            })
            .collect(),
        other => panic!("{col}: {other:?}"),
    }
}

fn names(out: &[Record]) -> Vec<String> {
    out.iter().map(|r| string(r, "name")).collect()
}

fn close(got: f64, want: f64, what: &str) {
    assert!((got - want).abs() < 1e-12, "{what}: got {got}, want {want}");
}

/// Query text `aspirin`, query vector `[1, 0, 0]`.
///
/// | doc      | text                                   | emb           | BM25 rank | vector rank |
/// |----------|----------------------------------------|---------------|-----------|-------------|
/// | kwOnly   | aspirin                                | none          | 1         | -           |
/// | both     | aspirin bleeding risk elderly patients | [1, 0, 0]     | 2         | 1           |
/// | vecOnly  | warfarin interaction                   | [0.9, 0.1, 0] | -         | 2           |
/// | far      | unrelated cardiology                   | [0, 1, 0]     | -         | 3           |
///
/// `kwOnly` outranks `both` on BM25 because the term is as frequent in a
/// shorter document. `kwOnly` has no embedding, so the vector index cannot
/// return it; `vecOnly` lacks the word, so BM25 cannot.
///
/// Citations: `both -> c1 -> c2 -> c3`, `kwOnly -> c1`, and
/// `both -[:MENTIONS]-> m1`.
fn corpus() -> GraphStore {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE (kw:Doc {name: 'kwOnly', text: 'aspirin'}) \
         CREATE (b:Doc {name: 'both', text: 'aspirin bleeding risk elderly patients', emb: [1.0, 0.0, 0.0]}) \
         CREATE (v:Doc {name: 'vecOnly', text: 'warfarin interaction', emb: [0.9, 0.1, 0.0]}) \
         CREATE (f:Doc {name: 'far', text: 'unrelated cardiology', emb: [0.0, 1.0, 0.0]}) \
         CREATE (c1:Cite {name: 'c1'}) CREATE (c2:Cite {name: 'c2'}) CREATE (c3:Cite {name: 'c3'}) \
         CREATE (m1:Mention {name: 'm1'}) \
         CREATE (b)-[:CITES]->(c1) CREATE (kw)-[:CITES]->(c1) \
         CREATE (c1)-[:CITES]->(c2) CREATE (c2)-[:CITES]->(c3) \
         CREATE (b)-[:MENTIONS]->(m1)",
    );
    write(
        &mut s,
        "CREATE FULLTEXT INDEX doc_text FOR (d:Doc) ON (d.text)",
    );
    write(
        &mut s,
        "CREATE VECTOR INDEX doc_emb FOR (d:Doc) ON (d.emb) \
         OPTIONS {dimensions: 3, similarity: 'cosine'}",
    );
    s
}

const RRF_K: f64 = 60.0;

fn rrf(ranks: &[usize]) -> f64 {
    ranks.iter().map(|r| 1.0 / (RRF_K + *r as f64)).sum()
}

fn search(config: &str) -> String {
    format!(
        "CALL db.hybrid.search('doc_text', 'aspirin', 'doc_emb', [1.0, 0.0, 0.0], {config}) \
         YIELD node, score, fusedScore, graphScore, bm25Rank, vectorRank, foundBy, hops, seed \
         RETURN node.name AS name, score, fusedScore, graphScore, bm25Rank, vectorRank, \
                foundBy, hops, seed.name AS seed"
    )
}

#[test]
fn a_keyword_only_and_a_vector_only_match_both_come_back() {
    let s = corpus();
    let out = rows(&s, &search("{}"));
    let got = names(&out);
    assert!(got.contains(&"kwOnly".to_string()), "{got:?}");
    assert!(got.contains(&"vecOnly".to_string()), "{got:?}");

    let row = |n: &str| out.iter().find(|r| string(r, "name") == n).unwrap();
    assert_eq!(strings(row("kwOnly"), "foundBy"), ["bm25"]);
    assert_eq!(int_or_null(row("kwOnly"), "vectorRank"), None);
    assert_eq!(strings(row("vecOnly"), "foundBy"), ["vector"]);
    assert_eq!(int_or_null(row("vecOnly"), "bm25Rank"), None);
    assert_eq!(strings(row("both"), "foundBy"), ["bm25", "vector"]);
}

#[test]
fn rrf_orders_by_hand_computed_ranks() {
    let s = corpus();
    let out = rows(&s, &search("{}"));
    assert_eq!(names(&out), ["both", "kwOnly", "vecOnly", "far"]);

    let expected = [
        ("both", Some(2), Some(1), rrf(&[2, 1])),
        ("kwOnly", Some(1), None, rrf(&[1])),
        ("vecOnly", None, Some(2), rrf(&[2])),
        ("far", None, Some(3), rrf(&[3])),
    ];
    for (r, (name, bm25, vector, fused)) in out.iter().zip(expected) {
        assert_eq!(string(r, "name"), name);
        assert_eq!(int_or_null(r, "bm25Rank"), bm25, "{name} bm25Rank");
        assert_eq!(int_or_null(r, "vectorRank"), vector, "{name} vectorRank");
        close(float(r, "fusedScore"), fused, name);
        // No expansion: the score is the fused score.
        close(float(r, "score"), fused, name);
        close(float(r, "graphScore"), 0.0, name);
        assert_eq!(int_or_null(r, "hops"), Some(0));
        assert_eq!(string(r, "seed"), name, "a seed is its own seed");
    }
}

#[test]
fn k_keeps_the_best_fused_seeds_and_rrf_k_and_weights_are_read() {
    let s = corpus();
    assert_eq!(names(&rows(&s, &search("{k: 2}"))), ["both", "kwOnly"]);

    // With the BM25 list weighted out, the order is the vector order.
    let out = rows(&s, &search("{bm25Weight: 0, rrfK: 10}"));
    let row = |n: &str| out.iter().find(|r| string(r, "name") == n).unwrap();
    close(float(row("both"), "fusedScore"), 1.0 / 11.0, "both");
    close(float(row("kwOnly"), "fusedScore"), 0.0, "kwOnly");
    assert_eq!(&names(&out)[..3], ["both", "vecOnly", "far"]);
}

#[test]
fn weighted_fusion_divides_each_list_by_its_best() {
    let s = corpus();
    let out = rows(&s, &search("{fusion: 'weighted'}"));
    // `both` is the best vector hit (1 of 1) and below the best BM25 hit,
    // `kwOnly` the best BM25 hit (1 of 1); `both` must lead on the sum.
    let row = |n: &str| out.iter().find(|r| string(r, "name") == n).unwrap();
    close(float(row("kwOnly"), "fusedScore"), 1.0, "kwOnly");
    let both = float(row("both"), "fusedScore");
    assert!(both > 1.0 && both < 2.0, "both: {both}");
    assert_eq!(names(&out)[0], "both");
}

#[test]
fn expansion_follows_only_the_named_types_and_stops_at_the_hop_cap() {
    let s = corpus();
    let out = rows(
        &s,
        &search("{k: 2, hops: 1, relTypes: ['CITES'], direction: 'out'}"),
    );
    let got = names(&out);
    assert_eq!(got.len(), 3, "two seeds and c1: {got:?}");
    assert!(got.contains(&"c1".to_string()));
    assert!(!got.contains(&"m1".to_string()), "MENTIONS is not followed");
    assert!(!got.contains(&"c2".to_string()), "c2 is two hops out");

    // c1 is one hop from both seeds: support is the sum of their fused
    // scores, each halved once (decay 0.5, graphWeight 1).
    let c1 = out.iter().find(|r| string(r, "name") == "c1").unwrap();
    let support = (rrf(&[2, 1]) + rrf(&[1])) * 0.5;
    close(float(c1, "graphScore"), support, "c1 graph");
    close(float(c1, "fusedScore"), 0.0, "c1 fused");
    close(float(c1, "score"), support, "c1 score");
    assert_eq!(strings(c1, "foundBy"), ["graph"]);
    assert_eq!(int_or_null(c1, "hops"), Some(1));
    assert_eq!(string(c1, "seed"), "both", "the seed contributing most");

    let two = names(&rows(
        &s,
        &search("{k: 2, hops: 2, relTypes: ['CITES'], direction: 'out'}"),
    ));
    assert!(two.contains(&"c2".to_string()), "{two:?}");
    assert!(
        !two.contains(&"c3".to_string()),
        "c3 is three hops out: {two:?}"
    );
}

#[test]
fn expansion_admits_no_more_than_max_expanded_nodes() {
    let s = corpus();
    for cap in [0usize, 1, 2] {
        let out = rows(
            &s,
            &search(&format!("{{k: 2, hops: 3, maxExpanded: {cap}}}")),
        );
        let seeds = out
            .iter()
            .filter(|r| int_or_null(r, "hops") == Some(0))
            .count();
        let expanded = out.len() - seeds;
        assert_eq!(seeds, 2, "cap {cap}");
        assert_eq!(expanded, cap, "cap {cap}: {:?}", names(&out));
    }
    // Uncapped, three outgoing hops reach every Cite and m1, and nothing
    // leads out to the two Docs that are not seeds.
    let out = rows(&s, &search("{k: 2, hops: 3, direction: 'out'}"));
    let mut got = names(&out);
    got.sort();
    assert_eq!(got, ["both", "c1", "c2", "c3", "kwOnly", "m1"]);
}

#[test]
fn hops_beyond_the_local_limit_is_refused() {
    let s = corpus();
    let e = error(&s, &search("{hops: 6}"));
    assert!(e.contains("hops"), "{e}");
}

#[test]
fn a_missing_index_is_a_clear_error() {
    let s = corpus();
    let e = error(
        &s,
        "CALL db.hybrid.search('nope', 'aspirin', 'doc_emb', [1.0, 0.0, 0.0]) YIELD node RETURN node",
    );
    assert!(
        e.contains("no full-text index named 'nope'") && e.contains("doc_text"),
        "{e}"
    );

    let e = error(
        &s,
        "CALL db.hybrid.search('doc_text', 'aspirin', 'nope', [1.0, 0.0, 0.0]) YIELD node RETURN node",
    );
    assert!(
        e.contains("no vector index named 'nope'") && e.contains("doc_emb"),
        "{e}"
    );

    let e = error(
        &s,
        "CALL db.hybrid.search('doc_text', 'aspirin', {label: 'Doc', property: 'nope'}, [1.0, 0.0, 0.0]) \
         YIELD node RETURN node",
    );
    assert!(e.contains("no vector index on :Doc(nope)"), "{e}");
}

#[test]
fn the_vector_index_can_be_addressed_by_label_and_property() {
    let s = corpus();
    let out = rows(
        &s,
        "CALL db.hybrid.search('doc_text', 'aspirin', {label: 'Doc', property: 'emb'}, [1.0, 0.0, 0.0]) \
         YIELD node RETURN node.name AS name",
    );
    assert_eq!(names(&out), ["both", "kwOnly", "vecOnly", "far"]);
}

#[test]
fn a_null_or_empty_query_skips_its_list() {
    let s = corpus();
    for text in ["null", "''", "'   '"] {
        let out = rows(
            &s,
            &format!(
                "CALL db.hybrid.search('doc_text', {text}, 'doc_emb', [1.0, 0.0, 0.0]) \
                 YIELD node, bm25Rank RETURN node.name AS name, bm25Rank"
            ),
        );
        assert_eq!(names(&out), ["both", "vecOnly", "far"], "text {text}");
        assert!(out.iter().all(|r| int_or_null(r, "bm25Rank").is_none()));
    }
    for vector in ["null", "[]"] {
        let out = rows(
            &s,
            &format!(
                "CALL db.hybrid.search('doc_text', 'aspirin', 'doc_emb', {vector}) \
                 YIELD node RETURN node.name AS name"
            ),
        );
        assert_eq!(names(&out), ["kwOnly", "both"], "vector {vector}");
    }
    // An unused index may be null.
    let out = rows(
        &s,
        "CALL db.hybrid.search(null, null, 'doc_emb', [1.0, 0.0, 0.0]) YIELD node RETURN node.name AS name",
    );
    assert_eq!(names(&out), ["both", "vecOnly", "far"]);

    let e = error(
        &s,
        "CALL db.hybrid.search('doc_text', null, 'doc_emb', []) YIELD node RETURN node",
    );
    assert!(
        e.contains("needs a query text, a query vector, or both"),
        "{e}"
    );

    let e = error(
        &s,
        "CALL db.hybrid.search(null, 'aspirin', 'doc_emb', null) YIELD node RETURN node",
    );
    assert!(e.contains("needs a full-text index"), "{e}");
}

#[test]
fn a_text_that_matches_nothing_returns_the_vector_list_alone() {
    let s = corpus();
    let out = rows(
        &s,
        "CALL db.hybrid.search('doc_text', 'zzzz', 'doc_emb', [1.0, 0.0, 0.0]) YIELD node \
         RETURN node.name AS name",
    );
    assert_eq!(names(&out), ["both", "vecOnly", "far"]);
}

#[test]
fn bad_arguments_and_config_keys_are_refused() {
    let s = corpus();
    let e = error(
        &s,
        "CALL db.hybrid.search('doc_text', 'aspirin') YIELD node RETURN node",
    );
    assert!(
        e.contains("takes (fulltextIndex, queryText, vectorIndex, queryVector)"),
        "{e}"
    );

    let e = error(&s, &search("{topK: 3}"));
    assert!(e.contains("unknown config key `topK`"), "{e}");

    let e = error(&s, &search("{fusion: 'max'}"));
    assert!(e.contains("`fusion` is 'rrf' or 'weighted'"), "{e}");

    let e = error(
        &s,
        "CALL db.hybrid.search('doc_text', 'aspirin', 'doc_emb', [1.0, 0.0]) YIELD node RETURN node",
    );
    assert!(e.to_lowercase().contains("dimension"), "{e}");

    let e = error(
        &s,
        "CALL db.hybrid.search('doc_text', 'aspirin', 'doc_emb', [1.0, 0.0, 0.0]) YIELD nodes RETURN nodes",
    );
    assert!(e.contains("no `nodes` column"), "{e}");
}

#[test]
fn every_row_carries_the_stage_timings() {
    let s = corpus();
    let out = rows(
        &s,
        "CALL db.hybrid.search('doc_text', 'aspirin', 'doc_emb', [1.0, 0.0, 0.0], {hops: 1}) \
         YIELD timings RETURN timings",
    );
    assert!(!out.is_empty());
    for r in &out {
        let Some(Value::Property(PropertyValue::Map(m))) = r.get("timings") else {
            panic!("timings: {:?}", r.get("timings"));
        };
        let mut keys: Vec<&str> = m.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "bm25Ms",
                "expansionMs",
                "fusionMs",
                "rerankMs",
                "totalMs",
                "vectorMs"
            ]
        );
        for (k, v) in m {
            match v {
                PropertyValue::Float(f) => assert!(f.is_finite() && *f >= 0.0, "{k}: {f}"),
                other => panic!("{k}: {other:?}"),
            }
        }
    }
}

#[test]
fn the_procedure_is_a_read() {
    assert!(!QueryEngine::new()
        .statement_is_write(
            "CALL db.hybrid.search('doc_text', 'aspirin', 'doc_emb', [1.0, 0.0, 0.0]) YIELD node RETURN node"
        )
        .unwrap());
}
