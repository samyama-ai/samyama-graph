//! The FalkorDB/RedisGraph migration corpus, checked on every run (DX-13, #1787).
//!
//! `benchmarks/compat/falkordb-idioms.cypher` holds idioms from FalkorDB's own
//! documentation, each run on FalkorDB before it was added, so a refusal here
//! is a gap in this engine and not a mistake in the corpus.
//!
//! The same asymmetric ratchet as `tests/compatibility_corpus.rs`: a query that
//! was accepted and is now refused fails; a refusal that starts being accepted
//! passes. When one of `KNOWN_REFUSALS` is fixed, raise `ACCEPTED_FLOOR` and
//! delete its line, and update the counts quoted in
//! `docs/MIGRATING-FROM-FALKORDB.md`.
//!
//! "Accepted" is parse-and-plan. The guide also states how many of these
//! actually run (32 of 53 on 2026-10-17); that number needs a live engine and is
//! not what this file measures.

use samyama::compat::{judge_all, split_queries};
use samyama::graph::GraphStore;

/// Where the corpus lives, relative to the crate root.
const CORPUS_PATH: &str = "benchmarks/compat/falkordb-idioms.cypher";

/// Fewer queries than this means the corpus was not found or not split, and
/// every assertion below would pass vacuously.
const MIN_CORPUS_QUERIES: usize = 50;

/// Accepted when this was recorded (2026-10-17): 38 of 53.
const ACCEPTED_FLOOR: usize = 38;

/// Idioms the engine refuses at plan time today, each with why. Matched as a
/// prefix of the query.
const KNOWN_REFUSALS: &[(&str, &str)] = &[
    (
        "CREATE INDEX FOR ()-[k:KNOWS]-()",
        "no index on a relationship property",
    ),
    (
        "CALL db.idx.fulltext.createNodeIndex",
        "full-text is CREATE FULLTEXT INDEX here",
    ),
    (
        "CALL db.idx.fulltext.queryNodes",
        "full-text query is db.index.fulltext.queryNodes",
    ),
    (
        "CALL db.idx.fulltext.drop",
        "full-text drop is DROP FULLTEXT INDEX",
    ),
    (
        "CREATE VECTOR INDEX FOR (p:Person) ON (p.embedding) OPTIONS {dimension:",
        "option keys are dimensions/similarity",
    ),
    (
        "CALL db.idx.vector.queryNodes",
        "vector query is db.index.vector.queryNodes",
    ),
    ("CALL db.indexes", "SHOW INDEXES here"),
    ("CALL dbms.procedures", "no procedure listing"),
    (
        "RETURN vec.euclideanDistance",
        "no vector distance functions",
    ),
    ("RETURN vec.cosineDistance", "no vector distance functions"),
    (
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) RETURN shortestPath",
        "shortestPath belongs in MATCH here",
    ),
    (
        "MATCH (a:Person) RETURN a.name, indegree",
        "no indegree/outdegree functions",
    ),
    ("MATCH (a:Person {name: 'Bob'}) RETURN toJSON", "no toJSON"),
    ("RETURN typeOf", "valueType here"),
    ("RETURN intern", "a FalkorDB memory hint with no equivalent"),
];

fn corpus() -> Vec<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(CORPUS_PATH);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "the corpus must be in the repository: {}: {e}",
            path.display()
        )
    });
    split_queries(&text)
}

#[test]
fn the_corpus_is_there_and_is_a_corpus() {
    let queries = corpus();
    assert!(
        queries.len() >= MIN_CORPUS_QUERIES,
        "the corpus should hold dozens of queries, found {}",
        queries.len()
    );
    assert!(
        queries.iter().all(|q| !q.trim().is_empty()),
        "the splitter emitted an empty query"
    );
}

#[test]
fn no_query_that_used_to_be_accepted_is_refused_now() {
    let store = GraphStore::new();
    let unexpected: Vec<String> = judge_all(&corpus(), &store)
        .iter()
        .filter_map(|v| {
            let r = v.refusal.as_ref()?;
            let known = KNOWN_REFUSALS
                .iter()
                .any(|(prefix, _)| v.query.starts_with(prefix));
            (!known).then(|| format!("{}\n      -> {} {}", v.query, r.short_code(), r.detail))
        })
        .collect();
    assert!(
        unexpected.is_empty(),
        "{} FalkorDB idiom(s) are refused and not in KNOWN_REFUSALS:\n  {}",
        unexpected.len(),
        unexpected.join("\n  ")
    );
}

#[test]
fn every_known_refusal_still_matches_a_corpus_query() {
    // A stale entry would silently excuse a future regression in a query that
    // happens to share its prefix.
    let queries = corpus();
    for (prefix, why) in KNOWN_REFUSALS {
        assert!(
            queries.iter().any(|q| q.starts_with(prefix)),
            "KNOWN_REFUSALS entry `{prefix}` ({why}) matches no query in the corpus"
        );
    }
}

#[test]
fn the_accepted_count_does_not_fall() {
    let store = GraphStore::new();
    let accepted = judge_all(&corpus(), &store)
        .iter()
        .filter(|v| v.accepted())
        .count();
    assert!(
        accepted >= ACCEPTED_FLOOR,
        "the FalkorDB corpus accepted {accepted} queries, down from {ACCEPTED_FLOOR}"
    );
}

#[test]
fn every_refusal_says_something_a_reader_can_act_on() {
    let store = GraphStore::new();
    for v in judge_all(&corpus(), &store) {
        if let Some(r) = v.refusal {
            assert!(
                r.detail.trim().len() > "Parse error:".len(),
                "the refusal for `{}` says only that it failed: {:?}",
                v.query,
                r.detail
            );
        }
    }
}
