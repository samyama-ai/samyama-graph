//! The migration corpus, checked on every run rather than when someone
//! remembers to look (INT-11).
//!
//! `benchmarks/compat/neo4j-idioms.cypher` is written from the outside: shapes
//! that appear in Neo4j's documentation and in application code, not a list of
//! what this engine supports. A corpus derived from our feature matrix would
//! report 100% of whatever we already do.
//!
//! This is a **ratchet**, and it is deliberately asymmetric:
//!
//! - a query that used to be accepted and is now refused **fails**, because
//!   that is a regression in what a migrating user can bring;
//! - a query that used to be refused and is now accepted **passes**, because
//!   improvement should never break a build.
//!
//! The floor is the count, so an improvement that is not recorded here makes
//! the floor stale rather than wrong. When you fix one of the refusals below,
//! raise `ACCEPTED_FLOOR` and delete its line from `KNOWN_REFUSALS` — the list
//! is there so the next reader knows which gaps are known rather than
//! discovering them one query at a time.
//!
//! A ratchet that reads zero is worse than no ratchet (#-LINT-RATCHET), so the
//! first assertion is that the corpus was found and parsed into queries at all.

use samyama::compat::{judge_all, split_queries};
use samyama::graph::GraphStore;

/// Queries the engine refuses today, each with why.
///
/// Matched as a prefix of the query, so an entry stays readable.
const KNOWN_REFUSALS: &[(&str, &str)] = &[
    (
        "CALL apoc.periodic.iterate",
        "an APOC procedure; we have no APOC namespace",
    ),
    (
        "MATCH (n) RETURN apoc.text.join",
        "a namespaced function in RETURN; the grammar takes no dotted function name there",
    ),
    (
        "CALL algo.pageRank() YIELD node, score WITH",
        "CALL ... YIELD ... WITH — samyama-graph#1375",
    ),
    (
        "MATCH (`my node`:Person)",
        "a backticked variable — samyama-graph#1373, the half not yet done",
    ),
    (
        "MATCH (:`Research Paper`)",
        "a backticked label — samyama-graph#1373; fixed on a branch, so this may already be accepted",
    ),
    (
        "MATCH (p:Person) USING INDEX",
        "a query hint — LANG-13, not implemented",
    ),
];

/// How many of the corpus the engine accepted when this was last recorded.
const ACCEPTED_FLOOR: usize = 53;

fn corpus() -> Vec<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benchmarks/compat/neo4j-idioms.cypher");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("the corpus must be in the repository: {}: {e}", path.display()));
    split_queries(&text)
}

#[test]
fn the_corpus_is_there_and_is_a_corpus() {
    // The check that stops this whole file from passing vacuously. A ratchet
    // reading an empty list reports success forever.
    let queries = corpus();
    assert!(
        queries.len() >= 50,
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
    let queries = corpus();
    let verdicts = judge_all(&queries, &store);

    let unexpected: Vec<String> = verdicts
        .iter()
        .filter_map(|v| {
            let r = v.refusal.as_ref()?;
            let known = KNOWN_REFUSALS
                .iter()
                .any(|(prefix, _)| v.query.starts_with(prefix));
            if known {
                None
            } else {
                Some(format!("{}\n      -> {} {}", v.query, r.short_code(), r.detail))
            }
        })
        .collect();

    assert!(
        unexpected.is_empty(),
        "{} corpus quer(ies) are refused and not in KNOWN_REFUSALS. Either a change \
         narrowed what a migrating user can bring, or this is a gap worth recording:\n  {}",
        unexpected.len(),
        unexpected.join("\n  ")
    );
}

#[test]
fn the_accepted_count_does_not_fall() {
    let store = GraphStore::new();
    let verdicts = judge_all(&corpus(), &store);
    let accepted = verdicts.iter().filter(|v| v.accepted()).count();
    assert!(
        accepted >= ACCEPTED_FLOOR,
        "the corpus accepted {accepted} queries, down from {ACCEPTED_FLOOR}. \
         A migration that worked yesterday does not today."
    );
}

#[test]
fn every_refusal_says_something_a_reader_can_act_on() {
    // "Parse error" is not a cause; `expected label` is. Grouping on a message
    // that carries no specifics puts every syntax failure in one bucket, which
    // is how a report becomes a number instead of a plan.
    let store = GraphStore::new();
    for v in judge_all(&corpus(), &store) {
        if let Some(r) = v.refusal {
            assert!(
                !r.detail.trim().is_empty(),
                "a refusal with no detail: {}",
                v.query
            );
            assert!(
                r.detail != "Parse error:" && r.detail.len() > "Parse error:".len(),
                "the refusal for `{}` says only that it failed: {:?}",
                v.query,
                r.detail
            );
        }
    }
}
