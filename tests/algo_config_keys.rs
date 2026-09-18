//! An algorithm config key the engine does not read is refused, not discarded.
//!
//! Every algorithm's parser asked the map for the keys it knew and ignored the
//! rest. So `algo.pageRank({writeProperty: 'pr'})` was accepted, streamed,
//! wrote nothing and reported success — the spelling a Neo4j GDS user reaches
//! for first — and `{iteratons: 100}` silently ran at the default. A request
//! the engine discards must not look like one it honoured (#1316).
//!
//! The guard found a case the moment it was added: `tests/h2_algorithms_from_cypher.rs`
//! passed `{topK: 3}` to `nodeSimilarity`, which reads `cutoff`. The test had
//! been asserting a result produced without the config it specified.

use samyama::graph::{EdgeType, GraphStore};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let a = s.create_node("N");
    let b = s.create_node("N");
    let c = s.create_node("N");
    s.create_edge(a, b, EdgeType::new("R")).unwrap();
    s.create_edge(b, c, EdgeType::new("R")).unwrap();
    s
}

fn run(s: &GraphStore, q: &str) -> Result<usize, String> {
    let parsed = parse_query(q).expect("parse");
    QueryExecutor::new(s)
        .execute(&parsed)
        .map(|r| r.records.len())
        .map_err(|e| e.to_string())
}

#[test]
fn an_unknown_config_key_is_refused_and_the_accepted_ones_are_named() {
    let s = store();
    let err = run(&s, "CALL algo.pageRank({writeProperty: 'pr'}) YIELD node, score RETURN node")
        .expect_err("a key the algorithm does not read must be refused");
    assert!(err.contains("writeProperty"), "the message names the key: {err}");
    assert!(err.contains("iterations") && err.contains("damping"),
            "and lists what it does read: {err}");
}

#[test]
fn a_typo_is_refused_rather_than_run_at_the_default() {
    let s = store();
    // The failure this prevents is silent: `iteratons` ran 20 iterations and
    // said nothing, so a caller tuning it saw no change and no error.
    let err = run(&s, "CALL algo.pageRank({iteratons: 100}) YIELD node, score RETURN node")
        .expect_err("a misspelled key must be refused");
    assert!(err.contains("iteratons"), "{err}");
}

#[test]
fn every_key_an_algorithm_reads_is_still_accepted() {
    let s = store();
    let src = s.get_nodes_by_label(&samyama::graph::Label::new("N"))[0].id.as_u64();
    for q in [
        "CALL algo.pageRank({iterations: 10, damping: 0.9}) YIELD node, score RETURN node".to_string(),
        "CALL algo.cdlp({maxIterations: 5}) YIELD node, community RETURN node".to_string(),
        "CALL algo.katz({alpha: 0.1, beta: 1.0, iterations: 20, tolerance: 0.0001}) YIELD node, score RETURN node".to_string(),
        "CALL algo.hits({iterations: 20, tolerance: 0.0001}) YIELD node, hub RETURN node".to_string(),
        "CALL algo.articleRank({dampingFactor: 0.85, iterations: 20}) YIELD node, score RETURN node".to_string(),
        // randomWalk needs a source node id as well as its config.
        format!("CALL algo.randomWalk({src}, {{steps: 5, seed: 42}}) YIELD node, step RETURN node"),
        "CALL algo.nodeSimilarity({cutoff: 0.1}) YIELD node, other, similarity RETURN node".to_string(),
    ] {
        assert!(run(&s, &q).is_ok(), "a documented key must still work: {q}");
    }
    let _ = src;
}

#[test]
fn a_call_with_no_config_is_untouched() {
    let s = store();
    assert!(run(&s, "CALL algo.pageRank() YIELD node, score RETURN node").is_ok());
    assert!(run(&s, "CALL algo.wcc() YIELD node, component RETURN node").is_ok());
}

#[test]
fn the_message_says_write_back_is_not_implemented() {
    // `writeProperty` and `mutate` are the two a GDS user tries first, and
    // "unknown key" alone would read as a typo rather than a missing feature.
    let s = store();
    let err = run(&s, "CALL algo.cdlp({mutate: true}) YIELD node, community RETURN node")
        .expect_err("refused");
    assert!(err.contains("ALGO-06") || err.contains("write-back"),
            "the message points at the unbuilt feature: {err}");
}
