//! Every algorithm returns the same rows in the same order on every run
//! (LANG-14, #1019).
//!
//! Five algorithms sorted their output on a key that is **not unique** — a
//! component id, a community id, a score that ties — with no tie-break. Rust's
//! `sort_by` is free to leave equal keys in any arrangement, so the order
//! within a tie came from whatever map iteration produced the rows.
//!
//! **A single run always looks sorted.** That is why this survived, and why a
//! comment reading "Sort by componentId for deterministic output" sat directly
//! above code that was not. The only way to see it is to run the query twice
//! and compare, which nothing did.
//!
//! LANG-14 is "same query + same data + any thread count ⇒ identical ordered
//! output". This file is the check.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// Deliberately full of ties: two identical triangles plus an isolated pair.
/// Every node in a triangle has the same degree, the same clustering
/// coefficient, and nearly the same PageRank, and the components are equal
/// sized — so a sort on any single key leaves most rows tied.
fn tie_heavy() -> GraphStore {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..8 {
        let x = s.create_node_with_labels([Label::new("N")]);
        s.set_node_property("default", x, "name", PropertyValue::String(format!("n{i}"))).unwrap();
        ids.push(x);
    }
    for (a, b) in [(0,1),(1,2),(2,0), (3,4),(4,5),(5,3), (6,7)] {
        s.create_edge(ids[a], ids[b], "R").unwrap();
    }
    s
}

fn run(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store).execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let cols = r.columns.clone();
    r.records.iter()
        .map(|rec| cols.iter().map(|c| format!("{:?}", rec.get(c))).collect::<Vec<_>>().join("|"))
        .collect()
}

/// The five that carried the bug, plus the ones added alongside them.
const QUERIES: &[&str] = &[
    "CALL algo.pageRank() YIELD node, score RETURN node.name AS n, score",
    "CALL algo.wcc() YIELD node, componentId RETURN node.name AS n, componentId AS c",
    "CALL algo.scc() YIELD node, componentId RETURN node.name AS n, componentId AS c",
    "CALL algo.cdlp() YIELD node, communityId RETURN node.name AS n, communityId AS c",
    "CALL algo.lcc() YIELD node, coefficient RETURN node.name AS n, coefficient AS x",
    "CALL algo.louvain() YIELD node, communityId RETURN node.name AS n, communityId AS c",
    "CALL algo.degree() YIELD node, score RETURN node.name AS n, score",
    "CALL algo.betweenness() YIELD node, score RETURN node.name AS n, score",
    "CALL algo.closeness() YIELD node, score RETURN node.name AS n, score",
    "CALL algo.harmonic() YIELD node, score RETURN node.name AS n, score",
    "CALL algo.kCore() YIELD node, score RETURN node.name AS n, score",
    "CALL algo.articleRank() YIELD node, score RETURN node.name AS n, score",
    "CALL algo.eccentricity() YIELD node, eccentricity RETURN node.name AS n, eccentricity AS e",
    "CALL algo.averageNeighborDegree() YIELD node, score RETURN node.name AS n, score",
    "CALL algo.triangleCount() YIELD triangles RETURN triangles AS t",
];

#[test]
fn every_algorithm_returns_the_same_order_twice() {
    let s = tie_heavy();
    for q in QUERIES {
        let a = run(&s, q);
        let b = run(&s, q);
        assert_eq!(a, b, "two runs disagreed on row order:\n  {q}");
    }
}

#[test]
fn and_the_same_order_across_ten_runs() {
    // Twice can agree by luck on a small map. Ten times is the check.
    let s = tie_heavy();
    for q in QUERIES {
        let first = run(&s, q);
        for i in 1..10 {
            assert_eq!(first, run(&s, q), "run {i} disagreed:\n  {q}");
        }
    }
}

#[test]
fn the_fixture_actually_has_ties() {
    // A determinism test over a fixture with no ties proves nothing: every
    // key would be unique and any sort would be stable by accident.
    let s = tie_heavy();
    let wcc = run(&s, "CALL algo.wcc() YIELD node, componentId RETURN componentId AS c");
    let distinct: std::collections::HashSet<&String> = wcc.iter().collect();
    assert!(distinct.len() < wcc.len(),
            "the fixture must produce tied keys or this file tests nothing: {wcc:?}");
}
