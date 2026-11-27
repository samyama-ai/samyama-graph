//! The algorithms built on the shared frontier module are real, callable
//! algorithms, and there are at least as many as ALGO-13 asks (#1695).
//!
//! `samyama_graph_algorithms::frontier::BUILT_ON_FRONTIER` lists the library
//! functions that walk the graph through the framework, and a test in that
//! crate checks each one really does. This side checks what a user sees: every
//! `CALL` name the list claims is in the catalogue `is_algorithm` reads, each
//! one runs from Cypher, and the count of catalogued algorithms on the
//! framework meets the target.

use std::collections::BTreeSet;

use samyama::graph::GraphStore;
use samyama::query::executor::algorithm_catalog::ALGORITHM_CATALOG;
use samyama::query::QueryEngine;
use samyama_graph_algorithms::frontier::{BUILT_ON_FRONTIER, FRONTIER_TARGET};

/// Every catalogued `CALL` name the list says reaches the framework.
fn listed_procedures() -> BTreeSet<&'static str> {
    BUILT_ON_FRONTIER
        .iter()
        .flat_map(|(_, _, calls)| calls.iter().copied())
        .collect()
}

#[test]
fn every_listed_call_name_is_a_catalogued_algorithm() {
    let catalogued: BTreeSet<&str> = ALGORITHM_CATALOG
        .iter()
        .flat_map(|e| std::iter::once(e.name).chain(e.aliases.iter().copied()))
        .collect();
    let missing: Vec<&str> = listed_procedures()
        .into_iter()
        .filter(|p| !catalogued.contains(p))
        .collect();
    assert!(missing.is_empty(), "listed but not callable: {missing:?}");
}

#[test]
fn at_least_the_target_number_of_callable_algorithms_use_the_framework() {
    let n = listed_procedures().len();
    assert!(
        n >= FRONTIER_TARGET,
        "{n} callable algorithms on the frontier, target {FRONTIER_TARGET}"
    );
}

/// A 4-cycle with a two-way tail, so every algorithm below has an answer:
/// strongly connected (the Wiener index refuses anything less),
/// bipartite, and with more than one shortest path between some pairs.
fn graph() -> (GraphStore, Vec<i64>) {
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..5).map(|_| s.create_node("P")).collect();
    for (a, b) in [(0, 1), (1, 2), (2, 3), (3, 0), (3, 4), (4, 3)] {
        s.create_edge(ids[a], ids[b], "L").unwrap();
    }
    // A vector per node, for `algo.graphRag`'s seeds.
    s.create_vector_index("P", "emb", 2, samyama::vector::DistanceMetric::Cosine).unwrap();
    for (i, id) in ids.iter().enumerate() {
        s.set_node_property("default", *id, "emb", samyama::graph::PropertyValue::Vector(vec![1.0, i as f32]))
            .unwrap();
    }
    let raw = ids.iter().map(|id| id.as_u64() as i64).collect();
    (s, raw)
}

#[test]
fn every_listed_algorithm_runs_from_cypher() {
    let (store, ids) = graph();
    let (a, b) = (ids[0], ids[2]);
    let call = |name: &str| -> String {
        match name {
            "algo.shortestPath" | "algo.allShortestPaths" => format!("CALL {name}({a}, {b})"),
            "algo.negativeSampling" => format!("CALL {name}({{strategy: 'khop', k: 2, count: 3}})"),
            "algo.graphRag" | "algo.retrieve" => {
                format!("CALL {name}({{query: [1.0, 0.0], label: 'P', property: 'emb'}})")
            }
            _ => format!("CALL {name}()"),
        }
    };
    let mut failed = Vec::new();
    for name in listed_procedures() {
        let q = call(name);
        match QueryEngine::new().execute(&q, &store) {
            Ok(rows) if !rows.records.is_empty() => {}
            Ok(_) => failed.push(format!("{q}: no rows")),
            Err(e) => failed.push(format!("{q}: {e}")),
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}
