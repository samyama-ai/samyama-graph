//! The graph-native, cost-based planner plans reads by default (#1729).
//!
//! VIS-06's plan diagram reads the cost block EXPLAIN prints --
//! `Candidates evaluated:` and `Chosen plan cost:` -- and a default server
//! printed none of it, because the planner that produces it was behind
//! `SAMYAMA_GRAPH_NATIVE=true`. It is on unless that variable says otherwise,
//! and `SAMYAMA_GRAPH_NATIVE=false` (or `0`) is the way back.
//!
//! One test, in its own binary: it sets the process environment, which other
//! tests running in parallel would otherwise see.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn explain(store: &GraphStore) -> String {
    let batch = QueryEngine::new()
        .execute("EXPLAIN MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN q", store)
        .expect("explain");
    format!("{:?}", batch.records[0].get("plan")).replace("\\n", "\n")
}

#[test]
fn reads_are_cost_planned_unless_turned_off() {
    let mut store = GraphStore::new();
    QueryEngine::new()
        .execute_mut(
            "CREATE (:Person {n: 1})-[:KNOWS]->(:Person {n: 2})",
            &mut store,
            "default",
        )
        .unwrap();

    std::env::remove_var("SAMYAMA_GRAPH_NATIVE");
    assert!(samyama::query::graph_native_enabled(), "on when unset");
    let plan = explain(&store);
    assert!(plan.contains("Candidates evaluated:"), "a default server shows the cost block:\n{plan}");
    assert!(plan.contains("Chosen plan cost:"), "{plan}");

    for off in ["false", "0", " false "] {
        std::env::set_var("SAMYAMA_GRAPH_NATIVE", off);
        assert!(!samyama::query::graph_native_enabled(), "{off:?} turns it off");
        let plan = explain(&store);
        assert!(!plan.contains("Candidates evaluated:"), "{off:?}: the rule-based planner has no cost block:\n{plan}");
    }
    for on in ["true", "1", ""] {
        std::env::set_var("SAMYAMA_GRAPH_NATIVE", on);
        assert!(samyama::query::graph_native_enabled(), "{on:?} leaves it on");
    }
    std::env::remove_var("SAMYAMA_GRAPH_NATIVE");
}
