//! A query result can name the plan that produced it (TRUST-06,
//! samyama-graph#1034).
//!
//! `/api/query` carried `engine_version` and `snapshot_version`; the third
//! provenance field, the plan, was missing because the plan never left
//! `QueryExecutor::execute`. The hash has to come from the plan that ran --
//! re-planning in the handler could describe a different one, since the planner
//! reads statistics that move with the data.
//!
//! These tests hold the hash to what it claims: stable for the same plan,
//! different for a different plan, carried by a cached result, recorded on
//! writes, and off unless a caller asks for it.

use samyama::graph::GraphStore;
use samyama::query::executor::operator::OperatorDescription;
use samyama::QueryEngine;

fn store(extra: usize) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..(3 + extra) {
        let n = store.create_node("A");
        store.set_node_property("default", n, "x", i as i64).unwrap();
    }
    for _ in 0..2 {
        store.create_node("B");
    }
    store
}

fn engine() -> QueryEngine {
    QueryEngine::new().with_plan_hash(true)
}

fn hash(engine: &QueryEngine, store: &GraphStore, q: &str) -> Option<u64> {
    engine.execute(q, store).unwrap_or_else(|e| panic!("`{q}`: {e}")).plan_hash
}

#[test]
fn the_same_query_names_the_same_plan() {
    let s = store(0);
    let e = engine();
    let a = hash(&e, &s, "MATCH (n:A) RETURN n.x AS x");
    assert!(a.is_some(), "no plan hash recorded");
    assert_eq!(a, hash(&e, &s, "MATCH (n:A) RETURN n.x AS x"));
    // A second engine, the same answer: nothing process-local goes into it.
    assert_eq!(a, hash(&engine(), &s, "MATCH (n:A) RETURN n.x AS x"));
}

#[test]
fn different_plans_hash_differently() {
    let s = store(0);
    let e = engine();
    let hashes = [
        hash(&e, &s, "MATCH (n:A) RETURN n.x AS x"),
        hash(&e, &s, "MATCH (n:B) RETURN n"),
        hash(&e, &s, "MATCH (n:A) WHERE n.x > 1 RETURN n.x AS x"),
        hash(&e, &s, "MATCH (n:A) RETURN n.x AS x ORDER BY x"),
    ];
    for i in 0..hashes.len() {
        for j in (i + 1)..hashes.len() {
            assert_ne!(hashes[i], hashes[j], "plans {i} and {j} hashed alike");
        }
    }
}

#[test]
fn more_data_under_the_same_plan_names_the_same_plan() {
    let e = engine();
    let q = "MATCH (n:A) RETURN n.x AS x";
    assert_eq!(hash(&e, &store(0), q), hash(&e, &store(20), q));
}

#[test]
fn a_cached_result_names_the_plan_that_computed_it() {
    let s = store(0);
    let e = engine();
    let q = "MATCH (n:A) RETURN n.x AS x";
    let (first, hit) = e.execute_cached(q, &s).unwrap();
    assert!(!hit);
    let (second, hit) = e.execute_cached(q, &s).unwrap();
    assert!(hit, "the second call should be served from the result cache");
    assert!(first.plan_hash.is_some());
    assert_eq!(second.plan_hash, first.plan_hash);
}

#[test]
fn a_write_names_its_plan_too() {
    let mut s = store(0);
    let e = engine();
    let batch = e.execute_mut("CREATE (n:C {x: 1}) RETURN n", &mut s, "default").unwrap();
    assert!(batch.plan_hash.is_some());
}

/// Off unless asked: describing a plan costs every query something.
#[test]
fn off_by_default() {
    let s = store(0);
    assert_eq!(QueryEngine::new().execute("MATCH (n:A) RETURN n", &s).unwrap().plan_hash, None);
}

/// The one data-dependent detail a description carries is a materialised
/// operator's row count; it must not change the hash.
#[test]
fn a_materialised_row_count_is_not_part_of_the_plan() {
    let leaf = |details: &str| OperatorDescription { name: "Materialized".into(), details: details.into(), children: vec![] };
    let root = |child: OperatorDescription| OperatorDescription {
        name: "Project".into(),
        details: "n".into(),
        children: vec![child],
    };
    assert_eq!(root(leaf("3 rows")).structural_hash(), root(leaf("4000 rows")).structural_hash());
    assert_ne!(root(leaf("3 rows")).structural_hash(), root(leaf("3 cols")).structural_hash());
    let other = OperatorDescription { name: "Filter".into(), details: "n".into(), children: vec![leaf("3 rows")] };
    assert_ne!(root(leaf("3 rows")).structural_hash(), other.structural_hash());
}
