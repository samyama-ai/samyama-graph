//! `subsumes()` with no hierarchy index declared (#721).
//!
//! The predicate is backed by the hierarchy index. With no index it does not
//! fall back to a traversal and does not complain — it answers **false for
//! every pair**, so a query returns a smaller, wrong number that looks like a
//! legitimate empty result.
//!
//! That also disables `examples/hier_benchmark`'s correctness gate for the 31
//! of 112 corpus entries whose baseline re-runs the same Cypher: for those the
//! "ground truth" is the same index-dependent query with the index taken away.
//!
//! Resolved by refusing. There is no traversal fallback to offer: without a
//! declaration there is no relationship type to walk, because the hierarchy
//! *is* the declaration. So the function now errors and names what is missing.
//!
//! The distinction the fix preserves: two nodes outside every hierarchy, when
//! hierarchies exist, are still legitimately `false`. Only "there is no
//! hierarchy at all, or every one is stale" is an error.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    match out.records.first().and_then(|r| r.get("n")) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("expected integer n, got {other:?}"),
    }
}

fn error(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    match QueryExecutor::new(store).execute(&q) {
        Ok(out) => panic!("`{cypher}` should have failed, got {:?}", out.records),
        Err(e) => e.to_string(),
    }
}

/// `c -[:BROADER]-> b -[:BROADER]-> a`, optionally with the index declared.
fn chain(with_index: bool) -> GraphStore {
    let mut store = GraphStore::new();
    for c in ["a", "b", "c"] {
        run(&mut store, &format!("CREATE (:Term {{code: \"{c}\"}})"));
    }
    for (x, y) in [("b", "a"), ("c", "b")] {
        run(
            &mut store,
            &format!(
                "MATCH (x:Term {{code:\"{x}\"}}), (y:Term {{code:\"{y}\"}}) \
                 CREATE (x)-[:BROADER]->(y)"
            ),
        );
    }
    if with_index {
        run(
            &mut store,
            "CREATE HIERARCHY INDEX t ON ()-[:BROADER]->() MEASURE units AGGREGATE sum, count",
        );
    }
    store
}

const SUBSUMED: &str =
    "MATCH (d:Term), (r:Term {code:\"a\"}) WHERE subsumes(d, r) RETURN count(d) AS n";
const BY_TRAVERSAL: &str =
    "MATCH (r:Term {code:\"a\"})<-[:BROADER*0..]-(d:Term) RETURN count(d) AS n";

/// The index and the traversal agree — this is what "subsumed by a" means.
#[test]
fn the_index_agrees_with_the_traversal() {
    let store = chain(true);
    assert_eq!(count(&store, SUBSUMED), 3);
    assert_eq!(count(&store, BY_TRAVERSAL), 3);
}

/// The traversal does not need the index, so the answer does not change.
#[test]
fn the_traversal_needs_no_index() {
    assert_eq!(count(&chain(false), BY_TRAVERSAL), 3);
}

/// Without any hierarchy declared, `subsumes` refuses instead of answering
/// `false` for every pair — which is what it used to do, giving `0` where the
/// relationship holds for three nodes.
#[test]
fn subsumes_without_an_index_refuses_rather_than_answering_no() {
    let why = error(&chain(false), SUBSUMED);
    assert!(why.contains("subsumes()"), "{why}");
    assert!(why.contains("no hierarchy index is declared"), "{why}");
    // The message has to say what to do about it, not just what is wrong.
    assert!(why.contains("CREATE HIERARCHY INDEX"), "{why}");
}

/// `hierarchy_rollup` and `hierarchy_lca` degrade the same way — to null and
/// to an empty list — and get the same guard.
#[test]
fn the_other_hierarchy_functions_refuse_too() {
    let store = chain(false);
    let rollup = error(
        &store,
        "MATCH (r:Term {code:\"a\"}) RETURN hierarchy_rollup(r, \"sum\") AS n",
    );
    assert!(rollup.contains("hierarchy_rollup()"), "{rollup}");
    let lca = error(
        &store,
        "MATCH (a:Term {code:\"b\"}), (b:Term {code:\"c\"}) RETURN hierarchy_lca(a, b) AS n",
    );
    assert!(lca.contains("hierarchy_lca()"), "{lca}");
}

/// Two nodes outside every hierarchy, where a hierarchy *does* exist, are
/// still `false`. That was the deliberate decision behind the old behaviour
/// and it is right — the fix narrows it rather than reversing it.
#[test]
fn nodes_outside_an_existing_hierarchy_are_still_false() {
    let mut store = chain(true);
    run(&mut store, "CREATE (:Other {code: \"x\"})");
    run(&mut store, "CREATE (:Other {code: \"y\"})");
    assert_eq!(
        count(
            &store,
            "MATCH (d:Other {code:\"x\"}), (r:Other {code:\"y\"}) \
             WHERE subsumes(d, r) RETURN count(d) AS n"
        ),
        0
    );
}
