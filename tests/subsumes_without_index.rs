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
//! The expectation below is the traversal's answer, which is the definition of
//! the relationship. It fails today and is `#[ignore]`d against #721 rather
//! than weakened, because either resolution — compute it, or refuse — makes it
//! pass, and asserting today's `0` would have to be rewritten by whoever picks
//! it up.

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

/// Without the index, `subsumes` answers `0` where the relationship holds for
/// three nodes. Answering *something* is the bug: an error would be fine.
#[test]
#[ignore = "#721: subsumes answers false for everything with no hierarchy index"]
fn subsumes_without_an_index_does_not_quietly_answer_no() {
    assert_eq!(count(&chain(false), SUBSUMED), 3);
}
