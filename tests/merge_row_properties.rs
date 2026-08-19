//! MERGE can key on a value that came from the row.
//!
//! ```cypher
//! UNWIND $rows AS row MERGE (n:N {id: row.id})
//! ```
//!
//! is the bulk-upsert idiom, and the planner used to refuse it outright:
//! "MERGE does not yet support a non-literal property value". Refusing was the
//! right call at the time — `property_exprs` was not evaluated at all, so MERGE
//! matched on the labels alone and every row found the first `:N`, creating one
//! node where the query asks for one per distinct key (#642).
//!
//! Two things had to be true for this to work, and each was separately broken:
//! the properties must be resolved against the row, and the MERGE must be able
//! to *see* the row. The node-only branch of the planner assigned over its
//! input operator while its comment claimed to use it, so the clause ran once
//! with nothing bound.
//!
//! The property that matters throughout is that the resolved values decide
//! **both** the match and the creation. A MERGE that searched on one set of
//! values and wrote another would create a node its own pattern could not
//! find, and running the query twice would make two — which is why every test
//! here runs its query a second time.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .len()
}

#[test]
fn merge_keyed_on_an_unwound_value_creates_one_node_per_distinct_key() {
    let mut store = GraphStore::new();
    run(&mut store, "UNWIND ['a', 'b', 'a'] AS x MERGE (n:N {v: x})");
    assert_eq!(count(&store, "MATCH (n:N) RETURN n"), 2, "'a' twice is still one node");
    assert_eq!(count(&store, "MATCH (n:N {v: 'a'}) RETURN n"), 1);
    assert_eq!(count(&store, "MATCH (n:N {v: 'b'}) RETURN n"), 1);
}

#[test]
fn running_the_upsert_again_changes_nothing() {
    // The real test of match-and-create agreeing. If MERGE searched on one set
    // of values and wrote another, the second run would double the graph.
    let mut store = GraphStore::new();
    for _ in 0..3 {
        run(&mut store, "UNWIND ['a', 'b', 'a'] AS x MERGE (n:N {v: x})");
        assert_eq!(count(&store, "MATCH (n:N) RETURN n"), 2);
    }
}

#[test]
fn merge_can_key_on_a_property_of_a_bound_node() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:Src {k: 'x'}), (:Src {k: 'y'})");
    run(&mut store, "MATCH (a:Src) MERGE (n:N {v: a.k})");
    assert_eq!(count(&store, "MATCH (n:N) RETURN n"), 2);
    assert_eq!(count(&store, "MATCH (n:N {v: 'x'}) RETURN n"), 1);

    run(&mut store, "MATCH (a:Src) MERGE (n:N {v: a.k})");
    assert_eq!(count(&store, "MATCH (n:N) RETURN n"), 2, "still idempotent");
}

#[test]
fn a_whole_pattern_merge_can_key_on_the_row() {
    // No MATCH binds these endpoints, so this is a whole-pattern merge rather
    // than the "wire an edge between two bound nodes" shape. Routing it to the
    // latter created nothing at all and reported success.
    let mut store = GraphStore::new();
    run(&mut store, "UNWIND [1, 2, 1] AS i MERGE (a:A {id: i})-[:R {w: i}]->(b:B {id: i})");
    assert_eq!(count(&store, "MATCH (n:A) RETURN n"), 2);
    assert_eq!(count(&store, "MATCH (n:B) RETURN n"), 2);
    assert_eq!(count(&store, "MATCH ()-[r:R]->() RETURN r"), 2);

    run(&mut store, "UNWIND [1, 2, 1] AS i MERGE (a:A {id: i})-[:R {w: i}]->(b:B {id: i})");
    assert_eq!(count(&store, "MATCH ()-[r:R]->() RETURN r"), 2, "still idempotent");
}

#[test]
fn merge_between_two_bound_nodes_still_reuses_them() {
    // The shape the MATCH-context operator exists for. It must keep working:
    // openCypher creates *fresh* nodes for an absent whole pattern, so binding
    // first is how you attach an edge to existing ones.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A {id: 1}), (:B {id: 1})");
    run(&mut store, "MATCH (a:A), (b:B) MERGE (a)-[:R]->(b)");
    assert_eq!(count(&store, "MATCH (n:A) RETURN n"), 1, "no second A was created");
    assert_eq!(count(&store, "MATCH (n:B) RETURN n"), 1);
    assert_eq!(count(&store, "MATCH ()-[r:R]->() RETURN r"), 1);
}
