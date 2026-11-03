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

// ---------------------------------------------------------------------------
// The relationship half: a MERGE that wires an edge between endpoints a MATCH
// already bound, keyed on a value from the row.
//
// ```cypher
// UNWIND $rows AS r MATCH (s {id: r.src}), (d {id: r.dst}) MERGE (s)-[:T {id: r.id}]->(d)
// ```
//
// is the bulk edge-upsert idiom, and it goes to `MatchMergeEdgeOperator`,
// which was only ever handed the literal half of the property map. The
// expressions were dropped: MERGE matched any `:T` between the pair, and when
// it created one, created it without the property. Two rows for one pair wrote
// ONE edge with no `id`, and the statement reported success. Found loading the
// INT-10 reference pipeline, where 60 transfers became 55 edges (#1715).
// ---------------------------------------------------------------------------

#[test]
fn an_edge_keyed_on_the_row_is_one_edge_per_distinct_key_between_bound_nodes() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A {id: 1}), (:B {id: 1})");
    let upsert = "UNWIND [{id: 't1'}, {id: 't2'}, {id: 't1'}] AS r \
                  MATCH (a:A {id: 1}), (b:B {id: 1}) MERGE (a)-[:T {id: r.id}]->(b)";
    run(&mut store, upsert);
    assert_eq!(count(&store, "MATCH ()-[t:T]->() RETURN t"), 2, "'t1' twice is still one edge");
    assert_eq!(count(&store, "MATCH ()-[t:T {id: 't1'}]->() RETURN t"), 1, "the key was written");
    assert_eq!(count(&store, "MATCH ()-[t:T {id: 't2'}]->() RETURN t"), 1);

    run(&mut store, upsert);
    assert_eq!(count(&store, "MATCH ()-[t:T]->() RETURN t"), 2, "still idempotent");
}

#[test]
fn an_edge_with_a_different_key_is_not_a_match() {
    // The silent half of the defect: the existing edge was "matched" whatever
    // its key, so a later SET overwrote it instead of creating a second.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (a:A {id: 1})-[:T {id: 'old', amount: 1}]->(b:B {id: 1})");
    run(
        &mut store,
        "UNWIND [{id: 'new', amount: 2}] AS r MATCH (a:A {id: 1}), (b:B {id: 1}) \
         MERGE (a)-[t:T {id: r.id}]->(b) SET t.amount = r.amount",
    );
    assert_eq!(count(&store, "MATCH ()-[t:T]->() RETURN t"), 2);
    assert_eq!(count(&store, "MATCH ()-[t:T {id: 'old', amount: 1}]->() RETURN t"), 1, "untouched");
    assert_eq!(count(&store, "MATCH ()-[t:T {id: 'new', amount: 2}]->() RETURN t"), 1);
}

#[test]
fn an_edge_can_key_on_a_property_of_another_bound_node() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A {id: 1}), (:B {id: 1}), (:Src {k: 'x'}), (:Src {k: 'y'})");
    let merge = "MATCH (s:Src) MATCH (a:A {id: 1}), (b:B {id: 1}) MERGE (a)-[:T {k: s.k}]->(b)";
    run(&mut store, merge);
    run(&mut store, merge);
    assert_eq!(count(&store, "MATCH ()-[t:T]->() RETURN t"), 2);
    assert_eq!(count(&store, "MATCH ()-[t:T {k: 'x'}]->() RETURN t"), 1);
    assert_eq!(count(&store, "MATCH ()-[t:T {k: 'y'}]->() RETURN t"), 1);
}

#[test]
fn literal_and_row_properties_on_one_edge_must_both_match() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (a:A {id: 1})-[:T {kind: 'other', id: 't1'}]->(b:B {id: 1})");
    let merge = "UNWIND ['t1'] AS x MATCH (a:A {id: 1}), (b:B {id: 1}) \
                 MERGE (a)-[:T {kind: 'wire', id: x}]->(b)";
    run(&mut store, merge);
    run(&mut store, merge);
    assert_eq!(count(&store, "MATCH ()-[t:T]->() RETURN t"), 2, "kind differs, so no match");
    assert_eq!(count(&store, "MATCH ()-[t:T {kind: 'wire', id: 't1'}]->() RETURN t"), 1);
}

#[test]
fn on_create_and_on_match_see_the_row_keyed_edge() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A {id: 1}), (:B {id: 1})");
    let merge = "UNWIND ['t1'] AS x MATCH (a:A {id: 1}), (b:B {id: 1}) \
                 MERGE (a)-[t:T {id: x}]->(b) ON CREATE SET t.created = true ON MATCH SET t.matched = true";
    run(&mut store, merge);
    assert_eq!(count(&store, "MATCH ()-[t:T {id: 't1', created: true}]->() RETURN t"), 1);
    assert_eq!(count(&store, "MATCH ()-[t:T]->() WHERE t.matched IS NULL RETURN t"), 1);
    run(&mut store, merge);
    assert_eq!(count(&store, "MATCH ()-[t:T {id: 't1', matched: true}]->() RETURN t"), 1);
    assert_eq!(count(&store, "MATCH ()-[t:T]->() RETURN t"), 1);
}

#[test]
fn an_undirected_edge_keyed_on_the_row_matches_either_way_round() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (a:A {id: 1})<-[:T {id: 't1'}]-(b:B {id: 1})");
    let merge = "UNWIND ['t1', 't2'] AS x MATCH (a:A {id: 1}), (b:B {id: 1}) MERGE (a)-[:T {id: x}]-(b)";
    run(&mut store, merge);
    run(&mut store, merge);
    assert_eq!(count(&store, "MATCH ()-[t:T]->() RETURN t"), 2, "t1 existed reversed; only t2 is new");
}
