//! `DELETE` refuses a node that still has relationships (#946).
//!
//! ```cypher
//! CREATE (x:X)
//! CREATE (x)-[:R]->(), (x)-[:R]->(), (x)-[:R]->()
//! MATCH (n:X) DELETE n
//! ```
//!
//! succeeded, and took all three relationships with it.
//!
//! The store stayed consistent — `delete_node` removes connected edges itself,
//! so there were never dangling relationships — which is exactly why nothing
//! complained. What actually happened is that **`DELETE n` silently behaved as
//! `DETACH DELETE n`**.
//!
//! That is destructive by surprise. The reason Cypher separates the two is
//! that deleting a node's relationships is a decision the user has to make out
//! loud; a user writing `DELETE n` is relying on the engine to stop them.
//!
//! So these tests count relationships, not rows.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) -> Result<(), String> {
    let query = parse_query(cypher).map_err(|e| format!("{e:?}"))?;
    let mut ex = MutQueryExecutor::new(store, "default".to_string());
    ex.execute(&query).map(|_| ()).map_err(|e| format!("{e:?}"))
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap();
    match QueryExecutor::new(store).execute(&q).unwrap().records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{other:?}"),
    }
}

fn nodes(store: &GraphStore) -> i64 { count(store, "MATCH (n) RETURN count(n) AS c") }
fn edges(store: &GraphStore) -> i64 { count(store, "MATCH ()-[r]->() RETURN count(r) AS c") }

/// `(x:X)` with three outgoing `:R` relationships.
fn hub() -> GraphStore {
    let mut store = GraphStore::new();
    let x = store.create_node("X");
    for _ in 0..3 {
        let o = store.create_node("");
        store.create_edge(x, o, "R").unwrap();
    }
    store
}

#[test]
fn deleting_a_connected_node_is_refused() {
    let mut store = hub();
    let err = run(&mut store, "MATCH (n:X) DELETE n").unwrap_err();
    assert!(err.contains("ConstraintVerificationFailed"), "{err}");
    // And nothing was destroyed on the way to refusing.
    assert_eq!(nodes(&store), 4);
    assert_eq!(edges(&store), 3);
}

#[test]
fn the_message_says_what_to_do_instead() {
    let mut store = hub();
    let err = run(&mut store, "MATCH (n:X) DELETE n").unwrap_err();
    assert!(err.contains("DETACH DELETE"), "{err}");
    assert!(err.contains('3'), "says how many relationships: {err}");
}

#[test]
fn detach_delete_still_works() {
    let mut store = hub();
    run(&mut store, "MATCH (n:X) DETACH DELETE n").unwrap();
    assert_eq!(nodes(&store), 3);
    assert_eq!(edges(&store), 0);
}

#[test]
fn an_unconnected_node_deletes_normally() {
    let mut store = hub();
    run(&mut store, "MATCH (n) WHERE NOT (n:X) DELETE n").unwrap_err(); // they are connected too
    let mut store2 = GraphStore::new();
    store2.create_node("Lonely");
    run(&mut store2, "MATCH (n:Lonely) DELETE n").unwrap();
    assert_eq!(nodes(&store2), 0);
}

#[test]
fn deleting_the_relationship_first_makes_the_node_deletable() {
    // The reason the check is at delete time and not at plan time:
    // `DELETE r, n` is legal because relationships go first, so by the time
    // the node is reached it is unconnected.
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    store.create_edge(a, b, "R").unwrap();
    run(&mut store, "MATCH (a:A)-[r]->(b:B) DELETE r, a").unwrap();
    assert_eq!(edges(&store), 0);
    assert_eq!(nodes(&store), 1, "only b remains");
}

#[test]
fn an_incoming_relationship_counts_too() {
    // Checking only outgoing edges would let `DELETE b` cascade on
    // `(a)-[r]->(b)`.
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    store.create_edge(a, b, "R").unwrap();
    let err = run(&mut store, "MATCH (b:B) DELETE b").unwrap_err();
    assert!(err.contains("ConstraintVerificationFailed"), "{err}");
    assert_eq!(edges(&store), 1);
}

#[test]
fn deleting_a_relationship_alone_is_unaffected() {
    let mut store = hub();
    run(&mut store, "MATCH ()-[r:R]->() DELETE r").unwrap();
    assert_eq!(edges(&store), 0);
    assert_eq!(nodes(&store), 4);
}
