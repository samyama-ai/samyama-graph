//! A list can hold nodes and relationships, not just property scalars.
//!
//! `PropertyValue::Array` cannot hold an entity, so a list of them had nowhere
//! to live. Two things followed from that, and both looked like small gaps
//! while being the same missing type:
//!
//! ```cypher
//! MATCH (a)-[r:T*]->(b) RETURN r       -- "Variable not found: r"
//! MATCH p = (a)-[:T*]->(b) RETURN nodes(p)  -- [1, 2] — the ids, not the nodes
//! ```
//!
//! `Value::List` is the missing variant. The variable-length relationship
//! variable is the headline case — Cypher defines `r` there as *a list of
//! relationships*, one per hop — but `nodes()` and `relationships()` were
//! quietly answering with integers all along, which is worse: an id is a
//! plausible-looking answer that no property access can be read from (#652).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn chain() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:A)-[:T {i: 1}]->(b:B)-[:T {i: 2}]->(c:C)")
        .expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");
    store
}

fn one(store: &GraphStore, cypher: &str, col: &str) -> Value {
    let q = parse_query(cypher).expect("query should parse");
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    assert!(!out.records.is_empty(), "`{cypher}` returned no rows");
    out.records[0].get(col).cloned().unwrap_or(Value::Null)
}

#[test]
fn a_variable_length_relationship_variable_is_a_list_of_relationships() {
    let store = chain();
    match one(&store, "MATCH (a:A)-[r:T*2]->(x) RETURN r", "r") {
        Value::List(items) => {
            assert_eq!(items.len(), 2, "one element per hop");
            assert!(
                items.iter().all(|i| matches!(i, Value::EdgeRef(..) | Value::Edge(..))),
                "elements are relationships, not ids: {items:?}"
            );
        }
        other => panic!("expected a list, got {other:?}"),
    }
}

#[test]
fn each_relationship_in_the_list_carries_its_real_type() {
    // Built with placeholder endpoints and a blank type at first, which
    // renders as `[:]` — a relationship-shaped hole rather than a
    // relationship.
    let store = chain();
    match one(&store, "MATCH (a:A)-[r:T*2]->(x) RETURN r", "r") {
        Value::List(items) => {
            for item in &items {
                match item {
                    Value::EdgeRef(_, src, tgt, ty) => {
                        assert_eq!(ty.as_str(), "T");
                        assert_ne!(src.as_u64(), tgt.as_u64(), "endpoints are real");
                    }
                    other => panic!("expected an edge ref, got {other:?}"),
                }
            }
        }
        other => panic!("expected a list, got {other:?}"),
    }
}

#[test]
fn size_counts_a_list_of_relationships() {
    let store = chain();
    assert_eq!(
        one(&store, "MATCH (a:A)-[r:T*2]->(x) RETURN size(r) AS n", "n"),
        Value::Property(PropertyValue::Integer(2))
    );
}

#[test]
fn nodes_and_relationships_return_entities_rather_than_ids() {
    let store = chain();
    match one(&store, "MATCH p = (a:A)-[:T*2]->(x) RETURN nodes(p) AS v", "v") {
        Value::List(items) => {
            assert_eq!(items.len(), 3, "three nodes across two hops");
            assert!(items.iter().all(|i| matches!(i, Value::NodeRef(_) | Value::Node(..))));
        }
        other => panic!("nodes() should be a list of nodes, got {other:?}"),
    }
    match one(&store, "MATCH p = (a:A)-[:T*2]->(x) RETURN relationships(p) AS v", "v") {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert!(items.iter().all(|i| matches!(i, Value::EdgeRef(..) | Value::Edge(..))));
        }
        other => panic!("relationships() should be a list of relationships, got {other:?}"),
    }
}

#[test]
fn a_single_hop_relationship_variable_is_unchanged() {
    // `[r]` without a length binds one relationship, not a list of one.
    let store = chain();
    assert!(matches!(
        one(&store, "MATCH (a:A)-[r:T]->(x) RETURN r", "r"),
        Value::Edge(..) | Value::EdgeRef(..)
    ));
}
