//! MERGE binds the relationships and paths its pattern names (#903).
//!
//! ```cypher
//! MERGE (a)-[r:R]->(b) RETURN r   -- VariableNotFound("r")
//! MERGE p = (a)-[:R]->(b) RETURN p -- VariableNotFound("p")
//! ```
//!
//! `merge_path` bound the node positions and stopped. The relationship
//! variables were carried through the whole search — collected from the
//! pattern, threaded into the backtracking — and then dropped, so a named path
//! had nothing to build from either.
//!
//! The search also compared **type and endpoints only**, ignoring the
//! properties it was given. `MERGE (a)-[:R {k: 1}]->(b)` matched a bare `:R`
//! edge and left the graph without the property the query asked for — the same
//! shape as #893, where the candidate set was built from less than the pattern
//! actually said.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) -> Vec<samyama::query::executor::Record> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` parses: {e:?}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` runs: {e:?}"))
        .records
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("count query parses");
    QueryExecutor::new(store).execute(&q).expect("count runs").records.len()
}

/// On the create branch and on the match branch — the same name, both times.
#[test]
fn a_relationship_variable_is_bound_whether_created_or_matched() {
    let mut store = GraphStore::new();

    let created = run(&mut store, "MERGE (a:A)-[r:R]->(b:B) RETURN r");
    assert!(
        matches!(created.first().and_then(|r| r.get("r")), Some(Value::EdgeRef(..)) | Some(Value::Edge(..))),
        "created: {:?}", created.first()
    );

    let matched = run(&mut store, "MERGE (a:A)-[r:R]->(b:B) RETURN r");
    assert!(
        matches!(matched.first().and_then(|r| r.get("r")), Some(Value::EdgeRef(..)) | Some(Value::Edge(..))),
        "matched: {:?}", matched.first()
    );
    assert_eq!(count(&store, "MATCH ()-[r:R]->() RETURN r"), 1, "the second MERGE matched");
}

#[test]
fn a_named_path_is_bound_on_both_branches() {
    let mut store = GraphStore::new();
    for _ in 0..2 {
        let rows = run(&mut store, "MERGE p = (a:A)-[:R]->(b:B) RETURN p");
        match rows.first().and_then(|r| r.get("p")) {
            Some(Value::Path { nodes, edges }) => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(edges.len(), 1);
            }
            other => panic!("expected a path, got {other:?}"),
        }
    }
}

/// The properties are part of what the pattern asks for.
#[test]
fn relationship_properties_are_part_of_the_match() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A)-[:R]->(:B)");
    run(&mut store, "MERGE (a:A)-[:R {k: 1}]->(b:B)");
    assert_eq!(
        count(&store, "MATCH ()-[r:R]->() RETURN r"),
        2,
        "the bare edge does not satisfy `{{k: 1}}`, so the pattern is created"
    );
    assert_eq!(count(&store, "MATCH ()-[r:R {k: 1}]->() RETURN r"), 1);
}

/// And an edge that does satisfy them is matched, not duplicated.
#[test]
fn a_relationship_with_the_right_properties_is_matched() {
    let mut store = GraphStore::new();
    run(&mut store, "MERGE (a:A)-[:R {k: 1}]->(b:B)");
    run(&mut store, "MERGE (a:A)-[:R {k: 1}]->(b:B)");
    assert_eq!(count(&store, "MATCH ()-[r:R]->() RETURN r"), 1);
    let rows = run(&mut store, "MATCH ()-[r:R]->() RETURN r.k AS k");
    assert_eq!(
        rows.first().and_then(|r| r.get("k")),
        Some(&Value::Property(PropertyValue::Integer(1)))
    );
}
