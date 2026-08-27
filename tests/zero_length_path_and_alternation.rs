//! Two patterns that parsed nowhere and bound nothing (#909).
//!
//! ```cypher
//! MATCH (a)-[:T|:T]->(b) RETURN b   -- parse error
//! MATCH p = (a) RETURN p            -- VariableNotFound("p")
//! ```
//!
//! `:A|:B` repeats the colon on each alternative, which is the spelling
//! openCypher's own scenarios use; only `:A|B` and `:A:B` parsed, so the query
//! failed outright rather than matching either type.
//!
//! A named path is bound by the expand that walks it — so a pattern with **no
//! segments** had no expand and nothing bound `p`. The zero-length path is the
//! one case where there is nothing to walk, and it is exactly the case the
//! walking code cannot reach.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn setup(cypher: &str) -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query(cypher).expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

fn rows(store: &GraphStore, cypher: &str) -> Vec<samyama::query::executor::Record> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` parses: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` runs: {e}"))
        .records
}

/// Every spelling of the alternation means the same thing.
#[test]
fn a_repeated_colon_in_a_type_alternation_parses() {
    let store = setup("CREATE (a:A)-[:T]->(b:B)");
    for cypher in [
        "MATCH (a)-[:T]->(b) RETURN b",
        "MATCH (a)-[:T|T]->(b) RETURN b",
        "MATCH (a)-[:T|:T]->(b) RETURN b",
        "MATCH (a)-[:T|:U]->(b) RETURN b",
        "MATCH (a)-[:U|:T]->(b) RETURN b",
        "MATCH (a)-[r:T|:U|:V]->(b) RETURN b",
    ] {
        assert_eq!(rows(&store, cypher).len(), 1, "`{cypher}`");
    }
}

/// A type that is not there still matches nothing — the alternation is a
/// filter, not a wildcard.
#[test]
fn an_alternation_of_absent_types_matches_nothing() {
    let store = setup("CREATE (a:A)-[:T]->(b:B)");
    assert_eq!(rows(&store, "MATCH (a)-[:U|:V]->(b) RETURN b").len(), 0);
}

/// One node, no relationships.
#[test]
fn a_zero_length_named_path_binds() {
    let store = setup("CREATE ()");
    let found = rows(&store, "MATCH p = (a) RETURN p");
    assert_eq!(found.len(), 1);
    match found[0].get("p") {
        Some(Value::Path { nodes, edges }) => {
            assert_eq!(nodes.len(), 1, "the one node it matched");
            assert!(edges.is_empty(), "and no relationships");
        }
        other => panic!("expected a path, got {other:?}"),
    }
}

/// The node variable is still bound alongside it, and a labelled form works.
#[test]
fn the_node_is_bound_beside_the_path() {
    let store = setup("CREATE (:A {v: 1})");
    let found = rows(&store, "MATCH p = (a:A) RETURN p, a");
    assert_eq!(found.len(), 1);
    assert!(matches!(found[0].get("p"), Some(Value::Path { .. })));
    assert!(matches!(
        found[0].get("a"),
        Some(Value::Node(..)) | Some(Value::NodeRef(_))
    ));
}

/// A path *with* segments is untouched — that is the case the expand already
/// handled, and it must keep working.
#[test]
fn a_path_with_segments_still_binds() {
    let store = setup("CREATE (:A)-[:T]->(:B)");
    let found = rows(&store, "MATCH p = (a:A)-[:T]->(b:B) RETURN p");
    assert_eq!(found.len(), 1);
    match found[0].get("p") {
        Some(Value::Path { nodes, edges }) => {
            assert_eq!(nodes.len(), 2);
            assert_eq!(edges.len(), 1);
        }
        other => panic!("expected a path, got {other:?}"),
    }
}
