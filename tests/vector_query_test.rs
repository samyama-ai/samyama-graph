use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::vector::DistanceMetric;
use samyama::query::QueryEngine;

#[test]
fn test_vector_call_query() {
    let mut store = GraphStore::new();
    
    // 1. Setup data
    store.create_vector_index("Person", "embedding", 3, DistanceMetric::Cosine).unwrap();
    
    let mut props1 = std::collections::HashMap::new();
    props1.insert("name".to_string(), "Alice".into());
    props1.insert("embedding".to_string(), PropertyValue::Vector(vec![1.0, 0.0, 0.0]));
    store.create_node_with_properties("default", vec![Label::new("Person")], props1);

    let mut props2 = std::collections::HashMap::new();
    props2.insert("name".to_string(), "Bob".into());
    props2.insert("embedding".to_string(), PropertyValue::Vector(vec![0.0, 1.0, 0.0]));
    store.create_node_with_properties("default", vec![Label::new("Person")], props2);
    
    // 2. Execute query
    let engine = QueryEngine::new();
    // In Cypher, vector literal is [0.1, 0.2, ...] - but our parser might need it to be passed via parameters or we use a hack for now.
    // Let's see if we can parse a simple CALL.
    // Currently our parser supports list literal in value rule.
    let query_str = "CALL db.index.vector.queryNodes('Person', 'embedding', [1.0, 0.1, 0.0], 1) YIELD node, score RETURN node.name, score";
    
    let result = engine.execute(query_str, &store).unwrap();
    
    assert_eq!(result.records.len(), 1);
    let record = &result.records[0];
    
    assert_eq!(record.get("node.name").unwrap().as_property().unwrap().as_string(), Some("Alice"));
    // score should be small
    let score = record.get("score").unwrap().as_property().unwrap().as_float().unwrap();
    assert!(score < 0.1);
}

#[test]
fn test_vector_hybrid_query() {
    let mut store = GraphStore::new();
    
    // 1. Setup data
    store.create_vector_index("Person", "embedding", 2, DistanceMetric::Cosine).unwrap();
    
    let alice = store.create_node("Person");
    store.set_node_property("default", alice, "name", "Alice").unwrap();
    store.set_node_property("default", alice, "embedding", PropertyValue::Vector(vec![1.0, 0.0])).unwrap();

    let bob = store.create_node("Person");
    store.set_node_property("default", bob, "name", "Bob").unwrap();
    store.set_node_property("default", bob, "embedding", PropertyValue::Vector(vec![0.0, 1.0])).unwrap();

    let charlie = store.create_node("Person");
    store.set_node_property("default", charlie, "name", "Charlie").unwrap();
    store.set_node_property("default", charlie, "embedding", PropertyValue::Vector(vec![1.0, 0.1])).unwrap();
    
    // Edges: Alice -> Charlie
    store.create_edge(alice, charlie, "KNOWS").unwrap();
    
    // 2. Execute hybrid query: Find nodes similar to [1, 0] AND connected to someone
    // MATCH (n)-[:KNOWS]->(m) 
    // CALL db.index.vector.queryNodes('Person', 'embedding', [1.0, 0.0], 10) YIELD node
    // WHERE n = node
    // RETURN n.name, m.name
    
    // Simplest hybrid: CALL then MATCH
    let query_str = "CALL db.index.vector.queryNodes('Person', 'embedding', [1.0, 0.0], 10) YIELD node MATCH (node)-[:KNOWS]->(friend) RETURN node.name, friend.name";
    
    let engine = QueryEngine::new();
    let result = engine.execute(query_str, &store).unwrap();
    
    // Should find Alice -> Charlie (Alice is most similar to [1, 0])
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].get("node.name").unwrap().as_property().unwrap().as_string(), Some("Alice"));
    assert_eq!(result.records[0].get("friend.name").unwrap().as_property().unwrap().as_string(), Some("Charlie"));
}

#[test]
fn test_create_vector_index_query() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    
    // 1. Create index via Cypher
    let ddl = "CREATE VECTOR INDEX person_idx FOR (n:Person) ON (n.embedding) OPTIONS {dimensions: 3, similarity: 'cosine'}";
    engine.execute_mut(ddl, &mut store, "default").unwrap();
    
    // 2. Add data
    engine.execute_mut("CREATE (n:Person {name: 'Alice', embedding: [1.0, 0.0, 0.0]})", &mut store, "default").unwrap();
    
    // 3. Query
    let query = "CALL db.index.vector.queryNodes('Person', 'embedding', [1.0, 0.1, 0.0], 1) YIELD node RETURN node.name";
    let result = engine.execute(query, &store).unwrap();
    
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].get("node.name").unwrap().as_property().unwrap().as_string(), Some("Alice"));
}

// ---------------------------------------------------------------------------
// CREATE VECTOR INDEX option validation (#474)
//
// Unrecognised OPTIONS keys were discarded silently, so the index was built
// with the 1536-dimension default and the statement reported success. The
// failure surfaced only at query time, in a message that pointed at the
// caller's vector rather than at the index they had misconfigured -- so the
// natural reading was "my embedding is the wrong size".
// ---------------------------------------------------------------------------

fn create_index_with(options: &str) -> Result<(), String> {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut("CREATE (:T {emb: [0.1,0.2,0.3,0.4]})", &mut store, "default")
        .unwrap();
    engine
        .execute_mut(
            &format!("CREATE VECTOR INDEX ix FOR (n:T) ON (n.emb) OPTIONS {options}"),
            &mut store,
            "default",
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[test]
fn the_documented_options_spelling_works_end_to_end() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut("CREATE (:T {emb: [0.1,0.2,0.3,0.4]})", &mut store, "default")
        .unwrap();
    engine
        .execute_mut(
            "CREATE VECTOR INDEX ix FOR (n:T) ON (n.emb) OPTIONS {dimensions: 4, similarity: \"cosine\"}",
            &mut store,
            "default",
        )
        .unwrap();
    // The point of the test is that a 4-dimensional probe now works, i.e. the
    // option was actually applied rather than accepted and dropped.
    let batch = engine
        .execute(
            "CALL db.index.vector.queryNodes(\"T\", \"emb\", [0.1,0.2,0.3,0.4], 5) YIELD node RETURN count(node) AS n",
            &store,
        )
        .expect("4-dim query against a 4-dim index");
    assert_eq!(batch.records.len(), 1);
}

#[test]
fn a_singular_dimension_key_is_rejected_rather_than_ignored() {
    // `dimension` vs `dimensions` is a coin flip without reading the source.
    let err = create_index_with("{dimension: 4, similarity: \"cosine\"}").unwrap_err();
    assert!(err.contains("unknown option"), "{err}");
    assert!(err.contains("dimension"), "{err}");
    assert!(err.contains("dimensions"), "the error should name the accepted spelling: {err}");
}

#[test]
fn unknown_option_keys_are_all_named() {
    let err = create_index_with("{banana: 4, wibble: \"x\"}").unwrap_err();
    assert!(err.contains("banana"), "{err}");
    assert!(err.contains("wibble"), "both unknown keys should be reported: {err}");
}

#[test]
fn option_values_of_the_wrong_type_are_rejected() {
    let err = create_index_with("{dimensions: \"4\"}").unwrap_err();
    assert!(err.contains("positive integer"), "{err}");

    let err = create_index_with("{dimensions: 0}").unwrap_err();
    assert!(err.contains("positive integer"), "zero is not a usable dimension: {err}");
}

#[test]
fn the_dimension_mismatch_error_points_at_the_index_not_the_vector() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut("CREATE (:T {emb: [0.1,0.2,0.3,0.4]})", &mut store, "default")
        .unwrap();
    // Omitting `dimensions` is still allowed and still defaults to 1536; what
    // must not happen is an error that reads as though the caller's vector is
    // at fault.
    engine
        .execute_mut("CREATE VECTOR INDEX ix FOR (n:T) ON (n.emb) OPTIONS {}", &mut store, "default")
        .unwrap();
    let err = engine
        .execute(
            "CALL db.index.vector.queryNodes(\"T\", \"emb\", [0.1,0.2,0.3,0.4], 5) YIELD node RETURN count(node) AS n",
            &store,
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("the index expects"), "{err}");
    assert!(err.contains("dimensions: 4"), "the error should show the fix: {err}");
    assert!(err.contains("default is 1536"), "and where 1536 came from: {err}");
}
