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
    props1.insert("embedding".to_string(), PropertyValue::Vector(vec![1.0, 0.0]));
    store.create_node_with_properties("default", vec![Label::new("Person")], props1);
    
    let mut props2 = std::collections::HashMap::new();
    props2.insert("name".to_string(), "Bob".into());
    props2.insert("embedding".to_string(), PropertyValue::Vector(vec![0.0, 1.0]));
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
    engine.execute_mut(ddl, &mut store).unwrap();
    
    // 2. Add data
    engine.execute_mut("CREATE (n:Person {name: 'Alice', embedding: [1.0, 0.0, 0.0]})", &mut store).unwrap();
    
    // 3. Query
    let query = "CALL db.index.vector.queryNodes('Person', 'embedding', [1.0, 0.1, 0.0], 1) YIELD node RETURN node.name";
    let result = engine.execute(query, &store).unwrap();
    
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].get("node.name").unwrap().as_property().unwrap().as_string(), Some("Alice"));
}
