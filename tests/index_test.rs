use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;

#[test]
fn test_property_index_usage() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create index first
    store.property_index.create_index(Label::new("Person"), "id".to_string());
    
    // Create nodes
    for i in 0..100 {
        let mut props = std::collections::HashMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        store.create_node_with_properties("default", vec![Label::new("Person")], props);
    }

    // Query with index
    // MATCH (n:Person) WHERE n.id = 50 RETURN n
    let query = "MATCH (n:Person) WHERE n.id = 50 RETURN n";
    let result = engine.execute(query, &store).unwrap();
    
    assert_eq!(result.records.len(), 1);
    let node_val = result.records[0].get("n").unwrap();
    let (_, node) = node_val.as_node().unwrap();
    assert_eq!(node.get_property("id").unwrap().as_integer(), Some(50));
}

#[test]
fn test_property_index_range() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    store.property_index.create_index(Label::new("Product"), "price".to_string());
    
    for i in 0..10 {
        let mut props = std::collections::HashMap::new();
        props.insert("price".to_string(), PropertyValue::Integer(i * 10));
        store.create_node_with_properties("default", vec![Label::new("Product")], props);
    }

    // MATCH (n:Product) WHERE n.price > 50 RETURN n
    let query = "MATCH (n:Product) WHERE n.price > 50 RETURN n";
    let result = engine.execute(query, &store).unwrap();
    
    // Should return 60, 70, 80, 90 (4 items)
    assert_eq!(result.records.len(), 4);
}

#[test]
fn test_create_index_ddl() {
    let mut store = GraphStore::new();
    let mut engine = QueryEngine::new();

    // 1. Create nodes
    for i in 0..10 {
        let mut props = std::collections::HashMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        store.create_node_with_properties("default", vec![Label::new("User")], props);
    }

    // 2. Create index using DDL (should backfill)
    let ddl = "CREATE INDEX ON :User(id)";
    engine.execute_mut(ddl, &mut store).unwrap();

    // 3. Verify index exists
    assert!(store.property_index.has_index(&Label::new("User"), "id"));

    // 4. Query using index
    let query = "MATCH (n:User) WHERE n.id = 5 RETURN n";
    let result = engine.execute(query, &store).unwrap();
    
    assert_eq!(result.records.len(), 1);
    let (_, node) = result.records[0].get("n").unwrap().as_node().unwrap();
    assert_eq!(node.get_property("id").unwrap().as_integer(), Some(5));
}
