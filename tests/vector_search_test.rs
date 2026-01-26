use samyama::graph::{GraphStore, Label, PropertyValue, PropertyMap};
use samyama::vector::DistanceMetric;

#[test]
fn test_vector_search_integration() {
    let mut store = GraphStore::new();
    
    // 1. Create a vector index
    store.create_vector_index("Person", "embedding", 3, DistanceMetric::Cosine).unwrap();
    
    // 2. Create nodes with vector properties
    let mut props1 = PropertyMap::new();
    props1.insert("name".to_string(), "Alice".into());
    props1.insert("embedding".to_string(), PropertyValue::Vector(vec![1.0, 0.0, 0.0]));
    store.create_node_with_properties("default", vec![Label::new("Person")], props1);

    let mut props2 = PropertyMap::new();
    props2.insert("name".to_string(), "Bob".into());
    props2.insert("embedding".to_string(), PropertyValue::Vector(vec![0.0, 1.0, 0.0]));
    store.create_node_with_properties("default", vec![Label::new("Person")], props2);

    let mut props3 = PropertyMap::new();
    props3.insert("name".to_string(), "Charlie".into());
    props3.insert("embedding".to_string(), PropertyValue::Vector(vec![0.0, 0.0, 1.0]));
    store.create_node_with_properties("default", vec![Label::new("Person")], props3);

    
    // 3. Search
    // Query vector is closest to Alice [1, 0, 0]
    let query = vec![0.9, 0.1, 0.0];
    let results = store.vector_search("Person", "embedding", &query, 1).unwrap();
    
    assert_eq!(results.len(), 1);
    let (node_id, distance) = results[0];
    
    let node = store.get_node(node_id).unwrap();
    assert_eq!(node.get_property("name").unwrap().as_string(), Some("Alice"));
    assert!(distance < 0.1); // Distance should be small
}

#[test]
fn test_vector_search_update() {
    let mut store = GraphStore::new();
    store.create_vector_index("Person", "embedding", 2, DistanceMetric::Cosine).unwrap();
    
    let node_id = store.create_node("Person");
    store.add_label_to_node("default", node_id, "Person").unwrap();
    
    // Initially not in index (no vector property)
    let results = store.vector_search("Person", "embedding", &vec![1.0, 0.0], 1).unwrap();
    assert_eq!(results.len(), 0);
    
    // Update property
    store.set_node_property("default", node_id, "embedding", PropertyValue::Vector(vec![1.0, 0.0])).unwrap();
    
    // Now should be found
    let results = store.vector_search("Person", "embedding", &vec![1.0, 0.0], 1).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, node_id);
}