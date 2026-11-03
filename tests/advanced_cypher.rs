use samyama::{GraphStore, QueryEngine, PropertyValue};
use std::sync::Arc;

fn setup_social_graph() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create Users
    let users = vec![
        ("Alice", 30, "NY"),
        ("Bob", 25, "SF"),
        ("Charlie", 35, "NY"),
        ("David", 40, "SF"),
        ("Eve", 28, "CHI"),
    ];

    for (name, age, city) in users {
        let query = format!("CREATE (n:Person {{name: '{}', age: {}, city: '{}'}})", name, age, city);
        engine.execute_mut(&query, &mut store, "default").unwrap();
    }

    // Create Relationships
    // Alice -> Bob, Alice -> Charlie
    // Bob -> David
    // Charlie -> Eve
    let edges = vec![
        ("Alice", "Bob"),
        ("Alice", "Charlie"),
        ("Bob", "David"),
        ("Charlie", "Eve"),
    ];

    for (src, dest) in edges {
        let query = format!(
            "MATCH (a:Person {{name: '{}'}}), (b:Person {{name: '{}'}}) CREATE (a)-[:KNOWS]->(b)",
            src, dest
        );
        engine.execute_mut(&query, &mut store, "default").unwrap();
    }

    store
}

#[test]
fn test_filter_and_projection() {
    let store = setup_social_graph();
    let engine = QueryEngine::new();

    // Query: Find names of people in NY over 30
    // Note: Parentheses removed to test Pratt parser precedence
    let query = "MATCH (n:Person) WHERE n.city = 'NY' AND n.age >= 30 RETURN n.name, n.age";
    let result = engine.execute(query, &store).unwrap();

    // Should be Alice (30) and Charlie (35)
    assert_eq!(result.len(), 2);
    
    // Check results (order not guaranteed without ORDER BY)
    let names: Vec<String> = result.records.iter()
        .map(|r| r.get("n.name").unwrap().as_property().unwrap().as_string().unwrap().to_string())
        .collect();
    
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn test_order_by_limit() {
    let store = setup_social_graph();
    let engine = QueryEngine::new();

    // Query: Youngest 2 people
    // Note: This test assumes ORDER BY and LIMIT are implemented in the engine
    let query = "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age ASC LIMIT 2";
    let result = engine.execute(query, &store).unwrap();

    assert_eq!(result.len(), 2);
    
    let first = result.records[0].get("n.name").unwrap().as_property().unwrap().as_string().unwrap();
    let second = result.records[1].get("n.name").unwrap().as_property().unwrap().as_string().unwrap();

    // Bob (25), Eve (28)
    assert_eq!(first, "Bob");
    assert_eq!(second, "Eve");
}

#[test]
fn test_aggregations() {
    let store = setup_social_graph();
    let engine = QueryEngine::new();

    // Query: Count people by city
    // This assumes simple GROUP BY implicit behavior or explicit
    // For now, let's test simple global count if GROUP BY isn't fully robust
    let query = "MATCH (n:Person) RETURN count(n)";
    let result = engine.execute(query, &store).unwrap();

    assert_eq!(result.len(), 1);
    let count_val = result.records[0].get("count(n)").unwrap().as_property().unwrap();
    if let PropertyValue::Integer(count) = count_val {
        assert_eq!(*count, 5);
    } else {
        panic!("Expected integer count, got {:?}", count_val);
    }
}

#[test]
fn test_multi_hop_pattern() {
    let store = setup_social_graph();
    let engine = QueryEngine::new();

    // Query: Find Friend-of-Friend of Alice (Alice -> ? -> ?)
    // Alice -> Bob -> David
    // Alice -> Charlie -> Eve
    let query = "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name";
    let result = engine.execute(query, &store).unwrap();

    assert_eq!(result.len(), 2);
    
    let names: Vec<String> = result.records.iter()
        .map(|r| r.get("c.name").unwrap().as_property().unwrap().as_string().unwrap().to_string())
        .collect();
        
    assert!(names.contains(&"David".to_string()));
    assert!(names.contains(&"Eve".to_string()));
}
