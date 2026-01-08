use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;

#[test]
fn test_pagerank_procedure() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create star graph
    // Center (Alice) -> Bob, Charlie
    // Bob -> Alice
    // Charlie -> Alice
    
    // Alice (Center)
    let alice = store.create_node("Person");
    store.set_node_property(alice, "name", "Alice").unwrap();
    
    // Bob
    let bob = store.create_node("Person");
    store.set_node_property(bob, "name", "Bob").unwrap();
    
    // Charlie
    let charlie = store.create_node("Person");
    store.set_node_property(charlie, "name", "Charlie").unwrap();
    
    store.create_edge(alice, bob, "KNOWS").unwrap();
    store.create_edge(alice, charlie, "KNOWS").unwrap();
    store.create_edge(bob, alice, "KNOWS").unwrap();
    store.create_edge(charlie, alice, "KNOWS").unwrap();
    
    // Run PageRank
    let query = "CALL algo.pageRank('Person', 'KNOWS') YIELD node, score RETURN node.name, score";
    let result = engine.execute(query, &store).unwrap();
    
    assert_eq!(result.records.len(), 3);
    
    // Alice should be first (highest score)
    let first = &result.records[0];
    assert_eq!(first.get("node.name").unwrap().as_property().unwrap().as_string(), Some("Alice"));
    
    let score = first.get("score").unwrap().as_property().unwrap().as_float().unwrap();
    assert!(score > 1.0);
}

#[test]
fn test_shortest_path_procedure() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    
    // 1 -> 2 -> 3
    let n1 = store.create_node("Node");
    let n2 = store.create_node("Node");
    let n3 = store.create_node("Node");
    
    store.create_edge(n1, n2, "LINK").unwrap();
    store.create_edge(n2, n3, "LINK").unwrap();
    
    // CALL algo.shortestPath(1, 3)
    let query = format!("CALL algo.shortestPath({}, {}) YIELD path, cost RETURN cost", n1.as_u64(), n3.as_u64());
    let result = engine.execute(&query, &store).unwrap();
    
    assert_eq!(result.records.len(), 1);
    let cost = result.records[0].get("cost").unwrap().as_property().unwrap().as_float().unwrap();
    assert_eq!(cost, 2.0);
}
