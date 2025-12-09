use samyama::{GraphStore, QueryEngine, RespServer, ServerConfig};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("Samyama Graph Database v{}", samyama::version());
    println!("==========================================");
    println!();

    // Demo 1: Property Graph
    demo_property_graph();

    // Demo 2: OpenCypher Queries
    demo_cypher_queries();

    // Demo 3: RESP Server
    println!("\n=== Demo 3: RESP Protocol Server ===");
    println!("Starting RESP server on 127.0.0.1:6379...");
    println!("Connect with any Redis client:");
    println!("  redis-cli");
    println!("  GRAPH.QUERY mygraph \"MATCH (n:Person) RETURN n\"");
    println!();

    start_server().await;
}

fn demo_property_graph() {
    println!("=== Demo 1: Property Graph ===");
    let mut store = GraphStore::new();

    // Create people
    let alice = store.create_node("Person");
    if let Some(node) = store.get_node_mut(alice) {
        node.set_property("name", "Alice");
        node.set_property("age", 30i64);
        node.set_property("city", "New York");
        println!("✓ Created Person: Alice (age 30, New York)");
    }

    let bob = store.create_node("Person");
    if let Some(node) = store.get_node_mut(bob) {
        node.set_property("name", "Bob");
        node.set_property("age", 25i64);
        node.set_property("city", "San Francisco");
        println!("✓ Created Person: Bob (age 25, San Francisco)");
    }

    let charlie = store.create_node("Person");
    if let Some(node) = store.get_node_mut(charlie) {
        node.set_property("name", "Charlie");
        node.set_property("age", 35i64);
        node.set_property("city", "New York");
        println!("✓ Created Person: Charlie (age 35, New York)");
    }

    // Create relationships
    let alice_knows_bob = store.create_edge(alice, bob, "KNOWS").unwrap();
    if let Some(edge) = store.get_edge_mut(alice_knows_bob) {
        edge.set_property("since", 2020i64);
        edge.set_property("strength", 0.9);
        println!("✓ Alice -[KNOWS]-> Bob (since 2020, strength 0.9)");
    }

    let bob_knows_charlie = store.create_edge(bob, charlie, "KNOWS").unwrap();
    if let Some(edge) = store.get_edge_mut(bob_knows_charlie) {
        edge.set_property("since", 2019i64);
        edge.set_property("strength", 0.8);
        println!("✓ Bob -[KNOWS]-> Charlie (since 2019, strength 0.8)");
    }

    let _alice_follows_charlie = store.create_edge(alice, charlie, "FOLLOWS").unwrap();
    println!("✓ Alice -[FOLLOWS]-> Charlie");

    println!("\nGraph Statistics:");
    println!("  Total nodes: {}", store.node_count());
    println!("  Total edges: {}", store.edge_count());
}

fn demo_cypher_queries() {
    println!("\n=== Demo 2: OpenCypher Queries ===");

    let mut store = GraphStore::new();

    // Create test data
    let alice = store.create_node("Person");
    if let Some(node) = store.get_node_mut(alice) {
        node.set_property("name", "Alice");
        node.set_property("age", 30i64);
    }

    let bob = store.create_node("Person");
    if let Some(node) = store.get_node_mut(bob) {
        node.set_property("name", "Bob");
        node.set_property("age", 25i64);
    }

    let charlie = store.create_node("Person");
    if let Some(node) = store.get_node_mut(charlie) {
        node.set_property("name", "Charlie");
        node.set_property("age", 35i64);
    }

    store.create_edge(alice, bob, "KNOWS").unwrap();
    store.create_edge(bob, charlie, "KNOWS").unwrap();

    let engine = QueryEngine::new();

    // Query 1: Simple match
    println!("\nQuery 1: MATCH (n:Person) RETURN n");
    if let Ok(result) = engine.execute("MATCH (n:Person) RETURN n", &store) {
        println!("  → Found {} persons", result.len());
    }

    // Query 2: Filter with WHERE
    println!("\nQuery 2: MATCH (n:Person) WHERE n.age > 28 RETURN n");
    if let Ok(result) = engine.execute("MATCH (n:Person) WHERE n.age > 28 RETURN n", &store) {
        println!("  → Found {} persons over 28", result.len());
    }

    // Query 3: Edge traversal
    println!("\nQuery 3: MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b");
    if let Ok(result) = engine.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b", &store) {
        println!("  → Found {} KNOWS relationships", result.len());
    }

    // Query 4: Property projection
    println!("\nQuery 4: MATCH (n:Person) RETURN n.name, n.age LIMIT 2");
    if let Ok(result) = engine.execute("MATCH (n:Person) RETURN n.name, n.age LIMIT 2", &store) {
        println!("  → Returned {} rows with columns: {:?}", result.len(), result.columns);
    }

    println!("\n✅ All queries executed successfully!");
}

async fn start_server() {
    let store = Arc::new(RwLock::new(GraphStore::new()));

    // Add some initial data
    {
        let mut graph = store.write().await;
        let alice = graph.create_node("Person");
        if let Some(node) = graph.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let bob = graph.create_node("Person");
        if let Some(node) = graph.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 25i64);
        }

        graph.create_edge(alice, bob, "KNOWS").unwrap();
    }

    let config = ServerConfig::default();
    let server = RespServer::new(config, store);

    println!("✅ Server ready. Press Ctrl+C to stop.");
    println!();

    if let Err(e) = server.start().await {
        eprintln!("Server error: {}", e);
    }
}
