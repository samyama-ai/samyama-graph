use samyama::{GraphStore, QueryEngine, RespServer, ServerConfig, PersistenceManager, http::HttpServer};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

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
    let bind_addr = std::env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1".to_string());
    let resp_port: u16 = std::env::var("RESP_PORT").unwrap_or_else(|_| "6379".to_string()).parse().expect("Invalid RESP_PORT");
    let http_port: u16 = std::env::var("HTTP_PORT").unwrap_or_else(|_| "8080".to_string()).parse().expect("Invalid HTTP_PORT");

    println!("\n=== Demo 3: RESP Protocol Server ===");
    println!("Starting RESP server on {}:{}...", bind_addr, resp_port);
    println!("Visualizer on http://{}:{}", bind_addr, http_port);
    println!("Connect with any Redis client:");
    println!("  redis-cli -p {}", resp_port);
    println!("  GRAPH.QUERY mygraph \"MATCH (n:Person) RETURN n\"");
    println!();

    start_server(&bind_addr, resp_port, http_port).await;
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

async fn start_server(bind_addr: &str, resp_port: u16, http_port: u16) {
    let mut config = ServerConfig::default();
    config.address = bind_addr.to_string();
    config.port = resp_port;
    
    // Ensure unique data path if running multiple instances locally
    if let Some(path) = config.data_path {
        config.data_path = Some(format!("{}_{}", path, resp_port));
    }
    
    let mut store = GraphStore::new();

    // Initialize persistence if data_path is configured
    let persistence = if let Some(ref data_path) = config.data_path {
        println!("Initializing persistence at: {}", data_path);

        match PersistenceManager::new(data_path) {
            Ok(pm) => {
                // Recover existing data from disk
                println!("Recovering data from disk...");
                match pm.recover("default") {
                    Ok((nodes, edges)) => {
                        println!("Recovered {} nodes and {} edges from disk", nodes.len(), edges.len());

                        // Rebuild in-memory GraphStore from recovered data
                        // Insert nodes first (edges depend on nodes existing)
                        for node in nodes {
                            store.insert_recovered_node(node);
                        }

                        // Then insert edges
                        for edge in edges {
                            if let Err(e) = store.insert_recovered_edge(edge) {
                                eprintln!("Warning: Failed to recover edge: {}", e);
                            }
                        }

                        info!("Recovery complete. GraphStore has {} nodes, {} edges",
                              store.node_count(), store.edge_count());
                    }
                    Err(e) => {
                        eprintln!("Warning: Recovery failed: {}. Starting with empty graph.", e);
                    }
                }

                Some(Arc::new(pm))
            }
            Err(e) => {
                eprintln!("Warning: Failed to initialize persistence: {}. Running in-memory only.", e);
                None
            }
        }
    } else {
        println!("No data_path configured. Running in-memory only (data will not survive restart).");
        None
    };

    // If no data was recovered, add some demo data
    if store.node_count() == 0 {
        println!("No existing data found. Adding demo data...");
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

        store.create_edge(alice, bob, "KNOWS").unwrap();
    }

    let store_arc = Arc::new(RwLock::new(store));

    // Create server with or without persistence
    let server = if let Some(pm) = persistence {
        println!("✅ Server ready WITH persistence. Data will be saved to disk.");
        RespServer::new_with_persistence(config, Arc::clone(&store_arc), pm)
    } else {
        println!("✅ Server ready (in-memory only). Data will NOT survive restart.");
        RespServer::new(config, Arc::clone(&store_arc))
    };

    println!("Connect with: redis-cli -p {}", resp_port);
    println!("Example: GRAPH.QUERY mygraph \"CREATE (n:Person {{name: 'Test'}})\"");
    println!();

    println!("Visualizer available at: http://localhost:{}", http_port);
    println!();

    let http_server = HttpServer::new(Arc::clone(&store_arc), http_port);

    let resp_handle = server.start();
    let http_handle = http_server.start();

    // Run both servers concurrently
    match tokio::try_join!(resp_handle, http_handle) {
        Ok(_) => {},
        Err(e) => eprintln!("Server error: {}", e),
    }
}