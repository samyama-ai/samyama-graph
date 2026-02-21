//! SDK Demo — demonstrates the samyama-sdk EmbeddedClient
//!
//! Shows how to use the unified SamyamaClient trait for:
//! - Creating nodes and edges via Cypher
//! - Running read-only queries
//! - Getting server status
//! - Working with query results
//!
//! Run with: cargo run --example sdk_demo

use samyama_sdk::{EmbeddedClient, SamyamaClient, GraphStore, Label};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    println!("=== Samyama SDK Demo (EmbeddedClient) ===\n");

    // --- Option 1: Create a fresh embedded client ---
    let client = EmbeddedClient::new();

    // Check status
    let status = client.status().await.unwrap();
    println!("Status: {} (v{})", status.status, status.version);
    println!("Initial: {} nodes, {} edges\n", status.storage.nodes, status.storage.edges);

    // --- Create a social network via Cypher ---
    println!("Creating social network...");

    let people = [
        ("Alice", 30, "Engineering"),
        ("Bob", 25, "Engineering"),
        ("Carol", 35, "Product"),
        ("Dave", 28, "Engineering"),
        ("Eve", 32, "Design"),
    ];

    for (name, age, dept) in &people {
        client.query("default", &format!(
            r#"CREATE (n:Person {{name: "{}", age: {}, department: "{}"}})"#,
            name, age, dept
        )).await.unwrap();
    }

    // Create relationships
    let rels = [
        ("Alice", "Bob", "MANAGES"),
        ("Alice", "Dave", "MANAGES"),
        ("Bob", "Carol", "COLLABORATES"),
        ("Carol", "Eve", "COLLABORATES"),
        ("Dave", "Eve", "KNOWS"),
        ("Alice", "Carol", "KNOWS"),
    ];

    for (from, to, rel_type) in &rels {
        client.query("default", &format!(
            r#"MATCH (a:Person {{name: "{}"}}), (b:Person {{name: "{}"}}) CREATE (a)-[:{}]->(b)"#,
            from, to, rel_type
        )).await.unwrap();
    }

    let status = client.status().await.unwrap();
    println!("Created: {} nodes, {} edges\n", status.storage.nodes, status.storage.edges);

    // --- Query: All people ---
    println!("All people:");
    let result = client.query_readonly("default",
        "MATCH (n:Person) RETURN n.name, n.age, n.department"
    ).await.unwrap();

    println!("  Columns: {:?}", result.columns);
    for row in &result.records {
        println!("  {:?}", row);
    }
    println!();

    // --- Query: Engineers ---
    println!("Engineers:");
    let result = client.query_readonly("default",
        r#"MATCH (n:Person) WHERE n.department = "Engineering" RETURN n.name, n.age"#
    ).await.unwrap();
    for row in &result.records {
        println!("  {:?}", row);
    }
    println!();

    // --- Query: Relationships ---
    println!("Who manages whom:");
    let result = client.query_readonly("default",
        "MATCH (a:Person)-[:MANAGES]->(b:Person) RETURN a.name, b.name"
    ).await.unwrap();
    for row in &result.records {
        println!("  {} manages {}", row[0], row[1]);
    }
    println!();

    // --- Query: 2-hop connections ---
    println!("2-hop connections from Alice:");
    let result = client.query_readonly("default",
        r#"MATCH (a:Person {name: "Alice"})-[]->(mid:Person)-[]->(b:Person)
           RETURN DISTINCT a.name, mid.name, b.name"#
    ).await.unwrap();
    for row in &result.records {
        println!("  {} -> {} -> {}", row[0], row[1], row[2]);
    }
    println!();

    // --- Option 2: Wrap an existing GraphStore ---
    println!("--- Using EmbeddedClient with existing GraphStore ---");
    let mut store = GraphStore::new();
    let n1 = store.create_node(Label::new("City"));
    if let Some(node) = store.get_node_mut(n1) {
        node.set_property("name", "San Francisco");
    }
    let n2 = store.create_node(Label::new("City"));
    if let Some(node) = store.get_node_mut(n2) {
        node.set_property("name", "New York");
    }
    store.create_edge(n1, n2, "CONNECTED_TO").unwrap();

    let client2 = EmbeddedClient::with_store(Arc::new(RwLock::new(store)));
    let result = client2.query_readonly("default",
        "MATCH (a:City)-[:CONNECTED_TO]->(b:City) RETURN a.name, b.name"
    ).await.unwrap();
    for row in &result.records {
        println!("  {} -> {}", row[0], row[1]);
    }

    // --- Ping ---
    let pong = client.ping().await.unwrap();
    println!("\nPing: {}", pong);

    // --- List graphs ---
    let graphs = client.list_graphs().await.unwrap();
    println!("Graphs: {:?}", graphs);

    println!("\n=== SDK Demo Complete ===");
}
