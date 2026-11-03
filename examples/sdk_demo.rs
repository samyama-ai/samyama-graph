//! SDK Demo — demonstrates the samyama-sdk EmbeddedClient
//!
//! Shows how to use the unified SamyamaClient trait for:
//! - Creating nodes and edges via Cypher
//! - Running read-only queries
//! - Getting server status
//! - Working with query results
//! - Algorithm extension trait (PageRank, WCC)
//! - Vector search extension trait
//!
//! Run with: cargo run --example sdk_demo

use samyama_sdk::{
    EmbeddedClient, SamyamaClient, AlgorithmClient, VectorClient,
    GraphStore, Label, NodeId, PageRankConfig, DistanceMetric,
};
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

    // --- NEW: Algorithm Extension Trait ---
    println!("--- PageRank (via AlgorithmClient) ---");
    let scores = client.page_rank(PageRankConfig::default(), Some("Person"), None).await;
    let mut ranked: Vec<_> = scores.iter().collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
    {
        let store = client.store_read().await;
        for (node_id, score) in ranked.iter().take(3) {
            let nid = NodeId::new(**node_id);
            if let Some(node) = store.get_node(nid) {
                let name = node.get_property("name")
                    .map(|p| format!("{:?}", p))
                    .unwrap_or_default();
                println!("  {} -> PageRank {:.4}", name, score);
            }
        }
    }
    println!();

    println!("--- WCC (via AlgorithmClient) ---");
    let wcc = client.weakly_connected_components(Some("Person"), None).await;
    println!("  {} weakly connected component(s)", wcc.components.len());
    println!();

    // --- NEW: Vector Search Extension Trait ---
    println!("--- Vector Search (via VectorClient) ---");
    client.create_vector_index("Person", "embedding", 3, DistanceMetric::Cosine)
        .await.unwrap();

    // Add embeddings to each person
    let embeddings = [
        [1.0f32, 0.0, 0.0],  // Alice
        [0.9, 0.1, 0.0],     // Bob
        [0.0, 1.0, 0.0],     // Carol
        [0.8, 0.2, 0.0],     // Dave
        [0.0, 0.9, 0.1],     // Eve
    ];
    {
        let store = client.store_read().await;
        let nodes: Vec<_> = store.all_nodes().iter().map(|n| n.id).collect();
        drop(store);
        for (i, emb) in embeddings.iter().enumerate() {
            if i < nodes.len() {
                client.add_vector("Person", "embedding", nodes[i], emb).await.unwrap();
            }
        }
    }

    let results = client.vector_search("Person", "embedding", &[0.95, 0.05, 0.0], 3).await.unwrap();
    println!("  Top 3 nearest to engineering embedding:");
    {
        let store = client.store_read().await;
        for (nid, dist) in &results {
            if let Some(node) = store.get_node(*nid) {
                let name = node.get_property("name")
                    .map(|p| format!("{:?}", p))
                    .unwrap_or_default();
                println!("    {} (distance: {:.4})", name, dist);
            }
        }
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
