use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::vector::DistanceMetric;
use samyama::query::QueryEngine;

fn main() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    println!("--- Samyama Graph RAG Demo ---");

    // 1. Setup Schema
    println!("1. Creating Vector Index...");
    store.create_vector_index("Document", "embedding", 3, DistanceMetric::Cosine).unwrap();

    // 2. Load Knowledge Base
    println!("2. Loading Knowledge Base...");
    
    // Nodes: Documents with embeddings
    // Doc 1: "Rust is a systems programming language." [1.0, 0.0, 0.0]
    // Doc 2: "Python is great for data science." [0.0, 1.0, 0.0]
    // Doc 3: "Graph databases store connected data." [0.0, 0.0, 1.0]
    
    let doc1 = store.create_node("Document");
    store.set_node_property("default", doc1, "title", "Rust Intro").unwrap();
    store.set_node_property("default", doc1, "embedding", PropertyValue::Vector(vec![1.0, 0.0, 0.0])).unwrap();
    
    let doc2 = store.create_node("Document");
    store.set_node_property("default", doc2, "title", "Python Data").unwrap();
    store.set_node_property("default", doc2, "embedding", PropertyValue::Vector(vec![0.0, 1.0, 0.0])).unwrap();
    
    let doc3 = store.create_node("Document");
    store.set_node_property("default", doc3, "title", "Graph DB").unwrap();
    store.set_node_property("default", doc3, "embedding", PropertyValue::Vector(vec![0.0, 0.0, 1.0])).unwrap();

    // Nodes: Authors
    let alice = store.create_node("Author");
    store.set_node_property("default", alice, "name", "Alice").unwrap();
    
    let bob = store.create_node("Author");
    store.set_node_property("default", bob, "name", "Bob").unwrap();

    // Relationships: Author -[WROTE]-> Document
    store.create_edge(alice, doc1, "WROTE").unwrap();
    store.create_edge(bob, doc2, "WROTE").unwrap();
    store.create_edge(alice, doc3, "WROTE").unwrap();

    // 3. Perform Graph RAG Query
    println!("3. Performing Graph RAG Query...");
    println!("Question: 'Tell me about systems programming and who wrote it.'");
    
    // Vector search for "systems programming" (closest to [1, 0, 0])
    // and then find the author via graph traversal
    let query = "
        CALL db.index.vector.queryNodes('Document', 'embedding', [0.9, 0.1, 0.0], 1) YIELD node, score
        MATCH (author)-[:WROTE]->(node)
        RETURN node.title as title, author.name as author, score
    ";

    let result = engine.execute(query, &store).unwrap();

    println!("Results:");
    for record in result.records {
        let title = record.get("title").unwrap().as_property().unwrap().as_string().unwrap();
        let author = record.get("author").unwrap().as_property().unwrap().as_string().unwrap();
        let score = record.get("score").unwrap().as_property().unwrap().as_float().unwrap();
        println!("  - Document: '{}', Author: {}, Similarity Score: {:.4}", title, author, 1.0 - score);
    }
}
