//! Comprehensive Benchmark Suite for Samyama Graph Database
//!
//! Measures performance of:
//! 1. Data Ingestion (Nodes/Edges)
//! 2. Vector Indexing & Search
//! 3. Graph Algorithms (PageRank, BFS, WCC)
//! 4. Cypher Query Execution

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;
use samyama::vector::DistanceMetric;
use std::time::Instant;
use rand::Rng;
use std::env;

const EDGES_PER_NODE: usize = 5;
const VECTOR_DIM: usize = 128;
const SEARCH_K: usize = 10;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let num_nodes = if args.len() > 1 {
        args[1].parse::<usize>().expect("Usage: cargo run --release --example full_benchmark <NUM_NODES>")
    } else {
        10_000
    };

    println!("=== Samyama Graph Database Benchmark ===");
    println!("Configuration:");
    println!("  Nodes: {}", num_nodes);
    println!("  Edges: ~{}", num_nodes * EDGES_PER_NODE);
    println!("  Vector Dim: {}", VECTOR_DIM);
    println!("  Indexing: Async Background Task");
    println!("----------------------------------------");

    // Initialize with async indexing
    let (mut store, rx) = GraphStore::with_async_indexing();
    let engine = QueryEngine::new();

    // Spawn background indexer
    let v_idx = store.vector_index.clone();
    let p_idx = store.property_index.clone();
    tokio::spawn(async move {
        GraphStore::start_background_indexer(rx, v_idx, p_idx).await;
    });

    // --- 1. Ingestion Benchmark ---
    println!("\n[1] Benchmarking Ingestion...");
    
    // Create Vector Index first (will be used by background worker)
    store.create_vector_index("Entity", "embedding", VECTOR_DIM, DistanceMetric::Cosine).unwrap();

    let mut rng = rand::thread_rng();
    let start_ingest = Instant::now();
    let mut node_ids = Vec::with_capacity(num_nodes);

    // Bulk create nodes
    for i in 0..num_nodes {
        let vec: Vec<f32> = (0..VECTOR_DIM).map(|_| rng.gen::<f32>()).collect();
        
        let mut props = std::collections::HashMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("embedding".to_string(), PropertyValue::Vector(vec));
        props.insert("score".to_string(), PropertyValue::Float(rng.gen::<f64>()));

        let id = store.create_node_with_properties("default", vec![Label::new("Entity")], props);
        node_ids.push(id);
    }
    let ingest_nodes_time = start_ingest.elapsed();
    println!("  Nodes created: {} in {:.2?}", num_nodes, ingest_nodes_time);
    println!("  Node Rate: {:.0} nodes/sec", num_nodes as f64 / ingest_nodes_time.as_secs_f64());

    // Bulk create edges (Random Graph)
    let start_edges = Instant::now();
    let mut edge_count = 0;
    for i in 0..num_nodes {
        for _ in 0..EDGES_PER_NODE {
            let target_idx = rng.gen_range(0..num_nodes);
            if i != target_idx {
                let source = node_ids[i];
                let target = node_ids[target_idx];
                store.create_edge(source, target, "LINKS_TO").unwrap();
                edge_count += 1;
            }
        }
    }
    let ingest_edges_time = start_edges.elapsed();
    println!("  Edges created: {} in {:.2?}", edge_count, ingest_edges_time);
    println!("  Edge Rate: {:.0} edges/sec", edge_count as f64 / ingest_edges_time.as_secs_f64());

    // Wait for indexing to catch up before searching
    println!("  Waiting 2s for background indexing to complete...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // --- 2. Vector Search Benchmark ---
    println!("\n[2] Benchmarking Vector Search (HNSW)...");
    let num_searches = 1000;
    let mut total_search_time = std::time::Duration::default();

    for _ in 0..num_searches {
        let query: Vec<f32> = (0..VECTOR_DIM).map(|_| rng.gen::<f32>()).collect();
        let t = Instant::now();
        let _results = store.vector_search("Entity", "embedding", &query, SEARCH_K).unwrap();
        total_search_time += t.elapsed();
    }
    
    println!("  Queries: {}", num_searches);
    println!("  Total Time: {:.2?}", total_search_time);
    println!("  Avg Latency: {:.2?}", total_search_time / num_searches as u32);
    println!("  QPS: {:.0}", num_searches as f64 / total_search_time.as_secs_f64());


    // --- 3. Graph Algorithms Benchmark ---
    println!("\n[3] Benchmarking Graph Algorithms...");

    // PageRank
    let start_pr = Instant::now();
    let pr_query = "CALL algo.pageRank('Entity', 'LINKS_TO') YIELD node, score RETURN node";
    let pr_res = engine.execute(pr_query, &store).unwrap();
    let pr_time = start_pr.elapsed();
    println!("  PageRank (20 iters): {:.2?} ({} nodes processed)", pr_time, pr_res.records.len());

    // Shortest Path (BFS)
    let start_node = node_ids[0];
    let end_node = node_ids[num_nodes / 2]; // Pick someone in the middle
    
    let start_bfs = Instant::now();
    let sp_query = format!("CALL algo.shortestPath({}, {}) YIELD path, cost RETURN cost", start_node.as_u64(), end_node.as_u64());
    let sp_res = engine.execute(&sp_query, &store).unwrap();
    let sp_time = start_bfs.elapsed();
    
    println!("  Shortest Path (BFS): {:.2?}", sp_time);
    if !sp_res.records.is_empty() {
        println!("    Path found! Cost: {:?}", sp_res.records[0].get("cost").unwrap());
    } else {
        println!("    No path found (random graph disconnected).");
    }


    // --- 4. Cypher Query Benchmark ---
    println!("\n[4] Benchmarking Cypher Queries...");
    
    // Create Index on Entity(id)
    println!("  Creating index on :Entity(id)...");
    let create_index_query = "CREATE INDEX ON :Entity(id)";
    engine.execute_mut(create_index_query, &mut store).unwrap();
    
    // Simple 1-hop traversal
    // MATCH (a:Entity)-[:LINKS_TO]->(b:Entity) WHERE a.id = 1 RETURN b.id
    let traversal_query = format!("MATCH (a:Entity)-[:LINKS_TO]->(b:Entity) WHERE a.id = {} RETURN b.id", 1);
    
    let start_cypher = Instant::now();
    let mut traversal_count = 0;
    for _ in 0..1000 {
        let res = engine.execute(&traversal_query, &store).unwrap();
        traversal_count += res.records.len();
    }
    let cypher_time = start_cypher.elapsed();
    
    println!("  Executed 1000 traversal queries in {:.2?} (Total records: {})", cypher_time, traversal_count);
    println!("  Avg Latency: {:.2?}", cypher_time / 1000);
    println!("  QPS: {:.0}", 1000.0 / cypher_time.as_secs_f64());

    println!("\n=== Benchmark Complete ===");
}