//! Comprehensive Benchmark Suite for Samyama Graph Database
//!
//! Measures performance of:
//! 1. Data Ingestion (Nodes/Edges)
//! 2. Vector Indexing & Search
//! 3. Graph Algorithms (PageRank, BFS, WCC)
//! 4. Cypher Query Execution

use samyama::{GraphStore, Label, PropertyValue, QueryEngine, PersistenceManager, DistanceMetric};

use std::time::Instant;

use std::sync::Arc;

use samyama::persistence::TenantManager;

use rand::Rng;

use tokio::sync::RwLock;



const VECTOR_DIM: usize = 64;

const NUM_NODES: usize = 10_000;

const EDGES_PER_NODE: usize = 5;

const SEARCH_K: usize = 10;



#[tokio::main]

async fn main() {

    tracing_subscriber::fmt::init();

    println!("--- Samyama Comprehensive Benchmark Suite ---");

    println!("Nodes: {}", NUM_NODES);

    println!("Edges: ~{}", NUM_NODES * EDGES_PER_NODE);

    println!("Vector Dim: {}", VECTOR_DIM);



    // Setup

    let (mut store, rx) = GraphStore::with_async_indexing();

    let engine = QueryEngine::new();

    let tenant_manager = Arc::new(TenantManager::new());

    

    // Start background indexer

    let v_idx = Arc::clone(&store.vector_index);

    let p_idx = Arc::clone(&store.property_index);

    tokio::spawn(async move {

        GraphStore::start_background_indexer(rx, v_idx, p_idx, tenant_manager).await;

    });



    // 1. Ingestion

    let node_ids = benchmark_ingestion(&mut store).await;



    // 2. Vector Search

    benchmark_vector_search(&store);



    // 3. K-Hop Traversal (The "Graphy" Test)

    benchmark_k_hop(&store, &engine, &node_ids);



    // 4. Concurrent Mixed Workload

    // Wrap store in RwLock/Arc for shared access

    // Note: GraphStore currently requires &mut for some ops, checking concurrency support

    // For this benchmark, we might need to skip if GraphStore isn't fully thread-safe for mixed Read/Write yet

    // Or we use the engine which handles locks? Engine usually takes &GraphStore.

    // benchmark_concurrent_load(store).await;

}



async fn benchmark_ingestion(store: &mut GraphStore) -> Vec<samyama::graph::NodeId> {

    println!("\n[1] Benchmarking Ingestion...");

    store.create_vector_index("Entity", "embedding", VECTOR_DIM, DistanceMetric::Cosine).unwrap();



    let mut rng = rand::thread_rng();

    let start = Instant::now();

    let mut node_ids = Vec::with_capacity(NUM_NODES);



    // Nodes

    for i in 0..NUM_NODES {

        let vec: Vec<f32> = (0..VECTOR_DIM).map(|_| rng.gen::<f32>()).collect();

        let mut props = samyama::PropertyMap::new();

        props.insert("id".to_string(), PropertyValue::Integer(i as i64));

        props.insert("embedding".to_string(), PropertyValue::Vector(vec));

        props.insert("score".to_string(), PropertyValue::Float(rng.gen::<f64>()));



        let id = store.create_node_with_properties("default", vec![Label::new("Entity")], props);

        node_ids.push(id);

    }

    let node_time = start.elapsed();

    println!("  Nodes: {:.2?} ({:.0} nodes/sec)", node_time, NUM_NODES as f64 / node_time.as_secs_f64());



    // Edges

    let start_edge = Instant::now();

    let mut edge_count = 0;

    for i in 0..NUM_NODES {

        for _ in 0..EDGES_PER_NODE {

            let target_idx = rng.gen_range(0..NUM_NODES);

            if i != target_idx {

                let source = node_ids[i];

                let target = node_ids[target_idx];

                store.create_edge(source, target, "LINKS_TO").unwrap();

                edge_count += 1;

            }

        }

    }

    let edge_time = start_edge.elapsed();

    println!("  Edges: {:.2?} ({:.0} edges/sec)", edge_time, edge_count as f64 / edge_time.as_secs_f64());



    // Wait for indexing

    println!("  Waiting for indexing...");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    

    node_ids

}



fn benchmark_vector_search(store: &GraphStore) {

    println!("\n[2] Benchmarking Vector Search (HNSW)...");

    let mut rng = rand::thread_rng();

    let num_searches = 1000;

    let mut total_time = std::time::Duration::default();



    for _ in 0..num_searches {

        let query: Vec<f32> = (0..VECTOR_DIM).map(|_| rng.gen::<f32>()).collect();

        let t = Instant::now();

        let _ = store.vector_search("Entity", "embedding", &query, SEARCH_K).unwrap();

        total_time += t.elapsed();

    }

    

    println!("  Avg Latency: {:.2?}", total_time / num_searches as u32);

    println!("  QPS: {:.0}", num_searches as f64 / total_time.as_secs_f64());

}



fn benchmark_k_hop(store: &GraphStore, engine: &QueryEngine, node_ids: &[samyama::graph::NodeId]) {

    println!("\n[3] Benchmarking K-Hop Traversal...");

    // 1-Hop

    let query_1 = "MATCH (a:Entity)-[:LINKS_TO]->(b:Entity) WHERE a.id = {} RETURN b.id";

    // 2-Hop

    let query_2 = "MATCH (a:Entity)-[:LINKS_TO]->(b)-[:LINKS_TO]->(c:Entity) WHERE a.id = {} RETURN c.id";

    

    let mut rng = rand::thread_rng();

    let num_queries = 100;



    // 1-Hop Test

    let start = Instant::now();

    for _ in 0..num_queries {

        let id = rng.gen_range(0..NUM_NODES);

        let q = query_1.replace("{}", &id.to_string());

        let _ = engine.execute(&q, store).unwrap();

    }

    println!("  1-Hop Latency: {:.2?}", start.elapsed() / num_queries as u32);



    // 2-Hop Test

    let start = Instant::now();

    for _ in 0..num_queries {

        let id = rng.gen_range(0..NUM_NODES);

        let q = query_2.replace("{}", &id.to_string());

        let _ = engine.execute(&q, store).unwrap();

    }

    println!("  2-Hop Latency: {:.2?}", start.elapsed() / num_queries as u32);

}
