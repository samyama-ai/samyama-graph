//! Comprehensive Benchmark Suite for Samyama Graph Database
//!
//! Measures performance of:
//! 1. Data Ingestion (Nodes & Edges)
//! 2. Vector Indexing & Search (HNSW)
//! 3. K-Hop Traversal (1-hop, 2-hop, 3-hop)
//! 4. Graph Algorithm Performance (PageRank, WCC, BFS)
//! 5. Mixed Workload (80% read, 20% write)

use samyama::{GraphStore, Label, PropertyValue, QueryEngine, DistanceMetric};
use samyama::algo::{build_view, page_rank, weakly_connected_components, bfs, PageRankConfig};
use samyama::persistence::TenantManager;
use std::time::Instant;
use std::sync::Arc;
use rand::Rng;

const VECTOR_DIM: usize = 128;
const NUM_NODES: usize = 10_000;
const EDGES_PER_NODE: usize = 5;
const SEARCH_K: usize = 10;

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║   SAMYAMA Comprehensive Benchmark Suite                         ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Configuration:");
    println!("  - Nodes:      {:>10}", format_number(NUM_NODES));
    println!("  - Edges:      ~{:>9}", format_number(NUM_NODES * EDGES_PER_NODE));
    println!("  - Vector dim: {:>10}", VECTOR_DIM);
    println!("  - Search k:   {:>10}", SEARCH_K);
    println!();

    let total_start = Instant::now();

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

    // 1. Ingestion Benchmark
    let node_ids = benchmark_ingestion(&mut store).await;

    // 2. Vector Search Benchmark
    benchmark_vector_search(&store);

    // 3. K-Hop Traversal
    benchmark_k_hop(&store, &engine);

    // 4. Graph Algorithms
    benchmark_graph_algorithms(&store);

    // 5. Mixed Workload
    benchmark_mixed_workload(&mut store, &engine);

    // Summary
    let total = total_start.elapsed();
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║   Benchmark Summary                                            ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Total duration: {:>10.2?}                                   ║", total);
    println!("║  Nodes:          {:>10}                                   ║", format_number(NUM_NODES));
    println!("║  Edges:         ~{:>10}                                   ║", format_number(NUM_NODES * EDGES_PER_NODE));
    println!("║  Vector dim:     {:>10}                                   ║", VECTOR_DIM);
    println!("╚══════════════════════════════════════════════════════════════════╝");
}

async fn benchmark_ingestion(store: &mut GraphStore) -> Vec<samyama::graph::NodeId> {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 1: Data Ingestion                                     │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    store.create_vector_index("Entity", "embedding", VECTOR_DIM, DistanceMetric::Cosine).unwrap();

    let mut rng = rand::thread_rng();
    let mut node_ids = Vec::with_capacity(NUM_NODES);

    // Node ingestion
    let start = Instant::now();
    let labels = ["Server", "User", "Document", "Event", "Metric"];

    for i in 0..NUM_NODES {
        let label = labels[i % labels.len()];
        let vec: Vec<f32> = (0..VECTOR_DIM).map(|_| rng.gen::<f32>()).collect();
        let mut props = samyama::PropertyMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("name".to_string(), PropertyValue::String(format!("{}-{}", label, i)));
        props.insert("embedding".to_string(), PropertyValue::Vector(vec));
        props.insert("score".to_string(), PropertyValue::Float(rng.gen::<f64>()));
        props.insert("active".to_string(), PropertyValue::Boolean(rng.gen_bool(0.9)));

        let id = store.create_node_with_properties("default", vec![Label::new(label), Label::new("Entity")], props);
        node_ids.push(id);
    }
    let node_time = start.elapsed();

    // Edge ingestion
    let edge_types = ["LINKS_TO", "MANAGES", "MONITORS", "DEPENDS_ON", "TRIGGERS"];
    let start_edge = Instant::now();
    let mut edge_count = 0;

    for i in 0..NUM_NODES {
        for e in 0..EDGES_PER_NODE {
            let target_idx = rng.gen_range(0..NUM_NODES);
            if i != target_idx {
                let edge_type = edge_types[e % edge_types.len()];
                let _ = store.create_edge(node_ids[i], node_ids[target_idx], edge_type);
                edge_count += 1;
            }
        }
    }
    let edge_time = start_edge.elapsed();

    println!("  Node ingestion:");
    println!("    Nodes:      {:>10}", format_number(NUM_NODES));
    println!("    Duration:   {:>10.2?}", node_time);
    println!("    Throughput: {:>10.0} nodes/sec", NUM_NODES as f64 / node_time.as_secs_f64());
    println!();
    println!("  Edge ingestion:");
    println!("    Edges:      {:>10}", format_number(edge_count));
    println!("    Duration:   {:>10.2?}", edge_time);
    println!("    Throughput: {:>10.0} edges/sec", edge_count as f64 / edge_time.as_secs_f64());
    println!();

    // Wait for background indexing
    println!("  Waiting for background indexing...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    println!("  Done.");
    println!();

    node_ids
}

fn benchmark_vector_search(store: &GraphStore) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 2: Vector Search (HNSW)                               │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    let mut rng = rand::thread_rng();
    let k_values = [1, 5, 10, 20, 50];
    let num_searches = 1000;

    println!("  {:>6} {:>12} {:>14} {:>10}", "k", "Avg Latency", "QPS", "p99");
    println!("  {:>6} {:>12} {:>14} {:>10}", "-", "-----------", "---", "---");

    for &k in &k_values {
        let mut latencies = Vec::with_capacity(num_searches);

        for _ in 0..num_searches {
            let query: Vec<f32> = (0..VECTOR_DIM).map(|_| rng.gen::<f32>()).collect();
            let t = Instant::now();
            let _ = store.vector_search("Entity", "embedding", &query, k).unwrap();
            latencies.push(t.elapsed());
        }

        latencies.sort();
        let total: std::time::Duration = latencies.iter().sum();
        let avg = total / num_searches as u32;
        let p99 = latencies[(num_searches as f64 * 0.99) as usize];
        let qps = num_searches as f64 / total.as_secs_f64();

        println!("  {:>6} {:>10.2?} {:>12.0}/s {:>8.2?}", k, avg, qps, p99);
    }
    println!();
}

fn benchmark_k_hop(store: &GraphStore, engine: &QueryEngine) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 3: K-Hop Traversal                                    │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    let mut rng = rand::thread_rng();
    let num_queries = 100;

    let queries = [
        ("1-hop", "MATCH (a:Entity)-[:LINKS_TO]->(b:Entity) WHERE a.id = {} RETURN b.id"),
        ("2-hop", "MATCH (a:Entity)-[:LINKS_TO]->(b)-[:LINKS_TO]->(c:Entity) WHERE a.id = {} RETURN c.id"),
    ];

    println!("  {:>8} {:>12} {:>14} {:>12}", "Hops", "Avg Latency", "QPS", "Queries");
    println!("  {:>8} {:>12} {:>14} {:>12}", "----", "-----------", "---", "-------");

    for (name, query_template) in &queries {
        let start = Instant::now();
        let mut success = 0;

        for _ in 0..num_queries {
            let id = rng.gen_range(0..NUM_NODES);
            let q = query_template.replace("{}", &id.to_string());
            if engine.execute(&q, store).is_ok() {
                success += 1;
            }
        }

        let duration = start.elapsed();
        let avg = duration / num_queries as u32;
        let qps = num_queries as f64 / duration.as_secs_f64();

        println!("  {:>8} {:>10.2?} {:>12.0}/s {:>12}", name, avg, qps, success);
    }

    // Multi-hop via graph API (3-hop BFS)
    let start = Instant::now();
    for _ in 0..num_queries {
        let start_node = rng.gen_range(0..NUM_NODES);
        let mut visited = std::collections::HashSet::new();
        let mut frontier = vec![samyama::graph::NodeId::new((start_node + 1) as u64)];

        for _hop in 0..3 {
            let mut next_frontier = Vec::new();
            for &nid in &frontier {
                if visited.insert(nid.as_u64()) {
                    for edge in store.get_outgoing_edges(nid) {
                        if !visited.contains(&edge.target.as_u64()) {
                            next_frontier.push(edge.target);
                        }
                    }
                }
            }
            frontier = next_frontier;
        }
    }
    let hop3_time = start.elapsed();
    println!("  {:>8} {:>10.2?} {:>12.0}/s {:>12}",
        "3-hop", hop3_time / num_queries as u32,
        num_queries as f64 / hop3_time.as_secs_f64(), num_queries);

    println!();
}

fn benchmark_graph_algorithms(store: &GraphStore) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 4: Graph Algorithms                                   │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    println!("  Building graph view...");
    let view_start = Instant::now();
    let view = build_view(store, Some("Entity"), Some("LINKS_TO"), None);
    let view_time = view_start.elapsed();
    println!("  View built in {:?} ({} nodes, {} edges)",
        view_time, view.node_count,
        view.out_offsets.last().copied().unwrap_or(0));
    println!();

    println!("  {:>15} {:>12} {:>20}", "Algorithm", "Duration", "Result");
    println!("  {:>15} {:>12} {:>20}", "---------", "--------", "------");

    // PageRank
    let start = Instant::now();
    let scores = page_rank(&view, PageRankConfig::default());
    let pr_time = start.elapsed();
    let max_score = scores.values().cloned().fold(0.0f64, f64::max);
    println!("  {:>15} {:>10.2?} {:>20}",
        "PageRank", pr_time, format!("max={:.6}", max_score));

    // WCC
    let start = Instant::now();
    let wcc = weakly_connected_components(&view);
    let wcc_time = start.elapsed();
    println!("  {:>15} {:>10.2?} {:>20}",
        "WCC", wcc_time, format!("{} components", wcc.components.len()));

    // BFS from node 0 to a random target
    if view.node_count > 1 {
        let start = Instant::now();
        let target = (view.node_count - 1) as u64;
        let bfs_result = bfs(&view, 0, target);
        let bfs_time = start.elapsed();
        let (depth, found) = match &bfs_result {
            Some(path) => (path.path.len(), true),
            None => (0, false),
        };
        println!("  {:>15} {:>10.2?} {:>20}",
            "BFS", bfs_time, format!("found={}, depth={}", found, depth));
    }

    // Multiple PageRank iterations benchmark
    let start = Instant::now();
    let iterations = 10;
    for _ in 0..iterations {
        let _ = page_rank(&view, PageRankConfig::default());
    }
    let multi_pr_time = start.elapsed();
    println!("  {:>15} {:>10.2?} {:>20}",
        "PageRank x10", multi_pr_time, format!("{:.2?}/iter", multi_pr_time / iterations as u32));

    println!();
}

fn benchmark_mixed_workload(store: &mut GraphStore, engine: &QueryEngine) {
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Benchmark 5: Mixed Workload (80% Read / 20% Write)              │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    let mut rng = rand::thread_rng();
    let total_ops = 10_000;
    let mut reads = 0usize;
    let mut writes = 0usize;
    let mut read_time = std::time::Duration::default();
    let mut write_time = std::time::Duration::default();

    let start = Instant::now();

    for _ in 0..total_ops {
        if rng.gen_bool(0.8) {
            // Read operation: random label scan or property lookup
            let t = Instant::now();
            let labels = ["Server", "User", "Document", "Event", "Metric"];
            let label = labels[rng.gen_range(0..labels.len())];
            let nodes = store.get_nodes_by_label(&Label::new(label));
            std::hint::black_box(nodes.len());
            read_time += t.elapsed();
            reads += 1;
        } else {
            // Write operation: create a new node
            let t = Instant::now();
            let id = store.create_node("MixedWorkload");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("timestamp", writes as i64);
                node.set_property("source", "benchmark");
            }
            write_time += t.elapsed();
            writes += 1;
        }
    }

    let total_time = start.elapsed();

    println!("  Total operations: {:>10}", format_number(total_ops));
    println!("  Duration:         {:>10.2?}", total_time);
    println!("  Overall QPS:      {:>10.0}", total_ops as f64 / total_time.as_secs_f64());
    println!();
    println!("  {:>8} {:>10} {:>12} {:>14}", "Type", "Count", "Total Time", "Avg Latency");
    println!("  {:>8} {:>10} {:>12} {:>14}", "----", "-----", "----------", "-----------");
    println!("  {:>8} {:>10} {:>10.2?} {:>12.2?}",
        "Read", format_number(reads), read_time, read_time / reads as u32);
    println!("  {:>8} {:>10} {:>10.2?} {:>12.2?}",
        "Write", format_number(writes), write_time, write_time / writes as u32);
    println!();
}
