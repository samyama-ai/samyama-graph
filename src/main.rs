use samyama::{GraphStore, NodeId, QueryEngine, RespServer, ServerConfig};
use samyama::http::HttpServer;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    println!("Samyama Graph Database v{}", samyama::version());
    println!("==========================================");
    println!();

    demo_property_graph();
    demo_cypher_queries();

    println!("\n=== Starting RESP Server ===");
    println!("Connect with: redis-cli");
    println!("Try: GRAPH.QUERY default \"MATCH (n) RETURN labels(n), count(n)\"");
    println!();

    start_server().await;
}

fn demo_property_graph() {
    println!("=== Demo 1: Property Graph ===");
    let mut store = GraphStore::new();

    let alice = store.create_node("Person");
    if let Some(node) = store.get_node_mut(alice) {
        node.set_property("name", "Alice");
        node.set_property("age", 30i64);
        println!("Created Person: Alice");
    }

    let bob = store.create_node("Person");
    if let Some(node) = store.get_node_mut(bob) {
        node.set_property("name", "Bob");
        node.set_property("age", 25i64);
        println!("Created Person: Bob");
    }

    store.create_edge(alice, bob, "KNOWS").unwrap();
    println!("Created: Alice -[KNOWS]-> Bob");
    println!("Total nodes: {}, edges: {}", store.node_count(), store.edge_count());
}

fn demo_cypher_queries() {
    println!("\n=== Demo 2: OpenCypher Queries ===");
    let mut store = GraphStore::new();

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

    let engine = QueryEngine::new();
    if let Ok(result) = engine.execute("MATCH (n:Person) RETURN n", &store) {
        println!("Query executed: Found {} persons", result.len());
    }
}

/// Load a single LDBC Graphalytics dataset into the graph store.
///
/// `max_vertices` caps the number of vertices loaded (edges are only created
/// when both endpoints are in the loaded set).
fn load_graphalytics_dataset(
    store: &mut GraphStore,
    dataset: &str,
    directed: bool,
    max_vertices: Option<usize>,
) -> bool {
    let data_dir = std::path::Path::new("data/graphalytics");

    // Try subdirectory first (XS layout), then flat (S-size tar extraction)
    let sub_v = data_dir.join(dataset).join(format!("{}.v", dataset));
    let sub_e = data_dir.join(dataset).join(format!("{}.e", dataset));
    let flat_v = data_dir.join(format!("{}.v", dataset));
    let flat_e = data_dir.join(format!("{}.e", dataset));

    let (v_path, e_path) = if sub_v.exists() && sub_e.exists() {
        (sub_v, sub_e)
    } else if flat_v.exists() && flat_e.exists() {
        (flat_v, flat_e)
    } else {
        println!("  Dataset '{}' not found in data/graphalytics/", dataset);
        println!("  Download with: ./scripts/download_graphalytics.sh --size S");
        return false;
    };

    let limit_str = match max_vertices {
        Some(n) => format!(" (limit: {} vertices)", n),
        None => String::new(),
    };
    println!("Loading LDBC Graphalytics: {}{}...", dataset, limit_str);

    // Phase 1: Read vertices
    let mut vid_to_node: HashMap<u64, NodeId> = HashMap::new();
    if let Ok(file) = File::open(&v_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            if let Some(cap) = max_vertices {
                if vid_to_node.len() >= cap {
                    break;
                }
            }
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Ok(vid) = trimmed.parse::<u64>() {
                let node_id = store.create_node("Vertex");
                if let Some(node) = store.get_node_mut(node_id) {
                    node.set_property("vid", vid as i64);
                    node.set_property("dataset", dataset);
                }
                vid_to_node.insert(vid, node_id);
            }
        }
    }

    // Phase 2: Read edges (only where both endpoints are loaded)
    let edge_type = if directed { "LINKS" } else { "CONNECTS" };
    let mut edge_count: usize = 0;
    if let Ok(file) = File::open(&e_path) {
        let reader = BufReader::new(file);
        for line in reader.lines().filter_map(|l| l.ok()) {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = if trimmed.contains('|') {
                trimmed.split('|').collect()
            } else {
                trimmed.split_whitespace().collect()
            };

            if parts.len() < 2 {
                continue;
            }

            let src = match parts[0].parse::<u64>() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let tgt = match parts[1].parse::<u64>() {
                Ok(v) => v,
                Err(_) => continue,
            };

            if let (Some(&s), Some(&t)) = (vid_to_node.get(&src), vid_to_node.get(&tgt)) {
                if let Ok(eid) = store.create_edge(s, t, edge_type) {
                    if parts.len() >= 3 {
                        if let Ok(w) = parts[2].parse::<f64>() {
                            if let Some(edge) = store.get_edge_mut(eid) {
                                edge.set_property("weight", w);
                            }
                        }
                    }
                    edge_count += 1;
                }
            }
        }
    }

    println!("  Loaded {} vertices, {} edges ({})",
             vid_to_node.len(), edge_count,
             if directed { "directed" } else { "undirected" });
    true
}

async fn start_server() {
    let (mut graph, rx) = GraphStore::with_async_indexing();

    let mut config = ServerConfig::default();
    config.address = std::env::args().find(|a| a.starts_with("--host"))
        .and_then(|_| std::env::args().skip_while(|a| a != "--host").nth(1))
        .unwrap_or_else(|| "127.0.0.1".to_string());
    config.port = std::env::args().find(|a| a.starts_with("--port"))
        .and_then(|_| std::env::args().skip_while(|a| a != "--port").nth(1))
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);

    // Parse --demo flag: medium (~100K vertices) or large (full dataset)
    let demo_mode: Option<String> = std::env::args()
        .position(|a| a == "--demo")
        .and_then(|pos| std::env::args().nth(pos + 1));

    // Initialize persistence FIRST (before loading data)
    let persistence = if let Some(path) = &config.data_path {
        match samyama::PersistenceManager::new(path) {
            Ok(pm) => Some(Arc::new(pm)),
            Err(e) => {
                eprintln!("Failed to initialize persistence: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Recover persisted data from RocksDB
    let mut recovered = false;
    if let Some(ref pm) = persistence {
        match pm.list_persisted_tenants() {
            Ok(tenants) if !tenants.is_empty() => {
                println!("Recovering data for {} tenant(s)...", tenants.len());
                for tenant in &tenants {
                    match pm.recover(tenant) {
                        Ok((nodes, edges)) => {
                            println!("  Tenant '{}': {} nodes, {} edges", tenant, nodes.len(), edges.len());
                            for node in nodes {
                                graph.insert_recovered_node(node);
                            }
                            for edge in edges {
                                if let Err(e) = graph.insert_recovered_edge(edge) {
                                    eprintln!("  Warning: edge recovery error: {}", e);
                                }
                            }
                            recovered = true;
                        }
                        Err(e) => eprintln!("  Error recovering tenant '{}': {}", tenant, e),
                    }
                }
                println!("Recovery complete. Total: {} nodes, {} edges in-memory", graph.node_count(), graph.edge_count());
            }
            Ok(_) => println!("No persisted tenants found."),
            Err(e) => eprintln!("Error listing persisted tenants: {}", e),
        }
    }

    // Load demo data based on --demo flag (skip if persisted data was recovered)
    if !recovered {
        match demo_mode.as_deref() {
            Some("medium") => {
                // ~100K vertex subset of datagen-7_5-fb (Facebook-like social graph)
                load_graphalytics_dataset(&mut graph, "datagen-7_5-fb", false, Some(100_000));
            }
            Some("large") => {
                // Full wiki-Talk dataset (2.4M vertices, 5M edges)
                load_graphalytics_dataset(&mut graph, "wiki-Talk", true, None);
            }
            Some(other) => {
                eprintln!("Unknown --demo mode '{}'. Use: --demo medium  or  --demo large", other);
                eprintln!("  medium  — datagen-7_5-fb (~100K vertices, undirected social graph)");
                eprintln!("  large   — wiki-Talk (2.4M vertices, directed discussion graph)");
            }
            None => {
                // Default: empty graph
            }
        }
    }

    println!("\nGraph Statistics:");
    println!("  Total nodes: {}", graph.node_count());
    println!("  Total edges: {}", graph.edge_count());

    let store = Arc::new(RwLock::new(graph));

    println!("\nServer starting on {}:{}", config.address, config.port);

    // Start background indexer now that store is wrapped in Arc
    if let Some(ref pm) = persistence {
        pm.start_indexer(&*store.read().await, rx);
    }

    // Start HTTP server for Visualizer API on port 8080
    let http_store = Arc::clone(&store);
    tokio::spawn(async move {
        let http_server = HttpServer::new(http_store, 8080);
        println!("HTTP server starting on port 8080 (visualizer + API)");
        if let Err(e) = http_server.start().await {
            eprintln!("HTTP server error: {}", e);
        }
    });

    let server = if let Some(pm) = persistence {
        RespServer::new_with_persistence(config, store, pm)
    } else {
        RespServer::new(config, store)
    };

    println!("Server ready. Press Ctrl+C to stop.\n");

    if let Err(e) = server.start().await {
        eprintln!("Server error: {}", e);
    }
}
