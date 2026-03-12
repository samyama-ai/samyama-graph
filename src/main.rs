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

/// Build a synthetic social network with rich schema.
///
/// Creates 6 node labels (Person, Company, City, Post, Comment, Tag) and
/// 8 edge types (KNOWS, WORKS_AT, LIVES_IN, WROTE, COMMENTED, REPLIED_TO,
/// LIKES, HAS_TAG) for a total of ~5,250 nodes and ~10,000 edges.
fn build_social_network(store: &mut GraphStore) {
    println!("Building synthetic social network...");

    // --- Reference data ---
    let first_names = [
        "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
        "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
        "Quinn", "Rosa", "Sam", "Tara", "Uma", "Vic", "Wendy", "Xander",
        "Yara", "Zane",
    ];
    let last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
        "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    ];
    let city_data = [
        ("New York", "US", 8_336_817i64), ("Los Angeles", "US", 3_979_576),
        ("Chicago", "US", 2_693_976), ("Houston", "US", 2_320_268),
        ("San Francisco", "US", 873_965), ("Seattle", "US", 737_015),
        ("Austin", "US", 978_908), ("Boston", "US", 675_647),
        ("Denver", "US", 715_522), ("Portland", "US", 652_503),
        ("London", "UK", 8_982_000), ("Manchester", "UK", 553_230),
        ("Edinburgh", "UK", 524_930), ("Berlin", "DE", 3_644_826),
        ("Munich", "DE", 1_471_508), ("Hamburg", "DE", 1_841_179),
        ("Paris", "FR", 2_161_000), ("Lyon", "FR", 513_275),
        ("Toronto", "CA", 2_794_356), ("Vancouver", "CA", 662_248),
        ("Sydney", "AU", 5_312_163), ("Melbourne", "AU", 5_078_193),
        ("Tokyo", "JP", 13_960_000), ("Singapore", "SG", 5_454_000),
        ("Bangalore", "IN", 8_443_675), ("Mumbai", "IN", 12_442_373),
        ("São Paulo", "BR", 12_325_232), ("Dublin", "IE", 544_107),
        ("Amsterdam", "NL", 872_680), ("Stockholm", "SE", 975_551),
    ];
    let company_data = [
        ("Acme Corp", "Technology", 5000i64), ("GlobalBank", "Finance", 12000),
        ("MediCare Plus", "Healthcare", 3500), ("EcoEnergy", "Energy", 2200),
        ("DataStream", "Technology", 800), ("CloudNine", "Technology", 1500),
        ("BioGenesis", "Healthcare", 4200), ("QuantumLeap", "Technology", 350),
        ("GreenField", "Agriculture", 6000), ("SkyRoute", "Logistics", 9000),
        ("NexGen AI", "Technology", 600), ("FinEdge", "Finance", 2800),
        ("UrbanBuild", "Construction", 7500), ("MediaPulse", "Media", 1200),
        ("AeroSpace X", "Aerospace", 4500), ("FoodChain", "Retail", 15000),
        ("CyberShield", "Security", 900), ("EduPath", "Education", 1800),
        ("TravelWise", "Travel", 3200), ("PharmaCore", "Healthcare", 5500),
    ];
    let tags = [
        "rust", "python", "javascript", "database", "graphdb", "ai",
        "machinelearning", "cloud", "devops", "kubernetes", "startup",
        "opensource", "performance", "security", "data", "engineering",
        "product", "design", "career", "remote",
    ];
    let post_topics = [
        "Just shipped a new feature for our graph database!",
        "Thoughts on knowledge graphs vs vector databases?",
        "Anyone using Cypher in production? Share your experience!",
        "The future of AI-native databases",
        "Graph algorithms that changed how we think about data",
        "Why property graphs beat RDF for most use cases",
        "Performance tuning tips for large-scale graph traversals",
        "How we reduced query latency by 10x with late materialization",
        "Building a real-time fraud detection system with graphs",
        "Open-source graph databases: a comparison",
        "The rise of multi-model databases",
        "Why every data engineer should learn graph theory",
        "Scaling graph analytics to billions of edges",
        "Our journey from PostgreSQL to a native graph database",
        "Community detection algorithms explained simply",
        "Vector search meets graph traversal: the best of both worlds",
        "What I learned building a distributed graph database in Rust",
        "Graph-powered recommendation engines",
        "The inverted LLM pattern: let graphs handle the data",
        "LDBC benchmark results: what they really mean",
    ];

    // --- Phase 1: Create City nodes ---
    let mut city_ids: Vec<NodeId> = Vec::with_capacity(30);
    for (name, country, population) in &city_data {
        let id = store.create_node("City");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", *name);
            node.set_property("country", *country);
            node.set_property("population", *population);
        }
        city_ids.push(id);
    }

    // --- Phase 2: Create Company nodes ---
    let mut company_ids: Vec<NodeId> = Vec::with_capacity(20);
    for (i, (name, industry, size)) in company_data.iter().enumerate() {
        let id = store.create_node("Company");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", *name);
            node.set_property("industry", *industry);
            node.set_property("employees", *size);
            node.set_property("founded", 1990i64 + (i as i64 * 3) % 35);
        }
        // LOCATED_IN edge: company → city
        let city = city_ids[i % city_ids.len()];
        let _ = store.create_edge(id, city, "LOCATED_IN");
        company_ids.push(id);
    }

    // --- Phase 3: Create Tag nodes ---
    let mut tag_ids: Vec<NodeId> = Vec::with_capacity(20);
    for tag in &tags {
        let id = store.create_node("Tag");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", *tag);
        }
        tag_ids.push(id);
    }

    // --- Phase 4: Create Person nodes ---
    let num_persons = 200;
    let mut person_ids: Vec<NodeId> = Vec::with_capacity(num_persons);
    for i in 0..num_persons {
        let first = first_names[i % first_names.len()];
        let last = last_names[i / first_names.len() % last_names.len()];
        let id = store.create_node("Person");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", format!("{} {}", first, last));
            node.set_property("age", 22i64 + (i as i64 * 7) % 45);
            node.set_property("email", format!("{}.{}@example.com", first.to_lowercase(), last.to_lowercase()));
        }
        // LIVES_IN edge: person → city
        let city = city_ids[i % city_ids.len()];
        let _ = store.create_edge(id, city, "LIVES_IN");
        // WORKS_AT edge: person → company
        let company = company_ids[i % company_ids.len()];
        if let Ok(eid) = store.create_edge(id, company, "WORKS_AT") {
            if let Some(edge) = store.get_edge_mut(eid) {
                edge.set_property("since", 2015i64 + (i as i64 * 3) % 11);
                edge.set_property("role", match i % 5 {
                    0 => "Engineer",
                    1 => "Manager",
                    2 => "Analyst",
                    3 => "Designer",
                    _ => "Director",
                });
            }
        }
        person_ids.push(id);
    }

    // --- Phase 5: KNOWS edges (social connections) ---
    let mut knows_count = 0usize;
    for i in 0..num_persons {
        // Each person knows 5-8 others (deterministic spread)
        let degree = 5 + i % 4;
        for d in 0..degree {
            let j = (i + 1 + d * 7 + i * 3) % num_persons;
            if i != j {
                if store.create_edge(person_ids[i], person_ids[j], "KNOWS").is_ok() {
                    knows_count += 1;
                }
            }
        }
    }

    // --- Phase 6: Create Post nodes ---
    let num_posts = 2000;
    let mut post_ids: Vec<NodeId> = Vec::with_capacity(num_posts);
    for i in 0..num_posts {
        let id = store.create_node("Post");
        if let Some(node) = store.get_node_mut(id) {
            let topic = post_topics[i % post_topics.len()];
            node.set_property("title", topic);
            node.set_property("content", format!("{}. This is post #{} with detailed thoughts on the topic.", topic, i));
            node.set_property("created_at", 1700000000000i64 + (i as i64 * 3_600_000));
            node.set_property("views", (i as i64 * 17) % 5000);
        }
        // WROTE edge: person → post
        let author = person_ids[i % num_persons];
        let _ = store.create_edge(author, id, "WROTE");
        // HAS_TAG edges: 1-3 tags per post
        let num_tags = 1 + i % 3;
        for t in 0..num_tags {
            let tag = tag_ids[(i + t * 7) % tag_ids.len()];
            let _ = store.create_edge(id, tag, "HAS_TAG");
        }
        post_ids.push(id);
    }

    // --- Phase 7: Create Comment nodes ---
    let num_comments = 3000;
    let mut comment_ids: Vec<NodeId> = Vec::with_capacity(num_comments);
    for i in 0..num_comments {
        let id = store.create_node("Comment");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("text", format!("Comment #{}: Great point! Here are my thoughts...", i));
            node.set_property("created_at", 1700100000000i64 + (i as i64 * 1_800_000));
        }
        // WROTE edge: person → comment
        let author = person_ids[i % num_persons];
        let _ = store.create_edge(author, id, "WROTE");
        // COMMENTED edge: comment → post (most comments reply to posts)
        if i % 3 != 0 || comment_ids.is_empty() {
            let post = post_ids[i % num_posts];
            let _ = store.create_edge(id, post, "COMMENTED");
        } else {
            // REPLIED_TO edge: comment → comment (threaded replies)
            let parent = comment_ids[(i * 7) % comment_ids.len()];
            let _ = store.create_edge(id, parent, "REPLIED_TO");
        }
        comment_ids.push(id);
    }

    // --- Phase 8: LIKES edges (person → post) ---
    let mut likes_count = 0usize;
    for i in 0..num_persons {
        // Each person likes 5-15 posts
        let num_likes = 5 + (i * 7) % 11;
        for l in 0..num_likes {
            let post = post_ids[(i * 13 + l * 37) % num_posts];
            if store.create_edge(person_ids[i], post, "LIKES").is_ok() {
                likes_count += 1;
            }
        }
    }

    let total_nodes = city_ids.len() + company_ids.len() + tag_ids.len()
        + person_ids.len() + post_ids.len() + comment_ids.len();
    println!("  Schema: 6 labels (Person, Company, City, Post, Comment, Tag)");
    println!("          8 edge types (KNOWS, WORKS_AT, LIVES_IN, LOCATED_IN, WROTE, COMMENTED, REPLIED_TO, LIKES, HAS_TAG)");
    println!("  Nodes:  {} Person, {} Company, {} City, {} Tag, {} Post, {} Comment",
             person_ids.len(), company_ids.len(), city_ids.len(),
             tag_ids.len(), post_ids.len(), comment_ids.len());
    println!("  Edges:  {} KNOWS, {} LIKES, {} Posts, {} Comments (total: {})",
             knows_count, likes_count, num_posts, num_comments,
             store.edge_count());
    println!("  Total:  {} nodes, {} edges", total_nodes, store.edge_count());
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

    // Parse --demo flag: social (rich schema) or large (scale stress test)
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
            Some("social") => {
                // Rich schema: 6 labels, 9 edge types, ~5K nodes, ~10K edges
                build_social_network(&mut graph);
            }
            Some("large") => {
                // Full wiki-Talk dataset (2.4M vertices, 5M edges)
                load_graphalytics_dataset(&mut graph, "wiki-Talk", true, None);
            }
            Some(other) => {
                eprintln!("Unknown --demo mode '{}'. Use: --demo social  or  --demo large", other);
                eprintln!("  social  — synthetic social network (5K nodes, 6 labels, 9 edge types)");
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
