//! Social Network Analysis Platform
//!
//! Demonstrates Samyama graph database capabilities with a 2000+ node
//! tech professional community. Includes PageRank influencer detection,
//! community detection (WCC/SCC), BFS information diffusion, network
//! statistics, and force-directed SVG visualization.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;
use samyama::algo::{
    build_view, page_rank, weakly_connected_components, strongly_connected_components,
    bfs, PageRankConfig,
};
use samyama::{NLQPipeline, TenantManager};
use samyama::persistence::tenant::{LLMProvider, NLQConfig};
use std::fs::File;
use std::io::Write;
use rand::Rng;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NUM_USERS: usize = 2000;
const NUM_COMMUNITIES: usize = 10;

const COMMUNITIES: [&str; NUM_COMMUNITIES] = [
    "AI/ML Engineers",
    "Frontend Developers",
    "Backend Engineers",
    "DevOps/SRE",
    "Data Engineers",
    "Mobile Developers",
    "Security Engineers",
    "Product Managers",
    "UX Designers",
    "QA Engineers",
];

const COMMUNITY_SKILLS: [[&str; 4]; NUM_COMMUNITIES] = [
    ["PyTorch", "TensorFlow", "JAX", "Transformers"],
    ["React", "Vue", "Angular", "Svelte"],
    ["Go", "Rust", "Java", "Node.js"],
    ["Kubernetes", "Terraform", "Ansible", "Docker"],
    ["Spark", "Kafka", "Airflow", "dbt"],
    ["Swift", "Kotlin", "Flutter", "React Native"],
    ["Penetration Testing", "SIEM", "Zero Trust", "Cryptography"],
    ["Roadmapping", "A/B Testing", "OKRs", "User Research"],
    ["Figma", "Prototyping", "Design Systems", "Accessibility"],
    ["Selenium", "Cypress", "Load Testing", "API Testing"],
];

const COMPANIES: [&str; 20] = [
    "Google", "Meta", "Apple", "Amazon", "Microsoft",
    "Netflix", "Stripe", "Airbnb", "Uber", "Databricks",
    "Snowflake", "Confluent", "HashiCorp", "Datadog", "Figma",
    "Vercel", "Supabase", "PlanetScale", "Railway", "Fly.io",
];

const FIRST_NAMES: [&str; 40] = [
    "Alice", "Bob", "Carlos", "Diana", "Elena",
    "Frank", "Grace", "Hiro", "Isha", "Jake",
    "Kenji", "Luna", "Miguel", "Nina", "Oscar",
    "Priya", "Qian", "Rafael", "Sara", "Tomasz",
    "Uma", "Viktor", "Wendy", "Xavier", "Yuki",
    "Zara", "Aiden", "Bianca", "Chloe", "Derek",
    "Elias", "Fatima", "Gavin", "Hannah", "Ivan",
    "Jasmine", "Kai", "Lena", "Mateo", "Nadia",
];

const LAST_NAMES: [&str; 40] = [
    "Chen", "Patel", "Kim", "Nguyen", "Garcia",
    "Muller", "Tanaka", "Singh", "Okonkwo", "Williams",
    "Johansson", "Rossi", "Fernandez", "Kowalski", "Sato",
    "Ali", "Larsen", "Dubois", "Schmidt", "Park",
    "Jensen", "Costa", "Ito", "Bakker", "Novak",
    "Shah", "Rivera", "Yamamoto", "Andersen", "Gupta",
    "Mendez", "Petrov", "Suzuki", "Eriksson", "Torres",
    "Nakamura", "Lund", "Ortiz", "Hoffmann", "Reyes",
];

const COMMUNITY_COLORS: [&str; NUM_COMMUNITIES] = [
    "#6366f1", // indigo  - AI/ML
    "#ec4899", // pink    - Frontend
    "#10b981", // emerald - Backend
    "#f59e0b", // amber   - DevOps
    "#0ea5e9", // sky     - Data
    "#8b5cf6", // violet  - Mobile
    "#ef4444", // red     - Security
    "#14b8a6", // teal    - Product
    "#f97316", // orange  - UX
    "#84cc16", // lime    - QA
];

// ---------------------------------------------------------------------------
// Layout helper
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Vec2 {
    x: f64,
    y: f64,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn is_claude_available() -> bool {
    std::process::Command::new("which")
        .arg("claude")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::main]
async fn main() {
    println!("=== Social Network Analysis Platform ===");
    println!("    Samyama Graph Database - Tech Professional Community\n");

    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let mut rng = rand::thread_rng();

    // -----------------------------------------------------------------------
    // Step 1: Build network (2000 users, 10K+ edges)
    // -----------------------------------------------------------------------
    println!("Step 1: Building social network graph");
    println!("------------------------------------------------------------------------");

    // Create user nodes
    let mut node_ids = Vec::with_capacity(NUM_USERS);
    let mut community_members: Vec<Vec<usize>> = vec![Vec::new(); NUM_COMMUNITIES];

    for i in 0..NUM_USERS {
        let community_idx = i % NUM_COMMUNITIES;
        let first = FIRST_NAMES[rng.gen_range(0..FIRST_NAMES.len())];
        let last = LAST_NAMES[rng.gen_range(0..LAST_NAMES.len())];
        let company = COMPANIES[rng.gen_range(0..COMPANIES.len())];
        let skill = COMMUNITY_SKILLS[community_idx][rng.gen_range(0..4)];
        let years_exp: i64 = rng.gen_range(1..20);

        let id = store.create_node(Label::new("User"));
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", format!("{} {}", first, last));
            node.set_property("company", company);
            node.set_property("primary_skill", skill);
            node.set_property("years_experience", years_exp);
            node.set_property("community", COMMUNITIES[community_idx]);
            node.set_property("community_idx", community_idx as i64);
        }

        node_ids.push(id);
        community_members[community_idx].push(i);
    }

    // Create edges: FOLLOWS, COLLABORATES, ENDORSED
    let mut edge_count: usize = 0;
    let mut follows_count: usize = 0;
    let mut collabs_count: usize = 0;
    let mut endorsed_count: usize = 0;

    for i in 0..NUM_USERS {
        let community_idx = i % NUM_COMMUNITIES;
        let src = node_ids[i];

        // Each node gets 4-7 connections on average -> targets ~11K edges total
        let degree = rng.gen_range(4..8);
        for _ in 0..degree {
            let is_intra = rng.gen_bool(0.80);
            let target_idx = if is_intra {
                // Pick from same community
                let members = &community_members[community_idx];
                members[rng.gen_range(0..members.len())]
            } else {
                // Pick from any other community
                rng.gen_range(0..NUM_USERS)
            };

            if i == target_idx {
                continue;
            }

            let tgt = node_ids[target_idx];
            let target_community = target_idx % NUM_COMMUNITIES;

            let edge_type = if community_idx == target_community {
                if rng.gen_bool(0.5) {
                    follows_count += 1;
                    "FOLLOWS"
                } else {
                    collabs_count += 1;
                    "COLLABORATES"
                }
            } else {
                if rng.gen_bool(0.6) {
                    follows_count += 1;
                    "FOLLOWS"
                } else {
                    endorsed_count += 1;
                    "ENDORSED"
                }
            };

            if store.create_edge(src, tgt, edge_type).is_ok() {
                edge_count += 1;
            }
        }
    }

    println!("  Nodes created:   {:>6}", NUM_USERS);
    println!("  Edges created:   {:>6}", edge_count);
    println!("    FOLLOWS:       {:>6}", follows_count);
    println!("    COLLABORATES:  {:>6}", collabs_count);
    println!("    ENDORSED:      {:>6}", endorsed_count);
    println!("  Communities:     {:>6}", NUM_COMMUNITIES);
    println!();

    // -----------------------------------------------------------------------
    // Step 2: PageRank - Influencer Identification
    // -----------------------------------------------------------------------
    println!("Step 2: Influencer Identification (PageRank)");
    println!("------------------------------------------------------------------------");

    let view = build_view(&store, None, None, None);
    let scores = page_rank(&view, PageRankConfig::default());

    // Sort by PageRank score descending
    let mut ranked: Vec<(u64, f64)> = scores.iter().map(|(&id, &s)| (id, s)).collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    println!();
    println!("  Top 10 Most Influential Professionals:");
    println!("  +------+----------------------------+------------------+------------------+-----------+");
    println!("  | Rank | Name                       | Company          | Community        | PageRank  |");
    println!("  +------+----------------------------+------------------+------------------+-----------+");

    for (rank, &(node_id, score)) in ranked.iter().take(10).enumerate() {
        let graph_nid = samyama::graph::NodeId::new(node_id);
        let nodes = store.all_nodes();
        let node = nodes.iter().find(|n| n.id == graph_nid).unwrap();

        let name = match node.get_property("name") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => "Unknown".to_string(),
        };
        let company = match node.get_property("company") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => "Unknown".to_string(),
        };
        let community = match node.get_property("community") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => "Unknown".to_string(),
        };

        println!(
            "  | {:>4} | {:<26} | {:<16} | {:<16} | {:>9.4} |",
            rank + 1, name, company, community, score
        );
    }
    println!("  +------+----------------------------+------------------+------------------+-----------+");
    println!();

    // -----------------------------------------------------------------------
    // Step 3: Community Detection (WCC)
    // -----------------------------------------------------------------------
    println!("Step 3: Community Detection (Weakly Connected Components)");
    println!("------------------------------------------------------------------------");

    let wcc = weakly_connected_components(&view);
    let num_wcc = wcc.components.len();

    // Compute component sizes
    let mut comp_sizes: Vec<(usize, usize)> = wcc.components.iter()
        .map(|(&comp_id, members)| (comp_id, members.len()))
        .collect();
    comp_sizes.sort_by(|a, b| b.1.cmp(&a.1));

    println!("  WCC components detected: {}", num_wcc);
    println!();
    println!("  Component Size Distribution (top 10):");
    println!("  +------------+--------+-------------------------------------------+");
    println!("  | Component  | Size   | Bar                                       |");
    println!("  +------------+--------+-------------------------------------------+");

    let max_bar = 40;
    let max_size = comp_sizes.first().map(|c| c.1).unwrap_or(1);
    for (idx, &(comp_id, size)) in comp_sizes.iter().take(10).enumerate() {
        let bar_len = (size as f64 / max_size as f64 * max_bar as f64) as usize;
        let bar: String = "#".repeat(bar_len);
        println!(
            "  | {:>10} | {:>6} | {:<41} |",
            idx + 1, size, bar
        );
        let _ = comp_id; // used for iteration
    }
    println!("  +------------+--------+-------------------------------------------+");

    if num_wcc == 1 {
        println!("  Note: Single connected component (highly connected network).");
        println!("  This is expected with ~5 edges/node in a 2000-node graph.");
    } else {
        println!("  Detected {} disconnected sub-communities.", num_wcc);
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 4: Echo Chamber Analysis (SCC)
    // -----------------------------------------------------------------------
    println!("Step 4: Echo Chamber Analysis (Strongly Connected Components)");
    println!("------------------------------------------------------------------------");

    let scc = strongly_connected_components(&view);
    let num_scc = scc.components.len();

    let mut scc_sizes: Vec<(usize, usize)> = scc.components.iter()
        .map(|(&comp_id, members)| (comp_id, members.len()))
        .collect();
    scc_sizes.sort_by(|a, b| b.1.cmp(&a.1));

    let large_sccs: Vec<&(usize, usize)> = scc_sizes.iter().filter(|c| c.1 >= 5).collect();
    let singleton_count = scc_sizes.iter().filter(|c| c.1 == 1).count();

    println!("  Total SCC components:       {:>6}", num_scc);
    println!("  Large echo chambers (>=5):  {:>6}", large_sccs.len());
    println!("  Singleton nodes:            {:>6}", singleton_count);
    println!();

    if !large_sccs.is_empty() {
        println!("  Largest Echo Chambers:");
        println!("  +------+--------+-------------------------------------------------------+");
        println!("  |   #  | Size   | Description                                           |");
        println!("  +------+--------+-------------------------------------------------------+");

        for (idx, &&(_, size)) in large_sccs.iter().take(5).enumerate() {
            let desc = if size > 500 {
                "Dominant mutual-follow cluster (potential filter bubble)"
            } else if size > 100 {
                "Large reciprocal network (strong information echo)"
            } else if size > 20 {
                "Medium tightly-knit group (specialized discussion circle)"
            } else {
                "Small mutual-endorsement cluster"
            };
            println!("  | {:>4} | {:>6} | {:<53} |", idx + 1, size, desc);
        }
        println!("  +------+--------+-------------------------------------------------------+");
    } else {
        println!("  No large echo chambers detected (low reciprocal edge density).");
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 5: Information Diffusion (BFS from top influencer)
    // -----------------------------------------------------------------------
    println!("Step 5: Information Diffusion Simulation (BFS)");
    println!("------------------------------------------------------------------------");

    let top_influencer_id = ranked[0].0;
    let top_name = {
        let graph_nid = samyama::graph::NodeId::new(top_influencer_id);
        let nodes = store.all_nodes();
        let node = nodes.iter().find(|n| n.id == graph_nid).unwrap();
        match node.get_property("name") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => "Unknown".to_string(),
        }
    };

    // BFS requires (source, target). We try multiple candidates from the bottom half
    // of the ranking to find a reachable target that demonstrates path traversal.
    let mut bfs_result = None;
    let mut chosen_target_id = 0u64;
    let mid = ranked.len() / 2;
    for &(candidate_id, _) in ranked[mid..].iter().take(50) {
        if candidate_id == top_influencer_id {
            continue;
        }
        if let Some(path) = bfs(&view, top_influencer_id, candidate_id) {
            chosen_target_id = candidate_id;
            bfs_result = Some(path);
            break;
        }
    }

    println!("  Source:  {} (top influencer, PageRank {:.4})", top_name, ranked[0].1);
    println!();

    match bfs_result {
        Some(path) => {
            let hops = path.cost as usize;
            println!("  BFS path to farthest-ranked node: {} hops", hops);
            println!("  Path length: {} nodes", path.path.len());
            println!();
            println!("  Diffusion Reach Estimate:");
            println!("  +----------+-------------------------------------------+");
            println!("  | Hops     | Interpretation                            |");
            println!("  +----------+-------------------------------------------+");
            println!("  | 1 hop    | Direct followers (immediate reach)        |");
            println!("  | 2 hops   | Friends-of-friends (viral threshold)      |");
            println!("  | 3 hops   | Three degrees of separation               |");
            if hops > 3 {
                println!("  | {} hops   | Actual path to lowest-ranked node         |", hops);
            }
            println!("  +----------+-------------------------------------------+");
            println!();

            // Show first few nodes in the path
            println!("  Path trace (first 6 hops):");
            for (step, &nid) in path.path.iter().take(6).enumerate() {
                let graph_nid = samyama::graph::NodeId::new(nid);
                let nodes = store.all_nodes();
                let node = nodes.iter().find(|n| n.id == graph_nid).unwrap();
                let name = match node.get_property("name") {
                    Some(PropertyValue::String(s)) => s.clone(),
                    _ => "?".to_string(),
                };
                let community = match node.get_property("community") {
                    Some(PropertyValue::String(s)) => s.clone(),
                    _ => "?".to_string(),
                };
                let arrow = if step == 0 { "  [START]" } else { "  ->" };
                println!("    {} {} ({}) [hop {}]", arrow, name, community, step);
            }
            if path.path.len() > 6 {
                println!("    ... ({} more hops)", path.path.len() - 6);
            }
        }
        None => {
            println!("  No path found (network is disconnected between source and target).");
        }
    }
    println!();

    // -----------------------------------------------------------------------
    // Step 6: Network Statistics
    // -----------------------------------------------------------------------
    println!("Step 6: Network Statistics");
    println!("------------------------------------------------------------------------");

    // Average degree
    let total_degree: usize = (0..view.node_count)
        .map(|idx| view.out_degree(idx) + view.in_degree(idx))
        .sum();
    let avg_degree = total_degree as f64 / view.node_count as f64;

    // Max degree
    let max_degree = (0..view.node_count)
        .map(|idx| view.out_degree(idx) + view.in_degree(idx))
        .max()
        .unwrap_or(0);

    // Min degree
    let min_degree = (0..view.node_count)
        .map(|idx| view.out_degree(idx) + view.in_degree(idx))
        .min()
        .unwrap_or(0);

    // Degree distribution histogram
    let mut degree_dist: HashMap<usize, usize> = HashMap::new();
    for idx in 0..view.node_count {
        let deg = view.out_degree(idx) + view.in_degree(idx);
        *degree_dist.entry(deg).or_insert(0) += 1;
    }

    // Clustering coefficient approximation (sample 200 nodes)
    let sample_size = 200.min(view.node_count);
    let mut clustering_sum = 0.0;
    let mut sampled = 0;
    for idx in (0..view.node_count).step_by(view.node_count / sample_size) {
        let neighbors: Vec<usize> = view.successors(idx).to_vec();
        let k = neighbors.len();
        if k < 2 {
            continue;
        }
        // Count edges between neighbors
        let mut triangles = 0;
        for i in 0..k {
            let ni_successors = view.successors(neighbors[i]);
            for j in (i + 1)..k {
                if ni_successors.contains(&neighbors[j]) {
                    triangles += 1;
                }
            }
        }
        let possible = k * (k - 1) / 2;
        if possible > 0 {
            clustering_sum += triangles as f64 / possible as f64;
            sampled += 1;
        }
    }
    let avg_clustering = if sampled > 0 {
        clustering_sum / sampled as f64
    } else {
        0.0
    };

    // Diameter estimate via BFS from a few nodes
    let mut max_path_len: usize = 0;
    let sample_nodes = [0usize, NUM_USERS / 4, NUM_USERS / 2, 3 * NUM_USERS / 4, NUM_USERS - 1];
    for &src_idx in &sample_nodes {
        if src_idx >= view.node_count {
            continue;
        }
        let src_id = view.index_to_node[src_idx];
        // Try reaching a few distant nodes
        for &tgt_idx in &sample_nodes {
            if tgt_idx >= view.node_count || src_idx == tgt_idx {
                continue;
            }
            let tgt_id = view.index_to_node[tgt_idx];
            if let Some(result) = bfs(&view, src_id, tgt_id) {
                let hops = result.cost as usize;
                if hops > max_path_len {
                    max_path_len = hops;
                }
            }
        }
    }

    // PageRank statistics
    let pr_values: Vec<f64> = scores.values().cloned().collect();
    let pr_max = pr_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let pr_min = pr_values.iter().cloned().fold(f64::INFINITY, f64::min);
    let pr_avg = pr_values.iter().sum::<f64>() / pr_values.len() as f64;

    println!();
    println!("  +-------------------------------+-------------------+");
    println!("  | Metric                        | Value             |");
    println!("  +-------------------------------+-------------------+");
    println!("  | Total nodes                   | {:>17} |", NUM_USERS);
    println!("  | Total edges                   | {:>17} |", edge_count);
    println!("  | Average degree                | {:>17.2} |", avg_degree);
    println!("  | Max degree                    | {:>17} |", max_degree);
    println!("  | Min degree                    | {:>17} |", min_degree);
    println!("  | Clustering coeff (approx)     | {:>17.4} |", avg_clustering);
    println!("  | Diameter estimate             | {:>17} |", max_path_len);
    println!("  | WCC components                | {:>17} |", num_wcc);
    println!("  | SCC components                | {:>17} |", num_scc);
    println!("  | PageRank max                  | {:>17.4} |", pr_max);
    println!("  | PageRank min                  | {:>17.4} |", pr_min);
    println!("  | PageRank avg                  | {:>17.4} |", pr_avg);
    println!("  +-------------------------------+-------------------+");
    println!();

    // Degree distribution (top buckets)
    let mut deg_buckets: Vec<(usize, usize)> = degree_dist.into_iter().collect();
    deg_buckets.sort_by_key(|&(deg, _)| deg);
    println!("  Degree Distribution (sampled buckets):");
    println!("  +----------+--------+-------------------------------------------+");
    println!("  | Degree   | Count  | Distribution                              |");
    println!("  +----------+--------+-------------------------------------------+");
    let max_count = deg_buckets.iter().map(|b| b.1).max().unwrap_or(1);
    for &(deg, count) in deg_buckets.iter().take(15) {
        let bar_len = (count as f64 / max_count as f64 * 40.0) as usize;
        let bar: String = "#".repeat(bar_len);
        println!("  | {:>8} | {:>6} | {:<41} |", deg, count, bar);
    }
    if deg_buckets.len() > 15 {
        println!("  |      ... |    ... | (truncated, {} total buckets)              |", deg_buckets.len());
    }
    println!("  +----------+--------+-------------------------------------------+");
    println!();

    // -----------------------------------------------------------------------
    // Step 7: SVG Visualization (Force-Directed Layout)
    // -----------------------------------------------------------------------
    println!("Step 7: Force-Directed SVG Visualization");
    println!("------------------------------------------------------------------------");

    // Build node index -> community_idx mapping
    let mut node_community: HashMap<u64, usize> = HashMap::new();
    for node in store.all_nodes() {
        let cidx = match node.get_property("community_idx") {
            Some(PropertyValue::Integer(v)) => *v as usize,
            _ => 0,
        };
        node_community.insert(node.id.as_u64(), cidx);
    }

    // Initialize positions randomly
    let mut positions: Vec<Vec2> = (0..view.node_count)
        .map(|_| Vec2 {
            x: rng.gen_range(100.0..900.0),
            y: rng.gen_range(100.0..900.0),
        })
        .collect();

    println!("  Running 100 physics iterations on {} nodes...", view.node_count);

    // Force-directed layout: 100 iterations
    // For performance with 2000 nodes, use sampling for repulsion
    for iter in 0..100 {
        let mut forces: Vec<Vec2> = vec![Vec2 { x: 0.0, y: 0.0 }; view.node_count];
        let temperature = 10.0 * (1.0 - iter as f64 / 100.0); // cooling

        // Repulsion: sample pairs for O(N*k) instead of O(N^2)
        // Each node repels ~50 random others
        let repulsion_samples = 50;
        for i in 0..view.node_count {
            for _ in 0..repulsion_samples {
                let j = rng.gen_range(0..view.node_count);
                if i == j {
                    continue;
                }
                let dx = positions[i].x - positions[j].x;
                let dy = positions[i].y - positions[j].y;
                let dist_sq = dx * dx + dy * dy + 1.0;
                let force = 8000.0 / dist_sq;
                forces[i].x += dx * force;
                forces[i].y += dy * force;
            }
        }

        // Attraction along edges
        for u_idx in 0..view.node_count {
            for &v_idx in view.successors(u_idx) {
                let dx = positions[v_idx].x - positions[u_idx].x;
                let dy = positions[v_idx].y - positions[u_idx].y;
                let dist = (dx * dx + dy * dy).sqrt().max(1.0);
                let force = (dist - 30.0) * 0.02;

                let fx = (dx / dist) * force;
                let fy = (dy / dist) * force;

                forces[u_idx].x += fx;
                forces[u_idx].y += fy;
                forces[v_idx].x -= fx;
                forces[v_idx].y -= fy;
            }
        }

        // Apply forces with temperature-based clamping
        for i in 0..view.node_count {
            positions[i].x += forces[i].x.clamp(-temperature, temperature);
            positions[i].y += forces[i].y.clamp(-temperature, temperature);

            // Center gravity
            positions[i].x += (500.0 - positions[i].x) * 0.01;
            positions[i].y += (500.0 - positions[i].y) * 0.01;

            // Keep in bounds
            positions[i].x = positions[i].x.clamp(20.0, 980.0);
            positions[i].y = positions[i].y.clamp(20.0, 980.0);
        }
    }

    println!("  Layout computation complete.");

    // Generate SVG
    let mut svg = String::with_capacity(2_000_000);
    svg.push_str(r#"<svg width="1000" height="1000" xmlns="http://www.w3.org/2000/svg" style="background-color: #0f172a;">"#);
    svg.push('\n');

    // Title
    svg.push_str(r##"<text x="500" y="30" text-anchor="middle" fill="#e2e8f0" font-family="sans-serif" font-size="18" font-weight="bold">Samyama Social Network - Tech Professional Community (2000 nodes)</text>"##);
    svg.push('\n');

    // Legend
    for (ci, &color) in COMMUNITY_COLORS.iter().enumerate() {
        let lx = 20 + (ci % 5) * 200;
        let ly = 960 + (ci / 5) * 20;
        svg.push_str(&format!(
            r##"<circle cx="{}" cy="{}" r="5" fill="{}"/><text x="{}" y="{}" fill="#94a3b8" font-family="sans-serif" font-size="10">{}</text>"##,
            lx, ly, color, lx + 10, ly + 4, COMMUNITIES[ci]
        ));
        svg.push('\n');
    }

    // Draw edges (low opacity)
    let edge_colors: HashMap<&str, &str> = HashMap::from([
        ("FOLLOWS", "#334155"),
        ("COLLABORATES", "#1e3a5f"),
        ("ENDORSED", "#3f2a1e"),
    ]);

    for node in store.all_nodes() {
        let src_id = node.id.as_u64();
        if let Some(&src_idx) = view.node_to_index.get(&src_id) {
            for edge in store.get_outgoing_edges(node.id) {
                let tgt_id = edge.target.as_u64();
                if let Some(&tgt_idx) = view.node_to_index.get(&tgt_id) {
                    let color = edge_colors
                        .get(edge.edge_type.as_str())
                        .unwrap_or(&"#334155");
                    svg.push_str(&format!(
                        r##"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="{}" stroke-width="0.3" opacity="0.4"/>"##,
                        positions[src_idx].x,
                        positions[src_idx].y,
                        positions[tgt_idx].x,
                        positions[tgt_idx].y,
                        color
                    ));
                    svg.push('\n');
                }
            }
        }
    }

    // Draw nodes (sized by PageRank, colored by community)
    // Normalize PageRank for radius
    let pr_range = (pr_max - pr_min).max(0.001);

    for idx in 0..view.node_count {
        let nid = view.index_to_node[idx];
        let cidx = *node_community.get(&nid).unwrap_or(&0);
        let color = COMMUNITY_COLORS[cidx % COMMUNITY_COLORS.len()];
        let rank_score = scores.get(&nid).unwrap_or(&pr_avg);
        let normalized = (rank_score - pr_min) / pr_range;
        let radius = 1.5 + normalized * 6.0; // 1.5 to 7.5

        svg.push_str(&format!(
            r##"<circle cx="{:.1}" cy="{:.1}" r="{:.1}" fill="{}" stroke="#1e293b" stroke-width="0.5" opacity="0.85"/>"##,
            positions[idx].x, positions[idx].y, radius, color
        ));
        svg.push('\n');
    }

    // Highlight top 5 influencers with labels
    for (rank, &(nid, _score)) in ranked.iter().take(5).enumerate() {
        if let Some(&idx) = view.node_to_index.get(&nid) {
            let graph_nid = samyama::graph::NodeId::new(nid);
            let nodes = store.all_nodes();
            let node = nodes.iter().find(|n| n.id == graph_nid).unwrap();
            let name = match node.get_property("name") {
                Some(PropertyValue::String(s)) => s.clone(),
                _ => "?".to_string(),
            };

            svg.push_str(&format!(
                r##"<circle cx="{:.1}" cy="{:.1}" r="8" fill="none" stroke="#fbbf24" stroke-width="2"/>"##,
                positions[idx].x, positions[idx].y
            ));
            svg.push_str(&format!(
                r##"<text x="{:.1}" y="{:.1}" fill="#fbbf24" font-family="sans-serif" font-size="9" text-anchor="middle">#{} {}</text>"##,
                positions[idx].x, positions[idx].y - 12.0, rank + 1, name
            ));
            svg.push('\n');
        }
    }

    svg.push_str("</svg>\n");

    let svg_path = "social_network.svg";
    let mut file = File::create(svg_path).unwrap();
    file.write_all(svg.as_bytes()).unwrap();

    println!("  Saved: {} ({:.1} KB)", svg_path, svg.len() as f64 / 1024.0);
    println!();

    // -----------------------------------------------------------------------
    // NLQ Social Network Intelligence (ClaudeCode)
    // -----------------------------------------------------------------------
    println!("========================================================================");
    println!("  NLQ Social Network Intelligence (ClaudeCode)");
    println!("========================================================================");
    println!();

    if is_claude_available() {
        println!("  [ok] Claude Code CLI detected — running NLQ queries");
        println!();

        let nlq_config = NLQConfig {
            enabled: true,
            provider: LLMProvider::ClaudeCode,
            model: String::new(),
            api_key: None,
            api_base_url: None,
            system_prompt: Some("You are a Cypher query expert for a social network graph.".to_string()),
        };

        let tenant_mgr = TenantManager::new();
        tenant_mgr.create_tenant("social_nlq".to_string(), "Social NLQ".to_string(), None).unwrap();
        tenant_mgr.update_nlq_config("social_nlq", Some(nlq_config.clone())).unwrap();

        let schema_summary = "Node labels: User\n\
                              Edge types: FOLLOWS, COLLABORATES, ENDORSED\n\
                              Properties: User(name, company, role, specialty, followers, following)";

        let nlq_pipeline = NLQPipeline::new(nlq_config).unwrap();

        let nlq_questions = vec![
            "Who are the most followed AI/ML engineers at Google?",
            "Find users who both follow and are followed by the same people",
        ];

        for (i, question) in nlq_questions.iter().enumerate() {
            println!("  NLQ Query {}: \"{}\"", i + 1, question);
            match nlq_pipeline.text_to_cypher(question, schema_summary).await {
                Ok(cypher) => {
                    println!("  Generated Cypher: {}", cypher);
                    match engine.execute(&cypher, &store) {
                        Ok(batch) => println!("  Results: {} records", batch.len()),
                        Err(e) => println!("  Execution error: {}", e),
                    }
                }
                Err(e) => println!("  NLQ translation error: {}", e),
            }
            println!();
        }
    } else {
        println!("  [skip] Claude Code CLI not found — skipping NLQ queries");
        println!("  Install: https://docs.anthropic.com/en/docs/claude-code");
    }
    println!();

    // -----------------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------------
    println!("========================================================================");
    println!("  ANALYSIS COMPLETE");
    println!("========================================================================");
    println!();
    println!("  Graph:          {} users, {} connections across {} communities",
        NUM_USERS, edge_count, NUM_COMMUNITIES);
    println!("  Top Influencer: {} (PageRank {:.4})", top_name, ranked[0].1);
    println!("  Connectivity:   {} WCC, {} SCC components", num_wcc, num_scc);
    println!("  Network:        avg degree {:.1}, clustering coeff {:.4}",
        avg_degree, avg_clustering);
    println!("  Visualization:  {}", svg_path);
    println!("  NLQ:            ClaudeCode pipeline (social network intelligence)");
    println!();
    println!("  Samyama Graph Database - Social Network Analysis Demo");
    println!("========================================================================");
}
