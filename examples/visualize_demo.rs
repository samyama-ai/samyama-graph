use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::algo::{build_view, page_rank, weakly_connected_components, PageRankConfig};
use std::fs::File;
use std::io::Write;
use rand::Rng;

const NUM_NODES: usize = 200;
const CLUSTERS: usize = 5;

#[derive(Clone, Copy)]
struct Point { x: f64, y: f64 }

fn main() {
    println!("=== Samyama Visualization Demo ===");
    
    // 1. Setup Graph
    let mut store = GraphStore::new();
    let mut rng = rand::thread_rng();
    
    println!("1. Generating {} nodes in {} clusters...", NUM_NODES, CLUSTERS);
    
    let mut nodes = Vec::new();
    for i in 0..NUM_NODES {
        let cluster = i % CLUSTERS;
        let id = store.create_node(Label::new(format!("Group{}", cluster)));
        store.set_node_property(id, "cluster", PropertyValue::Integer(cluster as i64)).unwrap();
        nodes.push((id, cluster));
    }

    // 2. Generate Edges (Prefer intra-cluster)
    println!("2. Connecting nodes...");
    for (i, (src, src_cluster)) in nodes.iter().enumerate() {
        // 3-5 connections per node
        let degree = rng.gen_range(3..6);
        for _ in 0..degree {
            // 80% chance to link within cluster
            let target_idx = if rng.gen_bool(0.8) {
                // Find node in same cluster (approximate)
                let offset = rng.gen_range(0..NUM_NODES/CLUSTERS);
                (src_cluster * (NUM_NODES/CLUSTERS) + offset) % NUM_NODES
            } else {
                rng.gen_range(0..NUM_NODES)
            };
            
            if i != target_idx {
                let (tgt, _) = nodes[target_idx];
                let _ = store.create_edge(*src, tgt, "CONNECTED_TO");
            }
        }
    }

    // 3. Run Analytics
    // Build view for analytics
    let view = build_view(&store, None, None, None);

    println!("3. Running PageRank for node sizing...");
    let scores = page_rank(&view, PageRankConfig::default());
    
    println!("4. Running Community Detection (WCC) for coloring...");
    let wcc = weakly_connected_components(&view);

    // 4. Layout & Render (Simple Force Directed)
    println!("5. Simulating Physics & Rendering...");
    let mut positions: Vec<Point> = (0..NUM_NODES).map(|_| Point {
        x: rng.gen_range(100.0..900.0),
        y: rng.gen_range(100.0..900.0)
    }).collect();

    // Physics Loop
    for _ in 0..100 {
        let mut forces: Vec<Point> = vec![Point { x: 0.0, y: 0.0 }; NUM_NODES];
        
        // Repulsion
        for i in 0..NUM_NODES {
            for j in (i+1)..NUM_NODES {
                let dx = positions[i].x - positions[j].x;
                let dy = positions[i].y - positions[j].y;
                let dist_sq = dx*dx + dy*dy + 0.1;
                let force = 5000.0 / dist_sq;
                let fx = dx * force;
                let fy = dy * force;
                
                forces[i].x += fx;
                forces[i].y += fy;
                forces[j].x -= fx;
                forces[j].y -= fy;
            }
        }
        
        // Attraction (Edges)
        let edges = store.all_nodes().iter().flat_map(|n| store.get_outgoing_edges(n.id)).collect::<Vec<_>>();
        for edge in edges {
            let src_idx = (edge.source.as_u64() - 1) as usize;
            let tgt_idx = (edge.target.as_u64() - 1) as usize;
            
            if src_idx < NUM_NODES && tgt_idx < NUM_NODES {
                let dx = positions[tgt_idx].x - positions[src_idx].x;
                let dy = positions[tgt_idx].y - positions[src_idx].y;
                let dist = (dx*dx + dy*dy).sqrt();
                let force = (dist - 50.0) * 0.05; // Spring constant
                
                let fx = (dx / dist) * force;
                let fy = (dy / dist) * force;
                
                forces[src_idx].x += fx;
                forces[src_idx].y += fy;
                forces[tgt_idx].x -= fx;
                forces[tgt_idx].y -= fy;
            }
        }
        
        // Apply
        for i in 0..NUM_NODES {
            positions[i].x += forces[i].x.clamp(-10.0, 10.0);
            positions[i].y += forces[i].y.clamp(-10.0, 10.0);
            
            // Center gravity
            positions[i].x += (500.0 - positions[i].x) * 0.01;
            positions[i].y += (500.0 - positions[i].y) * 0.01;
        }
    }

    // 5. Generate SVG
    let mut svg = String::new();
    svg.push_str(r#"<svg width="1000" height="1000" xmlns="http://www.w3.org/2000/svg" style="background-color: #0f172a;">"#);
    
    // Draw Edges
    let edges = store.all_nodes().iter().flat_map(|n| store.get_outgoing_edges(n.id)).collect::<Vec<_>>();
    for edge in edges {
        let src_idx = (edge.source.as_u64() - 1) as usize;
        let tgt_idx = (edge.target.as_u64() - 1) as usize;
        if src_idx < NUM_NODES && tgt_idx < NUM_NODES {
            svg.push_str(&format!(
                r##"<line x1="{:.1}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="#334155" stroke-width="1" opacity="0.6"/>"##,
                positions[src_idx].x, positions[src_idx].y, positions[tgt_idx].x, positions[tgt_idx].y
            ));
        }
    }

    // Draw Nodes
    let colors = ["#6366f1", "#ec4899", "#10b981", "#f59e0b", "#0ea5e9"];
    for i in 0..NUM_NODES {
        let id = nodes[i].0;
        let cluster = wcc.node_component.get(&id.as_u64()).unwrap_or(&0) % colors.len();
        let rank = scores.get(&id.as_u64()).unwrap_or(&1.0);
        let radius = 3.0 + (rank * 5.0); // Size by PageRank
        
        svg.push_str(&format!(
            r##"<circle cx="{:.1}" cy="{:.1}" r="{:.1}" fill="{}" stroke="#1e293b" stroke-width="1"/>"##,
            positions[i].x, positions[i].y, radius, colors[cluster]
        ));
    }
    
    svg.push_str("</svg>");

    let mut file = File::create("visualization.svg").unwrap();
    file.write_all(svg.as_bytes()).unwrap();
    println!("✅ Saved visualization.svg");
}
