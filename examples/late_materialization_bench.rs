//! Micro-benchmark: Late Materialization impact on traversal
//!
//! Compares:
//! 1. Raw graph API traversal (get_outgoing_edges - clones Edge)
//! 2. Lightweight traversal (get_outgoing_edge_targets - no clone)
//! 3. Cypher 1-hop query via QueryEngine (full pipeline)
//! 4. Cypher 2-hop query via QueryEngine (full pipeline)

use samyama::{GraphStore, Label, PropertyValue, QueryEngine};
use std::time::Instant;
use std::collections::HashSet;
use rand::Rng;

const NUM_NODES: usize = 10_000;
const EDGES_PER_NODE: usize = 5;
const NUM_QUERIES: usize = 500;

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║   Late Materialization Micro-Benchmark                          ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Nodes: {}  |  Edges/node: {}  |  Queries: {}", NUM_NODES, EDGES_PER_NODE, NUM_QUERIES);
    println!();

    // Build graph
    let mut store = GraphStore::new();
    let mut rng = rand::thread_rng();

    print!("  Building graph...");
    let build_start = Instant::now();
    for i in 0..NUM_NODES {
        let id = store.create_node("Entity");
        store.set_node_property("default", id, "id", i as i64).unwrap();
        store.set_node_property("default", id, "name", format!("node_{}", i)).unwrap();
        store.set_node_property("default", id, "score", (i as f64) * 0.1).unwrap();
    }
    for i in 0..NUM_NODES {
        let source = samyama::graph::NodeId::new((i + 1) as u64);
        for _ in 0..EDGES_PER_NODE {
            let target_idx = rng.gen_range(0..NUM_NODES);
            let target = samyama::graph::NodeId::new((target_idx + 1) as u64);
            if source != target {
                let _ = store.create_edge(source, target, "LINKS_TO");
            }
        }
    }
    println!(" done in {:?}", build_start.elapsed());
    println!("  Total nodes: {}  edges: {}", store.node_count(), store.edge_count());
    println!();

    // ── Benchmark 1: Raw 3-hop via get_outgoing_edges (clones Edge objects) ──
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ 1. Raw 3-hop: get_outgoing_edges (clones Edge)                  │");
    println!("└──────────────────────────────────────────────────────────────────┘");
    {
        let start = Instant::now();
        let mut total_reached = 0usize;
        for _ in 0..NUM_QUERIES {
            let start_node = samyama::graph::NodeId::new(rng.gen_range(1..=NUM_NODES as u64));
            let mut visited = HashSet::new();
            let mut frontier = vec![start_node];

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
            total_reached += visited.len();
        }
        let duration = start.elapsed();
        let avg = duration / NUM_QUERIES as u32;
        let qps = NUM_QUERIES as f64 / duration.as_secs_f64();
        println!("    Avg: {:>10.2?}  |  QPS: {:>8.0}  |  Avg reached: {:.0}",
            avg, qps, total_reached as f64 / NUM_QUERIES as f64);
    }
    println!();

    // ── Benchmark 2: Lightweight 3-hop via get_outgoing_edge_targets (no clone) ──
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ 2. Lightweight 3-hop: get_outgoing_edge_targets (no clone)      │");
    println!("└──────────────────────────────────────────────────────────────────┘");
    {
        let start = Instant::now();
        let mut total_reached = 0usize;
        for _ in 0..NUM_QUERIES {
            let start_node = samyama::graph::NodeId::new(rng.gen_range(1..=NUM_NODES as u64));
            let mut visited = HashSet::new();
            let mut frontier = vec![start_node];

            for _hop in 0..3 {
                let mut next_frontier = Vec::new();
                for &nid in &frontier {
                    if visited.insert(nid.as_u64()) {
                        for (_, _, target, _) in store.get_outgoing_edge_targets(nid) {
                            if !visited.contains(&target.as_u64()) {
                                next_frontier.push(target);
                            }
                        }
                    }
                }
                frontier = next_frontier;
            }
            total_reached += visited.len();
        }
        let duration = start.elapsed();
        let avg = duration / NUM_QUERIES as u32;
        let qps = NUM_QUERIES as f64 / duration.as_secs_f64();
        println!("    Avg: {:>10.2?}  |  QPS: {:>8.0}  |  Avg reached: {:.0}",
            avg, qps, total_reached as f64 / NUM_QUERIES as f64);
    }
    println!();

    // ── Benchmark 3: Cypher 1-hop via QueryEngine ──
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ 3. Cypher 1-hop: MATCH (a)-[:LINKS_TO]->(b) RETURN b.id        │");
    println!("└──────────────────────────────────────────────────────────────────┘");
    {
        let engine = QueryEngine::new();
        let start = Instant::now();
        let mut success = 0;
        for _ in 0..NUM_QUERIES {
            let id = rng.gen_range(0..NUM_NODES);
            let q = format!("MATCH (a:Entity)-[:LINKS_TO]->(b:Entity) WHERE a.id = {} RETURN b.id", id);
            if engine.execute(&q, &store).is_ok() {
                success += 1;
            }
        }
        let duration = start.elapsed();
        let avg = duration / NUM_QUERIES as u32;
        let qps = NUM_QUERIES as f64 / duration.as_secs_f64();
        println!("    Avg: {:>10.2?}  |  QPS: {:>8.0}  |  Success: {}/{}",
            avg, qps, success, NUM_QUERIES);
    }
    println!();

    // ── Benchmark 4: Cypher 2-hop via QueryEngine ──
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ 4. Cypher 2-hop: MATCH (a)-[]->(b)-[]->(c) RETURN c.id         │");
    println!("└──────────────────────────────────────────────────────────────────┘");
    {
        let engine = QueryEngine::new();
        let start = Instant::now();
        let mut success = 0;
        for _ in 0..NUM_QUERIES {
            let id = rng.gen_range(0..NUM_NODES);
            let q = format!("MATCH (a:Entity)-[:LINKS_TO]->(b)-[:LINKS_TO]->(c:Entity) WHERE a.id = {} RETURN c.id", id);
            if engine.execute(&q, &store).is_ok() {
                success += 1;
            }
        }
        let duration = start.elapsed();
        let avg = duration / NUM_QUERIES as u32;
        let qps = NUM_QUERIES as f64 / duration.as_secs_f64();
        println!("    Avg: {:>10.2?}  |  QPS: {:>8.0}  |  Success: {}/{}",
            avg, qps, success, NUM_QUERIES);
    }
    println!();

    // ── Benchmark 5: Cypher RETURN n (full materialization) ──
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ 5. Cypher scan + return: MATCH (n:Entity) RETURN n LIMIT 1000   │");
    println!("└──────────────────────────────────────────────────────────────────┘");
    {
        let engine = QueryEngine::new();
        let start = Instant::now();
        let iterations = 100;
        for _ in 0..iterations {
            let _ = engine.execute("MATCH (n:Entity) RETURN n LIMIT 1000", &store);
        }
        let duration = start.elapsed();
        let avg = duration / iterations as u32;
        println!("    Avg: {:>10.2?}  |  Iterations: {}", avg, iterations);
    }
    println!();

    // ── Benchmark 6: Cypher RETURN n.name (property only — no materialization) ──
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ 6. Cypher property: MATCH (n:Entity) RETURN n.name LIMIT 1000   │");
    println!("└──────────────────────────────────────────────────────────────────┘");
    {
        let engine = QueryEngine::new();
        let start = Instant::now();
        let iterations = 100;
        for _ in 0..iterations {
            let _ = engine.execute("MATCH (n:Entity) RETURN n.name LIMIT 1000", &store);
        }
        let duration = start.elapsed();
        let avg = duration / iterations as u32;
        println!("    Avg: {:>10.2?}  |  Iterations: {}", avg, iterations);
    }
    println!();

    println!("Done.");
}
