use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use samyama::snapshot::import_tenant;
use std::fs::File;
use std::io::BufReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = GraphStore::new();
    for s in ["pathways", "druginteractions"] {
        let path = format!("../{}-kg/data/{}.sgsnap", s, s);
        if let Ok(f) = File::open(&path) {
            let _ = import_tenant(&mut store, BufReader::new(f));
        }
    }
    eprintln!("Loaded {} nodes / {} edges", store.node_count(), store.edge_count());

    let engine = QueryEngine::new();
    let queries = [
        ("CT09-shape (Phase 1?)", "MATCH (a:Drug)-[:HAS_SIDE_EFFECT]->(b:SideEffect) RETURN b.name, count(a) AS n ORDER BY n DESC LIMIT 5"),
        ("CT08-shape (DISTINCT)", "MATCH (a:Drug)-[:HAS_SIDE_EFFECT]->(b:SideEffect) RETURN b.name, count(DISTINCT a) AS n ORDER BY n DESC LIMIT 5"),
    ];
    for (name, q) in queries {
        eprintln!("\n=== {} ===", name);
        eprintln!("Q: {}", q);
        if let Ok(r) = engine.execute(&format!("EXPLAIN {}", q), &store) {
            for rec in r.records.iter().take(3) {
                eprintln!("  {:?}", rec);
            }
        }
    }
    Ok(())
}
