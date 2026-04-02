use samyama_sdk::{EmbeddedClient, SamyamaClient};
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn load_queries(path: &str) -> Vec<(String, String, String)> {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let reader = BufReader::new(file);
    let mut queries = Vec::new();
    let mut first = true;
    for line in reader.lines() {
        let line = match line { Ok(l) => l, Err(_) => continue };
        if first { first = false; continue; }
        let parts: Vec<&str> = line.splitn(5, ',').collect();
        if parts.len() < 5 { continue; }
        let id = parts[0].trim_matches('"').to_string();
        let name = parts[1].trim_matches('"').to_string();
        let cypher = parts[4].trim_matches('"').to_string();
        if cypher.is_empty() { continue; }
        queries.push((id, name, cypher));
    }
    queries
}

async fn test_kg(name: &str, snap_path: &str, query_csv: &str) {
    eprintln!("\n============================================================");
    eprintln!("Testing: {}", name);
    eprintln!("============================================================");

    let client = EmbeddedClient::new();

    // Import
    let t = Instant::now();
    match client.import_snapshot("default", std::path::Path::new(snap_path)).await {
        Ok(s) => eprintln!("Imported: {} nodes, {} edges in {:.1}s", s.node_count, s.edge_count, t.elapsed().as_secs_f64()),
        Err(e) => { eprintln!("Import error: {}", e); return; }
    }

    // Run queries
    let queries = load_queries(query_csv);
    eprintln!("Running {} queries...\n", queries.len());

    let mut pass = 0;
    let mut empty = 0;
    let mut errors = 0;

    for (id, qname, cypher) in &queries {
        let t = Instant::now();
        match client.query("default", cypher).await {
            Ok(r) => {
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                if r.records.len() > 0 {
                    pass += 1;
                    let sample = if !r.columns.is_empty() {
                        format!("{:?}", r.records[0]).chars().take(100).collect::<String>()
                    } else { String::new() };
                    eprintln!("[{:.0}ms] {} {} -> {} rows  {}", ms, id, qname, r.records.len(), sample);
                } else {
                    empty += 1;
                    eprintln!("[{:.0}ms] {} {} -> 0 rows", ms, id, qname);
                }
            }
            Err(e) => {
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                errors += 1;
                eprintln!("[{:.0}ms] {} {} -> ERROR: {}", ms, id, qname, e);
            }
        }
    }

    eprintln!("\n{}: {} queries | {} pass | {} empty | {} error",
        name, queries.len(), pass, empty, errors);
}

#[tokio::main]
async fn main() {
    let book = std::env::var("BOOK_DIR").unwrap_or_else(|_|
        "../samyama-graph-book/src/data/benchmark".to_string());

    test_kg("Pathways",
        "../pathways-kg/data/pathways.sgsnap",
        &format!("{}/pathways-queries.csv", book)).await;

    test_kg("Drug Interactions",
        "../druginteractions-kg/data/druginteractions.sgsnap",
        &format!("{}/drug-interactions-queries.csv", book)).await;

    test_kg("Clinical Trials",
        "../clinicaltrials-kg/data/clinical-trials.sgsnap",
        &format!("{}/clinical-trials-queries.csv", book)).await;
}
