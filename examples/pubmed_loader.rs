//! PubMed Knowledge Graph Loader
//!
//! Loads pipe-delimited flat files (from parse_pubmed_xml.py) into GraphStore.
//! Files: articles.txt, authors.txt, mesh_terms.txt, chemicals.txt, citations.txt, grants.txt
//!
//! Usage:
//!   cargo run --release --example pubmed_loader -- --data-dir data/pubmed-parsed
//!   cargo run --release --example pubmed_loader -- --data-dir data/pubmed-parsed --snapshot pubmed.sgsnap

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod pubmed_common;
use pubmed_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: cargo run --release --example pubmed_loader [OPTIONS]");
        eprintln!("  --data-dir DIR    Directory with parsed PubMed flat files");
        eprintln!("  --snapshot PATH   Export snapshot after loading");
        eprintln!("  --query           Interactive Cypher REPL");
        eprintln!("  --max-articles N  Limit articles (0=all)");
        std::process::exit(0);
    }

    let data_dir = args.iter().position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "data/pubmed-parsed".to_string());

    let snapshot_path = args.iter().position(|a| a == "--snapshot")
        .map(|pos| PathBuf::from(args.get(pos + 1).expect("--snapshot requires PATH")));

    let max_articles: usize = args.iter().position(|a| a == "--max-articles")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let query_mode = args.iter().any(|a| a == "--query");

    eprintln!("PubMed Knowledge Graph Loader");
    eprintln!("  Data dir: {}", data_dir);
    if max_articles > 0 { eprintln!("  Max articles: {}", format_num(max_articles)); }
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        pubmed_common::load_dataset(&mut graph, &data_dir, max_articles)?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("PubMed load complete.");
    eprintln!("  Nodes: {}", format_num(result.total_nodes));
    eprintln!("  Edges: {}", format_num(result.total_edges));
    eprintln!("  Time:  {}", format_duration(total_elapsed));
    eprintln!("========================================");

    if let Some(ref snap_path) = snapshot_path {
        eprintln!("\nExporting snapshot to {}...", snap_path.display());
        let t = Instant::now();
        let s = client.export_snapshot("default", snap_path).await?;
        let sz = std::fs::metadata(snap_path).map(|m| m.len()).unwrap_or(0);
        eprintln!("Snapshot: {} nodes, {} edges ({:.1} MB) in {}",
            format_num(s.node_count as usize), format_num(s.edge_count as usize),
            sz as f64 / (1024.0 * 1024.0), format_duration(t.elapsed()));
    }

    if query_mode {
        eprintln!("\nCypher REPL (type 'quit' to exit)\n");
        let stdin = io::stdin();
        loop {
            eprint!("cypher> "); io::stderr().flush()?;
            let mut input = String::new();
            if stdin.lock().read_line(&mut input)? == 0 { break; }
            let q = input.trim();
            if q.is_empty() { continue; }
            if q == "quit" || q == "exit" { break; }
            match client.query("default", q).await {
                Ok(r) => {
                    if r.columns.is_empty() { eprintln!("(empty)"); }
                    else {
                        eprintln!("{}", r.columns.join(" | "));
                        for row in &r.records {
                            eprintln!("{}", row.iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join(" | "));
                        }
                        eprintln!("({} rows)", r.records.len());
                    }
                }
                Err(e) => eprintln!("ERROR: {}", e),
            }
            eprintln!();
        }
    }
    Ok(())
}
