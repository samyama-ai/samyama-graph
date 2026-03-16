//! Biological Pathways KG Loader — Samyama Graph Database
//!
//! Loads Reactome, STRING, and Gene Ontology data into GraphStore
//! via the Rust SDK API. Direct API calls (no Cypher parsing).
//!
//! Usage:
//!   cargo run --release --example pathways_loader -- --data-dir data/pathways
//!   cargo run --release --example pathways_loader -- --data-dir data/pathways --phases reactome,string
//!   cargo run --release --example pathways_loader -- --data-dir data/pathways --string-threshold 900
//!   cargo run --release --example pathways_loader -- --data-dir data/pathways --snapshot pathways.sgsnap

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod pathways_common;
use pathways_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    // --data-dir PATH (required)
    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data-dir") {
        PathBuf::from(
            args.get(pos + 1)
                .expect("--data-dir requires a path argument"),
        )
    } else {
        eprintln!("Usage: cargo run --release --example pathways_loader -- --data-dir <PATH>");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --data-dir PATH          Directory containing reactome/, string/, go/ subdirs (required)");
        eprintln!("  --phases PHASES          Comma-separated phases: reactome,string,go (default: all)");
        eprintln!("  --string-threshold N     STRING combined_score threshold (default: 700)");
        eprintln!("  --query                  Enter interactive Cypher REPL after loading");
        eprintln!("  --snapshot PATH          Export snapshot to .sgsnap file after loading");
        std::process::exit(1);
    };

    // --phases reactome,string,go (default all)
    let phases: Vec<String> = if let Some(pos) = args.iter().position(|a| a == "--phases") {
        args.get(pos + 1)
            .expect("--phases requires a comma-separated list")
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .collect()
    } else {
        vec![
            "reactome".to_string(),
            "string".to_string(),
            "go".to_string(),
        ]
    };

    // --string-threshold N (default 700)
    let string_threshold = if let Some(pos) = args.iter().position(|a| a == "--string-threshold") {
        args.get(pos + 1)
            .expect("--string-threshold requires a number")
            .parse::<i64>()
            .expect("--string-threshold must be a positive integer")
    } else {
        700
    };

    let query_mode = args.iter().any(|a| a == "--query");

    let snapshot_path = if let Some(pos) = args.iter().position(|a| a == "--snapshot") {
        Some(PathBuf::from(
            args.get(pos + 1)
                .expect("--snapshot requires a path argument"),
        ))
    } else {
        None
    };

    eprintln!("Biological Pathways KG Loader — Samyama Graph Database");
    eprintln!();

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        eprintln!("Expected subdirectories: reactome/, string/, go/");
        std::process::exit(1);
    }

    eprintln!("Data directory: {}", data_dir.display());
    eprintln!("Phases: {}", phases.join(", "));
    eprintln!("STRING threshold: {}", string_threshold);
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        pathways_common::load_dataset(&mut graph, &data_dir, &phases, string_threshold)?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("Pathways KG load complete.");
    eprintln!("  Nodes: {}", format_num(result.total_nodes));
    eprintln!("  Edges: {}", format_num(result.total_edges));
    eprintln!("  Time:  {}", format_duration(total_elapsed));
    eprintln!("========================================");

    // ========================================================================
    // OPTIONAL: Snapshot export
    // ========================================================================
    if let Some(ref snap_path) = snapshot_path {
        eprintln!();
        eprintln!("Exporting snapshot to {}...", snap_path.display());
        let snap_start = Instant::now();
        let snap_stats = client.export_snapshot("default", snap_path).await?;
        let snap_elapsed = snap_start.elapsed();
        let file_size = std::fs::metadata(snap_path).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "Snapshot exported: {} nodes, {} edges ({:.1} MB) in {}",
            format_num(snap_stats.node_count as usize),
            format_num(snap_stats.edge_count as usize),
            file_size as f64 / (1024.0 * 1024.0),
            format_duration(snap_elapsed),
        );
    }

    // ========================================================================
    // OPTIONAL: Interactive query mode
    // ========================================================================
    if query_mode {
        eprintln!();
        eprintln!("Entering query mode. Type Cypher queries or 'quit' to exit.");
        eprintln!();

        let stdin = io::stdin();
        loop {
            eprint!("cypher> ");
            io::stderr().flush()?;

            let mut input = String::new();
            if stdin.lock().read_line(&mut input)? == 0 {
                break;
            }
            let query = input.trim();
            if query.is_empty() {
                continue;
            }
            if query == "quit" || query == "exit" {
                break;
            }

            match client.query("default", query).await {
                Ok(result) => {
                    if result.columns.is_empty() {
                        eprintln!("(empty result)");
                    } else {
                        eprintln!("{}", result.columns.join(" | "));
                        eprintln!("{}", "-".repeat(result.columns.len() * 20));
                        for row in &result.records {
                            let vals: Vec<String> =
                                row.iter().map(|v| format!("{}", v)).collect();
                            eprintln!("{}", vals.join(" | "));
                        }
                        eprintln!("({} rows)", result.records.len());
                    }
                }
                Err(e) => eprintln!("ERROR: {}", e),
            }
            eprintln!();
        }
    }

    Ok(())
}
