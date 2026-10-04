//! AACT Dataset Loader — Samyama Graph Database
//!
//! Loads the full ClinicalTrials.gov (AACT) dataset from pipe-delimited flat
//! files into GraphStore via the Rust SDK API. Expected to load 575K studies
//! with ~2M nodes and ~10M edges in 3-8 minutes.
//!
//! Usage:
//!   cargo run --release --example aact_loader -- --data-dir data/aact
//!   cargo run --release --example aact_loader -- --data-dir data/aact --max-studies 10000
//!   cargo run --release --example aact_loader -- --data-dir data/aact --query
//!   cargo run --release --example aact_loader -- --data-dir data/aact --snapshot output.sgsnap

// Measure what ships: the server's allocator, not the system default (ADR-038).
#[global_allocator]
static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod aact_common;
use aact_common::{format_duration, format_num};

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
        eprintln!("Usage: cargo run --release --example aact_loader -- --data-dir <PATH>");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --data-dir PATH      Directory containing AACT .txt files (required)");
        eprintln!("  --max-studies N       Limit number of studies (0 = all, default 0)");
        eprintln!("  --query              Enter interactive Cypher REPL after loading");
        eprintln!("  --snapshot PATH      Export snapshot to .sgsnap file after loading");
        std::process::exit(1);
    };

    // --max-studies N (default 0 = all)
    let max_studies = if let Some(pos) = args.iter().position(|a| a == "--max-studies") {
        args.get(pos + 1)
            .expect("--max-studies requires a number")
            .parse::<usize>()
            .expect("--max-studies must be a positive integer")
    } else {
        0
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

    eprintln!("AACT Dataset Loader — Samyama Graph Database");
    eprintln!();

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        eprintln!("Download AACT flat files from https://aact.ctti-clinicaltrials.org/");
        std::process::exit(1);
    }

    eprintln!("Data directory: {}", data_dir.display());
    if max_studies > 0 {
        eprintln!("Max studies: {}", format_num(max_studies));
    } else {
        eprintln!("Max studies: all");
    }
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        aact_common::load_dataset(&mut graph, &data_dir, max_studies)?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("AACT load complete.");
    eprintln!(
        "  Nodes: {}",
        format_num(result.total_nodes)
    );
    eprintln!(
        "  Edges: {}",
        format_num(result.total_edges)
    );
    eprintln!(
        "  Time:  {}",
        format_duration(total_elapsed)
    );
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
                        // Print header
                        eprintln!("{}", result.columns.join(" | "));
                        eprintln!("{}", "-".repeat(result.columns.len() * 20));
                        // Print rows
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
