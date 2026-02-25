//! LDBC FinBench Dataset Loader — Samyama Graph Database
//!
//! Loads the LDBC FinBench dataset into GraphStore via the Rust SDK API.
//! Supports both CSV loading from disk and synthetic data generation.
//!
//! Usage:
//!   cargo run --release --example finbench_loader                          # Generate synthetic data in-memory
//!   cargo run --release --example finbench_loader -- --generate            # Generate CSV files to disk then load
//!   cargo run --release --example finbench_loader -- --data-dir /path      # Load from existing CSV files
//!   cargo run --release --example finbench_loader -- --query               # Drop into query loop after loading

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod finbench_common;
use finbench_common::{format_duration, format_num, GeneratorConfig};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data-dir") {
        Some(PathBuf::from(args.get(pos + 1).expect("--data-dir requires a path argument")))
    } else {
        None
    };

    let generate_csv = args.iter().any(|a| a == "--generate");
    let query_mode = args.iter().any(|a| a == "--query");

    eprintln!("LDBC FinBench Dataset Loader — Samyama v0.5.8");
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = if let Some(ref dir) = data_dir {
        // Load from existing CSV files
        if !dir.exists() {
            eprintln!("ERROR: Data directory not found: {}", dir.display());
            eprintln!("Run with --generate to create synthetic data, or provide a valid --data-dir");
            std::process::exit(1);
        }
        eprintln!("Loading FinBench dataset from CSV: {}", dir.display());
        eprintln!();
        let mut graph = client.store_write().await;
        finbench_common::load_dataset(&mut graph, dir)?
    } else if generate_csv {
        // Generate CSV files to disk, then load them
        let dir = finbench_common::default_data_dir();
        eprintln!("Generating FinBench CSV files to: {}", dir.display());
        eprintln!();
        let config = GeneratorConfig::default();
        finbench_common::write_csv_dataset(&dir, &config)?;
        eprintln!();
        eprintln!("Loading generated CSV files...");
        eprintln!();
        let mut graph = client.store_write().await;
        finbench_common::load_dataset(&mut graph, &dir)?
    } else {
        // Generate synthetic data directly in memory (fastest)
        eprintln!("Generating synthetic FinBench dataset in-memory...");
        eprintln!();
        let config = GeneratorConfig::default();
        let mut graph = client.store_write().await;
        finbench_common::generate_dataset(&mut graph, &config)
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("Total load time: {}", format_duration(total_elapsed));
    eprintln!("Graph ready. Nodes: {}, Edges: {}", format_num(result.total_nodes), format_num(result.total_edges));
    eprintln!("========================================");

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
            if stdin.lock().read_line(&mut input)? == 0 { break; }
            let query = input.trim();
            if query.is_empty() { continue; }
            if query == "quit" || query == "exit" { break; }

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
                            let vals: Vec<String> = row.iter()
                                .map(|v| format!("{}", v))
                                .collect();
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
