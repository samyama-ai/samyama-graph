//! LDBC SNB SF1 Dataset Loader — Samyama Graph Database
//!
//! Loads the LDBC Social Network Benchmark Scale Factor 1 dataset (~3.18M nodes, ~17M edges)
//! directly into GraphStore via the Rust API.
//!
//! Prerequisites:
//!   Download and extract LDBC SF1 data to:
//!     data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter/
//!
//! Usage:
//!   cargo run --release --example ldbc_loader
//!   cargo run --release --example ldbc_loader -- --data-dir /path/to/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter
//!   cargo run --release --example ldbc_loader -- --query   # drop into query loop after loading

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod ldbc_common;
use ldbc_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    let default_dir = "data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter";
    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data-dir") {
        PathBuf::from(args.get(pos + 1).expect("--data-dir requires a path argument"))
    } else {
        PathBuf::from(default_dir)
    };

    let query_mode = args.iter().any(|a| a == "--query");

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        eprintln!("Download LDBC SF1 data and extract to: {}", default_dir);
        std::process::exit(1);
    }

    eprintln!("Loading LDBC SNB SF1 dataset from: {}", data_dir.display());
    eprintln!();

    let client = EmbeddedClient::new();

    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        ldbc_common::load_dataset(&mut graph, &data_dir)?
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
