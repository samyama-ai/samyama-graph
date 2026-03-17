//! Disease Surveillance KG Loader — Samyama Graph Database
//!
//! Loads WHO GHO data (countries, diseases, vaccine coverage, health indicators)
//! into GraphStore via direct API calls.
//!
//! Usage:
//!   cargo run --release --example surveillance_loader -- --data-dir data/surveillance
//!   cargo run --release --example surveillance_loader -- --data-dir data/surveillance --snapshot surveillance.sgsnap

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod surveillance_common;
use surveillance_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data-dir") {
        PathBuf::from(args.get(pos + 1).expect("--data-dir requires a path"))
    } else {
        eprintln!("Usage: cargo run --release --example surveillance_loader -- --data-dir <PATH>");
        eprintln!("  --data-dir PATH   Directory with countries.json, disease_data.json, etc.");
        eprintln!("  --snapshot PATH   Export snapshot after loading");
        eprintln!("  --query           Enter interactive Cypher REPL");
        std::process::exit(1);
    };

    let query_mode = args.iter().any(|a| a == "--query");
    let snapshot_path = args.iter().position(|a| a == "--snapshot")
        .map(|pos| PathBuf::from(args.get(pos + 1).expect("--snapshot requires a path")));

    eprintln!("Disease Surveillance KG Loader — Samyama Graph Database");
    eprintln!("Data directory: {}", data_dir.display());
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        surveillance_common::load_dataset(&mut graph, &data_dir)?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("Disease Surveillance KG load complete.");
    eprintln!("  Countries:         {}", format_num(result.country_nodes));
    eprintln!("  Regions:           {}", format_num(result.region_nodes));
    eprintln!("  Diseases:          {}", format_num(result.disease_nodes));
    eprintln!("  Disease Reports:   {}", format_num(result.disease_report_nodes));
    eprintln!("  Vaccine Coverage:  {}", format_num(result.vaccine_coverage_nodes));
    eprintln!("  Health Indicators: {}", format_num(result.health_indicator_nodes));
    eprintln!("  ─────────────────────");
    eprintln!("  Total nodes:       {}", format_num(result.total_nodes));
    eprintln!("  Total edges:       {}", format_num(result.total_edges));
    eprintln!("  Time:              {}", format_duration(total_elapsed));
    eprintln!("========================================");

    if let Some(ref snap_path) = snapshot_path {
        eprintln!();
        eprintln!("Exporting snapshot to {}...", snap_path.display());
        let snap_start = Instant::now();
        let snap_stats = client.export_snapshot("default", snap_path).await?;
        let file_size = std::fs::metadata(snap_path).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "Snapshot exported: {} nodes, {} edges ({:.1} MB) in {}",
            format_num(snap_stats.node_count as usize),
            format_num(snap_stats.edge_count as usize),
            file_size as f64 / (1024.0 * 1024.0),
            format_duration(snap_start.elapsed()),
        );
    }

    if query_mode {
        eprintln!();
        eprintln!("Entering query mode. Type Cypher queries or 'quit' to exit.");
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
                        eprintln!("{}", result.columns.join(" | "));
                        for row in &result.records {
                            let vals: Vec<String> = row.iter().map(|v| format!("{}", v)).collect();
                            eprintln!("{}", vals.join(" | "));
                        }
                        eprintln!("({} rows)", result.records.len());
                    }
                }
                Err(e) => eprintln!("ERROR: {}", e),
            }
        }
    }

    Ok(())
}
