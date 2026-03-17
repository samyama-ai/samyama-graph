//! Drug Interactions & Pharmacogenomics KG Loader — Samyama Graph Database
//!
//! Loads DrugBank CC0, DGIdb, and SIDER data into GraphStore
//! via the Rust SDK API. Direct API calls (no Cypher parsing).
//!
//! Usage:
//!   cargo run --release --example druginteractions_loader -- --data-dir data/druginteractions
//!   cargo run --release --example druginteractions_loader -- --data-dir data/druginteractions --snapshot druginteractions.sgsnap
//!   cargo run --release --example druginteractions_loader -- --data-dir data/druginteractions --phases drugbank_dgidb
//!   cargo run --release --example druginteractions_loader -- --data-dir data/druginteractions --query

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod druginteractions_common;
use druginteractions_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--data-dir") {
        PathBuf::from(
            args.get(pos + 1)
                .expect("--data-dir requires a path argument"),
        )
    } else {
        eprintln!("Usage: cargo run --release --example druginteractions_loader -- --data-dir <PATH>");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --data-dir PATH   Directory with drugbank/, dgidb/, sider/ subdirs (required)");
        eprintln!("  --phases PHASES   Comma-separated: drugbank_dgidb,sider (default: all)");
        eprintln!("  --snapshot PATH   Export snapshot to .sgsnap file after loading");
        eprintln!("  --query           Enter interactive Cypher REPL after loading");
        std::process::exit(1);
    };

    let phases: Vec<String> = if let Some(pos) = args.iter().position(|a| a == "--phases") {
        args.get(pos + 1)
            .expect("--phases requires a comma-separated list")
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .collect()
    } else {
        vec!["all".to_string()]
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

    eprintln!("Drug Interactions KG Loader — Samyama Graph Database");
    eprintln!();

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        eprintln!("Expected subdirectories: drugbank/, dgidb/, sider/");
        std::process::exit(1);
    }

    eprintln!("Data directory: {}", data_dir.display());
    eprintln!("Phases: {}", phases.join(", "));
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        druginteractions_common::load_dataset(&mut graph, &data_dir, &phases)?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("Drug Interactions KG load complete.");
    eprintln!("  Drugs:        {}", format_num(result.drug_nodes));
    eprintln!("  Genes:        {}", format_num(result.gene_nodes));
    eprintln!("  Side Effects: {}", format_num(result.side_effect_nodes));
    eprintln!("  Indications:  {}", format_num(result.indication_nodes));
    eprintln!("  ─────────────────────");
    eprintln!("  Total nodes:  {}", format_num(result.total_nodes));
    eprintln!("  Total edges:  {}", format_num(result.total_edges));
    eprintln!("  Time:         {}", format_duration(total_elapsed));
    eprintln!("========================================");

    // Snapshot export
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

    // Interactive query mode
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
