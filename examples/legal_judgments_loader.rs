//! Legal Judgments Knowledge Graph Loader — Samyama Graph Database
//!
//! Loads the Indian Supreme Court judgments (2016) CSV dataset into GraphStore via the
//! Rust SDK API. Produces a graph of Cases, Judges, Parties, Acts and Topics.
//!
//! Reproduces a public reference demo (PostgreSQL + Apache AGE + pgvector) on Samyama —
//! one engine instead of three.
//!
//! Required files in --data-dir:
//!   judges.csv        cases.csv         parties.csv     acts.csv        topics.csv
//!   edge_decided.csv  edge_party_in.csv edge_cites.csv  edge_about.csv
//!
//! Download from: https://huggingface.co/datasets/Shreyasrao/Indian-law-supreme-court-judgements-2016
//! License: CC-BY-4.0
//!
//! Usage:
//!   cargo run --release --example legal_judgments_loader -- --data-dir data/legal-judgments
//!   cargo run --release --example legal_judgments_loader -- --data-dir data/legal-judgments --snapshot legal-judgments.sgsnap

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod legal_judgments_common;
use legal_judgments_common::{format_duration, format_num, REQUIRED_FILES};

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
        eprintln!("Usage: cargo run --release --example legal_judgments_loader -- --data-dir <PATH>");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --data-dir PATH    Directory containing the judgment CSV files (required)");
        eprintln!("  --snapshot PATH    Export snapshot to .sgsnap file after loading");
        eprintln!("  --query            Enter interactive Cypher REPL after loading");
        eprintln!();
        eprintln!("Required files:");
        eprintln!("  judges.csv        cases.csv         parties.csv     acts.csv       topics.csv");
        eprintln!("  edge_decided.csv  edge_party_in.csv edge_cites.csv  edge_about.csv");
        eprintln!();
        eprintln!("Download from: https://huggingface.co/datasets/Shreyasrao/Indian-law-supreme-court-judgements-2016");
        std::process::exit(1);
    };

    let snapshot_path = if let Some(pos) = args.iter().position(|a| a == "--snapshot") {
        Some(PathBuf::from(
            args.get(pos + 1)
                .expect("--snapshot requires a path argument"),
        ))
    } else {
        None
    };

    let query_mode = args.iter().any(|a| a == "--query");

    eprintln!("Legal Judgments KG Loader — Samyama Graph Database");
    eprintln!();

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        std::process::exit(1);
    }

    for fname in &REQUIRED_FILES {
        let p = data_dir.join(fname);
        if !p.exists() {
            eprintln!("ERROR: Required file not found: {}", p.display());
            std::process::exit(1);
        }
    }

    eprintln!("Data directory: {}", data_dir.display());
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        legal_judgments_common::load_dataset(&mut graph, &data_dir)?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("Legal Judgments KG load complete.");
    eprintln!("  Cases:        {}", format_num(result.case_count));
    eprintln!("  Judges:       {}", format_num(result.judge_count));
    eprintln!("  Parties:      {}", format_num(result.party_count));
    eprintln!("  Acts:         {}", format_num(result.act_count));
    eprintln!("  Topics:       {}", format_num(result.topic_count));
    eprintln!("  ─────────────────────────");
    eprintln!("  Total nodes:  {}", format_num(result.total_nodes));
    eprintln!("  Total edges:  {}", format_num(result.total_edges));
    eprintln!("  Time:         {}", format_duration(total_elapsed));
    eprintln!("========================================");

    if let Some(ref snap_path) = snapshot_path {
        eprintln!();
        eprintln!("Exporting snapshot to {} ...", snap_path.display());
        let snap_start = Instant::now();
        let snap_stats = client.export_snapshot("default", snap_path).await?;
        let snap_elapsed = snap_start.elapsed();
        let file_size = std::fs::metadata(snap_path).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "Snapshot exported: {} nodes, {} edges ({:.2} MB) in {}",
            format_num(snap_stats.node_count as usize),
            format_num(snap_stats.edge_count as usize),
            file_size as f64 / (1024.0 * 1024.0),
            format_duration(snap_elapsed),
        );
    }

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
                            let vals: Vec<String> = row.iter().map(|v| format!("{v}")).collect();
                            eprintln!("{}", vals.join(" | "));
                        }
                        eprintln!("({} rows)", result.records.len());
                    }
                }
                Err(e) => eprintln!("ERROR: {e}"),
            }
            eprintln!();
        }
    }

    Ok(())
}
