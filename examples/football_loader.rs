//! Football Knowledge Graph Loader — Samyama Graph Database
//!
//! Loads DataHub World Cup CSV datasets into GraphStore via the Rust SDK API.
//! Produces a graph of Tournaments, Teams, Players, Matches, Goals, Stadiums.
//!
//! Required files in --data-dir:
//!   tournaments.csv   matches.csv    teams.csv      players.csv
//!   goals.csv         stadiums.csv   managers.csv
//!
//! Download from: https://datahub.io/football/worldcup
//! License: Open Data Commons PDDL
//!
//! Usage:
//!   cargo run --release --example football_loader -- --data-dir data/football
//!   cargo run --release --example football_loader -- --data-dir data/football --snapshot football.sgsnap

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod football_common;
use football_common::{format_duration, format_num};

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
        eprintln!("Usage: cargo run --release --example football_loader -- --data-dir <PATH>");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --data-dir PATH    Directory containing World Cup CSV files (required)");
        eprintln!("  --snapshot PATH    Export snapshot to .sgsnap file after loading");
        eprintln!("  --query            Enter interactive Cypher REPL after loading");
        eprintln!();
        eprintln!("Required files:");
        eprintln!("  tournaments.csv  matches.csv  teams.csv  players.csv");
        eprintln!("  goals.csv        stadiums.csv managers.csv");
        eprintln!();
        eprintln!("Download from: https://datahub.io/football/worldcup");
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

    eprintln!("Football KG Loader — Samyama Graph Database");
    eprintln!();

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        std::process::exit(1);
    }

    for fname in &[
        "tournaments.csv", "matches.csv", "teams.csv", "players.csv",
        "goals.csv", "stadiums.csv", "managers.csv",
    ] {
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
        football_common::load_dataset(&mut graph, &data_dir)?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("Football KG load complete.");
    eprintln!("  Tournaments:  {}", format_num(result.tournament_count));
    eprintln!("  Teams:        {}", format_num(result.team_count));
    eprintln!("  Countries:    {}", format_num(result.country_count));
    eprintln!("  Stadiums:     {}", format_num(result.stadium_count));
    eprintln!("  Matches:      {}", format_num(result.match_count));
    eprintln!("  Players:      {}", format_num(result.player_count));
    eprintln!("  Goals:        {}", format_num(result.goal_count));
    eprintln!("  Managers:     {}", format_num(result.manager_count));
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
            "Snapshot exported: {} nodes, {} edges ({:.1} MB) in {}",
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
