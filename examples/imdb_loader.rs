//! IMDB Movies Knowledge Graph Loader — Samyama Graph Database
//!
//! Loads IMDB non-commercial TSV datasets into GraphStore via the Rust SDK API.
//! Produces a graph of Movies, Series, Persons, Genres, and Ratings with
//! ~100K–300K nodes and ~1M–3M edges depending on vote thresholds.
//!
//! Required files in --data-dir (plain or .gz accepted):
//!   title.basics.tsv      title.ratings.tsv
//!   name.basics.tsv       title.principals.tsv
//!
//! Optional:
//!   --akas PATH           title.akas.tsv to add AlternateTitle nodes
//!
//! Download from: https://developer.imdb.com/non-commercial-datasets/
//! License: IMDB Non-Commercial Use Only
//!
//! Usage:
//!   cargo run --release --example imdb_loader -- --data-dir data/imdb
//!   cargo run --release --example imdb_loader -- --data-dir data/imdb --min-votes 5000
//!   cargo run --release --example imdb_loader -- --data-dir data/imdb --snapshot imdb.sgsnap
//!   cargo run --release --example imdb_loader -- --data-dir data/imdb --query

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod imdb_common;
use imdb_common::{format_duration, format_num};

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
        eprintln!("Usage: cargo run --release --example imdb_loader -- --data-dir <PATH>");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --data-dir PATH         Directory containing IMDB .tsv files (required)");
        eprintln!("  --min-votes N           Min votes for movies to include (default: 1000)");
        eprintln!("  --min-votes-series N    Min votes for TV series to include (default: 500)");
        eprintln!("  --min-year N            Earliest start year to include (default: 1950)");
        eprintln!("  --snapshot PATH         Export snapshot to .sgsnap file after loading");
        eprintln!("  --query                 Enter interactive Cypher REPL after loading");
        eprintln!();
        eprintln!("Required files (plain .tsv or gzip .tsv.gz both accepted):");
        eprintln!("  title.basics.tsv        title.ratings.tsv");
        eprintln!("  name.basics.tsv         title.principals.tsv");
        eprintln!();
        eprintln!("Download from: https://developer.imdb.com/non-commercial-datasets/");
        std::process::exit(1);
    };

    let min_votes: i64 = if let Some(pos) = args.iter().position(|a| a == "--min-votes") {
        args.get(pos + 1)
            .expect("--min-votes requires a number")
            .parse()
            .expect("--min-votes must be a positive integer")
    } else {
        1000
    };

    let min_votes_series: i64 =
        if let Some(pos) = args.iter().position(|a| a == "--min-votes-series") {
            args.get(pos + 1)
                .expect("--min-votes-series requires a number")
                .parse()
                .expect("--min-votes-series must be a positive integer")
        } else {
            500
        };

    let min_year: i32 = if let Some(pos) = args.iter().position(|a| a == "--min-year") {
        args.get(pos + 1)
            .expect("--min-year requires a number")
            .parse()
            .expect("--min-year must be a positive integer")
    } else {
        1950
    };

    let query_mode = args.iter().any(|a| a == "--query");

    let akas_path = if let Some(pos) = args.iter().position(|a| a == "--akas") {
        Some(PathBuf::from(
            args.get(pos + 1)
                .expect("--akas requires a path argument"),
        ))
    } else {
        None
    };

    let snapshot_path = if let Some(pos) = args.iter().position(|a| a == "--snapshot") {
        Some(PathBuf::from(
            args.get(pos + 1)
                .expect("--snapshot requires a path argument"),
        ))
    } else {
        None
    };

    eprintln!("IMDB Movies KG Loader — Samyama Graph Database");
    eprintln!();

    if !data_dir.exists() {
        eprintln!("ERROR: Data directory not found: {}", data_dir.display());
        eprintln!("Download IMDB data from https://developer.imdb.com/non-commercial-datasets/");
        std::process::exit(1);
    }

    // Verify required files exist (plain or .gz)
    for fname in &[
        "title.basics.tsv",
        "title.ratings.tsv",
        "name.basics.tsv",
        "title.principals.tsv",
    ] {
        let plain = data_dir.join(fname);
        let gzipped = data_dir.join(format!("{fname}.gz"));
        if !plain.exists() && !gzipped.exists() {
            eprintln!("ERROR: Required file not found: {}", plain.display());
            eprintln!("       (also tried {}.gz)", plain.display());
            std::process::exit(1);
        }
    }

    eprintln!("Data directory:      {}", data_dir.display());
    eprintln!("Min votes (movies):  {}", format_num(min_votes as usize));
    eprintln!("Min votes (series):  {}", format_num(min_votes_series as usize));
    eprintln!("Min year:            {}", min_year);
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        imdb_common::load_dataset(
            &mut graph,
            &data_dir,
            min_votes,
            min_year,
            min_votes_series,
            akas_path.as_deref(),
        )?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("IMDB KG load complete.");
    eprintln!("  Movies:           {}", format_num(result.movie_count));
    eprintln!("  Series:           {}", format_num(result.series_count));
    eprintln!("  Persons:          {}", format_num(result.person_count));
    eprintln!("  Genres:           {}", format_num(result.genre_count));
    eprintln!("  Ratings:          {}", format_num(result.rating_count));
    if result.alt_title_count > 0 {
        eprintln!("  AlternateTitles:  {}", format_num(result.alt_title_count));
    }
    eprintln!("  ─────────────────────────");
    eprintln!("  Total nodes:      {}", format_num(result.total_nodes));
    eprintln!("  Total edges:      {}", format_num(result.total_edges));
    eprintln!("  Time:        {}", format_duration(total_elapsed));
    eprintln!("========================================");

    // ========================================================================
    // OPTIONAL: Snapshot export
    // ========================================================================
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
                                row.iter().map(|v| format!("{v}")).collect();
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
