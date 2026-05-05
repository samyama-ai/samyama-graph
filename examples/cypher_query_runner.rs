//! Cypher query runner for snapshot bundles.
//!
//! Imports a snapshot and runs every .cypher file in a directory,
//! splitting on `;` so multi-statement files work. Prints column header,
//! up to N rows, and total row count for each statement.
//!
//! Usage:
//!   cargo run --release --example cypher_query_runner -- \
//!     --snapshot data/phase1b_chained_v3.sgsnap \
//!     --queries-dir ../genomoncology-demo/queries \
//!     --max-rows 5
//!
//! Pass --snapshot multiple times to layer additional snapshots on top of
//! the first one. They are imported in order before any query runs:
//!
//!   cargo run --release --example cypher_query_runner -- \
//!     --snapshot data/phase1b_chained_v3.sgsnap \
//!     --snapshot data/baseline/chembl.sgsnap \
//!     --queries-dir ../genomoncology-demo/queries

use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

type Error = Box<dyn std::error::Error>;

fn arg(args: &[String], name: &str) -> Option<PathBuf> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let snapshots: Vec<PathBuf> = args
        .iter()
        .enumerate()
        .filter_map(|(i, a)| (a == "--snapshot").then(|| args.get(i + 1)))
        .flatten()
        .map(PathBuf::from)
        .collect();
    if snapshots.is_empty() {
        return Err("at least one --snapshot PATH required".into());
    }
    let queries_dir = arg(&args, "--queries-dir").ok_or("--queries-dir PATH required")?;
    let max_rows: usize = args
        .iter()
        .position(|a| a == "--max-rows")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    eprintln!("Cypher query runner");
    for s in &snapshots {
        eprintln!("  Snapshot:    {}", s.display());
    }
    eprintln!("  Queries dir: {}", queries_dir.display());
    eprintln!();

    let client = EmbeddedClient::new();

    for snap in &snapshots {
        let t = Instant::now();
        let stats = client.import_snapshot("default", snap).await?;
        eprintln!(
            "Imported {}: {} nodes, {} edges in {:.1}s",
            snap.file_name().and_then(|n| n.to_str()).unwrap_or("?"),
            stats.node_count,
            stats.edge_count,
            t.elapsed().as_secs_f64()
        );
    }
    eprintln!();

    let mut files: Vec<PathBuf> = std::fs::read_dir(&queries_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("cypher"))
        .collect();
    files.sort();

    for file in &files {
        let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("?");
        eprintln!("══════════════════════════════════════════════════════════");
        eprintln!("│ {}", name);
        eprintln!("══════════════════════════════════════════════════════════");
        let body = std::fs::read_to_string(file)?;
        // Split on semicolons that aren't inside quoted strings — naive but
        // sufficient for our hero queries.
        let statements: Vec<String> = split_statements(&body);
        for (i, stmt) in statements.iter().enumerate() {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            // Strip line-leading // comments so logging stays terse.
            let cleaned: String = stmt
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");
            let cleaned = cleaned.trim();
            if cleaned.is_empty() {
                continue;
            }
            eprintln!();
            eprintln!("--- statement {}/{} ---", i + 1, statements.len());
            // Show the first line of the statement as a hint.
            let first_line = cleaned.lines().next().unwrap_or("").trim();
            eprintln!("    > {}", first_line);
            let qt = Instant::now();
            match client.query("default", cleaned).await {
                Ok(r) => {
                    let elapsed = qt.elapsed();
                    if r.columns.is_empty() {
                        eprintln!("    (empty result, {:.0}ms)", elapsed.as_secs_f64() * 1000.0);
                        continue;
                    }
                    eprintln!("    columns: {}", r.columns.join(" | "));
                    let n = r.records.len();
                    let show = n.min(max_rows);
                    for row in &r.records[..show] {
                        let cells: Vec<String> = row.iter().map(|v| format!("{}", v)).collect();
                        eprintln!("    {}", cells.join(" | "));
                    }
                    if n > show {
                        eprintln!("    ... ({} more rows)", n - show);
                    }
                    eprintln!("    [{} rows in {:.0}ms]", n, elapsed.as_secs_f64() * 1000.0);
                }
                Err(e) => {
                    eprintln!("    ERROR: {}", e);
                }
            }
        }
        eprintln!();
    }
    Ok(())
}

/// Naive split on top-level semicolons. Tolerates // line comments but not
/// strings containing `;`. Sufficient for our hero queries.
fn split_statements(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_line_comment = false;
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        if in_line_comment {
            buf.push(c);
            if c == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        if c == '/' && chars.peek() == Some(&'/') {
            in_line_comment = true;
            buf.push(c);
            continue;
        }
        if c == ';' {
            out.push(std::mem::take(&mut buf));
            continue;
        }
        buf.push(c);
    }
    if !buf.trim().is_empty() {
        out.push(buf);
    }
    out
}
