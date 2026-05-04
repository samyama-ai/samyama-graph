//! AACT biomarker extractor — post-processing pass over an already-loaded
//! AACT graph that adds first-class :Biomarker nodes and REQUIRES_BIOMARKER
//! / TARGETS_GENE edges by mining `eligibilities.txt` free-text criteria.
//!
//! Chain after the AACT and HGNC loaders (or after importing snapshots that
//! contain :ClinicalTrial and :Gene nodes).
//!
//! Usage:
//!   cargo run --release --example aact_biomarker_extractor -- \
//!     --eligibilities data/aact/eligibilities.txt
//!   cargo run --release --example aact_biomarker_extractor -- \
//!     --eligibilities data/aact/eligibilities.txt --snapshot biomarkers.sgsnap

use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod aact_biomarker_common;
use aact_biomarker_common::{
    build_gene_set_and_index, build_trial_index, load_eligibilities,
};

type Error = Box<dyn std::error::Error>;

fn fmt_num(n: usize) -> String {
    let s = n.to_string();
    let mut r = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            r.push(',');
        }
        r.push(c);
    }
    r.chars().rev().collect()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: cargo run --release --example aact_biomarker_extractor [OPTIONS]");
        eprintln!("  --eligibilities PATH  AACT eligibilities.txt [required]");
        eprintln!("  --snapshot PATH       Export snapshot after extraction");
        eprintln!("  --max-rows N          Limit input rows (0=all, default 0)");
        std::process::exit(0);
    }

    let elig_path = args
        .iter()
        .position(|a| a == "--eligibilities")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .ok_or("--eligibilities PATH is required")?;

    let snapshot_path = args
        .iter()
        .position(|a| a == "--snapshot")
        .map(|pos| PathBuf::from(args.get(pos + 1).expect("--snapshot requires PATH")));

    let max_rows: usize = args
        .iter()
        .position(|a| a == "--max-rows")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    eprintln!("AACT Biomarker Extractor");
    eprintln!("  Eligibilities: {}", elig_path.display());
    if max_rows > 0 {
        eprintln!("  Max rows: {}", fmt_num(max_rows));
    }
    eprintln!();

    let client = EmbeddedClient::new();
    let start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        let trials = build_trial_index(&graph);
        let (gene_set, gene_index) = build_gene_set_and_index(&graph);
        eprintln!("  :ClinicalTrial bridge: {} NCT IDs", fmt_num(trials.len()));
        eprintln!("  :Gene set:             {} symbols", fmt_num(gene_set.len()));
        if trials.is_empty() {
            eprintln!(
                "WARN: no :ClinicalTrial nodes in store. Run aact_loader first or import its snapshot."
            );
        }
        if gene_set.is_empty() {
            eprintln!(
                "WARN: no :Gene nodes in store. Run hgnc_ensembl_loader first or import its snapshot."
            );
        }
        load_eligibilities(&mut graph, &elig_path, &trials, &gene_set, &gene_index, max_rows)?
    };

    let elapsed = start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("Biomarker extraction complete.");
    eprintln!("  Trials processed:        {}", fmt_num(result.trials_processed));
    eprintln!("  Trials with biomarkers:  {}", fmt_num(result.trials_with_biomarkers));
    eprintln!("  Biomarker nodes:         {}", fmt_num(result.biomarker_nodes));
    eprintln!("  REQUIRES_BIOMARKER edges:{}", fmt_num(result.requires_edges));
    eprintln!("  TARGETS_GENE edges:      {}", fmt_num(result.targets_gene_edges));
    eprintln!("  Time:                    {:.1}s", elapsed.as_secs_f64());
    eprintln!("========================================");

    if let Some(ref snap_path) = snapshot_path {
        eprintln!("\nExporting snapshot to {}...", snap_path.display());
        let s = client.export_snapshot("default", snap_path).await?;
        let sz = std::fs::metadata(snap_path).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "Snapshot: {} nodes, {} edges ({:.1} MB)",
            fmt_num(s.node_count as usize),
            fmt_num(s.edge_count as usize),
            sz as f64 / (1024.0 * 1024.0),
        );
    }
    Ok(())
}
