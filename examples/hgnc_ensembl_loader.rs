//! HGNC + Ensembl Loader
//!
//! Loads the canonical Gene identity layer from HGNC's `hgnc_complete_set.txt`
//! and bridges to existing :Protein nodes (UniProt) via :SAME_AS edges.
//! Ensembl GFF3 transcript layer is added in a follow-up pass.
//!
//! Usage:
//!   cargo run --release --example hgnc_ensembl_loader -- \
//!     --hgnc data/hgnc/hgnc_complete_set.txt
//!   cargo run --release --example hgnc_ensembl_loader -- \
//!     --hgnc data/hgnc/hgnc_complete_set.txt --snapshot hgnc.sgsnap

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, NodeId, PropertyValue, Label};

mod hgnc_ensembl_common;
use hgnc_ensembl_common::{format_duration, format_num, load_hgnc_tsv};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: cargo run --release --example hgnc_ensembl_loader [OPTIONS]");
        eprintln!("  --hgnc PATH       HGNC TSV (hgnc_complete_set.txt) [required]");
        eprintln!("  --snapshot PATH   Export snapshot after loading");
        eprintln!("  --max-rows N      Limit input rows (0=all, default 0)");
        std::process::exit(0);
    }

    let hgnc_path = args
        .iter()
        .position(|a| a == "--hgnc")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .ok_or("--hgnc PATH is required")?;

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

    eprintln!("HGNC + Ensembl Loader (Phase 1b)");
    eprintln!("  HGNC: {}", hgnc_path.display());
    if max_rows > 0 {
        eprintln!("  Max rows: {}", format_num(max_rows));
    }
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        // Build UniProt accession -> :Protein NodeId map by walking the existing
        // store. Empty on a fresh DB, populated when chained after the UniProt
        // loader's snapshot has been imported.
        let uniprot_map = build_uniprot_index(&graph);
        eprintln!(
            "  UniProt :Protein bridge: {} accessions",
            format_num(uniprot_map.len())
        );
        load_hgnc_tsv(
            &mut graph,
            &hgnc_path,
            if uniprot_map.is_empty() {
                None
            } else {
                Some(&uniprot_map)
            },
            max_rows,
        )?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("HGNC load complete.");
    eprintln!("  Gene nodes:    {}", format_num(result.gene_nodes));
    eprintln!("  SAME_AS edges: {}", format_num(result.same_as_edges));
    eprintln!("  Time:          {}", format_duration(total_elapsed));
    eprintln!("========================================");

    if let Some(ref snap_path) = snapshot_path {
        eprintln!("\nExporting snapshot to {}...", snap_path.display());
        let t = Instant::now();
        let s = client.export_snapshot("default", snap_path).await?;
        let sz = std::fs::metadata(snap_path).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "Snapshot: {} nodes, {} edges ({:.1} MB) in {}",
            format_num(s.node_count as usize),
            format_num(s.edge_count as usize),
            sz as f64 / (1024.0 * 1024.0),
            format_duration(t.elapsed()),
        );
    }
    Ok(())
}

/// Walk all :Protein nodes in the store and index them by `accession` property.
/// Used to build the bridge map for HGNC :SAME_AS edges.
fn build_uniprot_index(graph: &samyama_sdk::GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: Label = "Protein".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(acc)) = node.get_property("accession") {
            out.insert(acc.clone(), node.id);
        }
    }
    out
}
