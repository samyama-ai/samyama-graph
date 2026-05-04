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
use hgnc_ensembl_common::{format_duration, format_num, load_ensembl_gff3, load_hgnc_tsv};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: cargo run --release --example hgnc_ensembl_loader [OPTIONS]");
        eprintln!("  --hgnc PATH       HGNC TSV (hgnc_complete_set.txt) [required]");
        eprintln!("  --gff3 PATH       Ensembl GRCh38 GFF3 (.gff3 or .gff3.gz)");
        eprintln!("  --snapshot PATH   Export snapshot after loading");
        eprintln!("  --max-rows N      Limit input rows per source (0=all, default 0)");
        std::process::exit(0);
    }

    let hgnc_path = args
        .iter()
        .position(|a| a == "--hgnc")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .ok_or("--hgnc PATH is required")?;

    let gff3_path = args
        .iter()
        .position(|a| a == "--gff3")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from);

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

    eprintln!("HGNC + Ensembl Loader");
    eprintln!("  HGNC: {}", hgnc_path.display());
    if let Some(ref p) = gff3_path {
        eprintln!("  GFF3: {}", p.display());
    }
    if max_rows > 0 {
        eprintln!("  Max rows: {}", format_num(max_rows));
    }
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let mut result = {
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

    if let Some(ref gff_path) = gff3_path {
        let mut graph = client.store_write().await;
        let ensembl_map = build_ensembl_gene_index(&graph);
        eprintln!(
            "  Ensembl gene bridge:    {} ENSG IDs",
            format_num(ensembl_map.len())
        );
        let (transcripts, edges) =
            load_ensembl_gff3(&mut graph, gff_path, &ensembl_map, max_rows)?;
        result.transcript_nodes = transcripts;
        result.has_transcript_edges = edges;
    }

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("HGNC load complete.");
    eprintln!("  Gene nodes:           {}", format_num(result.gene_nodes));
    eprintln!("  SAME_AS edges:        {}", format_num(result.same_as_edges));
    eprintln!("  Transcript nodes:     {}", format_num(result.transcript_nodes));
    eprintln!("  HAS_TRANSCRIPT edges: {}", format_num(result.has_transcript_edges));
    eprintln!("  Time:                 {}", format_duration(total_elapsed));
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

/// Walk all :Protein nodes in the store and index them by their UniProt
/// identifier. v1.0 snapshots persist this as `uniprot_id`; older loaders
/// used `accession`. Both are supported.
///
/// Caveat: post-snapshot-import, properties live in the columnar store and
/// `Node.get_property` returns None — for a chain that starts from an
/// imported baseline, build the bridge via Cypher (see
/// `examples/phase1b_smoke.rs::build_index_via_cypher`).
fn build_uniprot_index(graph: &samyama_sdk::GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: Label = "Protein".into();
    for node in graph.get_nodes_by_label(&label) {
        let acc = node
            .get_property("uniprot_id")
            .or_else(|| node.get_property("accession"));
        if let Some(PropertyValue::String(acc)) = acc {
            out.insert(acc.clone(), node.id);
        }
    }
    out
}

/// Walk all :Gene nodes (just loaded from HGNC) and index by Ensembl gene ID
/// so the GFF3 pass can resolve `Parent=gene:ENSG...` to a NodeId.
fn build_ensembl_gene_index(graph: &samyama_sdk::GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: Label = "Gene".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(eid)) = node.get_property("ensembl_gene_id") {
            if !eid.is_empty() {
                out.insert(eid.clone(), node.id);
            }
        }
    }
    out
}
