//! CIViC Loader — Clinical Interpretations of Variants in Cancer (CC0 1.0).
//!
//! First slice: variants and their relation to genes. Bridges to existing
//! :Gene nodes (loaded by HGNC pass) by symbol, and to existing :Variant
//! nodes (ClinVar pass) by ClinVar ID. Evidence items, assertions, drugs,
//! and diseases are layered in subsequent commits.
//!
//! Source: nightly TSV bundle from https://civicdb.org/downloads/nightly/
//!
//! Usage:
//!   cargo run --release --example civic_loader -- \
//!     --variants data/civic/01-Jan-2024-VariantSummaries.tsv
//!   cargo run --release --example civic_loader -- \
//!     --variants data/civic/01-Jan-2024-VariantSummaries.tsv \
//!     --snapshot civic.sgsnap

use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod civic_common;
use civic_common::{
    build_clinvar_index, build_gene_symbol_index, format_duration, format_num,
    load_civic_variants_tsv,
};

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: cargo run --release --example civic_loader [OPTIONS]");
        eprintln!("  --variants PATH   CIViC VariantSummaries TSV [required]");
        eprintln!("  --snapshot PATH   Export snapshot after loading");
        eprintln!("  --max-rows N      Limit input rows (0=all, default 0)");
        std::process::exit(0);
    }

    let variants_path = args
        .iter()
        .position(|a| a == "--variants")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .ok_or("--variants PATH is required")?;

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

    eprintln!("CIViC Loader");
    eprintln!("  Variants: {}", variants_path.display());
    if max_rows > 0 {
        eprintln!("  Max rows: {}", format_num(max_rows));
    }
    eprintln!();

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        let gene_idx = build_gene_symbol_index(&graph);
        let clinvar_idx = build_clinvar_index(&graph);
        eprintln!("  :Gene bridge:    {} symbols", format_num(gene_idx.len()));
        eprintln!("  :Variant bridge: {} ClinVar IDs", format_num(clinvar_idx.len()));
        load_civic_variants_tsv(&mut graph, &variants_path, &gene_idx, &clinvar_idx, max_rows)?
    };

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("========================================");
    eprintln!("CIViC variant load complete.");
    eprintln!("  Variant nodes:      {}", format_num(result.variant_nodes));
    eprintln!("  HAS_VARIANT edges:  {}", format_num(result.has_variant_edges));
    eprintln!("  SAME_AS edges:      {}", format_num(result.same_as_edges));
    eprintln!("  Time:               {}", format_duration(total_elapsed));
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
