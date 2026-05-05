//! OncoKB Loader (license-gated; scaffold runs offline against fixtures).
//!
//! Pulls oncology-evidence JSON exports from the OncoKB v1 API. License is
//! academic (free) at https://www.oncokb.org/account/register; turnaround
//! is ~1-2 weeks. Without a token the public endpoints return 401, so this
//! binary takes pre-downloaded JSON files rather than fetching directly.
//! When the token arrives, save the responses with curl and pass the paths.
//!
//! Usage:
//!   cargo run --release --example oncokb_loader -- \
//!     --curated-genes data/oncokb/allCuratedGenes.json
//!   cargo run --release --example oncokb_loader -- \
//!     --curated-genes data/oncokb/allCuratedGenes.json \
//!     --actionable-variants data/oncokb/allActionableVariants.json \
//!     --snapshot oncokb.sgsnap
//!
//! Saving the JSON exports (after license arrives):
//!   curl -H "Authorization: Bearer $ONCOKB_TOKEN" \
//!     "https://www.oncokb.org/api/v1/utils/allCuratedGenes.json" \
//!     -o data/oncokb/allCuratedGenes.json
//!   curl -H "Authorization: Bearer $ONCOKB_TOKEN" \
//!     "https://www.oncokb.org/api/v1/utils/allActionableVariants.json" \
//!     -o data/oncokb/allActionableVariants.json

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, Label, NodeId, PropertyValue, SamyamaClient};

mod oncokb_common;
use oncokb_common::{load_actionable_variants_json, load_curated_genes_json};

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

fn arg(args: &[String], name: &str) -> Option<PathBuf> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: cargo run --release --example oncokb_loader [OPTIONS]");
        eprintln!("  --curated-genes PATH         allCuratedGenes.json [required]");
        eprintln!("  --actionable-variants PATH   allActionableVariants.json (optional)");
        eprintln!("  --snapshot PATH              Export snapshot after loading");
        std::process::exit(0);
    }

    let genes_path = arg(&args, "--curated-genes").ok_or("--curated-genes PATH required")?;
    let variants_path = arg(&args, "--actionable-variants");
    let snapshot_path = arg(&args, "--snapshot");

    eprintln!("OncoKB Loader");
    eprintln!("  Curated genes:   {}", genes_path.display());
    if let Some(p) = &variants_path {
        eprintln!("  Actionable vars: {}", p.display());
    }
    eprintln!();

    let client = EmbeddedClient::new();
    let total = Instant::now();

    let result = {
        let mut graph = client.store_write().await;
        let hgnc_index = build_hgnc_symbol_index(&graph);
        eprintln!(
            "  HGNC :Gene bridge: {} symbols",
            fmt_num(hgnc_index.len())
        );

        let mut total = load_curated_genes_json(
            &mut graph,
            &genes_path,
            if hgnc_index.is_empty() { None } else { Some(&hgnc_index) },
        )?;
        eprintln!(
            "  Curated genes: {} :OncoKBGene nodes, {} SAME_AS edges",
            fmt_num(total.gene_nodes),
            fmt_num(total.same_as_edges)
        );

        if let Some(ref vp) = variants_path {
            let oncokb_index = build_oncokb_gene_index(&graph);
            let var_res =
                load_actionable_variants_json(&mut graph, vp, &oncokb_index)?;
            total.variant_nodes = var_res.variant_nodes;
            total.drug_nodes = var_res.drug_nodes;
            total.curated_edges = var_res.curated_edges;
            total.therapeutic_edges = var_res.therapeutic_edges;
            eprintln!(
                "  Actionable variants: {} :Variant, {} :Drug, {} CURATED_AS_ONCOGENIC, {} HAS_THERAPEUTIC_IMPLICATION",
                fmt_num(var_res.variant_nodes),
                fmt_num(var_res.drug_nodes),
                fmt_num(var_res.curated_edges),
                fmt_num(var_res.therapeutic_edges)
            );
        }
        total
    };

    eprintln!();
    eprintln!("========================================");
    eprintln!("OncoKB load complete.");
    eprintln!("  :OncoKBGene nodes:                 {}", fmt_num(result.gene_nodes));
    eprintln!("  :Variant (OncoKB) nodes:           {}", fmt_num(result.variant_nodes));
    eprintln!("  :Drug nodes:                       {}", fmt_num(result.drug_nodes));
    eprintln!("  SAME_AS (OncoKBGene -> HGNC) edges:{}", fmt_num(result.same_as_edges));
    eprintln!("  CURATED_AS_ONCOGENIC edges:        {}", fmt_num(result.curated_edges));
    eprintln!("  HAS_THERAPEUTIC_IMPLICATION edges: {}", fmt_num(result.therapeutic_edges));
    eprintln!("  Time:                              {:.1}s", total.elapsed().as_secs_f64());
    eprintln!("========================================");

    if let Some(ref snap) = snapshot_path {
        eprintln!("\nExporting snapshot to {}...", snap.display());
        let s = client.export_snapshot("default", snap).await?;
        let sz = std::fs::metadata(snap).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "Snapshot: {} nodes, {} edges ({:.1} MB)",
            fmt_num(s.node_count as usize),
            fmt_num(s.edge_count as usize),
            sz as f64 / (1024.0 * 1024.0),
        );
    }
    Ok(())
}

fn build_hgnc_symbol_index(graph: &samyama_sdk::GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: Label = "Gene".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(sym)) = node.get_property("symbol") {
            if !sym.is_empty() {
                out.insert(sym.clone(), node.id);
            }
        }
    }
    out
}

fn build_oncokb_gene_index(graph: &samyama_sdk::GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: Label = "OncoKBGene".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(sym)) = node.get_property("hugo_symbol") {
            if !sym.is_empty() {
                out.insert(sym.clone(), node.id);
            }
        }
    }
    out
}
