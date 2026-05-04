//! Phase 1b end-to-end smoke runner.
//!
//! Loads HGNC + Ensembl, then CIViC against the same in-process store so the
//! gene-symbol and clinvar-id bridge indexes actually populate. Reports node
//! and edge counts and runs a couple of dedup Cypher queries.
//!
//! Usage:
//!   cargo run --release --example phase1b_smoke -- \
//!     --hgnc data/hgnc/hgnc_complete_set.txt \
//!     --gff3 data/ensembl/Homo_sapiens.GRCh38.111.chr.gff3.gz \
//!     --civic-variants data/civic/nightly-VariantSummaries.tsv \
//!     --snapshot data/phase1b.sgsnap

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, NodeId, PropertyValue, SamyamaClient};

mod hgnc_ensembl_common;
mod civic_common;

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

fn build_ensembl_gene_index(graph: &samyama_sdk::GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: samyama_sdk::Label = "Gene".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(eid)) = node.get_property("ensembl_gene_id") {
            if !eid.is_empty() {
                out.insert(eid.clone(), node.id);
            }
        }
    }
    out
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    let hgnc = arg(&args, "--hgnc").ok_or("--hgnc PATH required")?;
    let gff3 = arg(&args, "--gff3");
    let civic = arg(&args, "--civic-variants");
    let snapshot = arg(&args, "--snapshot");

    eprintln!("Phase 1b smoke runner");
    eprintln!("  HGNC:           {}", hgnc.display());
    if let Some(p) = &gff3 {
        eprintln!("  Ensembl GFF3:   {}", p.display());
    }
    if let Some(p) = &civic {
        eprintln!("  CIViC variants: {}", p.display());
    }
    eprintln!();

    let client = EmbeddedClient::new();
    let total = Instant::now();

    // ── Phase 1: HGNC + Ensembl ──────────────────────────────────────
    {
        let mut graph = client.store_write().await;
        let res = hgnc_ensembl_common::load_hgnc_tsv(&mut graph, &hgnc, None, 0)?;
        eprintln!(
            "HGNC: {} :Gene nodes",
            fmt_num(res.gene_nodes)
        );
        if let Some(ref p) = gff3 {
            let ensembl_idx = build_ensembl_gene_index(&graph);
            eprintln!(
                "Ensembl bridge index: {} ENSG IDs",
                fmt_num(ensembl_idx.len())
            );
            let (transcripts, edges) =
                hgnc_ensembl_common::load_ensembl_gff3(&mut graph, p, &ensembl_idx, 0)?;
            eprintln!(
                "Ensembl: {} :Transcript nodes + {} HAS_TRANSCRIPT edges",
                fmt_num(transcripts),
                fmt_num(edges)
            );
        }
    }

    // ── Phase 2: CIViC ───────────────────────────────────────────────
    if let Some(ref p) = civic {
        let mut graph = client.store_write().await;
        let gene_idx = civic_common::build_gene_symbol_index(&graph);
        let clinvar_idx = civic_common::build_clinvar_index(&graph);
        eprintln!(
            "CIViC bridges in store: {} :Gene symbols, {} :Variant clinvar_ids",
            fmt_num(gene_idx.len()),
            fmt_num(clinvar_idx.len())
        );
        let res = civic_common::load_civic_variants_tsv(
            &mut graph,
            p,
            &gene_idx,
            &clinvar_idx,
            0,
        )?;
        eprintln!(
            "CIViC: {} :Variant nodes, {} HAS_VARIANT edges, {} SAME_AS edges",
            fmt_num(res.variant_nodes),
            fmt_num(res.has_variant_edges),
            fmt_num(res.same_as_edges)
        );
    }

    eprintln!();
    eprintln!("Total wall: {:.1}s", total.elapsed().as_secs_f64());
    eprintln!();

    // ── Phase 3: Cypher smoke queries ────────────────────────────────
    eprintln!("── Cypher smoke ────────────────────────────────────────────");
    for (label, q) in [
        (
            "Total :Gene nodes",
            "MATCH (g:Gene) RETURN count(g) AS n",
        ),
        (
            "Total :Transcript nodes",
            "MATCH (t:Transcript) RETURN count(t) AS n",
        ),
        (
            "Total :Variant nodes (CIViC + ClinVar pre-existing)",
            "MATCH (v:Variant) RETURN count(v) AS n",
        ),
        (
            "Total HAS_TRANSCRIPT edges",
            "MATCH ()-[r:HAS_TRANSCRIPT]->() RETURN count(r) AS n",
        ),
        (
            "Total HAS_VARIANT edges (CIViC Gene -> Variant)",
            "MATCH ()-[r:HAS_VARIANT]->() RETURN count(r) AS n",
        ),
        (
            "Total SAME_AS edges (cross-KG dedup)",
            "MATCH ()-[r:SAME_AS]->() RETURN count(r) AS n",
        ),
        (
            "Sample: BRCA1 -> CIViC variants",
            "MATCH (g:Gene {symbol:'BRCA1'})-[:HAS_VARIANT]->(v:Variant) \
             RETURN g.symbol AS gene, count(v) AS variants",
        ),
        (
            "Sample: TP53 -> Transcripts (canonical first)",
            "MATCH (g:Gene {symbol:'TP53'})-[:HAS_TRANSCRIPT]->(t:Transcript) \
             RETURN count(t) AS transcripts",
        ),
    ] {
        match client.query("default", q).await {
            Ok(r) => {
                let cells: Vec<String> = r
                    .records
                    .first()
                    .map(|row| row.iter().map(|v| format!("{}", v)).collect())
                    .unwrap_or_else(|| vec!["(no rows)".into()]);
                eprintln!("  {:55} {}", label, cells.join(" | "));
            }
            Err(e) => eprintln!("  {:55} ERROR: {}", label, e),
        }
    }

    if let Some(ref snap) = snapshot {
        eprintln!();
        eprintln!("Exporting snapshot to {}...", snap.display());
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
