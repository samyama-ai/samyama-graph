//! Generic property-equality bridge.
//!
//! Adds `:SAME_AS` (or any chosen edge type) between two labels whose
//! nodes share a value on a chosen property — the standard cross-KG
//! identity bridge pattern. Reusable for ChemblTarget.uniprot_accession
//! ↔ Protein.uniprot_id, OncoKBGene.hugo_symbol ↔ Gene.symbol, etc.
//!
//! Exists because samyama-graph has no property indexes today, so a
//! Cypher join like
//!
//!     MATCH (a:LeftLabel) MATCH (b:RightLabel)
//!     WHERE a.foo = b.bar
//!
//! degenerates to a full nested-loop scan and OOMs on multi-hundred-K
//! label populations. A one-shot pre-pass that materialises the join
//! as `:SAME_AS` edges turns those queries into cheap edge traversals.
//!
//! Reads property values via Cypher (handles snapshot-imported nodes
//! whose properties live in the columnar store and are invisible to
//! `Node.get_property`).
//!
//! Usage:
//!   cargo run --release --example property_bridge -- \
//!     --snapshot data/phase1b_chained_v3.sgsnap \
//!     --snapshot data/baseline/chembl.sgsnap \
//!     --source-label ChemblTarget --source-prop uniprot_accession \
//!     --target-label Protein      --target-prop uniprot_id \
//!     --edge-type SAME_AS \
//!     --snapshot-out data/phase1b_chained_v4.sgsnap

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, NodeId, SamyamaClient};

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

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn args_multi(args: &[String], name: &str) -> Vec<PathBuf> {
    args.iter()
        .enumerate()
        .filter_map(|(i, a)| (a == name).then(|| args.get(i + 1)))
        .flatten()
        .map(PathBuf::from)
        .collect()
}

/// Build a property -> NodeId index for one (label, property) pair via Cypher.
/// Strips the surrounding quotes the Display impl wraps strings in.
async fn build_index(
    client: &EmbeddedClient,
    label: &str,
    prop: &str,
) -> Result<HashMap<String, NodeId>, Error> {
    let q = format!(
        "MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN n.{prop} AS k, id(n) AS v"
    );
    let r = client.query("default", &q).await?;
    let mut out = HashMap::with_capacity(r.records.len());
    for row in &r.records {
        if row.len() < 2 {
            continue;
        }
        let key = format!("{}", row[0]);
        let key = key.trim_matches('"').to_string();
        if key.is_empty() || key == "null" {
            continue;
        }
        let id_str = format!("{}", row[1]);
        if let Ok(id) = id_str.parse::<u64>() {
            out.insert(key, NodeId::from(id));
        }
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!(
            "Usage: cargo run --release --example property_bridge [OPTIONS]\n\
             \n\
             --snapshot PATH        Snapshot to import (repeatable; layered in order)\n\
             --source-label LABEL   Label of nodes whose property drives the lookup\n\
             --source-prop PROP     Property on source nodes (looked up in target index)\n\
             --target-label LABEL   Label of nodes to be matched against\n\
             --target-prop PROP     Property on target nodes (used as the lookup key)\n\
             --edge-type TYPE       Edge type to create (default SAME_AS)\n\
             --snapshot-out PATH    Export combined snapshot afterwards (optional)"
        );
        std::process::exit(0);
    }

    let snapshots = args_multi(&args, "--snapshot");
    if snapshots.is_empty() {
        return Err("at least one --snapshot required".into());
    }
    let source_label = arg(&args, "--source-label").ok_or("--source-label required")?;
    let source_prop = arg(&args, "--source-prop").ok_or("--source-prop required")?;
    let target_label = arg(&args, "--target-label").ok_or("--target-label required")?;
    let target_prop = arg(&args, "--target-prop").ok_or("--target-prop required")?;
    let edge_type = arg(&args, "--edge-type").unwrap_or_else(|| "SAME_AS".to_string());
    let snapshot_out = arg(&args, "--snapshot-out").map(PathBuf::from);

    eprintln!("Property bridge");
    for s in &snapshots {
        eprintln!("  Import:        {}", s.display());
    }
    eprintln!("  Source:        :{source_label} . {source_prop}");
    eprintln!("  Target:        :{target_label} . {target_prop}");
    eprintln!("  Edge type:     {edge_type}");
    eprintln!();

    let client = EmbeddedClient::new();
    let total = Instant::now();

    for snap in &snapshots {
        let t = Instant::now();
        let stats = client.import_snapshot("default", snap).await?;
        eprintln!(
            "Imported {}: {} nodes, {} edges in {:.1}s",
            snap.file_name().and_then(|n| n.to_str()).unwrap_or("?"),
            fmt_num(stats.node_count as usize),
            fmt_num(stats.edge_count as usize),
            t.elapsed().as_secs_f64()
        );
    }

    let t = Instant::now();
    let target_index = build_index(&client, &target_label, &target_prop).await?;
    eprintln!(
        "Built target index :{}.{}: {} entries in {:.1}s",
        target_label,
        target_prop,
        fmt_num(target_index.len()),
        t.elapsed().as_secs_f64()
    );

    let t = Instant::now();
    let source_pairs = build_index(&client, &source_label, &source_prop).await?;
    eprintln!(
        "Walked source :{}.{}: {} entries in {:.1}s",
        source_label,
        source_prop,
        fmt_num(source_pairs.len()),
        t.elapsed().as_secs_f64()
    );

    let mut created = 0usize;
    let mut missing_target = 0usize;
    let t = Instant::now();
    {
        let mut graph = client.store_write().await;
        for (key, src_id) in &source_pairs {
            match target_index.get(key) {
                Some(&tgt_id) => {
                    if graph.create_edge(*src_id, tgt_id, edge_type.as_str()).is_ok() {
                        created += 1;
                    }
                }
                None => missing_target += 1,
            }
        }
    }
    eprintln!(
        "Created {} {} edges in {:.1}s ({} source rows had no target match)",
        fmt_num(created),
        edge_type,
        t.elapsed().as_secs_f64(),
        fmt_num(missing_target)
    );

    eprintln!();
    eprintln!("Total wall: {:.1}s", total.elapsed().as_secs_f64());

    if let Some(ref path) = snapshot_out {
        eprintln!();
        eprintln!("Exporting snapshot to {}...", path.display());
        let t = Instant::now();
        let s = client.export_snapshot("default", path).await?;
        let sz = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "Snapshot: {} nodes, {} edges ({:.1} MB) in {:.1}s",
            fmt_num(s.node_count as usize),
            fmt_num(s.edge_count as usize),
            sz as f64 / (1024.0 * 1024.0),
            t.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use samyama_sdk::{GraphStore, PropertyValue};

    fn run_bridge_in_memory(
        graph: &mut GraphStore,
        source_label: &str,
        source_prop: &str,
        target_label: &str,
        target_prop: &str,
        edge_type: &str,
    ) -> (usize, usize) {
        // Walk target nodes via Node API (works for fresh nodes; tests
        // don't import snapshots so columnar storage isn't in play).
        let label: samyama_sdk::Label = target_label.into();
        let mut tgt_idx: HashMap<String, NodeId> = HashMap::new();
        for n in graph.get_nodes_by_label(&label) {
            if let Some(PropertyValue::String(s)) = n.get_property(target_prop) {
                tgt_idx.insert(s.clone(), n.id);
            }
        }
        let label: samyama_sdk::Label = source_label.into();
        let src_pairs: Vec<(String, NodeId)> = graph
            .get_nodes_by_label(&label)
            .iter()
            .filter_map(|n| {
                n.get_property(source_prop).and_then(|v| match v {
                    PropertyValue::String(s) => Some((s.clone(), n.id)),
                    _ => None,
                })
            })
            .collect();
        let mut created = 0usize;
        let mut missing = 0usize;
        for (key, sid) in src_pairs {
            match tgt_idx.get(&key) {
                Some(&tid) => {
                    if graph.create_edge(sid, tid, edge_type).is_ok() {
                        created += 1;
                    }
                }
                None => missing += 1,
            }
        }
        (created, missing)
    }

    #[test]
    fn creates_edges_when_property_values_match() {
        let mut g = GraphStore::new();
        let p1 = g.create_node("Protein");
        g.get_node_mut(p1).unwrap().set_property("uniprot_id", PropertyValue::String("P38398".into()));
        let p2 = g.create_node("Protein");
        g.get_node_mut(p2).unwrap().set_property("uniprot_id", PropertyValue::String("Q53GA5".into()));
        let ct1 = g.create_node("ChemblTarget");
        g.get_node_mut(ct1).unwrap().set_property("uniprot_accession", PropertyValue::String("P38398".into()));
        let ct2 = g.create_node("ChemblTarget");
        g.get_node_mut(ct2).unwrap().set_property("uniprot_accession", PropertyValue::String("MISSING".into()));

        let (created, missing) = run_bridge_in_memory(
            &mut g,
            "ChemblTarget",
            "uniprot_accession",
            "Protein",
            "uniprot_id",
            "SAME_AS",
        );
        assert_eq!(created, 1);
        assert_eq!(missing, 1);
    }

    #[test]
    fn skips_source_nodes_without_the_property() {
        let mut g = GraphStore::new();
        let p1 = g.create_node("Protein");
        g.get_node_mut(p1).unwrap().set_property("uniprot_id", PropertyValue::String("P38398".into()));
        // ChemblTarget without uniprot_accession property at all.
        g.create_node("ChemblTarget");
        let (created, missing) = run_bridge_in_memory(
            &mut g,
            "ChemblTarget",
            "uniprot_accession",
            "Protein",
            "uniprot_id",
            "SAME_AS",
        );
        assert_eq!(created, 0);
        assert_eq!(missing, 0);
    }

    #[test]
    fn handles_one_to_many_target_match_by_overwriting_index() {
        // If two targets share the same uniprot_id, the second overwrites the
        // first in the index — by design (this is meant to be a 1-1 identity
        // bridge; multi-match would require explicit modeling).
        let mut g = GraphStore::new();
        let p1 = g.create_node("Protein");
        g.get_node_mut(p1).unwrap().set_property("uniprot_id", PropertyValue::String("P38398".into()));
        let p2 = g.create_node("Protein");
        g.get_node_mut(p2).unwrap().set_property("uniprot_id", PropertyValue::String("P38398".into()));
        let ct1 = g.create_node("ChemblTarget");
        g.get_node_mut(ct1).unwrap().set_property("uniprot_accession", PropertyValue::String("P38398".into()));
        let (created, missing) = run_bridge_in_memory(
            &mut g,
            "ChemblTarget",
            "uniprot_accession",
            "Protein",
            "uniprot_id",
            "SAME_AS",
        );
        assert_eq!(created, 1);
        assert_eq!(missing, 0);
    }

    #[test]
    fn supports_arbitrary_edge_type() {
        let mut g = GraphStore::new();
        let gene = g.create_node("OncoKBGene");
        g.get_node_mut(gene).unwrap().set_property("hugo_symbol", PropertyValue::String("BRAF".into()));
        let hgnc = g.create_node("Gene");
        g.get_node_mut(hgnc).unwrap().set_property("symbol", PropertyValue::String("BRAF".into()));
        let (created, _) = run_bridge_in_memory(
            &mut g,
            "OncoKBGene",
            "hugo_symbol",
            "Gene",
            "symbol",
            "ALIASES",
        );
        assert_eq!(created, 1);
    }
}
