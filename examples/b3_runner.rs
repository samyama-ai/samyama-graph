// Paper 5 B3 — Samyama-side benchmark runner.
// Loads N snapshots into a single GraphStore and runs queries from a CSV
// (one row per query: id,name,kg,category,hops,cypher).
//
// Usage:
//   cargo run --release --example b3_runner -- \
//       --snapshots a.sgsnap,b.sgsnap,c.sgsnap \
//       --queries b3_subset_queries.csv \
//       --csv samyama_results.csv

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use samyama::snapshot::import_tenant;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::time::Instant;

fn parse_args() -> HashMap<String, String> {
    let mut args = HashMap::new();
    let raw: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < raw.len() {
        if raw[i].starts_with("--") {
            let key = raw[i][2..].to_string();
            let val = raw.get(i + 1).cloned().unwrap_or_default();
            args.insert(key, val);
            i += 2;
        } else {
            i += 1;
        }
    }
    args
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let snapshots = args
        .get("snapshots")
        .ok_or("--snapshots required")?
        .split(',')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let queries_path = args.get("queries").ok_or("--queries required")?.clone();
    let out_path = args.get("csv").ok_or("--csv required")?.clone();
    let warm: usize = args
        .get("warm")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let runs: usize = args
        .get("runs")
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    eprintln!("[b3] loading {} snapshots into single store...", snapshots.len());
    let mut store = GraphStore::new();
    let load_start = Instant::now();
    for snap in &snapshots {
        eprintln!("[b3]   {}", snap);
        let r = BufReader::new(File::open(snap)?);
        let stats = import_tenant(&mut store, r)?;
        eprintln!(
            "[b3]     +{} nodes, +{} edges (merged={})",
            stats.node_count, stats.edge_count, stats.merged_count
        );
    }
    eprintln!(
        "[b3] loaded total nodes={} edges={} in {:?}",
        store.node_count(),
        store.edge_count(),
        load_start.elapsed()
    );

    // Create indexes (mirror unified_benchmark Phase 3 for the PW + DI + CT subset).
    // This is the single most important methodological fix: without these, the engine
    // does full label scans for `WHERE x.prop = literal` filters.
    eprintln!("[b3] creating indexes...");
    let idx_start = Instant::now();
    let indexes: &[&str] = &[
        // Pathways KG
        "CREATE INDEX ON :Protein(name)",
        "CREATE INDEX ON :Protein(gene_name)",
        "CREATE INDEX ON :Protein(uniprot_id)",
        "CREATE INDEX ON :Pathway(name)",
        "CREATE INDEX ON :GOTerm(name)",
        // Drug Interactions KG
        "CREATE INDEX ON :Drug(name)",
        "CREATE INDEX ON :Drug(drugbank_id)",
        "CREATE INDEX ON :Gene(gene_name)",
        "CREATE INDEX ON :Gene(name)",
        "CREATE INDEX ON :SideEffect(name)",
        "CREATE INDEX ON :Indication(name)",
        "CREATE INDEX ON :AdverseEvent(name)",
        // Clinical Trials KG
        "CREATE INDEX ON :ClinicalTrial(nct_id)",
        "CREATE INDEX ON :Condition(name)",
        "CREATE INDEX ON :Intervention(name)",
        "CREATE INDEX ON :Sponsor(name)",
        "CREATE INDEX ON :Site(country)",
    ];
    let idx_engine = QueryEngine::new();
    let mut idx_ok = 0;
    for idx_stmt in indexes {
        match idx_engine.execute_mut(idx_stmt, &mut store, "default") {
            Ok(_) => idx_ok += 1,
            Err(e) => eprintln!("[b3]   skip: {} ({})", idx_stmt, e),
        }
    }
    eprintln!(
        "[b3] {} of {} indexes created in {:?}",
        idx_ok,
        indexes.len(),
        idx_start.elapsed()
    );

    // Graph-native planner is opt-in via SAMYAMA_GRAPH_NATIVE=true. We let the
    // caller control it rather than forcing it on, so we can A/B test the planner's
    // impact independently of the index fix.

    // Read queries CSV
    let qreader = BufReader::new(File::open(&queries_path)?);
    let mut queries: Vec<(String, String, String)> = Vec::new(); // (id, kg, cypher)
    for (i, line) in qreader.lines().enumerate() {
        let line = line?;
        if i == 0 {
            continue;
        } // header
        // Parse CSV: id,name,kg,category,hops,cypher (cypher may be quoted with commas inside)
        if let Some((id, kg, cypher)) = parse_csv_row(&line) {
            queries.push((id, kg, cypher));
        }
    }
    eprintln!("[b3] {} queries loaded", queries.len());

    let engine = QueryEngine::new();
    let mut out = BufWriter::new(File::create(&out_path)?);
    writeln!(out, "id,kg,system,latency_ms,row_count,status")?;

    for (id, kg, cypher) in &queries {
        // Warm-up runs
        for _ in 0..warm {
            let _ = engine.execute(cypher, &store);
        }
        // Measured runs
        let mut latencies_ms: Vec<f64> = Vec::with_capacity(runs);
        let mut row_count: usize = 0;
        let mut status = "pass".to_string();
        for _ in 0..runs {
            let start = Instant::now();
            match engine.execute(cypher, &store) {
                Ok(batch) => {
                    let dt = start.elapsed();
                    latencies_ms.push(dt.as_secs_f64() * 1000.0);
                    row_count = batch.records.len();
                }
                Err(e) => {
                    // Sanitize for CSV: collapse newlines + commas to spaces/semicolons
                    let msg = e.to_string()
                        .lines().next().unwrap_or("error").to_string()
                        .replace(',', ";");
                    status = format!("error:{}", msg);
                    latencies_ms.push(-1.0);
                    break;
                }
            }
        }
        if status == "pass" && row_count == 0 {
            status = "empty".to_string();
        }
        latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = latencies_ms[latencies_ms.len() / 2];
        eprintln!(
            "[b3]   {} ({}) {:.2}ms rows={} {}",
            id, kg, median, row_count, status
        );
        writeln!(
            out,
            "{},{},samyama,{:.3},{},{}",
            id, kg, median, row_count, status
        )?;
    }

    eprintln!("[b3] done — wrote {}", out_path);
    Ok(())
}

// Minimal CSV parser that respects double-quoted fields containing commas.
fn parse_csv_row(line: &str) -> Option<(String, String, String)> {
    let mut fields: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    fields.push(cur);
    if fields.len() < 6 {
        return None;
    }
    // id,name,kg,category,hops,cypher
    Some((fields[0].clone(), fields[2].clone(), fields[5].clone()))
}
