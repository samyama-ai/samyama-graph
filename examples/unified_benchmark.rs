//! Unified Benchmark (200+ queries)
//!
//! Loads up to 9 KGs into one graph using the optimal method for each:
//! - PubMed, Clinical Trials, Pathways, FAERS, UniProt: snapshot import
//! - Drug Interactions, Surveillance, Health Determinants, Health Systems: direct Rust loaders
//!
//! Usage:
//!   cargo run --release --example unified_benchmark -- \
//!     --pubmed-snap ~/samyama/pubmed-v2.sgsnap \
//!     --ct-snap ~/samyama/clinical-trials.sgsnap \
//!     --pw-snap ~/samyama/pathways.sgsnap \
//!     --faers-snap ~/samyama/faers-full.sgsnap \
//!     --uniprot-snap ~/samyama/uniprot.sgsnap \
//!     --di-data ~/kg-data/druginteractions \
//!     --surv-data ~/kg-data/surveillance \
//!     --hd-data ~/kg-data/health-determinants \
//!     --hs-data ~/kg-data/health-systems \
//!     --queries ~/samyama

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use samyama_sdk::{EmbeddedClient, SamyamaClient};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Oracle {
    #[serde(default)]
    status: OracleStatus,
    #[serde(default)]
    rows: Option<usize>,
    #[serde(default)]
    min: Option<usize>,
    #[serde(default)]
    max: Option<usize>,
    #[serde(default)]
    #[allow(dead_code)]
    reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
enum OracleStatus {
    #[default]
    NonEmpty,
    DataDependent,
    Aggregation,
    Exact,
    Range,
}

#[derive(Debug, Deserialize)]
struct OracleFile {
    #[serde(default)]
    queries: HashMap<String, Oracle>,
}

fn load_oracle(path: &std::path::Path) -> HashMap<String, Oracle> {
    match fs::read_to_string(path) {
        Ok(s) => match serde_yaml::from_str::<OracleFile>(&s) {
            Ok(f) => {
                eprintln!("Loaded oracle with {} entries from {:?}", f.queries.len(), path);
                f.queries
            }
            Err(e) => {
                eprintln!("Warning: failed to parse oracle {:?}: {}", path, e);
                HashMap::new()
            }
        },
        Err(_) => HashMap::new(),
    }
}

/// Classify a result given rows returned and the oracle.
/// Returns (status_str, counts_as_pass).
fn classify(rows: usize, oracle: Option<&Oracle>) -> (&'static str, bool) {
    match oracle.map(|o| o.status).unwrap_or(OracleStatus::NonEmpty) {
        OracleStatus::NonEmpty => {
            if rows > 0 { ("pass", true) } else { ("empty", false) }
        }
        OracleStatus::DataDependent => {
            if rows > 0 { ("pass", true) } else { ("pass_data_gap", true) }
        }
        OracleStatus::Aggregation => ("pass", true), // any result (including 0 row) is fine
        OracleStatus::Exact => {
            let want = oracle.and_then(|o| o.rows).unwrap_or(0);
            if rows == want { ("pass", true) } else { ("fail_count", false) }
        }
        OracleStatus::Range => {
            let lo = oracle.and_then(|o| o.min).unwrap_or(1);
            let hi = oracle.and_then(|o| o.max).unwrap_or(usize::MAX);
            if rows >= lo && rows <= hi { ("pass", true) } else { ("fail_count", false) }
        }
    }
}

mod druginteractions_common;
mod health_determinants_common;
mod health_systems_common;
mod surveillance_common;

type Error = Box<dyn std::error::Error>;

fn parse_csv_queries(path: &std::path::Path) -> Vec<(String, String, String, String)> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return vec![],
    };
    let reader = BufReader::new(file);
    let mut queries = Vec::new();
    let mut num_columns = 0;

    for (i, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        if i == 0 {
            num_columns = line.split(',').count();
            continue;
        }
        let cypher = if let Some(pos) = line.rfind(",\"") {
            let raw = &line[pos + 1..];
            raw.trim_matches('"').to_string()
        } else {
            let skip = if num_columns >= 6 { 5 } else { 4 };
            let parts: Vec<&str> = line.splitn(skip + 1, ',').collect();
            if parts.len() > skip {
                parts[skip].to_string()
            } else {
                continue;
            }
        };
        let parts: Vec<&str> = line.splitn(4, ',').collect();
        if parts.len() < 4 {
            continue;
        }
        let id = parts[0].to_string();
        let name = parts[1].to_string();
        let category = parts[2].to_string();
        if cypher.contains("MATCH") || cypher.contains("RETURN") {
            queries.push((id, name, category, cypher));
        }
    }
    queries
}

fn get_arg(args: &[String], flag: &str) -> Option<PathBuf> {
    args.iter()
        .position(|a| a == flag)
        .map(|p| PathBuf::from(&args[p + 1]))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();

    let pubmed_snap = get_arg(&args, "--pubmed-snap");
    let ct_snap = get_arg(&args, "--ct-snap");
    let pw_snap = get_arg(&args, "--pw-snap");
    let faers_snap = get_arg(&args, "--faers-snap");
    let uniprot_snap = get_arg(&args, "--uniprot-snap");
    let omop_snap = get_arg(&args, "--omop-snap");
    let di_snap = get_arg(&args, "--di-snap");
    let surv_snap = get_arg(&args, "--surv-snap");
    let hd_snap = get_arg(&args, "--hd-snap");
    let hs_snap = get_arg(&args, "--hs-snap");
    let di_data = get_arg(&args, "--di-data");
    let surv_data = get_arg(&args, "--surv-data");
    let hd_data = get_arg(&args, "--hd-data");
    let hs_data = get_arg(&args, "--hs-data");
    let study_refs = get_arg(&args, "--study-refs");
    let queries_dir = get_arg(&args, "--queries").unwrap_or_else(|| PathBuf::from("."));
    let oracle_path = get_arg(&args, "--oracle")
        .unwrap_or_else(|| queries_dir.join("expected_rows.yaml"));
    let oracle = load_oracle(&oracle_path);

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    // ── Phase 1: Import large snapshots ──
    for (name, path) in &[
        ("PubMed", &pubmed_snap),
        ("Clinical Trials", &ct_snap),
        ("Pathways", &pw_snap),
        ("FAERS", &faers_snap),
        ("UniProt", &uniprot_snap),
        ("OMOP", &omop_snap),
        ("Drug Interactions", &di_snap),
        ("Surveillance", &surv_snap),
        ("Health Determinants", &hd_snap),
        ("Health Systems", &hs_snap),
    ] {
        if let Some(ref p) = path {
            eprint!("Importing {} snapshot... ", name);
            let t0 = Instant::now();
            let stats = client.import_snapshot("default", p).await?;
            eprintln!(
                "{} nodes, {} edges in {:.1}s",
                stats.node_count,
                stats.edge_count,
                t0.elapsed().as_secs_f64()
            );
        }
    }

    // ── Phase 2: Run direct loaders (HashMap properties, correct IDs) ──
    {
        let mut graph = client.store_write().await;

        if let Some(ref dir) = di_data {
            eprint!("Loading Drug Interactions (direct)... ");
            let t0 = Instant::now();
            let all_phases: Vec<String> = vec![
                "drugbank_dgidb".into(),
                "sider".into(),
                "chembl_ttd".into(),
                "openfda".into(),
            ];
            let r = druginteractions_common::load_dataset(&mut graph, dir, &all_phases)?;
            eprintln!(
                "{} nodes, {} edges in {:.1}s",
                r.total_nodes,
                r.total_edges,
                t0.elapsed().as_secs_f64()
            );
        }

        if let Some(ref dir) = surv_data {
            eprint!("Loading Surveillance (direct)... ");
            let t0 = Instant::now();
            let r = surveillance_common::load_dataset(&mut graph, dir)?;
            eprintln!(
                "{} nodes, {} edges in {:.1}s",
                r.total_nodes,
                r.total_edges,
                t0.elapsed().as_secs_f64()
            );
        }

        if let Some(ref dir) = hd_data {
            eprint!("Loading Health Determinants (direct)... ");
            let t0 = Instant::now();
            let r = health_determinants_common::load_dataset(&mut graph, dir)?;
            eprintln!(
                "{} nodes, {} edges in {:.1}s",
                r.total_nodes,
                r.total_edges,
                t0.elapsed().as_secs_f64()
            );
        }

        if let Some(ref dir) = hs_data {
            eprint!("Loading Health Systems (direct)... ");
            let t0 = Instant::now();
            let r = health_systems_common::load_dataset(&mut graph, dir)?;
            eprintln!(
                "{} nodes, {} edges in {:.1}s",
                r.total_nodes,
                r.total_edges,
                t0.elapsed().as_secs_f64()
            );
        }
    }

    let import_elapsed = total_start.elapsed();
    eprintln!("\nAll data loaded in {:.1}s", import_elapsed.as_secs_f64());

    // ── Phase 2b: Set nct_id on Articles from study_references.txt ──
    // Then build REFERENCED_IN edges.
    {
        let mut graph = client.store_write().await;

        // Step 1: Read study_references.txt and set nct_id on matching Article nodes
        if let Some(ref refs_path) = study_refs {
            eprintln!("Setting nct_id on Articles from study_references.txt...");
            let refs_start = Instant::now();

            // Build pmid → Article NodeId lookup
            let articles = graph.get_nodes_by_label(&"Article".into());
            let mut pmid_to_article: std::collections::HashMap<String, samyama_sdk::NodeId> =
                std::collections::HashMap::new();
            for a in &articles {
                let col_val = graph
                    .node_columns
                    .get_property(a.id.as_u64() as usize, "pmid");
                if let samyama_sdk::PropertyValue::String(pmid) = col_val {
                    if !pmid.is_empty() {
                        pmid_to_article.insert(pmid, a.id);
                    }
                }
            }
            eprintln!("  {} Articles with pmid indexed", pmid_to_article.len());

            // Read study_references.txt (pipe-delimited: id|nct_id|pmid|reference_type|citation)
            let mut nct_set = 0u64;
            if let Ok(file) = std::fs::File::open(refs_path) {
                let reader = std::io::BufReader::with_capacity(4 * 1024 * 1024, file);
                for line in reader.lines() {
                    let line = match line {
                        Ok(l) => l,
                        Err(_) => continue,
                    };
                    let fields: Vec<&str> = line.split('|').collect();
                    if fields.len() < 3 {
                        continue;
                    }
                    let nct_id = fields[1].trim();
                    let pmid = fields[2].trim();
                    if pmid.is_empty() || nct_id.is_empty() {
                        continue;
                    }
                    if let Some(&article_id) = pmid_to_article.get(pmid) {
                        graph.set_column_property(
                            article_id,
                            "nct_id",
                            samyama_sdk::PropertyValue::String(nct_id.to_string()),
                        );
                        nct_set += 1;
                    }
                }
            }
            eprintln!(
                "  {} articles tagged with nct_id in {:.1}s",
                nct_set,
                refs_start.elapsed().as_secs_f64()
            );
        }

        // Step 2: Build REFERENCED_IN edges
        eprintln!("Building NCT bridge (Article → ClinicalTrial)...");
        let bridge_start = Instant::now();

        // Build nct_id → ClinicalTrial NodeId lookup from existing CT nodes
        let ct_nodes = graph.get_nodes_by_label(&"ClinicalTrial".into());
        let mut nct_to_ct: std::collections::HashMap<String, samyama_sdk::NodeId> =
            std::collections::HashMap::new();
        for ct in &ct_nodes {
            // Check HashMap property
            if let Some(samyama_sdk::PropertyValue::String(nct)) = ct.get_property("nct_id") {
                nct_to_ct.insert(nct.clone(), ct.id);
            }
            // Check ColumnStore
            let col_val = graph
                .node_columns
                .get_property(ct.id.as_u64() as usize, "nct_id");
            if let samyama_sdk::PropertyValue::String(nct) = col_val {
                if !nct.is_empty() {
                    nct_to_ct.insert(nct, ct.id);
                }
            }
        }
        eprintln!("  {} ClinicalTrial nodes with nct_id", nct_to_ct.len());

        // Scan articles with nct_id and create edges
        let article_nodes = graph.get_nodes_by_label(&"Article".into());
        let mut bridge_count = 0;
        let mut article_ids_with_nct: Vec<(samyama_sdk::NodeId, String)> = Vec::new();

        for article in &article_nodes {
            let col_val = graph
                .node_columns
                .get_property(article.id.as_u64() as usize, "nct_id");
            if let samyama_sdk::PropertyValue::String(nct) = col_val {
                if !nct.is_empty() {
                    article_ids_with_nct.push((article.id, nct));
                }
            }
        }

        for (article_id, nct) in &article_ids_with_nct {
            if let Some(&ct_id) = nct_to_ct.get(nct) {
                let _ = graph.create_edge(*article_id, ct_id, "REFERENCED_IN");
                bridge_count += 1;
            }
        }
        eprintln!(
            "  {} REFERENCED_IN edges created in {:.1}s",
            bridge_count,
            bridge_start.elapsed().as_secs_f64()
        );
    }

    // ── Phase 3: Create indexes ──
    eprintln!("Creating indexes...");
    let idx_start = Instant::now();
    let indexes = &[
        "CREATE INDEX ON :Article(pmid)",
        "CREATE INDEX ON :Author(name)",
        "CREATE INDEX ON :MeSHTerm(name)",
        "CREATE INDEX ON :Chemical(name)",
        "CREATE INDEX ON :Journal(title)",
        "CREATE INDEX ON :Grant(agency)",
        "CREATE INDEX ON :ClinicalTrial(nct_id)",
        "CREATE INDEX ON :Condition(name)",
        "CREATE INDEX ON :Intervention(name)",
        "CREATE INDEX ON :Sponsor(name)",
        "CREATE INDEX ON :Protein(name)",
        "CREATE INDEX ON :Protein(gene_name)",
        "CREATE INDEX ON :Pathway(name)",
        "CREATE INDEX ON :GOTerm(name)",
        "CREATE INDEX ON :Drug(name)",
        "CREATE INDEX ON :Drug(drugbank_id)",
        "CREATE INDEX ON :Gene(gene_name)",
        "CREATE INDEX ON :SideEffect(name)",
        "CREATE INDEX ON :Country(iso_code)",
        "CREATE INDEX ON :Country(name)",
        "CREATE INDEX ON :Region(code)",
        "CREATE INDEX ON :Region(who_code)",
        "CREATE INDEX ON :Disease(indicator_code)",
        "CREATE INDEX ON :Disease(name)",
        "CREATE INDEX ON :SocioeconomicIndicator(id)",
        "CREATE INDEX ON :EnvironmentalFactor(id)",
        "CREATE INDEX ON :NutritionIndicator(id)",
        "CREATE INDEX ON :DemographicProfile(id)",
        "CREATE INDEX ON :WaterResource(id)",
        "CREATE INDEX ON :EmergencyResponse(id)",
        "CREATE INDEX ON :HealthWorkforce(id)",
        "CREATE INDEX ON :VaccineCoverage(id)",
        // FAERS
        "CREATE INDEX ON :AdverseEventCase(case_id)",
        "CREATE INDEX ON :Reaction(preferred_term)",
        // UniProt
        "CREATE INDEX ON :Protein(uniprot_id)",
        "CREATE INDEX ON :Protein(gene_name)",
        "CREATE INDEX ON :Organism(name)",
        "CREATE INDEX ON :GOTerm(go_id)",
        // OMOP
        "CREATE INDEX ON :Person(person_id)",
        "CREATE INDEX ON :Visit(encounter_id)",
        "CREATE INDEX ON :ConditionOccurrence(snomed_code)",
        "CREATE INDEX ON :DrugExposure(rxnorm_code)",
        "CREATE INDEX ON :Measurement(loinc_code)",
    ];
    let mut idx_ok = 0;
    for idx in indexes {
        if client.query("default", idx).await.is_ok() {
            idx_ok += 1;
        }
    }
    eprintln!(
        "  {} indexes created in {:.1}s\n",
        idx_ok,
        idx_start.elapsed().as_secs_f64()
    );

    // ── Phase 4: Load and run queries ──
    let mut all_queries = Vec::new();
    for filename in &[
        "pubmed-queries.csv",
        "clinical-trials-queries.csv",
        "pathways-queries.csv",
        "drug-interactions-queries.csv",
        "cross-kg-queries.csv",
        "health-determinants-queries.csv",
        "health-systems-queries.csv",
        "public-health-cross-kg-queries.csv",
        "expanded-queries.csv",
        "uniprot-queries.csv",
        "faers-queries.csv",
        "omop-queries.csv",
        "mega-benchmark-queries.csv",
    ] {
        let path = queries_dir.join(filename);
        let queries = parse_csv_queries(&path);
        if !queries.is_empty() {
            eprintln!("Loaded {} queries from {}", queries.len(), filename);
        }
        all_queries.extend(queries);
    }

    eprintln!("\nRunning {} queries...\n", all_queries.len());
    println!("id,name,category,time_ms,rows,status,sample_result");

    let mut pass = 0;
    let mut pass_data_gap = 0;
    let mut empty = 0;
    let mut fail_count = 0;
    let mut errors = 0;

    for (id, name, category, cypher) in &all_queries {
        let t0 = Instant::now();
        let result = client.query("default", cypher).await;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(r) => {
                let rows = r.records.len();
                let (status, counted_pass) = classify(rows, oracle.get(id));
                let sample = r
                    .records
                    .first()
                    .map(|row| {
                        let vals: Vec<String> = row.iter().map(|v| format!("{}", v)).collect();
                        format!("[{}]", vals.join("; "))
                    })
                    .unwrap_or_else(|| "[]".to_string());
                let sample_esc = sample.replace('"', "\"\"");
                println!(
                    "{},{},{},{:.1},{},{},\"{}\"",
                    id, name, category, ms, rows, status, sample_esc
                );
                let tag = match status {
                    "pass" => "PASS",
                    "pass_data_gap" => "PASS*",
                    "fail_count" => "FAIL#",
                    _ => "EMPTY",
                };
                eprintln!(
                    "  {} {}: {} rows in {:.1}ms [{}]",
                    tag, id, rows, ms, name
                );
                match status {
                    "pass" => pass += 1,
                    "pass_data_gap" => pass_data_gap += 1,
                    "fail_count" => fail_count += 1,
                    _ => empty += 1,
                }
                let _ = counted_pass; // accounted for above
            }
            Err(e) => {
                let msg = format!("{}", e)
                    .replace('"', "'")
                    .chars()
                    .take(200)
                    .collect::<String>();
                println!("{},{},{},{:.1},0,error,\"{}\"", id, name, category, ms, msg);
                eprintln!(
                    "  ERROR {}: {} [{:.1}ms]",
                    id,
                    &msg[..msg.len().min(80)],
                    ms
                );
                errors += 1;
            }
        }
    }

    eprintln!("\n========================================");
    let total_pass = pass + pass_data_gap;
    eprintln!(
        "Results: {}/{} pass ({} full + {} data_gap), {} empty, {} fail_count, {} error",
        total_pass,
        all_queries.len(),
        pass,
        pass_data_gap,
        empty,
        fail_count,
        errors
    );
    eprintln!("Total time: {:.1}s", total_start.elapsed().as_secs_f64());
    eprintln!("========================================");

    Ok(())
}
