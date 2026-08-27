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
//!
//! `--queries` takes a directory (every `.csv` in it) or a single `.csv`, and
//! may be repeated. Every file considered is reported, including the ones that
//! yielded nothing and why.

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

/// What one CSV file contributed to the suite — including what it did *not*.
///
/// A benchmark that silently runs a subset reports a denominator that does not
/// exist (#683). An 81-query suite reported as 56 because one file was not on a
/// hardcoded list, and as 76 because rows split across physical lines by CSV
/// quoting failed a content sniff. Every path that drops input now names it.
#[derive(Debug, Default, PartialEq)]
struct LoadReport {
    loaded: usize,
    /// Records whose Cypher column held nothing that looks like a query.
    skipped_no_cypher: usize,
    /// Records with fewer fields than the header.
    skipped_short: usize,
    /// Set when the file could not be read or held no header.
    unreadable: Option<String>,
}

impl LoadReport {
    /// One line per file *considered*, never silence.
    fn describe(&self, filename: &str) -> String {
        if let Some(why) = &self.unreadable {
            return format!("  {filename}: not read ({why})");
        }
        let mut line = format!("  {filename}: {} queries", self.loaded);
        let mut skips = Vec::new();
        if self.skipped_no_cypher > 0 {
            skips.push(format!("{} rows skipped: no MATCH/RETURN", self.skipped_no_cypher));
        }
        if self.skipped_short > 0 {
            skips.push(format!("{} rows skipped: fewer fields than the header", self.skipped_short));
        }
        if !skips.is_empty() {
            line.push_str(&format!(" ({})", skips.join(", ")));
        }
        line
    }
}

/// Split CSV text into records, honouring quoted fields.
///
/// The previous parser worked line by line, so a field containing a newline —
/// which is how a multi-line Cypher query is written in CSV — became two
/// malformed rows and vanished. A quoted `"` is written `""`.
fn csv_rows(text: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    in_quotes = false;
                }
            }
            '"' if field.is_empty() => in_quotes = true,
            ',' if !in_quotes => row.push(std::mem::take(&mut field)),
            '\n' if !in_quotes => {
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            '\r' if !in_quotes => {}
            other => field.push(other),
        }
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    rows
}

/// Index of the first header field named by `names`, else `fallback`.
fn column_index(header: &[String], names: &[&str], fallback: usize) -> usize {
    header
        .iter()
        .position(|h| names.contains(&h.trim().trim_matches('"').to_ascii_lowercase().as_str()))
        .unwrap_or(fallback)
}

fn parse_csv_queries(path: &std::path::Path) -> (Vec<(String, String, String, String)>, LoadReport) {
    let text = match fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            return (
                vec![],
                LoadReport { unreadable: Some(e.to_string()), ..Default::default() },
            )
        }
    };
    let mut records = csv_rows(&text).into_iter();
    let header = match records.next() {
        Some(h) => h,
        None => {
            return (
                vec![],
                LoadReport { unreadable: Some("empty file".into()), ..Default::default() },
            )
        }
    };

    let id_col = column_index(&header, &["id", "query_id"], 0);
    let name_col = column_index(&header, &["name", "title"], 1);
    let cat_col = column_index(&header, &["category", "kg", "suite"], 2);
    // Every suite we ship names the column. An unnamed one has always been last.
    let cypher_col = column_index(
        &header,
        &["cypher", "query", "query_text", "statement"],
        header.len().saturating_sub(1),
    );
    let widest = [id_col, name_col, cat_col, cypher_col].into_iter().max().unwrap_or(0);

    let mut queries = Vec::new();
    let mut report = LoadReport::default();
    for record in records {
        if record.iter().all(|f| f.trim().is_empty()) {
            continue;
        }
        if record.len() <= widest {
            report.skipped_short += 1;
            continue;
        }
        let cypher = record[cypher_col].trim().to_string();
        if !(cypher.contains("MATCH") || cypher.contains("RETURN")) {
            report.skipped_no_cypher += 1;
            continue;
        }
        queries.push((
            record[id_col].trim().to_string(),
            record[name_col].trim().to_string(),
            record[cat_col].trim().to_string(),
            cypher,
        ));
    }
    report.loaded = queries.len();
    (queries, report)
}

/// Every `.csv` in `dir`, sorted — plus `dir` itself if it names one file.
///
/// The list used to be hardcoded, so a query file that was present but unnamed
/// was skipped without a word (#683).
fn query_files(dir: &std::path::Path) -> Result<Vec<PathBuf>, String> {
    if dir.extension().is_some_and(|e| e.eq_ignore_ascii_case("csv")) {
        return Ok(vec![dir.to_path_buf()]);
    }
    let entries = fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e.eq_ignore_ascii_case("csv")))
        .collect();
    files.sort();
    Ok(files)
}

/// The value following `flag`, or an error naming the flag.
///
/// `args[p + 1]` was unchecked, so a value-taking flag passed last panicked with
/// `index out of bounds` instead of saying which flag was short of a value.
fn arg_value<'a>(args: &'a [String], flag: &str) -> Result<Option<&'a String>, String> {
    let Some(pos) = args.iter().position(|a| a == flag) else {
        return Ok(None);
    };
    match args.get(pos + 1) {
        Some(v) if !v.starts_with("--") => Ok(Some(v)),
        _ => Err(format!("{flag} needs a value")),
    }
}

/// Every value given for `flag`, in command-line order.
///
/// `--queries` may name a directory or a single `.csv`, and may be repeated —
/// a suite whose files live in more than one directory is one suite, and the
/// count it reports has to be the count it ran (#683).
fn arg_values<'a>(args: &'a [String], flag: &str) -> Result<Vec<&'a String>, String> {
    let mut out = Vec::new();
    for (pos, _) in args.iter().enumerate().filter(|(_, a)| *a == flag) {
        match args.get(pos + 1) {
            Some(v) if !v.starts_with("--") => out.push(v),
            _ => return Err(format!("{flag} needs a value")),
        }
    }
    Ok(out)
}

fn get_arg(args: &[String], flag: &str) -> Option<PathBuf> {
    match arg_value(args, flag) {
        Ok(v) => v.map(PathBuf::from),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    }
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
    let clinvar_snap = get_arg(&args, "--clinvar-snap");
    let chembl_snap = get_arg(&args, "--chembl-snap");
    let opentargets_snap = get_arg(&args, "--opentargets-snap");
    let hpo_snap = get_arg(&args, "--hpo-snap");
    let mondo_snap = get_arg(&args, "--mondo-snap");
    let combined_snapshot = get_arg(&args, "--combined-snapshot");
    let skip_queries = args.iter().any(|a| a == "--skip-queries");
    // Optional cross-KG entity dedup (ADR-018): merge nodes sharing any of these
    // property keys within a label (e.g. Disease by mondo_id across ClinVar/OpenTargets/MONDO).
    let dedup_keys_str: Option<String> = args
        .iter()
        .position(|a| a == "--dedup-keys")
        .and_then(|i| args.get(i + 1))
        .cloned();
    let dedup_keys: Vec<&str> = dedup_keys_str
        .as_deref()
        .map(|s| s.split(',').map(|k| k.trim()).filter(|k| !k.is_empty()).collect())
        .unwrap_or_default();
    let di_data = get_arg(&args, "--di-data");
    let surv_data = get_arg(&args, "--surv-data");
    let hd_data = get_arg(&args, "--hd-data");
    let hs_data = get_arg(&args, "--hs-data");
    let study_refs = get_arg(&args, "--study-refs");
    let query_paths: Vec<PathBuf> = match arg_values(&args, "--queries") {
        Ok(v) if v.is_empty() => vec![PathBuf::from(".")],
        Ok(v) => v.into_iter().map(PathBuf::from).collect(),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };
    let oracle_path = get_arg(&args, "--oracle").unwrap_or_else(|| {
        let first = &query_paths[0];
        if first.is_dir() { first.join("expected_rows.yaml") } else { PathBuf::from("expected_rows.yaml") }
    });
    let oracle = load_oracle(&oracle_path);

    let client = EmbeddedClient::new();
    let total_start = Instant::now();

    // ── Phase 1: Import cross-KG entity sources FIRST, with dedup ──
    // Ordered small -> large deliberately. `import_tenant_with_dedup` rebuilds its
    // index by scanning `store.all_nodes()` on EVERY import, so deduping while the
    // store is still small keeps total scan cost ~65M node visits instead of ~2.5B
    // (which is what deduping after the 207M-node bulk load would cost). See #316.
    for (name, path) in &[
        ("HPO", &hpo_snap),
        ("MONDO", &mondo_snap),
        ("Health Systems", &hs_snap),
        ("Pathways", &pw_snap),
        ("Health Determinants", &hd_snap),
        ("Surveillance", &surv_snap),
        ("Drug Interactions", &di_snap),
        ("UniProt", &uniprot_snap),
        ("OpenTargets", &opentargets_snap),
        ("ChEMBL", &chembl_snap),
        ("Clinical Trials", &ct_snap),
        ("FAERS", &faers_snap),
        ("ClinVar", &clinvar_snap),
    ] {
        if let Some(ref p) = path {
            if dedup_keys.is_empty() {
                eprint!("Importing {} snapshot... ", name);
            } else {
                eprint!("Importing {} snapshot (dedup)... ", name);
            }
            let t0 = Instant::now();
            let stats = if dedup_keys.is_empty() {
                client.import_snapshot("default", p).await?
            } else {
                client.import_snapshot_dedup("default", p, &dedup_keys).await?
            };
            eprintln!(
                "{} nodes, {} edges, {} MERGED in {:.1}s",
                stats.node_count,
                stats.edge_count,
                stats.merged_count,
                t0.elapsed().as_secs_f64()
            );
        }
    }

    // ── Phase 2: Import bulk sources LAST, plain ──
    // Per the .sgsnap header inventory, PubMed (Article/Author/Chemical/Grant/
    // Journal/MeSHTerm) and OMOP (Person/Visit/ConditionOccurrence/DrugExposure/
    // Measurement/ProcedureOccurrence) share NO label with any other snapshot, so
    // they can never merge — running them through dedup is pure cost.
    for (name, path) in &[
        ("PubMed", &pubmed_snap),
        ("OMOP", &omop_snap),
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

    // ── Combined-snapshot export (optional) ──
    if let Some(ref cs) = combined_snapshot {
        eprintln!("Exporting combined snapshot to {} ...", cs.display());
        let t0 = Instant::now();
        client.export_snapshot("default", cs).await?;
        eprintln!("Combined snapshot exported in {:.1}s", t0.elapsed().as_secs_f64());
    }
    if skip_queries {
        eprintln!("--skip-queries set; skipping query phase.");
        return Ok(());
    }

    // ── Phase 4: Load and run queries ──
    let mut all_queries = Vec::new();
    let mut files: Vec<PathBuf> = Vec::new();
    for dir in &query_paths {
        match query_files(dir) {
            Ok(f) => {
                if f.is_empty() {
                    eprintln!("No .csv query files in {}", dir.display());
                }
                files.extend(f);
            }
            Err(e) => {
                eprintln!("error: --queries {e}");
                std::process::exit(2);
            }
        }
    }
    if !files.is_empty() {
        eprintln!("Query files considered:");
    }
    let mut skipped_rows = 0usize;
    for path in &files {
        let filename = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        let (queries, report) = parse_csv_queries(path);
        eprintln!("{}", report.describe(&filename));
        skipped_rows += report.skipped_no_cypher + report.skipped_short;
        all_queries.extend(queries);
    }
    eprintln!(
        "{} file{} considered, {} queries loaded, {} rows skipped",
        files.len(),
        if files.len() == 1 { "" } else { "s" },
        all_queries.len(),
        skipped_rows
    );

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(name: &str, body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        (dir, path)
    }

    /// A Cypher query written across several physical lines is one CSV record.
    ///
    /// The old parser read line by line, so the continuation lines failed the
    /// `contains("MATCH")` sniff and the row disappeared without a word — this
    /// is how an 81-row suite reported as 76 (#683).
    #[test]
    fn a_query_split_across_lines_is_one_query_not_zero() {
        let (_d, path) = write(
            "q.csv",
            "id,name,category,cypher\n\
             Q1,Split,cross,\"MATCH (a:Person)\n-[:KNOWS]->(b)\nRETURN a, b\"\n",
        );
        let (queries, report) = parse_csv_queries(&path);
        assert_eq!(report.loaded, 1);
        assert_eq!(report.skipped_no_cypher, 0);
        assert!(queries[0].3.contains("KNOWS"), "{:?}", queries[0].3);
        assert!(queries[0].3.contains("RETURN a, b"), "{:?}", queries[0].3);
    }

    /// A comma inside a quoted field does not end the field.
    #[test]
    fn a_quoted_comma_does_not_split_a_field() {
        let (_d, path) = write(
            "q.csv",
            "id,name,category,cypher\nQ1,N,c,\"MATCH (a) RETURN a.x, a.y\"\n",
        );
        let (queries, _) = parse_csv_queries(&path);
        assert_eq!(queries[0].3, "MATCH (a) RETURN a.x, a.y");
    }

    /// Dropping a row is allowed. Dropping it quietly is not.
    #[test]
    fn a_row_that_is_not_a_query_is_counted_and_named() {
        let (_d, path) = write(
            "q.csv",
            "id,name,category,cypher\n\
             Q1,Good,c,\"MATCH (a) RETURN a\"\n\
             Q2,Prose,c,\"see the design note\"\n",
        );
        let (queries, report) = parse_csv_queries(&path);
        assert_eq!((report.loaded, report.skipped_no_cypher), (1, 1));
        assert_eq!(queries.len(), 1);
        assert!(
            report.describe("q.csv").contains("1 rows skipped: no MATCH/RETURN"),
            "{}",
            report.describe("q.csv")
        );
    }

    /// The Cypher column is found by name, not by counting commas.
    #[test]
    fn the_cypher_column_is_located_by_header_name() {
        let (_d, path) = write(
            "q.csv",
            "id,name,category,cypher,notes,owner\n\
             Q1,N,c,\"MATCH (a) RETURN a\",slow,ml\n",
        );
        let (queries, report) = parse_csv_queries(&path);
        assert_eq!(report.loaded, 1);
        assert_eq!(queries[0].3, "MATCH (a) RETURN a");
    }

    /// A file that cannot be read is reported, not treated as empty.
    #[test]
    fn an_unreadable_file_says_so() {
        let report = parse_csv_queries(std::path::Path::new("/nonexistent/q.csv")).1;
        assert!(report.unreadable.is_some());
        assert!(report.describe("q.csv").contains("not read"), "{}", report.describe("q.csv"));
    }

    /// Every `.csv` in the directory is considered — the list is not hardcoded.
    #[test]
    fn every_csv_in_the_directory_is_considered() {
        let dir = tempfile::tempdir().unwrap();
        for n in ["b-queries.csv", "a-queries.csv", "notes.md"] {
            fs::write(dir.path().join(n), "id,name,category,cypher\n").unwrap();
        }
        let files = query_files(dir.path()).unwrap();
        let names: Vec<String> =
            files.iter().map(|p| p.file_name().unwrap().to_string_lossy().to_string()).collect();
        assert_eq!(names, vec!["a-queries.csv", "b-queries.csv"]);
    }

    /// `--queries` may also name a single file.
    #[test]
    fn a_single_csv_path_is_accepted() {
        let (_d, path) = write("q.csv", "id,name,category,cypher\n");
        assert_eq!(query_files(&path).unwrap(), vec![path]);
    }

    /// `--queries` may be repeated: one suite, several directories.
    #[test]
    fn queries_may_be_given_more_than_once() {
        let args: Vec<String> = ["prog", "--queries", "a", "--queries", "b/x.csv"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(arg_values(&args, "--queries").unwrap(), vec!["a", "b/x.csv"]);
        assert!(arg_values(&args, "--absent").unwrap().is_empty());

        let short: Vec<String> =
            ["prog", "--queries", "a", "--queries"].iter().map(|s| s.to_string()).collect();
        assert!(arg_values(&short, "--queries").is_err());
    }

    /// A value-taking flag passed last used to panic with `index out of bounds`.
    #[test]
    fn a_flag_without_a_value_is_an_error_not_a_panic() {
        let args: Vec<String> = ["prog", "--study-refs"].iter().map(|s| s.to_string()).collect();
        assert_eq!(arg_value(&args, "--study-refs"), Err("--study-refs needs a value".into()));

        let args: Vec<String> =
            ["prog", "--study-refs", "--queries", "d"].iter().map(|s| s.to_string()).collect();
        assert!(arg_value(&args, "--study-refs").is_err());
        assert_eq!(arg_value(&args, "--queries").unwrap().unwrap(), "d");
        assert_eq!(arg_value(&args, "--absent").unwrap(), None);
    }
}
