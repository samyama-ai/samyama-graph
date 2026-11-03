//! Drug Interactions & Pharmacogenomics KG data loading utilities.
//!
//! Loads DrugBank CC0, DGIdb, and SIDER data into GraphStore
//! at high speed using direct API calls (no Cypher parsing).
//!
//! Schema: 5 node labels (Drug, Gene, SideEffect, Indication), 4 edge types.
//! Data sources:
//!   - DrugBank CC0: https://go.drugbank.com/releases/latest
//!   - DGIdb: https://dgidb.org/downloads
//!   - SIDER: http://sideeffects.embl.de/download/

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

use samyama_sdk::{GraphStore, NodeId, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// LOAD RESULT
// ============================================================================

pub struct LoadResult {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub drug_nodes: usize,
    pub gene_nodes: usize,
    pub side_effect_nodes: usize,
    pub indication_nodes: usize,
    pub bioactivity_nodes: usize,
    pub adverse_event_nodes: usize,
    pub interaction_edges: usize,
    pub side_effect_edges: usize,
    pub indication_edges: usize,
    pub bioactivity_edges: usize,
    pub adverse_event_edges: usize,
}

// ============================================================================
// ID MAPS
// ============================================================================

struct IdMaps {
    drug: HashMap<String, NodeId>,          // drugbank_id -> NodeId
    gene: HashMap<String, NodeId>,          // gene_name -> NodeId
    side_effect: HashMap<String, NodeId>,   // meddra_id -> NodeId
    indication: HashMap<String, NodeId>,    // meddra_id -> NodeId
    bioactivity: HashMap<String, NodeId>,   // chembl_assay_id -> NodeId
    adverse_event: HashMap<String, NodeId>, // term -> NodeId
    // Name lookups
    drug_name_to_dbid: HashMap<String, String>,  // lowercase name -> drugbank_id
    // Edge dedup
    interaction_edges: HashSet<String>,
    side_effect_edges: HashSet<String>,
    indication_edges: HashSet<String>,
    bioactivity_edges: HashSet<String>,
    adverse_event_edges: HashSet<String>,
}

impl IdMaps {
    fn new() -> Self {
        Self {
            drug: HashMap::new(),
            bioactivity: HashMap::new(),
            adverse_event: HashMap::new(),
            gene: HashMap::new(),
            side_effect: HashMap::new(),
            indication: HashMap::new(),
            drug_name_to_dbid: HashMap::new(),
            interaction_edges: HashSet::new(),
            side_effect_edges: HashSet::new(),
            indication_edges: HashSet::new(),
            bioactivity_edges: HashSet::new(),
            adverse_event_edges: HashSet::new(),
        }
    }
}

// ============================================================================
// FORMATTING
// ============================================================================

pub fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        let mins = (secs / 60.0).floor() as u64;
        let rem = secs - (mins as f64 * 60.0);
        format!("{}m {:.1}s", mins, rem)
    }
}

fn clean_str(s: &str) -> String {
    s.replace('"', "").replace('\n', " ").replace('\r', "")
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

pub fn load_dataset(
    graph: &mut GraphStore,
    data_dir: &Path,
    phases: &[String],
) -> Result<LoadResult, Error> {
    let mut maps = IdMaps::new();
    let mut result = LoadResult {
        total_nodes: 0,
        total_edges: 0,
        drug_nodes: 0,
        gene_nodes: 0,
        side_effect_nodes: 0,
        indication_nodes: 0,
        bioactivity_nodes: 0,
        adverse_event_nodes: 0,
        interaction_edges: 0,
        side_effect_edges: 0,
        indication_edges: 0,
        bioactivity_edges: 0,
        adverse_event_edges: 0,
    };

    if phases.contains(&"drugbank_dgidb".to_string()) || phases.contains(&"all".to_string()) {
        let t = Instant::now();
        let (drugs, genes, edges) = load_drugbank_dgidb(graph, &mut maps, data_dir)?;
        result.drug_nodes = drugs;
        result.gene_nodes = genes;
        result.interaction_edges = edges;
        eprintln!(
            "  Phase 1 (DrugBank+DGIdb): {} drugs, {} genes, {} interactions [{}]",
            format_num(drugs), format_num(genes), format_num(edges),
            format_duration(t.elapsed())
        );
    }

    if phases.contains(&"sider".to_string()) || phases.contains(&"all".to_string()) {
        let t = Instant::now();
        let (se_nodes, se_edges, ind_nodes, ind_edges) =
            load_sider(graph, &mut maps, data_dir)?;
        result.side_effect_nodes = se_nodes;
        result.side_effect_edges = se_edges;
        result.indication_nodes = ind_nodes;
        result.indication_edges = ind_edges;
        eprintln!(
            "  Phase 2 (SIDER): {} side effects ({} edges), {} indications ({} edges) [{}]",
            format_num(se_nodes), format_num(se_edges),
            format_num(ind_nodes), format_num(ind_edges),
            format_duration(t.elapsed())
        );
    }

    if phases.contains(&"chembl".to_string()) || phases.contains(&"all".to_string()) {
        let t = Instant::now();
        let (bio_nodes, bio_edges, new_genes) =
            load_chembl(graph, &mut maps, data_dir)?;
        result.bioactivity_nodes = bio_nodes;
        result.bioactivity_edges = bio_edges;
        result.gene_nodes += new_genes;
        eprintln!(
            "  Phase 3 (ChEMBL): {} bioactivities, {} edges, {} new genes [{}]",
            format_num(bio_nodes), format_num(bio_edges), format_num(new_genes),
            format_duration(t.elapsed())
        );
    }

    if phases.contains(&"openfda".to_string()) || phases.contains(&"all".to_string()) {
        let t = Instant::now();
        let (ae_nodes, ae_edges) =
            load_openfda(graph, &mut maps, data_dir)?;
        result.adverse_event_nodes = ae_nodes;
        result.adverse_event_edges = ae_edges;
        eprintln!(
            "  Phase 4 (OpenFDA): {} adverse events, {} edges [{}]",
            format_num(ae_nodes), format_num(ae_edges),
            format_duration(t.elapsed())
        );
    }

    result.total_nodes = result.drug_nodes + result.gene_nodes
        + result.side_effect_nodes + result.indication_nodes
        + result.bioactivity_nodes + result.adverse_event_nodes;
    result.total_edges = result.interaction_edges
        + result.side_effect_edges + result.indication_edges
        + result.bioactivity_edges + result.adverse_event_edges;

    Ok(result)
}

// ============================================================================
// PHASE 1: DrugBank CC0 + DGIdb
// ============================================================================

fn load_drugbank_dgidb(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    data_dir: &Path,
) -> Result<(usize, usize, usize), Error> {
    // --- DrugBank vocabulary CSV ---
    let drugbank_path = data_dir.join("drugbank").join("drugbank_vocabulary.csv");
    let mut drug_count = 0;
    let mut synonym_count = 0;

    if drugbank_path.exists() {
        let file = File::open(&drugbank_path)?;
        let reader = BufReader::new(file);
        let mut first = true;
        let mut col_idx: HashMap<String, usize> = HashMap::new();

        for line in reader.lines() {
            let line = line?;
            if first {
                // Parse CSV header
                for (i, col) in line.split(',').enumerate() {
                    col_idx.insert(col.trim().to_string(), i);
                }
                first = false;
                continue;
            }
            let fields = parse_csv_line(&line);
            let dbid = field(&fields, &col_idx, "DrugBank ID");
            let name = field(&fields, &col_idx, "Common name");
            let cas = field(&fields, &col_idx, "CAS");
            let synonyms = field(&fields, &col_idx, "Synonyms");

            if dbid.is_empty() || name.is_empty() {
                continue;
            }
            if maps.drug.contains_key(&dbid) {
                continue;
            }

            let id = graph.create_node("Drug");
            if let Some(n) = graph.get_node_mut(id) {
                n.set_property("drugbank_id", PropertyValue::String(dbid.clone()));
                n.set_property("name", PropertyValue::String(clean_str(&name)));
                if !cas.is_empty() {
                    n.set_property("cas_number", PropertyValue::String(cas));
                }
            }
            // Index common name
            maps.drug_name_to_dbid.insert(name.to_lowercase(), dbid.clone());
            // Index ALL synonyms (pipe-delimited) for cross-source matching
            for syn in synonyms.split('|') {
                let syn = syn.trim();
                if !syn.is_empty() {
                    maps.drug_name_to_dbid.insert(syn.to_lowercase(), dbid.clone());
                    synonym_count += 1;
                }
            }
            maps.drug.insert(dbid, id);
            drug_count += 1;
        }
        eprintln!("    DrugBank: {} Drug nodes, {} synonyms indexed", format_num(drug_count), format_num(synonym_count));
    }

    // --- DGIdb interactions TSV ---
    let interactions_path = data_dir.join("dgidb").join("interactions.tsv");
    let mut gene_count = 0;
    let mut edge_count = 0;

    if interactions_path.exists() {
        let file = File::open(&interactions_path)?;
        let reader = BufReader::new(file);
        let mut first = true;
        let mut col_idx: HashMap<String, usize> = HashMap::new();

        for line in reader.lines() {
            let line = line?;
            if first {
                for (i, col) in line.split('\t').enumerate() {
                    col_idx.insert(col.trim().to_string(), i);
                }
                first = false;
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            let gene_name = tfield(&fields, &col_idx, "gene_name");
            let drug_name = tfield(&fields, &col_idx, "drug_name");
            let drug_claim = tfield(&fields, &col_idx, "drug_claim_name");
            let int_type = tfield(&fields, &col_idx, "interaction_type");
            let source = tfield(&fields, &col_idx, "interaction_source_db_name");

            if gene_name.is_empty() || drug_name.is_empty() {
                continue;
            }

            // Resolve drug — try drug_name, then drug_claim_name
            let dbid = maps.drug_name_to_dbid.get(&drug_name.to_lowercase())
                .or_else(|| maps.drug_name_to_dbid.get(&drug_claim.to_lowercase()))
                .cloned();
            let dbid = match dbid {
                Some(id) => id,
                None => continue,
            };
            let drug_node = match maps.drug.get(&dbid) {
                Some(&id) => id,
                None => continue,
            };

            // Create Gene if new
            let gene_node = if let Some(&id) = maps.gene.get(&gene_name) {
                id
            } else {
                let id = graph.create_node("Gene");
                if let Some(n) = graph.get_node_mut(id) {
                    n.set_property("gene_name", PropertyValue::String(gene_name.clone()));
                }
                maps.gene.insert(gene_name.clone(), id);
                gene_count += 1;
                id
            };

            // Create edge if not duplicate
            let edge_key = format!("{}|{}", dbid, gene_name);
            if maps.interaction_edges.contains(&edge_key) {
                continue;
            }
            maps.interaction_edges.insert(edge_key);

            let edge_id = graph.create_edge(drug_node, gene_node, "INTERACTS_WITH_GENE")
                .map_err(|e| format!("Edge creation failed: {}", e))?;
            if !int_type.is_empty() && int_type != "NULL" {
                graph.set_edge_property_sparse(edge_id, "interaction_type", PropertyValue::String(int_type));
            }
            if !source.is_empty() {
                graph.set_edge_property_sparse(edge_id, "source", PropertyValue::String(source));
            }
            edge_count += 1;
        }
        eprintln!("    DGIdb: {} Gene nodes, {} interaction edges",
            format_num(gene_count), format_num(edge_count));
    }

    Ok((drug_count, gene_count, edge_count))
}

// ============================================================================
// PHASE 2: SIDER
// ============================================================================

fn load_sider(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    data_dir: &Path,
) -> Result<(usize, usize, usize, usize), Error> {
    let sider_dir = data_dir.join("sider");

    // Load CID -> drug name mapping
    let mut cid_to_name: HashMap<String, String> = HashMap::new();
    let names_path = sider_dir.join("drug_names.tsv");
    if names_path.exists() {
        let file = File::open(&names_path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 2 {
                cid_to_name.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
            }
        }
        eprintln!("    SIDER drug_names: {} mappings", format_num(cid_to_name.len()));
    }

    // --- Side effects ---
    let se_path = sider_dir.join("meddra_all_se.tsv");
    let mut se_nodes = 0;
    let mut se_edges = 0;

    if se_path.exists() {
        let file = File::open(&se_path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 6 {
                continue;
            }
            // SIDER format: CID | CID_flat | UMLS_from | type | UMLS_se | se_name
            let cid = parts[0].trim();
            let meddra_id = parts[4].trim();
            let se_name = parts[5].trim();

            if meddra_id.is_empty() || se_name.is_empty() {
                continue;
            }

            // Resolve CID -> drug name -> drugbank_id -> NodeId
            let drug_name = match cid_to_name.get(cid) {
                Some(n) => n.clone(),
                None => continue,
            };
            let dbid = match maps.drug_name_to_dbid.get(&drug_name.to_lowercase()) {
                Some(id) => id.clone(),
                None => continue,
            };
            let drug_node = match maps.drug.get(&dbid) {
                Some(&id) => id,
                None => continue,
            };

            // Create SideEffect node if new
            let se_node = if let Some(&id) = maps.side_effect.get(meddra_id) {
                id
            } else {
                let id = graph.create_node("SideEffect");
                if let Some(n) = graph.get_node_mut(id) {
                    n.set_property("meddra_id", PropertyValue::String(meddra_id.to_string()));
                    n.set_property("name", PropertyValue::String(clean_str(se_name)));
                }
                maps.side_effect.insert(meddra_id.to_string(), id);
                se_nodes += 1;
                id
            };

            // Create edge if not duplicate
            let edge_key = format!("{}|{}", dbid, meddra_id);
            if maps.side_effect_edges.contains(&edge_key) {
                continue;
            }
            maps.side_effect_edges.insert(edge_key);

            let _ = graph.create_edge(drug_node, se_node, "HAS_SIDE_EFFECT");
            se_edges += 1;
        }
    }

    // --- Indications ---
    let ind_path = sider_dir.join("meddra_all_indications.tsv");
    let mut ind_nodes = 0;
    let mut ind_edges = 0;

    if ind_path.exists() {
        let file = File::open(&ind_path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 7 {
                continue;
            }
            let cid = parts[0].trim();
            let method = parts[2].trim();
            let meddra_id = parts[5].trim();
            let ind_name = parts[6].trim();

            if meddra_id.is_empty() || ind_name.is_empty() {
                continue;
            }

            let drug_name = match cid_to_name.get(cid) {
                Some(n) => n.clone(),
                None => continue,
            };
            let dbid = match maps.drug_name_to_dbid.get(&drug_name.to_lowercase()) {
                Some(id) => id.clone(),
                None => continue,
            };
            let drug_node = match maps.drug.get(&dbid) {
                Some(&id) => id,
                None => continue,
            };

            let ind_node = if let Some(&id) = maps.indication.get(meddra_id) {
                id
            } else {
                let id = graph.create_node("Indication");
                if let Some(n) = graph.get_node_mut(id) {
                    n.set_property("meddra_id", PropertyValue::String(meddra_id.to_string()));
                    n.set_property("name", PropertyValue::String(clean_str(ind_name)));
                }
                maps.indication.insert(meddra_id.to_string(), id);
                ind_nodes += 1;
                id
            };

            let edge_key = format!("{}|{}", dbid, meddra_id);
            if maps.indication_edges.contains(&edge_key) {
                continue;
            }
            maps.indication_edges.insert(edge_key);

            let edge_id = graph.create_edge(drug_node, ind_node, "HAS_INDICATION")
                .map_err(|e| format!("Edge creation failed: {}", e))?;
            if !method.is_empty() {
                graph.set_edge_property_sparse(edge_id, "method", PropertyValue::String(method.to_string()));
            }
            ind_edges += 1;
        }
    }

    Ok((se_nodes, se_edges, ind_nodes, ind_edges))
}

// ============================================================================
// PHASE 3: ChEMBL bioactivities
// ============================================================================

fn load_chembl(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    data_dir: &Path,
) -> Result<(usize, usize, usize), Error> {
    let chembl_path = data_dir.join("chembl").join("chembl_activities.tsv");
    if !chembl_path.exists() {
        eprintln!("    ChEMBL: skipped (no chembl_activities.tsv)");
        return Ok((0, 0, 0));
    }

    let file = File::open(&chembl_path)?;
    let reader = BufReader::new(file);
    let mut bio_nodes = 0usize;
    let mut bio_edges = 0usize;
    let mut new_genes = 0usize;
    let mut first = true;
    let mut col_idx: HashMap<String, usize> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        if first {
            for (i, col) in line.split('\t').enumerate() {
                col_idx.insert(col.trim().to_string(), i);
            }
            first = false;
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let chembl_id = tfield(&fields, &col_idx, "chembl_id");
        let assay_id = tfield(&fields, &col_idx, "chembl_assay_id");
        let assay_type = tfield(&fields, &col_idx, "assay_type");
        let std_type = tfield(&fields, &col_idx, "standard_type");
        let std_value = tfield(&fields, &col_idx, "standard_value");
        let std_units = tfield(&fields, &col_idx, "standard_units");
        let pchembl = tfield(&fields, &col_idx, "pchembl_value");
        let gene_name = tfield(&fields, &col_idx, "gene_name");
        let target_name = tfield(&fields, &col_idx, "target_name");

        if assay_id.is_empty() {
            continue;
        }

        // Create Bioactivity node if new
        if !maps.bioactivity.contains_key(&assay_id) {
            let nid = graph.create_node("Bioactivity");
            if let Some(n) = graph.get_node_mut(nid) {
                n.set_property("chembl_assay_id", PropertyValue::String(assay_id.clone()));
                if !assay_type.is_empty() {
                    n.set_property("assay_type", PropertyValue::String(assay_type.clone()));
                }
                if !std_type.is_empty() {
                    n.set_property("standard_type", PropertyValue::String(std_type.clone()));
                }
                if !std_value.is_empty() {
                    if let Ok(v) = std_value.parse::<f64>() {
                        n.set_property("standard_value", PropertyValue::Float(v));
                    }
                }
                if !std_units.is_empty() {
                    n.set_property("standard_units", PropertyValue::String(std_units.clone()));
                }
                if !pchembl.is_empty() {
                    if let Ok(v) = pchembl.parse::<f64>() {
                        n.set_property("pchembl_value", PropertyValue::Float(v));
                    }
                }
                if !target_name.is_empty() {
                    n.set_property("target_name", PropertyValue::String(target_name));
                }
            }
            maps.bioactivity.insert(assay_id.clone(), nid);
            bio_nodes += 1;
        }

        let bio_node = maps.bioactivity[&assay_id];

        // HAS_BIOACTIVITY edge (Drug -> Bioactivity) — resolve drug by chembl_id
        let dbid = maps.drug_name_to_dbid.get(&chembl_id.to_lowercase()).cloned();
        if let Some(ref dbid) = dbid {
            if let Some(&drug_node) = maps.drug.get(dbid) {
                let edge_key = format!("{}|{}", dbid, assay_id);
                if !maps.bioactivity_edges.contains(&edge_key) {
                    maps.bioactivity_edges.insert(edge_key);
                    let _ = graph.create_edge(drug_node, bio_node, "HAS_BIOACTIVITY");
                    bio_edges += 1;
                }
            }
        }

        // BIOACTIVITY_TARGET edge (Bioactivity -> Gene)
        if !gene_name.is_empty() {
            let gene_node = if let Some(&id) = maps.gene.get(&gene_name) {
                id
            } else {
                let id = graph.create_node("Gene");
                if let Some(n) = graph.get_node_mut(id) {
                    n.set_property("gene_name", PropertyValue::String(gene_name.clone()));
                }
                maps.gene.insert(gene_name.clone(), id);
                new_genes += 1;
                id
            };
            let bt_key = format!("{}|{}", assay_id, gene_name);
            if !maps.bioactivity_edges.contains(&bt_key) {
                maps.bioactivity_edges.insert(bt_key);
                let _ = graph.create_edge(bio_node, gene_node, "BIOACTIVITY_TARGET");
                bio_edges += 1;
            }
        }

        if bio_nodes % 100_000 == 0 && bio_nodes > 0 {
            eprint!("\r    ChEMBL: {} bioactivities, {} edges", format_num(bio_nodes), format_num(bio_edges));
        }
    }
    eprintln!("\r    ChEMBL: {} bioactivities, {} edges, {} new genes",
        format_num(bio_nodes), format_num(bio_edges), format_num(new_genes));

    Ok((bio_nodes, bio_edges, new_genes))
}

// ============================================================================
// PHASE 4: OpenFDA FAERS adverse events
// ============================================================================

fn load_openfda(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    data_dir: &Path,
) -> Result<(usize, usize), Error> {
    let openfda_path = data_dir.join("openfda").join("adverse_events.tsv");
    if !openfda_path.exists() {
        eprintln!("    OpenFDA: skipped (no adverse_events.tsv)");
        return Ok((0, 0));
    }

    let file = File::open(&openfda_path)?;
    let reader = BufReader::new(file);
    let mut ae_nodes = 0usize;
    let mut ae_edges = 0usize;
    let mut first = true;
    let mut col_idx: HashMap<String, usize> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        if first {
            for (i, col) in line.split('\t').enumerate() {
                col_idx.insert(col.trim().to_string(), i);
            }
            first = false;
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let dbid = tfield(&fields, &col_idx, "drugbank_id");
        let ae_term = tfield(&fields, &col_idx, "adverse_event_term");
        let count_str = tfield(&fields, &col_idx, "count");

        if dbid.is_empty() || ae_term.is_empty() {
            continue;
        }

        // Create AdverseEvent node if new
        let ae_key = ae_term.to_lowercase();
        let ae_node = if let Some(&id) = maps.adverse_event.get(&ae_key) {
            id
        } else {
            let id = graph.create_node("AdverseEvent");
            if let Some(n) = graph.get_node_mut(id) {
                n.set_property("term", PropertyValue::String(ae_term.clone()));
                n.set_property("source", PropertyValue::String("OpenFDA_FAERS".to_string()));
            }
            maps.adverse_event.insert(ae_key.clone(), id);
            ae_nodes += 1;
            id
        };

        // HAS_ADVERSE_EVENT edge (Drug -> AdverseEvent)
        if let Some(&drug_node) = maps.drug.get(&dbid) {
            let edge_key = format!("{}|{}", dbid, ae_key);
            if !maps.adverse_event_edges.contains(&edge_key) {
                maps.adverse_event_edges.insert(edge_key);
                let eid = graph.create_edge(drug_node, ae_node, "HAS_ADVERSE_EVENT")
                    .map_err(|e| format!("Edge creation failed: {}", e))?;
                if let Ok(count) = count_str.parse::<i64>() {
                    graph.set_edge_property_sparse(eid, "report_count", PropertyValue::Integer(count));
                }
                ae_edges += 1;
            }
        }
    }
    eprintln!("    OpenFDA: {} adverse events, {} edges", format_num(ae_nodes), format_num(ae_edges));

    Ok((ae_nodes, ae_edges))
}

// ============================================================================
// CSV/TSV HELPERS
// ============================================================================

/// Parse a CSV line respecting quoted fields with commas.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in line.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current.trim().to_string());
    fields
}

/// Get field from CSV parsed line by column name.
fn field(fields: &[String], col_idx: &HashMap<String, usize>, name: &str) -> String {
    col_idx
        .get(name)
        .and_then(|&i| fields.get(i))
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// Get field from TSV line by column name.
fn tfield(fields: &[&str], col_idx: &HashMap<String, usize>, name: &str) -> String {
    col_idx
        .get(name)
        .and_then(|&i| fields.get(i))
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}
