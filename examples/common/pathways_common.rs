//! Biological Pathways KG data loading utilities.
//!
//! Loads Reactome, STRING, and Gene Ontology data into GraphStore
//! at high speed using direct API calls (no Cypher parsing).
//!
//! Schema: 8 node labels, 10 edge types.
//! Data sources:
//!   - Reactome: https://reactome.org/download-data
//!   - STRING: https://string-db.org/cgi/download
//!   - Gene Ontology: http://geneontology.org/docs/downloads/

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
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
}

// ============================================================================
// PHASE COUNTS
// ============================================================================

struct PhaseCounts {
    nodes: usize,
    edges: usize,
}

impl PhaseCounts {
    fn new() -> Self {
        Self { nodes: 0, edges: 0 }
    }
}

// ============================================================================
// ID MAPPINGS (dedup tracking)
// ============================================================================

pub struct IdMaps {
    // Node dedup maps
    pub pathway: HashMap<String, NodeId>,
    pub protein: HashMap<String, NodeId>,
    pub gene: HashMap<String, NodeId>,
    pub reaction: HashMap<String, NodeId>,
    pub complex: HashMap<String, NodeId>,
    pub compound: HashMap<String, NodeId>,
    pub disease: HashMap<String, NodeId>,
    pub go_term: HashMap<String, NodeId>,
    pub drug: HashMap<String, NodeId>,
    // Edge dedup sets
    pub protein_pathway: HashSet<String>,
    pub interacts_with: HashSet<String>,
    pub annotated_with: HashSet<String>,
    pub encodes: HashSet<String>,
    // Reverse lookup
    pub gene_name_to_uid: HashMap<String, String>,
}

impl IdMaps {
    pub fn new() -> Self {
        Self {
            pathway: HashMap::new(),
            protein: HashMap::new(),
            gene: HashMap::new(),
            reaction: HashMap::new(),
            complex: HashMap::new(),
            compound: HashMap::new(),
            disease: HashMap::new(),
            go_term: HashMap::new(),
            drug: HashMap::new(),
            protein_pathway: HashSet::new(),
            interacts_with: HashSet::new(),
            annotated_with: HashSet::new(),
            encodes: HashSet::new(),
            gene_name_to_uid: HashMap::new(),
        }
    }
}

// ============================================================================
// FORMATTING HELPERS
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

// ============================================================================
// STRING HELPERS
// ============================================================================

fn clean_str(s: &str) -> String {
    s.replace('"', "").replace('\n', " ").replace('\r', "")
}

// ============================================================================
// NODE CREATION HELPERS
// ============================================================================

fn get_or_create_pathway(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    pathway_id: &str,
    name: &str,
    organism: &str,
    counts: &mut PhaseCounts,
) -> NodeId {
    if let Some(&id) = maps.pathway.get(pathway_id) {
        return id;
    }
    let id = graph.create_node("Pathway");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("pathway_id", PropertyValue::String(pathway_id.to_string()));
        if !name.is_empty() {
            n.set_property("name", PropertyValue::String(clean_str(name)));
        }
        if !organism.is_empty() {
            n.set_property("organism", PropertyValue::String(organism.to_string()));
        }
    }
    maps.pathway.insert(pathway_id.to_string(), id);
    counts.nodes += 1;
    id
}

fn get_or_create_protein(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    uniprot_id: &str,
    name: &str,
    counts: &mut PhaseCounts,
) -> NodeId {
    if let Some(&id) = maps.protein.get(uniprot_id) {
        // Update name if previously created without one
        if !name.is_empty() {
            if let Some(n) = graph.get_node_mut(id) {
                if n.get_property("name").is_none() {
                    n.set_property("name", PropertyValue::String(clean_str(name)));
                }
            }
        }
        return id;
    }
    let id = graph.create_node("Protein");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("uniprot_id", PropertyValue::String(uniprot_id.to_string()));
        if !name.is_empty() {
            n.set_property("name", PropertyValue::String(clean_str(name)));
        }
    }
    maps.protein.insert(uniprot_id.to_string(), id);
    counts.nodes += 1;
    id
}

fn get_or_create_reaction(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    reaction_id: &str,
    name: &str,
    reaction_type: &str,
    counts: &mut PhaseCounts,
) -> NodeId {
    if let Some(&id) = maps.reaction.get(reaction_id) {
        return id;
    }
    let id = graph.create_node("Reaction");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("reaction_id", PropertyValue::String(reaction_id.to_string()));
        if !name.is_empty() {
            n.set_property("name", PropertyValue::String(clean_str(name)));
        }
        if !reaction_type.is_empty() {
            n.set_property("reaction_type", PropertyValue::String(reaction_type.to_string()));
        }
    }
    maps.reaction.insert(reaction_id.to_string(), id);
    counts.nodes += 1;
    id
}

fn get_or_create_complex(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    complex_id: &str,
    name: &str,
    counts: &mut PhaseCounts,
) -> NodeId {
    if let Some(&id) = maps.complex.get(complex_id) {
        return id;
    }
    let id = graph.create_node("Complex");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("complex_id", PropertyValue::String(complex_id.to_string()));
        if !name.is_empty() {
            n.set_property("name", PropertyValue::String(clean_str(name)));
        }
    }
    maps.complex.insert(complex_id.to_string(), id);
    counts.nodes += 1;
    id
}

fn get_or_create_go_term(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    go_id: &str,
    name: &str,
    namespace: &str,
    definition: &str,
    counts: &mut PhaseCounts,
) -> NodeId {
    if let Some(&id) = maps.go_term.get(go_id) {
        return id;
    }
    let id = graph.create_node("GOTerm");
    if let Some(n) = graph.get_node_mut(id) {
        n.set_property("go_id", PropertyValue::String(go_id.to_string()));
        if !name.is_empty() {
            n.set_property("name", PropertyValue::String(clean_str(name)));
        }
        if !namespace.is_empty() {
            n.set_property("namespace", PropertyValue::String(namespace.to_string()));
        }
        if !definition.is_empty() {
            // Truncate long definitions
            let def = if definition.len() > 500 {
                let mut end = 500;
                while end > 0 && !definition.is_char_boundary(end) {
                    end -= 1;
                }
                &definition[..end]
            } else {
                definition
            };
            n.set_property("definition", PropertyValue::String(clean_str(def)));
        }
    }
    maps.go_term.insert(go_id.to_string(), id);
    counts.nodes += 1;
    id
}

// ============================================================================
// PROGRESS REPORTING
// ============================================================================

fn report_progress(label: &str, count: usize, t0: &Instant, is_tty: bool) {
    if count % 10_000 == 0 && count > 0 {
        let elapsed = t0.elapsed().as_secs_f64();
        let rate = count as f64 / elapsed;
        if is_tty {
            eprint!("\r");
        }
        eprint!(
            "  [{}] {} items ({:.0}/s, {:.1}s)",
            label,
            format_num(count),
            rate,
            elapsed,
        );
        if is_tty {
            eprint!("          ");
        } else {
            eprintln!();
        }
        io::stderr().flush().ok();
    }
}

// ============================================================================
// PHASE 1: REACTOME
// ============================================================================

fn load_reactome(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    data_dir: &Path,
    counts: &mut PhaseCounts,
) -> Result<(), Error> {
    let reactome_dir = data_dir.join("reactome");
    if !reactome_dir.exists() {
        eprintln!("  WARNING: reactome/ directory not found, skipping");
        return Ok(());
    }

    let is_tty = io::stderr().is_terminal();
    let t0 = Instant::now();

    // ------------------------------------------------------------------
    // 1. ReactomePathways.txt — Pathway nodes (human only)
    // ------------------------------------------------------------------
    let pathways_file = reactome_dir.join("ReactomePathways.txt");
    if pathways_file.exists() {
        eprintln!("  Loading ReactomePathways.txt...");
        let file = File::open(&pathways_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut pathway_count = 0usize;

        for line_result in reader.lines() {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 3 {
                continue;
            }
            let reactome_id = fields[0].trim();
            let name = fields[1].trim();
            let organism = fields[2].trim();

            if organism != "Homo sapiens" {
                continue;
            }

            get_or_create_pathway(graph, maps, reactome_id, name, organism, counts);
            pathway_count += 1;
            report_progress("Pathways", pathway_count, &t0, is_tty);
        }
        if is_tty {
            eprintln!();
        }
        eprintln!("    Pathways: {}", format_num(pathway_count));
    } else {
        eprintln!("  WARNING: ReactomePathways.txt not found");
    }

    // ------------------------------------------------------------------
    // 2. ReactomePathwaysRelation.txt — CHILD_OF edges (child -> parent)
    // ------------------------------------------------------------------
    let relations_file = reactome_dir.join("ReactomePathwaysRelation.txt");
    if relations_file.exists() {
        eprintln!("  Loading ReactomePathwaysRelation.txt...");
        let file = File::open(&relations_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut rel_count = 0usize;

        for line_result in reader.lines() {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 2 {
                continue;
            }
            let parent_id = fields[0].trim();
            let child_id = fields[1].trim();

            // Only create edges between known human pathways
            let parent_nid = maps.pathway.get(parent_id).copied();
            let child_nid = maps.pathway.get(child_id).copied();

            if let (Some(p_nid), Some(c_nid)) = (parent_nid, child_nid) {
                let _ = graph.create_edge(c_nid, p_nid, "CHILD_OF");
                counts.edges += 1;
                rel_count += 1;
                report_progress("PathwayRels", rel_count, &t0, is_tty);
            }
        }
        if is_tty {
            eprintln!();
        }
        eprintln!("    CHILD_OF edges: {}", format_num(rel_count));
    } else {
        eprintln!("  WARNING: ReactomePathwaysRelation.txt not found");
    }

    // ------------------------------------------------------------------
    // 3. UniProt2Reactome_All_Levels.txt — Protein nodes + PARTICIPATES_IN edges
    //    Cols: uniprot_id, reactome_id, url, name, evidence, organism
    // ------------------------------------------------------------------
    let uniprot_file = reactome_dir.join("UniProt2Reactome_All_Levels.txt");
    if uniprot_file.exists() {
        eprintln!("  Loading UniProt2Reactome_All_Levels.txt...");
        let file = File::open(&uniprot_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut participates_count = 0usize;
        let mut line_count = 0usize;

        for line_result in reader.lines() {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 6 {
                continue;
            }
            let uniprot_id = fields[0].trim();
            let reactome_id = fields[1].trim();
            // fields[2] = url
            let pathway_name = fields[3].trim();
            let evidence = fields[4].trim();
            let organism = fields[5].trim();

            if organism != "Homo sapiens" {
                continue;
            }

            // Create/get protein node (deduped)
            let protein_nid = get_or_create_protein(graph, maps, uniprot_id, "", counts);

            // Create/get pathway node (may already exist from step 1)
            let pathway_nid = get_or_create_pathway(
                graph, maps, reactome_id, pathway_name, "Homo sapiens", counts,
            );

            // Edge dedup: "uid|pwid"
            let dedup_key = format!("{}|{}", uniprot_id, reactome_id);
            if !maps.protein_pathway.contains(&dedup_key) {
                if !evidence.is_empty() {
                    let mut props = samyama_sdk::PropertyMap::new();
                    props.insert(
                        "evidence".to_string(),
                        PropertyValue::String(evidence.to_string()),
                    );
                    let _ = graph.create_edge_with_properties(
                        protein_nid,
                        pathway_nid,
                        "PARTICIPATES_IN",
                        props,
                    );
                } else {
                    let _ = graph.create_edge(protein_nid, pathway_nid, "PARTICIPATES_IN");
                }
                counts.edges += 1;
                participates_count += 1;
                maps.protein_pathway.insert(dedup_key);
            }

            line_count += 1;
            report_progress("UniProt2Reactome", line_count, &t0, is_tty);
        }
        if is_tty {
            eprintln!();
        }
        eprintln!(
            "    Proteins: {}, PARTICIPATES_IN edges: {}",
            format_num(maps.protein.len()),
            format_num(participates_count),
        );
    } else {
        eprintln!("  WARNING: UniProt2Reactome_All_Levels.txt not found");
    }

    // ------------------------------------------------------------------
    // 4. reactome.homo_sapiens.interactions.tab-delimited.txt — Reaction nodes + CATALYZES edges
    //    Header row (skip). Cols: 0=uniprot1, 2=name1, 4=uniprot2, 6=name2, 7=type, last=reactome_id
    // ------------------------------------------------------------------
    let interactions_file =
        reactome_dir.join("reactome.homo_sapiens.interactions.tab-delimited.txt");
    if interactions_file.exists() {
        eprintln!("  Loading reactome interactions...");
        let file = File::open(&interactions_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut lines = reader.lines();

        // Skip header
        let _header = lines.next();

        let mut reaction_count = 0usize;
        let mut catalyzes_count = 0usize;
        let mut line_count = 0usize;

        for line_result in lines {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 8 {
                continue;
            }

            let uniprot1 = fields[0].trim();
            let name1 = fields.get(2).map(|s| s.trim()).unwrap_or("");
            let uniprot2 = fields.get(4).map(|s| s.trim()).unwrap_or("");
            let name2 = fields.get(6).map(|s| s.trim()).unwrap_or("");
            let reaction_type = fields[7].trim();
            let reactome_id = fields.last().map(|s| s.trim()).unwrap_or("");

            // Skip if UniProt IDs look invalid
            if uniprot1.is_empty() || !uniprot1.chars().next().unwrap_or(' ').is_alphanumeric() {
                continue;
            }

            // Create/get protein nodes
            let prot1_nid = get_or_create_protein(graph, maps, uniprot1, name1, counts);
            if !uniprot2.is_empty()
                && uniprot2.chars().next().unwrap_or(' ').is_alphanumeric()
            {
                let _prot2_nid = get_or_create_protein(graph, maps, uniprot2, name2, counts);
            }

            // Create/get reaction node
            if !reactome_id.is_empty() {
                let reaction_nid = get_or_create_reaction(
                    graph,
                    maps,
                    reactome_id,
                    "",
                    reaction_type,
                    counts,
                );
                if maps.reaction.len() > reaction_count {
                    reaction_count = maps.reaction.len();
                }

                // CATALYZES: protein1 -> reaction
                let _ = graph.create_edge(prot1_nid, reaction_nid, "CATALYZES");
                counts.edges += 1;
                catalyzes_count += 1;
            }

            line_count += 1;
            report_progress("Interactions", line_count, &t0, is_tty);
        }
        if is_tty {
            eprintln!();
        }
        eprintln!(
            "    Reactions: {}, CATALYZES edges: {}",
            format_num(maps.reaction.len()),
            format_num(catalyzes_count),
        );
    } else {
        eprintln!("  WARNING: reactome interactions file not found");
    }

    // ------------------------------------------------------------------
    // 5. ComplexParticipantsPubMedIdentifiers_human.txt — Complex nodes + COMPONENT_OF edges
    //    Header row (skip). Cols: 0=complex_id, 1=name, 2=participant_uids (pipe-sep)
    // ------------------------------------------------------------------
    let complex_file = reactome_dir.join("ComplexParticipantsPubMedIdentifiers_human.txt");
    if complex_file.exists() {
        eprintln!("  Loading complexes...");
        let file = File::open(&complex_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut lines = reader.lines();

        // Skip header
        let _header = lines.next();

        let mut component_count = 0usize;
        let mut line_count = 0usize;

        for line_result in lines {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 3 {
                continue;
            }

            let complex_id = fields[0].trim();
            let name = fields[1].trim();
            let participants = fields[2].trim();

            if complex_id.is_empty() {
                continue;
            }

            let complex_nid = get_or_create_complex(graph, maps, complex_id, name, counts);

            // Participants are pipe-separated UniProt IDs
            for uid in participants.split('|') {
                let uid = uid.trim();
                if uid.is_empty() {
                    continue;
                }
                // Only link to proteins we already know about
                if let Some(&protein_nid) = maps.protein.get(uid) {
                    let _ = graph.create_edge(protein_nid, complex_nid, "COMPONENT_OF");
                    counts.edges += 1;
                    component_count += 1;
                }
            }

            line_count += 1;
            report_progress("Complexes", line_count, &t0, is_tty);
        }
        if is_tty {
            eprintln!();
        }
        eprintln!(
            "    Complexes: {}, COMPONENT_OF edges: {}",
            format_num(maps.complex.len()),
            format_num(component_count),
        );
    } else {
        eprintln!("  WARNING: ComplexParticipantsPubMedIdentifiers_human.txt not found");
    }

    let elapsed = t0.elapsed();
    eprintln!(
        "  Reactome phase complete: {} nodes, {} edges in {}",
        format_num(counts.nodes),
        format_num(counts.edges),
        format_duration(elapsed),
    );

    Ok(())
}

// ============================================================================
// PHASE 2: STRING
// ============================================================================

fn load_string(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    data_dir: &Path,
    threshold: i64,
    counts: &mut PhaseCounts,
) -> Result<(), Error> {
    let string_dir = data_dir.join("string");
    if !string_dir.exists() {
        eprintln!("  WARNING: string/ directory not found, skipping");
        return Ok(());
    }

    let is_tty = io::stderr().is_terminal();
    let t0 = Instant::now();

    // ------------------------------------------------------------------
    // 1. 9606.protein.aliases.v12.0.txt — Build ENSP -> UniProt mapping
    //    Header row (skip). Cols: 0=string_id, 1=alias, 2=source
    //    Filter source contains "UniProt_AC"
    // ------------------------------------------------------------------
    let mut ensp_to_uniprot: HashMap<String, String> = HashMap::new();
    let aliases_file = string_dir.join("9606.protein.aliases.v12.0.txt");
    if aliases_file.exists() {
        eprintln!("  Loading STRING aliases (ENSP -> UniProt mapping)...");
        let file = File::open(&aliases_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut lines = reader.lines();

        // Skip header
        let _header = lines.next();
        let mut alias_count = 0usize;

        for line_result in lines {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 3 {
                continue;
            }
            let string_id = fields[0].trim();
            let alias = fields[1].trim();
            let source = fields[2].trim();

            if !source.contains("UniProt_AC") {
                continue;
            }

            // Strip "9606." prefix from string_id
            let ensp = if let Some(stripped) = string_id.strip_prefix("9606.") {
                stripped
            } else {
                string_id
            };

            ensp_to_uniprot.insert(ensp.to_string(), alias.to_string());
            alias_count += 1;
            report_progress("Aliases", alias_count, &t0, is_tty);
        }
        if is_tty {
            eprintln!();
        }
        eprintln!(
            "    ENSP->UniProt mappings: {}",
            format_num(ensp_to_uniprot.len()),
        );
    } else {
        eprintln!("  WARNING: 9606.protein.aliases.v12.0.txt not found");
    }

    // ------------------------------------------------------------------
    // 2. 9606.protein.info.v12.0.txt — Build ENSP -> preferred_name mapping
    //    Header row (skip). Cols: 0=string_id, 1=preferred_name, 2=size, 3=annotation
    // ------------------------------------------------------------------
    let mut ensp_to_name: HashMap<String, String> = HashMap::new();
    let info_file = string_dir.join("9606.protein.info.v12.0.txt");
    if info_file.exists() {
        eprintln!("  Loading STRING protein info...");
        let file = File::open(&info_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut lines = reader.lines();

        // Skip header
        let _header = lines.next();
        let mut info_count = 0usize;

        for line_result in lines {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 2 {
                continue;
            }
            let string_id = fields[0].trim();
            let preferred_name = fields[1].trim();

            let ensp = if let Some(stripped) = string_id.strip_prefix("9606.") {
                stripped
            } else {
                string_id
            };

            if !preferred_name.is_empty() {
                ensp_to_name.insert(ensp.to_string(), preferred_name.to_string());
            }
            info_count += 1;
            report_progress("ProteinInfo", info_count, &t0, is_tty);
        }
        if is_tty {
            eprintln!();
        }
        eprintln!(
            "    ENSP->name mappings: {}",
            format_num(ensp_to_name.len()),
        );
    } else {
        eprintln!("  WARNING: 9606.protein.info.v12.0.txt not found");
    }

    // ------------------------------------------------------------------
    // 3. 9606.protein.links.v12.0.txt — INTERACTS_WITH edges
    //    SPACE-separated with header. Cols: protein1, protein2, combined_score
    //    Filter score >= threshold. Map to UniProt. Dedup via sorted pair.
    // ------------------------------------------------------------------
    let links_file = string_dir.join("9606.protein.links.v12.0.txt");
    if links_file.exists() {
        eprintln!(
            "  Loading STRING protein links (threshold >= {})...",
            threshold,
        );
        let file = File::open(&links_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut lines = reader.lines();

        // Skip header
        let _header = lines.next();
        let mut interaction_count = 0usize;
        let mut line_count = 0usize;
        let mut skipped_unmapped = 0usize;

        for line_result in lines {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            // Space-separated
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 3 {
                continue;
            }
            let protein1_raw = fields[0];
            let protein2_raw = fields[1];
            let score: i64 = match fields[2].parse() {
                Ok(s) => s,
                Err(_) => continue,
            };

            if score < threshold {
                continue;
            }

            // Strip "9606." prefix
            let ensp1 = if let Some(stripped) = protein1_raw.strip_prefix("9606.") {
                stripped
            } else {
                protein1_raw
            };
            let ensp2 = if let Some(stripped) = protein2_raw.strip_prefix("9606.") {
                stripped
            } else {
                protein2_raw
            };

            // Map to UniProt
            let uid1 = match ensp_to_uniprot.get(ensp1) {
                Some(u) => u.clone(),
                None => {
                    skipped_unmapped += 1;
                    continue;
                }
            };
            let uid2 = match ensp_to_uniprot.get(ensp2) {
                Some(u) => u.clone(),
                None => {
                    skipped_unmapped += 1;
                    continue;
                }
            };

            if uid1 == uid2 {
                continue;
            }

            // Dedup via sorted pair
            let (a, b) = if uid1 < uid2 {
                (&uid1, &uid2)
            } else {
                (&uid2, &uid1)
            };
            let dedup_key = format!("{}|{}", a, b);
            if maps.interacts_with.contains(&dedup_key) {
                continue;
            }

            // Get protein name from info file
            let name1 = ensp_to_name.get(ensp1).map(|s| s.as_str()).unwrap_or("");
            let name2 = ensp_to_name.get(ensp2).map(|s| s.as_str()).unwrap_or("");

            // Create protein nodes if not already present
            let prot1_nid = get_or_create_protein(graph, maps, &uid1, name1, counts);
            let prot2_nid = get_or_create_protein(graph, maps, &uid2, name2, counts);

            // Create INTERACTS_WITH edge with combined_score
            let mut props = samyama_sdk::PropertyMap::new();
            props.insert(
                "combined_score".to_string(),
                PropertyValue::Integer(score),
            );
            let _ = graph.create_edge_with_properties(
                prot1_nid,
                prot2_nid,
                "INTERACTS_WITH",
                props,
            );
            counts.edges += 1;
            interaction_count += 1;
            maps.interacts_with.insert(dedup_key);

            line_count += 1;
            report_progress("StringLinks", line_count, &t0, is_tty);
        }
        if is_tty {
            eprintln!();
        }
        eprintln!(
            "    INTERACTS_WITH edges: {} (skipped {} unmapped)",
            format_num(interaction_count),
            format_num(skipped_unmapped),
        );
    } else {
        eprintln!("  WARNING: 9606.protein.links.v12.0.txt not found");
    }

    let elapsed = t0.elapsed();
    eprintln!(
        "  STRING phase complete: {} nodes, {} edges in {}",
        format_num(counts.nodes),
        format_num(counts.edges),
        format_duration(elapsed),
    );

    Ok(())
}

// ============================================================================
// PHASE 3: GENE ONTOLOGY
// ============================================================================

fn load_go(
    graph: &mut GraphStore,
    maps: &mut IdMaps,
    data_dir: &Path,
    counts: &mut PhaseCounts,
) -> Result<(), Error> {
    let go_dir = data_dir.join("go");
    if !go_dir.exists() {
        eprintln!("  WARNING: go/ directory not found, skipping");
        return Ok(());
    }

    let is_tty = io::stderr().is_terminal();
    let t0 = Instant::now();

    // ------------------------------------------------------------------
    // 1. go.json — GOTerm nodes and hierarchy edges
    //    Parse graphs[0].nodes[] for nodes, graphs[0].edges[] for edges
    // ------------------------------------------------------------------
    let go_json_file = go_dir.join("go.json");
    if go_json_file.exists() {
        eprintln!("  Loading go.json...");
        let file = File::open(&go_json_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let data: serde_json::Value = serde_json::from_reader(reader)?;

        // --- Nodes ---
        let mut go_node_count = 0usize;
        if let Some(graphs) = data.get("graphs").and_then(|v| v.as_array()) {
            if let Some(graph0) = graphs.first() {
                // Parse nodes
                if let Some(nodes) = graph0.get("nodes").and_then(|v| v.as_array()) {
                    eprintln!("    Parsing {} GO nodes...", format_num(nodes.len()));
                    for node_val in nodes {
                        let id_uri = node_val
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if id_uri.is_empty() {
                            continue;
                        }

                        // Extract GO ID from URI: strip prefix, convert GO_0008150 -> GO:0008150
                        let go_id = extract_go_id(id_uri);
                        if go_id.is_empty() || !go_id.starts_with("GO:") {
                            continue;
                        }

                        let name = node_val
                            .get("lbl")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");

                        // Extract namespace from meta.basicPropertyValues
                        let namespace = extract_go_namespace(node_val);

                        // Extract definition from meta.definition.val
                        let definition = node_val
                            .get("meta")
                            .and_then(|m| m.get("definition"))
                            .and_then(|d| d.get("val"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");

                        get_or_create_go_term(
                            graph, maps, &go_id, name, &namespace, definition, counts,
                        );
                        go_node_count += 1;
                        report_progress("GOTerms", go_node_count, &t0, is_tty);
                    }
                    if is_tty {
                        eprintln!();
                    }
                    eprintln!("    GOTerm nodes: {}", format_num(go_node_count));
                }

                // --- Hierarchy edges ---
                if let Some(edges) = graph0.get("edges").and_then(|v| v.as_array()) {
                    eprintln!("    Parsing {} GO edges...", format_num(edges.len()));
                    let mut is_a_count = 0usize;
                    let mut part_of_count = 0usize;
                    let mut regulates_count = 0usize;
                    let mut edge_idx = 0usize;

                    for edge_val in edges {
                        let sub_uri = edge_val
                            .get("sub")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let pred = edge_val
                            .get("pred")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let obj_uri = edge_val
                            .get("obj")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");

                        let sub_id = extract_go_id(sub_uri);
                        let obj_id = extract_go_id(obj_uri);

                        if sub_id.is_empty() || obj_id.is_empty() {
                            continue;
                        }

                        let sub_nid = maps.go_term.get(&sub_id).copied();
                        let obj_nid = maps.go_term.get(&obj_id).copied();

                        if let (Some(s_nid), Some(o_nid)) = (sub_nid, obj_nid) {
                            // Map predicate to edge type
                            let edge_type = if pred == "is_a" {
                                "IS_A"
                            } else if pred.contains("BFO:0000050") || pred.contains("BFO_0000050")
                            {
                                "PART_OF"
                            } else if pred.contains("RO:0002211") || pred.contains("RO_0002211") {
                                "REGULATES"
                            } else {
                                // Skip unknown predicates
                                continue;
                            };

                            let _ = graph.create_edge(s_nid, o_nid, edge_type);
                            counts.edges += 1;

                            match edge_type {
                                "IS_A" => is_a_count += 1,
                                "PART_OF" => part_of_count += 1,
                                "REGULATES" => regulates_count += 1,
                                _ => {}
                            }
                        }

                        edge_idx += 1;
                        report_progress("GOEdges", edge_idx, &t0, is_tty);
                    }
                    if is_tty {
                        eprintln!();
                    }
                    eprintln!(
                        "    GO hierarchy edges: IS_A={}, PART_OF={}, REGULATES={}",
                        format_num(is_a_count),
                        format_num(part_of_count),
                        format_num(regulates_count),
                    );
                }
            }
        }
    } else {
        eprintln!("  WARNING: go.json not found");
    }

    // ------------------------------------------------------------------
    // 2. goa_human.gaf — ANNOTATED_WITH edges (protein -> goterm)
    //    Tab-sep, skip lines starting with '!'
    //    Cols: 1=uniprot_id, 3=qualifier, 4=go_id, 6=evidence_code
    // ------------------------------------------------------------------
    let gaf_file = go_dir.join("goa_human.gaf");
    if gaf_file.exists() {
        eprintln!("  Loading goa_human.gaf...");
        let file = File::open(&gaf_file)?;
        let reader = BufReader::with_capacity(1 << 16, file);
        let mut annotated_count = 0usize;
        let mut line_count = 0usize;

        for line_result in reader.lines() {
            let line = line_result?;
            if line.starts_with('!') || line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() < 7 {
                continue;
            }

            let uniprot_id = fields[1].trim();
            // fields[3] = qualifier (e.g. "enables", "part_of")
            let go_id = fields[4].trim();
            let evidence_code = fields[6].trim();

            // Only link proteins already in our maps (from Reactome/STRING)
            let protein_nid = match maps.protein.get(uniprot_id) {
                Some(&nid) => nid,
                None => continue,
            };

            let goterm_nid = match maps.go_term.get(go_id) {
                Some(&nid) => nid,
                None => continue,
            };

            // Edge dedup: "uid|goid"
            let dedup_key = format!("{}|{}", uniprot_id, go_id);
            if maps.annotated_with.contains(&dedup_key) {
                continue;
            }

            if !evidence_code.is_empty() {
                let mut props = samyama_sdk::PropertyMap::new();
                props.insert(
                    "evidence_code".to_string(),
                    PropertyValue::String(evidence_code.to_string()),
                );
                let _ = graph.create_edge_with_properties(
                    protein_nid,
                    goterm_nid,
                    "ANNOTATED_WITH",
                    props,
                );
            } else {
                let _ = graph.create_edge(protein_nid, goterm_nid, "ANNOTATED_WITH");
            }
            counts.edges += 1;
            annotated_count += 1;
            maps.annotated_with.insert(dedup_key);

            line_count += 1;
            report_progress("GOAnnotations", line_count, &t0, is_tty);
        }
        if is_tty {
            eprintln!();
        }
        eprintln!(
            "    ANNOTATED_WITH edges: {}",
            format_num(annotated_count),
        );
    } else {
        eprintln!("  WARNING: goa_human.gaf not found");
    }

    let elapsed = t0.elapsed();
    eprintln!(
        "  GO phase complete: {} nodes, {} edges in {}",
        format_num(counts.nodes),
        format_num(counts.edges),
        format_duration(elapsed),
    );

    Ok(())
}

// ============================================================================
// GO HELPERS
// ============================================================================

/// Extract a GO ID from a URI string.
/// Handles forms like:
///   "http://purl.obolibrary.org/obo/GO_0008150" -> "GO:0008150"
///   "GO:0008150" -> "GO:0008150"
fn extract_go_id(uri: &str) -> String {
    // Direct GO ID
    if uri.starts_with("GO:") {
        return uri.to_string();
    }

    // URI form: look for GO_ at the end
    if let Some(pos) = uri.rfind("GO_") {
        let raw = &uri[pos..];
        // Convert GO_0008150 -> GO:0008150
        return raw.replacen('_', ":", 1);
    }

    String::new()
}

/// Extract namespace from a GO node's meta.basicPropertyValues.
/// Looks for a property with pred containing "hasOBONamespace".
fn extract_go_namespace(node_val: &serde_json::Value) -> String {
    let bpvs = node_val
        .get("meta")
        .and_then(|m| m.get("basicPropertyValues"))
        .and_then(|v| v.as_array());

    if let Some(arr) = bpvs {
        for item in arr {
            let pred = item
                .get("pred")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if pred.contains("hasOBONamespace") {
                return item
                    .get("val")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
            }
        }
    }

    String::new()
}

// ============================================================================
// PUBLIC: LOAD DATASET
// ============================================================================

pub fn load_dataset(
    graph: &mut GraphStore,
    data_dir: &Path,
    phases: &[String],
    string_threshold: i64,
) -> Result<LoadResult, Error> {
    let mut maps = IdMaps::new();
    let mut total_nodes = 0usize;
    let mut total_edges = 0usize;

    // Phase 1: Reactome
    if phases.iter().any(|p| p == "reactome") {
        eprintln!("Phase 1/3: Reactome");
        let mut counts = PhaseCounts::new();
        load_reactome(graph, &mut maps, data_dir, &mut counts)?;
        total_nodes += counts.nodes;
        total_edges += counts.edges;
        eprintln!();
    }

    // Phase 2: STRING
    if phases.iter().any(|p| p == "string") {
        eprintln!("Phase 2/3: STRING");
        let mut counts = PhaseCounts::new();
        load_string(graph, &mut maps, data_dir, string_threshold, &mut counts)?;
        total_nodes += counts.nodes;
        total_edges += counts.edges;
        eprintln!();
    }

    // Phase 3: Gene Ontology
    if phases.iter().any(|p| p == "go") {
        eprintln!("Phase 3/3: Gene Ontology");
        let mut counts = PhaseCounts::new();
        load_go(graph, &mut maps, data_dir, &mut counts)?;
        total_nodes += counts.nodes;
        total_edges += counts.edges;
        eprintln!();
    }

    // Summary
    eprintln!("--- Entity Summary ---");
    eprintln!("  Pathways:   {}", format_num(maps.pathway.len()));
    eprintln!("  Proteins:   {}", format_num(maps.protein.len()));
    eprintln!("  Reactions:  {}", format_num(maps.reaction.len()));
    eprintln!("  Complexes:  {}", format_num(maps.complex.len()));
    eprintln!("  GO Terms:   {}", format_num(maps.go_term.len()));

    Ok(LoadResult {
        total_nodes,
        total_edges,
    })
}
