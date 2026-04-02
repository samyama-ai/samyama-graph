//! AACT (ClinicalTrials.gov) data loading utilities.
//!
//! Loads pipe-delimited flat files from the AACT database dump into
//! GraphStore at high speed using direct API calls (no Cypher parsing).
//!
//! Two-phase loading: all nodes first (with HashMap dedup), then all edges.
//! String-based IDs (nct_id) with cross-table resolution via IdMaps.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal};
use std::path::Path;
use std::time::Instant;

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
// ID MAPPINGS
// ============================================================================

/// ID maps for cross-table edge resolution.
pub struct IdMaps {
    pub trial: HashMap<String, NodeId>,             // nct_id -> NodeId
    pub condition: HashMap<String, NodeId>,          // name.lower() -> NodeId
    pub intervention: HashMap<String, NodeId>,       // name.lower() -> NodeId
    pub sponsor: HashMap<String, NodeId>,            // name.lower() -> NodeId
    pub site: HashMap<String, NodeId>,               // "facility|city".lower() -> NodeId
    pub adverse_event: HashMap<String, NodeId>,      // term.lower() -> NodeId
    pub mesh: HashMap<String, NodeId>,               // name.lower() -> NodeId
    pub publication: HashMap<String, NodeId>,         // pmid -> NodeId
    // Per-trial maps for non-deduped entities
    pub arm_group: HashMap<String, NodeId>,          // "nct_id|label" -> NodeId
    pub outcome: HashMap<String, NodeId>,            // "nct_id|measure" -> NodeId
    // AACT row-id maps for design_group_interventions join
    pub intervention_row_id: HashMap<String, String>,    // AACT id -> intervention name
    pub group_row_id: HashMap<String, (String, String)>, // AACT id -> (nct_id, label)
}

impl IdMaps {
    pub fn new() -> Self {
        Self {
            trial: HashMap::new(),
            condition: HashMap::new(),
            intervention: HashMap::new(),
            sponsor: HashMap::new(),
            site: HashMap::new(),
            adverse_event: HashMap::new(),
            mesh: HashMap::new(),
            publication: HashMap::new(),
            arm_group: HashMap::new(),
            outcome: HashMap::new(),
            intervention_row_id: HashMap::new(),
            group_row_id: HashMap::new(),
        }
    }
}

// ============================================================================
// FIELD HELPERS
// ============================================================================

/// Get a field by column index, trimmed. Returns "" if index is out of bounds.
fn field<'a>(fields: &[&'a str], idx: usize) -> &'a str {
    fields.get(idx).map(|s| s.trim()).unwrap_or("")
}

/// Find the index of a column by header name. Returns None if not found.
fn col_index(headers: &[&str], name: &str) -> Option<usize> {
    headers.iter().position(|h| h.trim() == name)
}

/// Truncate a string to at most `max_len` characters (char-boundary safe).
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        // Find a safe char boundary
        let mut end = max_len;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s[..end].to_string()
    }
}

// ============================================================================
// FORMATTING
// ============================================================================

pub fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

pub fn format_duration(d: std::time::Duration) -> String {
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

/// Print a final summary line, clearing any inline progress.
fn print_done(msg: &str) {
    eprintln!("\r{:80}", msg);
}

// ============================================================================
// PRE-LOAD HELPERS
// ============================================================================

/// Pre-load brief_summaries.txt into a HashMap<nct_id, description>.
fn preload_brief_summaries(data_dir: &Path) -> Result<HashMap<String, String>, Error> {
    let path = data_dir.join("brief_summaries.txt");
    let mut map: HashMap<String, String> = HashMap::new();

    if !path.exists() {
        eprintln!("  brief_summaries.txt not found, skipping summaries");
        return Ok(map);
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = match lines.next() {
        Some(Ok(h)) => h,
        _ => return Ok(map),
    };
    let headers: Vec<&str> = header.split('|').collect();
    let nct_col = col_index(&headers, "nct_id");
    let desc_col = col_index(&headers, "description");
    if nct_col.is_none() || desc_col.is_none() {
        eprintln!("  brief_summaries.txt: missing nct_id or description column");
        return Ok(map);
    }
    let nct_col = nct_col.unwrap();
    let desc_col = desc_col.unwrap();

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, nct_col);
        let desc = field(&fields, desc_col);
        if !nct_id.is_empty() && !desc.is_empty() {
            map.insert(nct_id.to_string(), truncate(desc, 500));
        }
    }

    eprintln!("  Pre-loaded {} brief summaries", format_num(map.len()));
    Ok(map)
}

// ============================================================================
// NODE LOADERS
// ============================================================================

/// Load studies.txt -> ClinicalTrial nodes.
fn load_studies(
    graph: &mut GraphStore,
    data_dir: &Path,
    max_studies: usize,
    summaries: &HashMap<String, String>,
    ids: &mut IdMaps,
) -> Result<usize, Error> {
    let path = data_dir.join("studies.txt");
    if !path.exists() {
        return Err("studies.txt not found".into());
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty studies.txt")??;
    let headers: Vec<&str> = header.split('|').collect();

    let c_nct = col_index(&headers, "nct_id");
    let c_brief_title = col_index(&headers, "brief_title");
    let c_official_title = col_index(&headers, "official_title");
    let c_study_type = col_index(&headers, "study_type");
    let c_phase = col_index(&headers, "phase");
    let c_overall_status = col_index(&headers, "overall_status");
    let c_enrollment = col_index(&headers, "enrollment");
    let c_start_date = col_index(&headers, "start_date");
    let c_completion_date = col_index(&headers, "completion_date");
    let c_primary_completion = col_index(&headers, "primary_completion_date");
    let c_last_updated = col_index(&headers, "last_update_submitted_date");
    let c_results_first = col_index(&headers, "results_first_submitted_date");
    let c_why_stopped = col_index(&headers, "why_stopped");

    if c_nct.is_none() {
        return Err("studies.txt: missing nct_id column".into());
    }
    let c_nct = c_nct.unwrap();

    let mut count = 0usize;
    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        if nct_id.is_empty() {
            continue;
        }
        if max_studies > 0 && count >= max_studies {
            break;
        }

        let node_id = graph.create_node("ClinicalTrial");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("nct_id", PropertyValue::String(nct_id.to_string()));

            if let Some(ci) = c_brief_title {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("title", PropertyValue::String(v.to_string()));
                }
            }
            if let Some(ci) = c_official_title {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("official_title", PropertyValue::String(v.to_string()));
                }
            }
            if let Some(summary) = summaries.get(nct_id) {
                node.set_property(
                    "brief_summary",
                    PropertyValue::String(summary.clone()),
                );
            }
            if let Some(ci) = c_study_type {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("study_type", PropertyValue::String(v.to_string()));
                }
            }
            if let Some(ci) = c_phase {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("phase", PropertyValue::String(v.to_string()));
                }
            }
            if let Some(ci) = c_overall_status {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property(
                        "overall_status",
                        PropertyValue::String(v.to_string()),
                    );
                }
            }
            if let Some(ci) = c_enrollment {
                let v = field(&fields, ci);
                if let Ok(n) = v.parse::<i64>() {
                    node.set_property("enrollment", PropertyValue::Integer(n));
                }
            }
            if let Some(ci) = c_start_date {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("start_date", PropertyValue::String(v.to_string()));
                }
            }
            if let Some(ci) = c_completion_date {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property(
                        "completion_date",
                        PropertyValue::String(v.to_string()),
                    );
                }
            }
            if let Some(ci) = c_primary_completion {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property(
                        "primary_completion_date",
                        PropertyValue::String(v.to_string()),
                    );
                }
            }
            if let Some(ci) = c_last_updated {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property(
                        "last_updated",
                        PropertyValue::String(v.to_string()),
                    );
                }
            }
            // has_results: true if results_first_submitted_date is non-empty
            let has_results = c_results_first
                .map(|ci| !field(&fields, ci).is_empty())
                .unwrap_or(false);
            node.set_property("has_results", PropertyValue::Boolean(has_results));

            if let Some(ci) = c_why_stopped {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("why_stopped", PropertyValue::String(v.to_string()));
                }
            }
        }

        ids.trial.insert(nct_id.to_string(), node_id);
        count += 1;

        if count % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!("\r  ClinicalTrial  {:>12} nodes...          ", format_num(count));
        }
    }

    Ok(count)
}

/// Load conditions.txt -> Condition nodes (dedup by name.lower()).
/// Also builds edges: ClinicalTrial -[:STUDIES]-> Condition.
fn load_conditions(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    let path = data_dir.join("conditions.txt");
    if !path.exists() {
        eprintln!("  WARNING: conditions.txt not found, skipping");
        return Ok((0, 0));
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty conditions.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    let c_name = col_index(&headers, "name");
    if c_nct.is_none() || c_name.is_none() {
        eprintln!("  WARNING: conditions.txt missing required columns");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();
    let c_name = c_name.unwrap();

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        let name = field(&fields, c_name);
        if nct_id.is_empty() || name.is_empty() {
            continue;
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue, // trial not loaded (max_studies filter)
        };

        let key = name.to_lowercase();
        let cond_node = if let Some(&existing) = ids.condition.get(&key) {
            existing
        } else {
            let node_id = graph.create_node("Condition");
            if let Some(node) = graph.get_node_mut(node_id) {
                node.set_property("name", PropertyValue::String(name.to_string()));
            }
            ids.condition.insert(key, node_id);
            node_count += 1;
            node_id
        };

        // ClinicalTrial -[:STUDIES]-> Condition
        if graph.create_edge(trial_node, cond_node, "STUDIES").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  Condition      {:>12} nodes, {:>12} STUDIES edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

/// Load interventions.txt -> Intervention nodes (dedup by name.lower()).
/// Also builds edges: ClinicalTrial -[:TESTS]-> Intervention.
/// Populates intervention_row_id map for design_group_interventions.
fn load_interventions(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    let path = data_dir.join("interventions.txt");
    if !path.exists() {
        eprintln!("  WARNING: interventions.txt not found, skipping");
        return Ok((0, 0));
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty interventions.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    let c_name = col_index(&headers, "name");
    let c_id = col_index(&headers, "id");
    let c_type = col_index(&headers, "intervention_type");
    let c_desc = col_index(&headers, "description");
    if c_nct.is_none() || c_name.is_none() {
        eprintln!("  WARNING: interventions.txt missing required columns");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();
    let c_name = c_name.unwrap();

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        let name = field(&fields, c_name);
        if nct_id.is_empty() || name.is_empty() {
            continue;
        }

        // Save row id -> name mapping for design_group_interventions
        if let Some(ci) = c_id {
            let row_id = field(&fields, ci);
            if !row_id.is_empty() {
                ids.intervention_row_id
                    .insert(row_id.to_string(), name.to_string());
            }
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue,
        };

        let key = name.to_lowercase();
        let interv_node = if let Some(&existing) = ids.intervention.get(&key) {
            existing
        } else {
            let node_id = graph.create_node("Intervention");
            if let Some(node) = graph.get_node_mut(node_id) {
                node.set_property("name", PropertyValue::String(name.to_string()));
                if let Some(ci) = c_type {
                    let v = field(&fields, ci);
                    if !v.is_empty() {
                        node.set_property("type", PropertyValue::String(v.to_string()));
                    }
                }
                if let Some(ci) = c_desc {
                    let v = field(&fields, ci);
                    if !v.is_empty() {
                        node.set_property(
                            "description",
                            PropertyValue::String(truncate(v, 300)),
                        );
                    }
                }
            }
            ids.intervention.insert(key, node_id);
            node_count += 1;
            node_id
        };

        // ClinicalTrial -[:TESTS]-> Intervention
        if graph.create_edge(trial_node, interv_node, "TESTS").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  Intervention   {:>12} nodes, {:>12} TESTS edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

/// Load design_groups.txt -> ArmGroup nodes (no global dedup, per-trial).
/// Also builds edges: ClinicalTrial -[:HAS_ARM]-> ArmGroup.
/// Populates group_row_id map for design_group_interventions.
fn load_design_groups(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    let path = data_dir.join("design_groups.txt");
    if !path.exists() {
        eprintln!("  WARNING: design_groups.txt not found, skipping");
        return Ok((0, 0));
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty design_groups.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    let c_title = col_index(&headers, "title");
    let c_id = col_index(&headers, "id");
    let c_type = col_index(&headers, "group_type");
    let c_desc = col_index(&headers, "description");
    if c_nct.is_none() || c_title.is_none() {
        eprintln!("  WARNING: design_groups.txt missing required columns");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();
    let c_title = c_title.unwrap();

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        let title = field(&fields, c_title);
        if nct_id.is_empty() || title.is_empty() {
            continue;
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue,
        };

        // Save row id -> (nct_id, title) for design_group_interventions
        if let Some(ci) = c_id {
            let row_id = field(&fields, ci);
            if !row_id.is_empty() {
                ids.group_row_id
                    .insert(row_id.to_string(), (nct_id.to_string(), title.to_string()));
            }
        }

        let node_id = graph.create_node("ArmGroup");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("label", PropertyValue::String(title.to_string()));
            node.set_property(
                "trial_nct_id",
                PropertyValue::String(nct_id.to_string()),
            );
            if let Some(ci) = c_type {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("type", PropertyValue::String(v.to_string()));
                }
            }
            if let Some(ci) = c_desc {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property(
                        "description",
                        PropertyValue::String(truncate(v, 300)),
                    );
                }
            }
        }

        // Composite key for later USES edge resolution
        let composite_key = format!("{}|{}", nct_id, title);
        ids.arm_group.insert(composite_key, node_id);
        node_count += 1;

        // ClinicalTrial -[:HAS_ARM]-> ArmGroup
        if graph.create_edge(trial_node, node_id, "HAS_ARM").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  ArmGroup       {:>12} nodes, {:>12} HAS_ARM edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

/// Load sponsors.txt -> Sponsor nodes (dedup by name.lower(), lead sponsors only).
/// Also builds edges: ClinicalTrial -[:SPONSORED_BY]-> Sponsor.
fn load_sponsors(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    let path = data_dir.join("sponsors.txt");
    if !path.exists() {
        eprintln!("  WARNING: sponsors.txt not found, skipping");
        return Ok((0, 0));
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty sponsors.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    let c_name = col_index(&headers, "name");
    let c_role = col_index(&headers, "lead_or_collaborator");
    let c_class = col_index(&headers, "agency_class");
    if c_nct.is_none() || c_name.is_none() {
        eprintln!("  WARNING: sponsors.txt missing required columns");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();
    let c_name = c_name.unwrap();

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        let name = field(&fields, c_name);
        if nct_id.is_empty() || name.is_empty() {
            continue;
        }

        // Only lead sponsors
        if let Some(ci) = c_role {
            let role = field(&fields, ci).to_lowercase();
            if !role.is_empty() && role != "lead" {
                continue;
            }
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue,
        };

        let key = name.to_lowercase();
        let sponsor_node = if let Some(&existing) = ids.sponsor.get(&key) {
            existing
        } else {
            let node_id = graph.create_node("Sponsor");
            if let Some(node) = graph.get_node_mut(node_id) {
                node.set_property("name", PropertyValue::String(name.to_string()));
                if let Some(ci) = c_class {
                    let v = field(&fields, ci);
                    if !v.is_empty() {
                        node.set_property("class", PropertyValue::String(v.to_string()));
                    }
                }
            }
            ids.sponsor.insert(key, node_id);
            node_count += 1;
            node_id
        };

        // ClinicalTrial -[:SPONSORED_BY]-> Sponsor
        if graph.create_edge(trial_node, sponsor_node, "SPONSORED_BY").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  Sponsor        {:>12} nodes, {:>12} SPONSORED_BY edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

/// Load design_outcomes.txt -> Outcome nodes (per-trial, no global dedup).
/// Also builds edges: ClinicalTrial -[:MEASURES]-> Outcome.
fn load_outcomes(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    let path = data_dir.join("design_outcomes.txt");
    if !path.exists() {
        eprintln!("  WARNING: design_outcomes.txt not found, skipping");
        return Ok((0, 0));
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty design_outcomes.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    let c_measure = col_index(&headers, "measure");
    let c_type = col_index(&headers, "outcome_type");
    let c_time = col_index(&headers, "time_frame");
    let c_desc = col_index(&headers, "description");
    if c_nct.is_none() || c_measure.is_none() {
        eprintln!("  WARNING: design_outcomes.txt missing required columns");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();
    let c_measure = c_measure.unwrap();

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        let measure = field(&fields, c_measure);
        if nct_id.is_empty() || measure.is_empty() {
            continue;
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue,
        };

        let node_id = graph.create_node("Outcome");
        if let Some(node) = graph.get_node_mut(node_id) {
            node.set_property("measure", PropertyValue::String(measure.to_string()));
            node.set_property(
                "trial_nct_id",
                PropertyValue::String(nct_id.to_string()),
            );
            if let Some(ci) = c_type {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("type", PropertyValue::String(v.to_string()));
                }
            }
            if let Some(ci) = c_time {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property("time_frame", PropertyValue::String(v.to_string()));
                }
            }
            if let Some(ci) = c_desc {
                let v = field(&fields, ci);
                if !v.is_empty() {
                    node.set_property(
                        "description",
                        PropertyValue::String(truncate(v, 300)),
                    );
                }
            }
        }

        // Composite key for lookup
        let composite_key = format!("{}|{}", nct_id, measure);
        ids.outcome.insert(composite_key, node_id);
        node_count += 1;

        // ClinicalTrial -[:MEASURES]-> Outcome
        if graph.create_edge(trial_node, node_id, "MEASURES").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  Outcome        {:>12} nodes, {:>12} MEASURES edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

/// Load facilities.txt -> Site nodes (dedup by "facility|city".lower()).
/// Also builds edges: ClinicalTrial -[:CONDUCTED_AT]-> Site.
fn load_facilities(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    let path = data_dir.join("facilities.txt");
    if !path.exists() {
        eprintln!("  WARNING: facilities.txt not found, skipping");
        return Ok((0, 0));
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty facilities.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    let c_name = col_index(&headers, "name");
    let c_city = col_index(&headers, "city");
    let c_state = col_index(&headers, "state");
    let c_country = col_index(&headers, "country");
    let c_zip = col_index(&headers, "zip");
    if c_nct.is_none() {
        eprintln!("  WARNING: facilities.txt missing nct_id column");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        if nct_id.is_empty() {
            continue;
        }

        let facility = c_name.map(|ci| field(&fields, ci)).unwrap_or("");
        let city = c_city.map(|ci| field(&fields, ci)).unwrap_or("");

        // Skip rows with neither facility nor city
        if facility.is_empty() && city.is_empty() {
            continue;
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue,
        };

        // Dedup key: "facility|city".lower()
        let key = format!("{}|{}", facility, city).to_lowercase();
        let site_node = if let Some(&existing) = ids.site.get(&key) {
            existing
        } else {
            let node_id = graph.create_node("Site");
            if let Some(node) = graph.get_node_mut(node_id) {
                if !facility.is_empty() {
                    node.set_property(
                        "facility",
                        PropertyValue::String(facility.to_string()),
                    );
                }
                if !city.is_empty() {
                    node.set_property("city", PropertyValue::String(city.to_string()));
                }
                if let Some(ci) = c_state {
                    let v = field(&fields, ci);
                    if !v.is_empty() {
                        node.set_property("state", PropertyValue::String(v.to_string()));
                    }
                }
                if let Some(ci) = c_country {
                    let v = field(&fields, ci);
                    if !v.is_empty() {
                        node.set_property("country", PropertyValue::String(v.to_string()));
                    }
                }
                if let Some(ci) = c_zip {
                    let v = field(&fields, ci);
                    if !v.is_empty() {
                        node.set_property("zip", PropertyValue::String(v.to_string()));
                    }
                }
            }
            ids.site.insert(key, node_id);
            node_count += 1;
            node_id
        };

        // ClinicalTrial -[:CONDUCTED_AT]-> Site
        if graph.create_edge(trial_node, site_node, "CONDUCTED_AT").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  Site           {:>12} nodes, {:>12} CONDUCTED_AT edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

/// Load reported_events.txt -> AdverseEvent nodes (dedup by term.lower()).
/// Also builds edges: ClinicalTrial -[:REPORTED]-> AdverseEvent.
fn load_reported_events(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    // Try multiple filenames across AACT versions
    let path = if data_dir.join("reported_events.txt").exists() {
        data_dir.join("reported_events.txt")
    } else if data_dir.join("reported_event_totals.txt").exists() {
        data_dir.join("reported_event_totals.txt")
    } else {
        eprintln!("  WARNING: reported_events.txt not found, skipping");
        return Ok((0, 0));
    };

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty reported_events file")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    // Column name varies across AACT versions
    let c_term = col_index(&headers, "event_term")
        .or_else(|| col_index(&headers, "adverse_event_term"))
        .or_else(|| col_index(&headers, "term"));
    let c_organ = col_index(&headers, "organ_system")
        .or_else(|| col_index(&headers, "classification"))
        .or_else(|| col_index(&headers, "category"));

    if c_nct.is_none() || c_term.is_none() {
        eprintln!("  WARNING: reported_events missing required columns (nct_id, term)");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();
    let c_term = c_term.unwrap();

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        let term = field(&fields, c_term);
        if nct_id.is_empty() || term.is_empty() {
            continue;
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue,
        };

        let key = term.to_lowercase();
        let ae_node = if let Some(&existing) = ids.adverse_event.get(&key) {
            existing
        } else {
            let node_id = graph.create_node("AdverseEvent");
            if let Some(node) = graph.get_node_mut(node_id) {
                node.set_property("term", PropertyValue::String(term.to_string()));
                if let Some(ci) = c_organ {
                    let v = field(&fields, ci);
                    if !v.is_empty() {
                        node.set_property(
                            "organ_system",
                            PropertyValue::String(v.to_string()),
                        );
                    }
                }
                node.set_property(
                    "source_vocabulary",
                    PropertyValue::String("MedDRA".to_string()),
                );
            }
            ids.adverse_event.insert(key, node_id);
            node_count += 1;
            node_id
        };

        // ClinicalTrial -[:REPORTED]-> AdverseEvent
        if graph.create_edge(trial_node, ae_node, "REPORTED").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  AdverseEvent   {:>12} nodes, {:>12} REPORTED edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

/// Load browse_conditions.txt -> MeSHDescriptor nodes (dedup by name.lower()).
/// Also builds edges: Condition -[:CODED_AS_MESH]-> MeSHDescriptor.
///
/// For each row, we find all Condition nodes linked to the trial via STUDIES
/// edges and create CODED_AS_MESH edges from those conditions to the MeSH term.
fn load_browse_conditions(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    let path = data_dir.join("browse_conditions.txt");
    if !path.exists() {
        eprintln!("  WARNING: browse_conditions.txt not found, skipping");
        return Ok((0, 0));
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty browse_conditions.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    let c_mesh = col_index(&headers, "mesh_term");
    if c_nct.is_none() || c_mesh.is_none() {
        eprintln!("  WARNING: browse_conditions.txt missing required columns");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();
    let c_mesh = c_mesh.unwrap();

    // Build reverse map: trial NodeId -> list of Condition NodeIds
    // by scanning existing STUDIES edges
    let mut trial_to_conditions: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for (_nct_str, &trial_node) in &ids.trial {
        // get_outgoing_edges returns Vec<&Edge>
        let targets: Vec<NodeId> = graph
            .get_outgoing_edges(trial_node)
            .into_iter()
            .filter(|edge| edge.edge_type.as_str() == "STUDIES")
            .map(|edge| edge.target)
            .collect();
        if !targets.is_empty() {
            trial_to_conditions.insert(trial_node, targets);
        }
    }

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;
    // Track edges already created to avoid duplicates
    let mut seen_edges: std::collections::HashSet<(NodeId, NodeId)> =
        std::collections::HashSet::new();

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        let mesh_term = field(&fields, c_mesh);
        if nct_id.is_empty() || mesh_term.is_empty() {
            continue;
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue,
        };

        let key = mesh_term.to_lowercase();
        let mesh_node = if let Some(&existing) = ids.mesh.get(&key) {
            existing
        } else {
            let node_id = graph.create_node("MeSHDescriptor");
            if let Some(node) = graph.get_node_mut(node_id) {
                node.set_property("name", PropertyValue::String(mesh_term.to_string()));
            }
            ids.mesh.insert(key, node_id);
            node_count += 1;
            node_id
        };

        // Link each Condition of this trial to the MeSH descriptor
        if let Some(cond_nodes) = trial_to_conditions.get(&trial_node) {
            for &cond_node in cond_nodes {
                let edge_key = (cond_node, mesh_node);
                if seen_edges.contains(&edge_key) {
                    continue;
                }
                if graph.create_edge(cond_node, mesh_node, "CODED_AS_MESH").is_ok() {
                    edge_count += 1;
                    seen_edges.insert(edge_key);
                }
            }
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  MeSHDescriptor {:>12} nodes, {:>12} CODED_AS_MESH edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

/// Load study_references.txt -> Publication nodes (dedup by pmid).
/// Also builds edges: ClinicalTrial -[:PUBLISHED_IN]-> Publication.
fn load_study_references(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &mut IdMaps,
) -> Result<(usize, usize), Error> {
    let path = data_dir.join("study_references.txt");
    if !path.exists() {
        eprintln!("  WARNING: study_references.txt not found, skipping");
        return Ok((0, 0));
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty study_references.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_nct = col_index(&headers, "nct_id");
    let c_pmid = col_index(&headers, "pmid");
    let c_citation = col_index(&headers, "citation");
    let c_ref_type = col_index(&headers, "reference_type");
    if c_nct.is_none() || c_pmid.is_none() {
        eprintln!("  WARNING: study_references.txt missing required columns");
        return Ok((0, 0));
    }
    let c_nct = c_nct.unwrap();
    let c_pmid = c_pmid.unwrap();

    let mut node_count = 0usize;
    let mut edge_count = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let nct_id = field(&fields, c_nct);
        let pmid = field(&fields, c_pmid);
        if nct_id.is_empty() || pmid.is_empty() {
            continue;
        }

        let trial_node = match ids.trial.get(nct_id) {
            Some(&n) => n,
            None => continue,
        };

        let pub_node = if let Some(&existing) = ids.publication.get(pmid) {
            existing
        } else {
            let node_id = graph.create_node("Publication");
            if let Some(node) = graph.get_node_mut(node_id) {
                node.set_property("pmid", PropertyValue::String(pmid.to_string()));
                // Extract title from citation: first sentence before ". "
                if let Some(ci) = c_citation {
                    let citation = field(&fields, ci);
                    if !citation.is_empty() {
                        if let Some(first_sentence) = citation.split(". ").next() {
                            let title = truncate(first_sentence, 200);
                            if !title.is_empty() {
                                node.set_property(
                                    "title",
                                    PropertyValue::String(title),
                                );
                            }
                        }
                    }
                }
                if let Some(ci) = c_ref_type {
                    let v = field(&fields, ci);
                    if !v.is_empty() {
                        node.set_property(
                            "reference_type",
                            PropertyValue::String(v.to_string()),
                        );
                    }
                }
            }
            ids.publication.insert(pmid.to_string(), node_id);
            node_count += 1;
            node_id
        };

        // ClinicalTrial -[:PUBLISHED_IN]-> Publication
        if graph.create_edge(trial_node, pub_node, "PUBLISHED_IN").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  Publication    {:>12} nodes, {:>12} PUBLISHED_IN edges...          ",
                format_num(node_count),
                format_num(edge_count)
            );
        }
    }

    Ok((node_count, edge_count))
}

// ============================================================================
// EDGE-ONLY LOADERS
// ============================================================================

/// Load design_group_interventions.txt -> USES edges (ArmGroup -> Intervention).
/// Uses the row-id maps built during load_interventions and load_design_groups.
fn load_design_group_interventions(
    graph: &mut GraphStore,
    data_dir: &Path,
    ids: &IdMaps,
) -> Result<usize, Error> {
    let path = data_dir.join("design_group_interventions.txt");
    if !path.exists() {
        eprintln!("  WARNING: design_group_interventions.txt not found, skipping");
        return Ok(0);
    }

    let file = File::open(&path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty design_group_interventions.txt")??;
    let headers: Vec<&str> = header.split('|').collect();
    let c_dg_id = col_index(&headers, "design_group_id");
    let c_interv_id = col_index(&headers, "intervention_id");
    if c_dg_id.is_none() || c_interv_id.is_none() {
        eprintln!("  WARNING: design_group_interventions.txt missing required columns");
        return Ok(0);
    }
    let c_dg_id = c_dg_id.unwrap();
    let c_interv_id = c_interv_id.unwrap();

    let mut edge_count = 0usize;
    let mut skipped = 0usize;
    let mut row_num = 0usize;

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }
        row_num += 1;
        let fields: Vec<&str> = line.split('|').collect();
        let dg_id = field(&fields, c_dg_id);
        let interv_id = field(&fields, c_interv_id);
        if dg_id.is_empty() || interv_id.is_empty() {
            continue;
        }

        // Resolve group row id -> (nct_id, title) -> arm_group NodeId
        let group_info = match ids.group_row_id.get(dg_id) {
            Some(info) => info,
            None => {
                skipped += 1;
                continue;
            }
        };
        let arm_key = format!("{}|{}", group_info.0, group_info.1);
        let arm_node = match ids.arm_group.get(&arm_key) {
            Some(&n) => n,
            None => {
                skipped += 1;
                continue;
            }
        };

        // Resolve intervention row id -> name -> intervention NodeId
        let interv_name = match ids.intervention_row_id.get(interv_id) {
            Some(name) => name.to_lowercase(),
            None => {
                skipped += 1;
                continue;
            }
        };
        let interv_node = match ids.intervention.get(&interv_name) {
            Some(&n) => n,
            None => {
                skipped += 1;
                continue;
            }
        };

        // ArmGroup -[:USES]-> Intervention
        if graph.create_edge(arm_node, interv_node, "USES").is_ok() {
            edge_count += 1;
        }

        if row_num % 100_000 == 0 && io::stderr().is_terminal() {
            eprint!(
                "\r  USES edges     {:>12} edges...          ",
                format_num(edge_count)
            );
        }
    }

    if skipped > 0 {
        eprintln!("  (skipped {} unresolved USES rows)", format_num(skipped));
    }

    Ok(edge_count)
}

// ============================================================================
// MAIN LOADER
// ============================================================================

/// Load the full AACT dataset from pipe-delimited flat files into GraphStore.
///
/// # Arguments
/// * `graph` - Mutable reference to GraphStore
/// * `data_dir` - Path to directory containing AACT .txt files
/// * `max_studies` - Maximum number of studies to load (0 = all)
///
/// # Returns
/// `LoadResult` with total node and edge counts.
// ============================================================================
// STEP 13: Drug enrichment from DrugBank vocabulary (offline, no API)
// ============================================================================

/// Match DRUG-type Intervention names against DrugBank vocabulary (names + synonyms).
/// Creates Drug nodes and CODED_AS_DRUG edges. No external API calls needed.
fn enrich_drugs_from_drugbank(
    graph: &mut GraphStore,
    vocab_path: &Path,
    ids: &IdMaps,
) -> Result<(usize, usize), Error> {
    // Build lookup: lowercase name/synonym -> (drugbank_id, canonical_name)
    let mut name_to_drug: HashMap<String, (String, String)> = HashMap::new();

    let file = File::open(vocab_path)?;
    let reader = BufReader::new(file);
    let mut first = true;
    let mut col_idx: HashMap<String, usize> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        if first {
            // Parse CSV header
            for (i, col) in line.split(',').enumerate() {
                col_idx.insert(col.trim().trim_matches('"').to_string(), i);
            }
            first = false;
            continue;
        }
        // Parse CSV with quoted fields
        let fields = parse_drugbank_csv(&line);
        let dbid = fields.get(*col_idx.get("DrugBank ID").unwrap_or(&0))
            .map(|s| s.trim().to_string()).unwrap_or_default();
        let common_name = fields.get(*col_idx.get("Common name").unwrap_or(&2))
            .map(|s| s.trim().to_string()).unwrap_or_default();
        let synonyms_str = fields.get(*col_idx.get("Synonyms").unwrap_or(&5))
            .map(|s| s.trim().to_string()).unwrap_or_default();

        if dbid.is_empty() || common_name.is_empty() {
            continue;
        }

        let entry = (dbid.clone(), common_name.clone());
        name_to_drug.insert(common_name.to_lowercase(), entry.clone());

        // Index synonyms
        for syn in synonyms_str.split('|') {
            let syn = syn.trim();
            if !syn.is_empty() {
                name_to_drug.entry(syn.to_lowercase())
                    .or_insert_with(|| entry.clone());
            }
        }
    }

    eprintln!("    DrugBank vocabulary: {} names/synonyms indexed", format_num(name_to_drug.len()));

    // Match interventions against DrugBank
    let mut drug_nodes: HashMap<String, NodeId> = HashMap::new();  // dbid -> NodeId
    let mut node_count = 0usize;
    let mut edge_count = 0usize;

    for (interv_name_lower, &interv_node) in &ids.intervention {
        // Check if this intervention is a DRUG type
        let is_drug = graph.get_node(interv_node)
            .and_then(|n| n.get_property("type"))
            .map(|v| matches!(v, PropertyValue::String(s) if s == "DRUG"))
            .unwrap_or(false);

        if !is_drug {
            continue;
        }

        // Try matching intervention name against DrugBank
        if let Some((dbid, canonical_name)) = name_to_drug.get(interv_name_lower) {
            let drug_node = if let Some(&existing) = drug_nodes.get(dbid) {
                existing
            } else {
                let nid = graph.create_node("Drug");
                if let Some(n) = graph.get_node_mut(nid) {
                    n.set_property("drugbank_id", PropertyValue::String(dbid.clone()));
                    n.set_property("name", PropertyValue::String(canonical_name.clone()));
                }
                drug_nodes.insert(dbid.clone(), nid);
                node_count += 1;
                nid
            };

            // Intervention -[:CODED_AS_DRUG]-> Drug
            if graph.create_edge(interv_node, drug_node, "CODED_AS_DRUG").is_ok() {
                edge_count += 1;
            }
        }
    }

    eprintln!("    Matched: {} Drug nodes, {} CODED_AS_DRUG edges (from {} drug-type interventions)",
        format_num(node_count), format_num(edge_count),
        format_num(ids.intervention.len()));

    Ok((node_count, edge_count))
}

/// Parse a CSV line with quoted fields containing commas and pipes.
fn parse_drugbank_csv(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in line.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current);
    fields
}

pub fn load_dataset(
    graph: &mut GraphStore,
    data_dir: &Path,
    max_studies: usize,
) -> Result<LoadResult, Error> {
    // Check if data files might be in a subdirectory
    let data_dir = if data_dir.join("studies.txt").exists() {
        data_dir.to_path_buf()
    } else {
        // Look one level down
        let mut found = None;
        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                if entry.path().is_dir() && entry.path().join("studies.txt").exists() {
                    found = Some(entry.path());
                    eprintln!("  Found data files in {}", entry.path().display());
                    break;
                }
            }
        }
        found.unwrap_or_else(|| data_dir.to_path_buf())
    };

    if !data_dir.join("studies.txt").exists() {
        return Err(format!(
            "studies.txt not found in {}. Download AACT flat files first.",
            data_dir.display()
        )
        .into());
    }

    let mut ids = IdMaps::new();
    let mut total_nodes = 0usize;
    let mut total_edges = 0usize;

    // ====================================================================
    // Step 1: Pre-load brief summaries
    // ====================================================================
    eprintln!("--- Step 1: Pre-loading brief summaries ---");
    let summaries = preload_brief_summaries(&data_dir)?;

    // ====================================================================
    // Step 2: Load ClinicalTrial nodes (studies.txt)
    // ====================================================================
    eprintln!("--- Step 2: Loading studies ---");
    let t = Instant::now();
    let n = load_studies(graph, &data_dir, max_studies, &summaries, &mut ids)?;
    print_done(&format!(
        "  ClinicalTrial  {:>12} nodes ({})",
        format_num(n),
        format_duration(t.elapsed())
    ));
    total_nodes += n;

    if ids.trial.is_empty() {
        eprintln!("  No studies loaded, aborting.");
        return Ok(LoadResult {
            total_nodes: 0,
            total_edges: 0,
        });
    }
    // Drop summaries to free memory
    drop(summaries);

    // ====================================================================
    // Step 3: Load conditions (Condition nodes + STUDIES edges)
    // ====================================================================
    eprintln!("--- Step 3: Loading conditions ---");
    let t = Instant::now();
    let (nn, ne) = load_conditions(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  Condition      {:>12} nodes, {:>12} STUDIES edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 4: Load interventions (Intervention nodes + TESTS edges)
    // ====================================================================
    eprintln!("--- Step 4: Loading interventions ---");
    let t = Instant::now();
    let (nn, ne) = load_interventions(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  Intervention   {:>12} nodes, {:>12} TESTS edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 5: Load design groups (ArmGroup nodes + HAS_ARM edges)
    // ====================================================================
    eprintln!("--- Step 5: Loading arm groups ---");
    let t = Instant::now();
    let (nn, ne) = load_design_groups(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  ArmGroup       {:>12} nodes, {:>12} HAS_ARM edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 6: Load design group -> intervention links (USES edges)
    // ====================================================================
    eprintln!("--- Step 6: Loading arm-intervention links ---");
    let t = Instant::now();
    let ne = load_design_group_interventions(graph, &data_dir, &ids)?;
    print_done(&format!(
        "  USES edges     {:>12} ({})",
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_edges += ne;

    // ====================================================================
    // Step 7: Load sponsors (Sponsor nodes + SPONSORED_BY edges)
    // ====================================================================
    eprintln!("--- Step 7: Loading sponsors ---");
    let t = Instant::now();
    let (nn, ne) = load_sponsors(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  Sponsor        {:>12} nodes, {:>12} SPONSORED_BY edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 8: Load outcomes (Outcome nodes + MEASURES edges)
    // ====================================================================
    eprintln!("--- Step 8: Loading outcomes ---");
    let t = Instant::now();
    let (nn, ne) = load_outcomes(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  Outcome        {:>12} nodes, {:>12} MEASURES edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 9: Load facilities (Site nodes + CONDUCTED_AT edges)
    // ====================================================================
    eprintln!("--- Step 9: Loading facilities ---");
    let t = Instant::now();
    let (nn, ne) = load_facilities(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  Site           {:>12} nodes, {:>12} CONDUCTED_AT edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 10: Load adverse events (AdverseEvent nodes + REPORTED edges)
    // ====================================================================
    eprintln!("--- Step 10: Loading adverse events ---");
    let t = Instant::now();
    let (nn, ne) = load_reported_events(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  AdverseEvent   {:>12} nodes, {:>12} REPORTED edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 11: Load MeSH cross-references (MeSHDescriptor nodes + CODED_AS_MESH edges)
    // ====================================================================
    eprintln!("--- Step 11: Loading MeSH cross-references ---");
    let t = Instant::now();
    let (nn, ne) = load_browse_conditions(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  MeSHDescriptor {:>12} nodes, {:>12} CODED_AS_MESH edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 12: Load study references (Publication nodes + PUBLISHED_IN edges)
    // ====================================================================
    eprintln!("--- Step 12: Loading publication references ---");
    let t = Instant::now();
    let (nn, ne) = load_study_references(graph, &data_dir, &mut ids)?;
    print_done(&format!(
        "  Publication    {:>12} nodes, {:>12} PUBLISHED_IN edges ({})",
        format_num(nn),
        format_num(ne),
        format_duration(t.elapsed())
    ));
    total_nodes += nn;
    total_edges += ne;

    // ====================================================================
    // Step 13: Drug enrichment (DrugBank cross-reference, no API calls)
    // ====================================================================
    let drugbank_vocab = data_dir.join("..").join("drugbank_vocabulary.csv");
    // Also check a few common locations
    let drugbank_vocab = if drugbank_vocab.exists() {
        Some(drugbank_vocab)
    } else {
        // Try sibling druginteractions-kg data dir
        let alt = data_dir.join("..").join("..").join("..").join("druginteractions-kg")
            .join("data").join("drugbank").join("drugbank_vocabulary.csv");
        if alt.exists() { Some(alt) } else { None }
    };

    if let Some(vocab_path) = drugbank_vocab {
        eprintln!("--- Step 13: Drug enrichment (DrugBank cross-reference) ---");
        let t = Instant::now();
        let (nn, ne) = enrich_drugs_from_drugbank(graph, &vocab_path, &ids)?;
        print_done(&format!(
            "  Drug           {:>12} nodes, {:>12} CODED_AS_DRUG edges ({})",
            format_num(nn),
            format_num(ne),
            format_duration(t.elapsed())
        ));
        total_nodes += nn;
        total_edges += ne;
    } else {
        eprintln!("--- Step 13: Drug enrichment skipped (no drugbank_vocabulary.csv found) ---");
        eprintln!("  Hint: place drugbank_vocabulary.csv alongside the AACT data dir,");
        eprintln!("  or ensure druginteractions-kg/data/drugbank/ is a sibling directory.");
    }

    Ok(LoadResult {
        total_nodes,
        total_edges,
    })
}
