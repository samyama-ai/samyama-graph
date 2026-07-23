//! Legal Judgments KG data loading utilities.
//!
//! Loads the Indian Supreme Court judgments (2016) CSV dataset into GraphStore via
//! direct API calls.
//!
//! Schema: 5 node labels, 4 edge types.
//!   Case{id,title,year,month}
//!   Judge{name}
//!   Party{name}
//!   Act{name}
//!   Topic{text,category}
//!
//!   (:Judge)-[:DECIDED]->(:Case)
//!   (:Party)-[:PARTY_IN{role}]->(:Case)
//!   (:Case)-[:CITES{section}]->(:Act)
//!   (:Case)-[:ABOUT]->(:Topic)
//!
//! The cited `section` is kept as a property on the CITES edge, so section-level
//! questions ("how many judgments cite IPC 302?") are answerable.
//!
//! Data source: https://huggingface.co/datasets/Shreyasrao/Indian-law-supreme-court-judgements-2016
//! License: CC-BY-4.0 (source data via Dattam Labs / AWS Open Data)

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;

use samyama_sdk::{GraphStore, NodeId, PropertyMap, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// LOAD RESULT
// ============================================================================

pub struct LoadResult {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub case_count: usize,
    pub judge_count: usize,
    pub party_count: usize,
    pub act_count: usize,
    pub topic_count: usize,
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
    if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        let mins = secs as u64 / 60;
        let rem = secs - (mins as f64 * 60.0);
        format!("{}m {:.1}s", mins, rem)
    }
}

// ============================================================================
// CSV PARSING HELPERS
// ============================================================================

/// Parse a single CSV line respecting double-quoted fields ("" → literal ").
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        current.push('"');
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            }
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            _ => current.push(c),
        }
    }
    fields.push(current);
    fields
}

/// Read a CSV file into (header, rows).
fn read_csv(path: &Path) -> Result<(Vec<String>, Vec<Vec<String>>), Error> {
    let file = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut lines = BufReader::new(file).lines();
    let header = match lines.next() {
        Some(l) => parse_csv_line(&l?),
        None => return Err(format!("{} is empty", path.display()).into()),
    };
    let mut rows = Vec::new();
    for l in lines {
        let l = l?;
        if l.trim().is_empty() {
            continue;
        }
        rows.push(parse_csv_line(&l));
    }
    Ok((header, rows))
}

/// Index of a named column in a CSV header.
fn col(header: &[String], name: &str) -> Result<usize, Error> {
    header
        .iter()
        .position(|h| h == name)
        .ok_or_else(|| format!("missing column '{name}'").into())
}

fn csv_field(fields: &[String], idx: usize) -> &str {
    fields.get(idx).map(|s| s.as_str()).unwrap_or("")
}

// ============================================================================
// DATASET LOADER
// ============================================================================

/// The 9 CSV files this loader expects in `data_dir`.
pub const REQUIRED_FILES: [&str; 9] = [
    "judges.csv",
    "cases.csv",
    "parties.csv",
    "acts.csv",
    "topics.csv",
    "edge_decided.csv",
    "edge_party_in.csv",
    "edge_cites.csv",
    "edge_about.csv",
];

pub fn load_dataset(graph: &mut GraphStore, data_dir: &Path) -> Result<LoadResult, Error> {
    let mut edge_count = 0usize;

    // ---- NODES (name → NodeId maps let us wire the edges up afterwards) ----

    let mut judge_ids: HashMap<String, NodeId> = HashMap::new();
    let (h, rows) = read_csv(&data_dir.join("judges.csv"))?;
    let c_name = col(&h, "name")?;
    for r in &rows {
        let name = csv_field(r, c_name).to_string();
        let nid = graph.create_node("Judge");
        if let Some(n) = graph.get_node_mut(nid) {
            n.set_property("name", PropertyValue::String(name.clone()));
        }
        judge_ids.insert(name, nid);
    }

    let mut party_ids: HashMap<String, NodeId> = HashMap::new();
    let (h, rows) = read_csv(&data_dir.join("parties.csv"))?;
    let c_name = col(&h, "name")?;
    for r in &rows {
        let name = csv_field(r, c_name).to_string();
        let nid = graph.create_node("Party");
        if let Some(n) = graph.get_node_mut(nid) {
            n.set_property("name", PropertyValue::String(name.clone()));
        }
        party_ids.insert(name, nid);
    }

    let mut act_ids: HashMap<String, NodeId> = HashMap::new();
    let (h, rows) = read_csv(&data_dir.join("acts.csv"))?;
    let c_name = col(&h, "name")?;
    for r in &rows {
        let name = csv_field(r, c_name).to_string();
        let nid = graph.create_node("Act");
        if let Some(n) = graph.get_node_mut(nid) {
            n.set_property("name", PropertyValue::String(name.clone()));
        }
        act_ids.insert(name, nid);
    }

    let mut topic_ids: HashMap<String, NodeId> = HashMap::new();
    let (h, rows) = read_csv(&data_dir.join("topics.csv"))?;
    let (c_text, c_cat) = (col(&h, "text")?, col(&h, "category")?);
    for r in &rows {
        let text = csv_field(r, c_text).to_string();
        let nid = graph.create_node("Topic");
        if let Some(n) = graph.get_node_mut(nid) {
            n.set_property("text", PropertyValue::String(text.clone()));
            n.set_property(
                "category",
                PropertyValue::String(csv_field(r, c_cat).to_string()),
            );
        }
        topic_ids.insert(text, nid);
    }

    let mut case_ids: HashMap<String, NodeId> = HashMap::new();
    let (h, rows) = read_csv(&data_dir.join("cases.csv"))?;
    let (c_id, c_title) = (col(&h, "id")?, col(&h, "title")?);
    let (c_year, c_month) = (col(&h, "year")?, col(&h, "month")?);
    for r in &rows {
        let id = csv_field(r, c_id).to_string();
        let nid = graph.create_node("Case");
        if let Some(n) = graph.get_node_mut(nid) {
            n.set_property("id", PropertyValue::String(id.clone()));
            n.set_property(
                "title",
                PropertyValue::String(csv_field(r, c_title).to_string()),
            );
            if let Ok(y) = csv_field(r, c_year).parse::<i64>() {
                n.set_property("year", PropertyValue::Integer(y));
            }
            if let Ok(m) = csv_field(r, c_month).parse::<i64>() {
                n.set_property("month", PropertyValue::Integer(m));
            }
        }
        case_ids.insert(id, nid);
    }

    // ---- EDGES ----

    // (:Judge)-[:DECIDED]->(:Case)
    let (h, rows) = read_csv(&data_dir.join("edge_decided.csv"))?;
    let (c_j, c_c) = (col(&h, "judge_name")?, col(&h, "case_id")?);
    for r in &rows {
        if let (Some(&j), Some(&c)) = (
            judge_ids.get(csv_field(r, c_j)),
            case_ids.get(csv_field(r, c_c)),
        ) {
            graph.create_edge(j, c, "DECIDED")?;
            edge_count += 1;
        }
    }

    // (:Party)-[:PARTY_IN{role}]->(:Case)
    let (h, rows) = read_csv(&data_dir.join("edge_party_in.csv"))?;
    let (c_p, c_c) = (col(&h, "party_name")?, col(&h, "case_id")?);
    let c_role = col(&h, "role").ok();
    for r in &rows {
        if let (Some(&p), Some(&c)) = (
            party_ids.get(csv_field(r, c_p)),
            case_ids.get(csv_field(r, c_c)),
        ) {
            let role = c_role.map(|i| csv_field(r, i)).unwrap_or("");
            if role.is_empty() {
                graph.create_edge(p, c, "PARTY_IN")?;
            } else {
                let mut props = PropertyMap::new();
                props.insert("role".to_string(), PropertyValue::String(role.to_string()));
                graph.create_edge_with_properties(p, c, "PARTY_IN", props)?;
            }
            edge_count += 1;
        }
    }

    // (:Case)-[:CITES{section}]->(:Act)  — the section rides on the edge
    let (h, rows) = read_csv(&data_dir.join("edge_cites.csv"))?;
    let (c_c, c_a, c_s) = (col(&h, "case_id")?, col(&h, "act")?, col(&h, "section")?);
    for r in &rows {
        if let (Some(&c), Some(&a)) = (
            case_ids.get(csv_field(r, c_c)),
            act_ids.get(csv_field(r, c_a)),
        ) {
            let mut props = PropertyMap::new();
            props.insert(
                "section".to_string(),
                PropertyValue::String(csv_field(r, c_s).to_string()),
            );
            graph.create_edge_with_properties(c, a, "CITES", props)?;
            edge_count += 1;
        }
    }

    // (:Case)-[:ABOUT]->(:Topic)
    let (h, rows) = read_csv(&data_dir.join("edge_about.csv"))?;
    let (c_c, c_t) = (col(&h, "case_id")?, col(&h, "topic_text")?);
    for r in &rows {
        if let (Some(&c), Some(&t)) = (
            case_ids.get(csv_field(r, c_c)),
            topic_ids.get(csv_field(r, c_t)),
        ) {
            graph.create_edge(c, t, "ABOUT")?;
            edge_count += 1;
        }
    }

    let total_nodes =
        case_ids.len() + judge_ids.len() + party_ids.len() + act_ids.len() + topic_ids.len();

    Ok(LoadResult {
        total_nodes,
        total_edges: edge_count,
        case_count: case_ids.len(),
        judge_count: judge_ids.len(),
        party_count: party_ids.len(),
        act_count: act_ids.len(),
        topic_count: topic_ids.len(),
    })
}
