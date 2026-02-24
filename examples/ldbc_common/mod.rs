//! Shared LDBC SNB SF1 data loading utilities.
//!
//! Used by both `ldbc_loader` and `ldbc_benchmark` examples.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, IsTerminal};
use std::path::Path;

use samyama_sdk::{GraphStore, NodeId, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// ID MAPPINGS
// ============================================================================

pub struct IdMaps {
    pub place: HashMap<i64, NodeId>,
    pub organisation: HashMap<i64, NodeId>,
    pub tag: HashMap<i64, NodeId>,
    pub tagclass: HashMap<i64, NodeId>,
    pub person: HashMap<i64, NodeId>,
    pub forum: HashMap<i64, NodeId>,
    pub post: HashMap<i64, NodeId>,
    pub comment: HashMap<i64, NodeId>,
}

impl IdMaps {
    pub fn new() -> Self {
        Self {
            place: HashMap::new(),
            organisation: HashMap::new(),
            tag: HashMap::new(),
            tagclass: HashMap::new(),
            person: HashMap::new(),
            forum: HashMap::new(),
            post: HashMap::new(),
            comment: HashMap::new(),
        }
    }
}

// ============================================================================
// GENERIC HELPERS
// ============================================================================

/// Load nodes from a pipe-delimited CSV file.
/// `parse_props` receives the header-keyed row and returns (key, PropertyValue) pairs.
pub fn load_nodes<F>(
    path: &Path,
    label: &str,
    graph: &mut GraphStore,
    id_map: &mut HashMap<i64, NodeId>,
    parse_props: F,
) -> Result<usize, Error>
where
    F: Fn(&[&str], &[&str]) -> Vec<(&'static str, PropertyValue)>,
{
    if !path.exists() {
        eprintln!("  WARNING: {} not found, skipping", path.display());
        return Ok(0);
    }

    let file = File::open(path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('|').collect();

    let id_col = headers.iter().position(|h| *h == "id")
        .ok_or_else(|| format!("No 'id' column in {}", path.display()))?;

    let mut count = 0usize;
    for line_result in lines {
        let line = line_result?;
        if line.is_empty() { continue; }

        let fields: Vec<&str> = line.split('|').collect();
        if fields.len() <= id_col { continue; }

        let ldbc_id: i64 = fields[id_col].parse()?;

        let node_id = graph.create_node(label);

        // Set properties
        let props = parse_props(&headers, &fields);
        if let Some(node) = graph.get_node_mut(node_id) {
            for (key, val) in props {
                node.set_property(key, val);
            }
            // Store the LDBC id as a property too
            node.set_property("id", ldbc_id);
        }

        id_map.insert(ldbc_id, node_id);
        count += 1;

        if count % 500_000 == 0 && io::stderr().is_terminal() {
            eprint!("\r  {:16} {:>12} nodes...          ", label, format_num(count));
        }
    }

    Ok(count)
}

/// Load edges from a pipe-delimited CSV file.
/// `parse_props` receives (headers, fields) and returns edge properties (may be empty).
pub fn load_edges<F>(
    path: &Path,
    edge_type: &str,
    graph: &mut GraphStore,
    src_map: &HashMap<i64, NodeId>,
    tgt_map: &HashMap<i64, NodeId>,
    parse_props: F,
) -> Result<usize, Error>
where
    F: Fn(&[&str], &[&str]) -> Vec<(&'static str, PropertyValue)>,
{
    if !path.exists() {
        eprintln!("  WARNING: {} not found, skipping", path.display());
        return Ok(0);
    }

    let file = File::open(path)?;
    let reader = BufReader::with_capacity(1 << 16, file);
    let mut lines = reader.lines();

    let header = lines.next().ok_or("Empty file")??;
    let headers: Vec<&str> = header.split('|').collect();

    let mut count = 0usize;
    let mut skipped = 0usize;
    for line_result in lines {
        let line = line_result?;
        if line.is_empty() { continue; }

        let fields: Vec<&str> = line.split('|').collect();
        if fields.len() < 2 { continue; }

        let src_id: i64 = match fields[0].parse() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };
        let tgt_id: i64 = match fields[1].parse() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };

        let src_node = match src_map.get(&src_id) {
            Some(&n) => n,
            None => { skipped += 1; continue; }
        };
        let tgt_node = match tgt_map.get(&tgt_id) {
            Some(&n) => n,
            None => { skipped += 1; continue; }
        };

        match graph.create_edge(src_node, tgt_node, edge_type) {
            Ok(edge_id) => {
                let props = parse_props(&headers, &fields);
                if !props.is_empty() {
                    if let Some(edge) = graph.get_edge_mut(edge_id) {
                        for (key, val) in props {
                            edge.set_property(key, val);
                        }
                    }
                }
                count += 1;
            }
            Err(_) => { skipped += 1; }
        }

        if count % 500_000 == 0 && count > 0 && io::stderr().is_terminal() {
            eprint!("\r  {:42} {:>12} edges...          ", edge_type, format_num(count));
        }
    }

    if skipped > 0 {
        eprintln!("  (skipped {} rows for {})", format_num(skipped), edge_type);
    }

    Ok(count)
}

// ============================================================================
// PROPERTY PARSERS
// ============================================================================

pub fn field_str(headers: &[&str], fields: &[&str], name: &str) -> Option<String> {
    headers.iter().position(|h| *h == name)
        .and_then(|i| fields.get(i))
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
}

pub fn field_i64(headers: &[&str], fields: &[&str], name: &str) -> Option<i64> {
    headers.iter().position(|h| *h == name)
        .and_then(|i| fields.get(i))
        .and_then(|v| v.parse().ok())
}

pub fn props_place(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "name") { props.push(("name", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "url") { props.push(("url", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "type") { props.push(("type", PropertyValue::String(v))); }
    props
}

pub fn props_organisation(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "type") { props.push(("type", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "name") { props.push(("name", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "url") { props.push(("url", PropertyValue::String(v))); }
    props
}

pub fn props_tag(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "name") { props.push(("name", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "url") { props.push(("url", PropertyValue::String(v))); }
    props
}

pub fn props_tagclass(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    props_tag(headers, fields)
}

pub fn props_person(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "firstName") { props.push(("firstName", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "lastName") { props.push(("lastName", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "gender") { props.push(("gender", PropertyValue::String(v))); }
    if let Some(v) = field_i64(headers, fields, "birthday") { props.push(("birthday", PropertyValue::DateTime(v))); }
    if let Some(v) = field_i64(headers, fields, "creationDate") { props.push(("creationDate", PropertyValue::DateTime(v))); }
    if let Some(v) = field_str(headers, fields, "locationIP") { props.push(("locationIP", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "browserUsed") { props.push(("browserUsed", PropertyValue::String(v))); }
    props
}

pub fn props_forum(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "title") { props.push(("title", PropertyValue::String(v))); }
    if let Some(v) = field_i64(headers, fields, "creationDate") { props.push(("creationDate", PropertyValue::DateTime(v))); }
    props
}

pub fn props_post(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_str(headers, fields, "imageFile") { props.push(("imageFile", PropertyValue::String(v))); }
    if let Some(v) = field_i64(headers, fields, "creationDate") { props.push(("creationDate", PropertyValue::DateTime(v))); }
    if let Some(v) = field_str(headers, fields, "locationIP") { props.push(("locationIP", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "browserUsed") { props.push(("browserUsed", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "language") { props.push(("language", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "content") { props.push(("content", PropertyValue::String(v))); }
    if let Some(v) = field_i64(headers, fields, "length") { props.push(("length", PropertyValue::Integer(v))); }
    props
}

pub fn props_comment(headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = field_i64(headers, fields, "creationDate") { props.push(("creationDate", PropertyValue::DateTime(v))); }
    if let Some(v) = field_str(headers, fields, "locationIP") { props.push(("locationIP", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "browserUsed") { props.push(("browserUsed", PropertyValue::String(v))); }
    if let Some(v) = field_str(headers, fields, "content") { props.push(("content", PropertyValue::String(v))); }
    if let Some(v) = field_i64(headers, fields, "length") { props.push(("length", PropertyValue::Integer(v))); }
    props
}

// Edge property parsers

pub fn no_props(_headers: &[&str], _fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    Vec::new()
}

pub fn props_creation_date(_headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if fields.len() > 2 {
        if let Ok(v) = fields[2].parse::<i64>() {
            props.push(("creationDate", PropertyValue::DateTime(v)));
        }
    }
    props
}

pub fn props_class_year(_headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if fields.len() > 2 {
        if let Ok(v) = fields[2].parse::<i64>() {
            props.push(("classYear", PropertyValue::Integer(v)));
        }
    }
    props
}

pub fn props_work_from(_headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if fields.len() > 2 {
        if let Ok(v) = fields[2].parse::<i64>() {
            props.push(("workFrom", PropertyValue::Integer(v)));
        }
    }
    props
}

pub fn props_join_date(_headers: &[&str], fields: &[&str]) -> Vec<(&'static str, PropertyValue)> {
    let mut props = Vec::new();
    if fields.len() > 2 {
        if let Ok(v) = fields[2].parse::<i64>() {
            props.push(("joinDate", PropertyValue::DateTime(v)));
        }
    }
    props
}

// ============================================================================
// MULTI-VALUE ATTRIBUTES
// ============================================================================

/// Load person_email_emailaddress and person_speaks_language into person nodes.
/// Multiple rows per person → stored as Array property.
pub fn load_multi_value_attrs(
    graph: &mut GraphStore,
    data_dir: &Path,
    person_ids: &HashMap<i64, NodeId>,
) -> Result<(), Error> {
    // Email addresses
    let email_path = data_dir.join("dynamic/person_email_emailaddress_0_0.csv");
    if email_path.exists() {
        let mut email_map: HashMap<i64, Vec<String>> = HashMap::new();
        let file = File::open(&email_path)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        lines.next(); // skip header
        for line_result in lines {
            let line = line_result?;
            if line.is_empty() { continue; }
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() < 2 { continue; }
            if let Ok(pid) = parts[0].parse::<i64>() {
                email_map.entry(pid).or_default().push(parts[1].to_string());
            }
        }
        for (pid, emails) in &email_map {
            if let Some(&node_id) = person_ids.get(pid) {
                if let Some(node) = graph.get_node_mut(node_id) {
                    if emails.len() == 1 {
                        node.set_property("email", PropertyValue::String(emails[0].clone()));
                    } else {
                        let arr: Vec<PropertyValue> = emails.iter()
                            .map(|e| PropertyValue::String(e.clone()))
                            .collect();
                        node.set_property("email", PropertyValue::Array(arr));
                    }
                }
            }
        }
        eprintln!("  Enriched {} persons with email addresses", format_num(email_map.len()));
    }

    // Languages
    let lang_path = data_dir.join("dynamic/person_speaks_language_0_0.csv");
    if lang_path.exists() {
        let mut lang_map: HashMap<i64, Vec<String>> = HashMap::new();
        let file = File::open(&lang_path)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        lines.next(); // skip header
        for line_result in lines {
            let line = line_result?;
            if line.is_empty() { continue; }
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() < 2 { continue; }
            if let Ok(pid) = parts[0].parse::<i64>() {
                lang_map.entry(pid).or_default().push(parts[1].to_string());
            }
        }
        for (pid, langs) in &lang_map {
            if let Some(&node_id) = person_ids.get(pid) {
                if let Some(node) = graph.get_node_mut(node_id) {
                    if langs.len() == 1 {
                        node.set_property("speaks", PropertyValue::String(langs[0].clone()));
                    } else {
                        let arr: Vec<PropertyValue> = langs.iter()
                            .map(|l| PropertyValue::String(l.clone()))
                            .collect();
                        node.set_property("speaks", PropertyValue::Array(arr));
                    }
                }
            }
        }
        eprintln!("  Enriched {} persons with languages", format_num(lang_map.len()));
    }

    Ok(())
}

// ============================================================================
// FORMATTING
// ============================================================================

pub fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { result.push(','); }
        result.push(ch);
    }
    result.chars().rev().collect()
}

pub fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else {
        format!("{:.1}s", secs)
    }
}

/// Print a final summary line, clearing any inline progress.
pub fn print_done(msg: &str) {
    eprintln!("\r{:80}", msg);
}

// ============================================================================
// FULL DATASET LOADER
// ============================================================================

/// Load result with node/edge counts.
pub struct LoadResult {
    pub total_nodes: usize,
    pub total_edges: usize,
}

/// Load the entire LDBC SF1 dataset into the given GraphStore.
/// Returns total node and edge counts.
pub fn load_dataset(graph: &mut GraphStore, data_dir: &Path) -> Result<LoadResult, Error> {
    let mut ids = IdMaps::new();
    let mut total_nodes = 0usize;
    let mut total_edges = 0usize;

    let static_dir = data_dir.join("static");
    let dynamic_dir = data_dir.join("dynamic");

    // ====================================================================
    // PHASE 1: Load all nodes
    // ====================================================================
    eprintln!("=== Phase 1: Loading Nodes ===");

    let t = std::time::Instant::now();
    let n = load_nodes(&static_dir.join("place_0_0.csv"), "Place", graph, &mut ids.place, props_place)?;
    print_done(&format!("  Place:         {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&static_dir.join("organisation_0_0.csv"), "Organisation", graph, &mut ids.organisation, props_organisation)?;
    print_done(&format!("  Organisation:  {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&static_dir.join("tag_0_0.csv"), "Tag", graph, &mut ids.tag, props_tag)?;
    print_done(&format!("  Tag:           {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&static_dir.join("tagclass_0_0.csv"), "TagClass", graph, &mut ids.tagclass, props_tagclass)?;
    print_done(&format!("  TagClass:      {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&dynamic_dir.join("person_0_0.csv"), "Person", graph, &mut ids.person, props_person)?;
    print_done(&format!("  Person:        {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&dynamic_dir.join("forum_0_0.csv"), "Forum", graph, &mut ids.forum, props_forum)?;
    print_done(&format!("  Forum:         {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&dynamic_dir.join("post_0_0.csv"), "Post", graph, &mut ids.post, props_post)?;
    print_done(&format!("  Post:          {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    let t = std::time::Instant::now();
    let n = load_nodes(&dynamic_dir.join("comment_0_0.csv"), "Comment", graph, &mut ids.comment, props_comment)?;
    print_done(&format!("  Comment:       {:>12} nodes ({})", format_num(n), format_duration(t.elapsed())));
    total_nodes += n;

    eprintln!();

    // ====================================================================
    // PHASE 1b: Multi-value attributes
    // ====================================================================
    eprintln!("=== Phase 1b: Multi-Value Attributes ===");
    load_multi_value_attrs(graph, data_dir, &ids.person)?;
    eprintln!();

    // ====================================================================
    // PHASE 2: Load all edges
    // ====================================================================
    eprintln!("=== Phase 2: Loading Edges ===");

    // --- Static edges ---

    let t = std::time::Instant::now();
    let n = load_edges(&static_dir.join("place_isPartOf_place_0_0.csv"), "IS_PART_OF", graph, &ids.place, &ids.place, no_props)?;
    print_done(&format!("  IS_PART_OF (Place->Place):             {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&static_dir.join("organisation_isLocatedIn_place_0_0.csv"), "IS_LOCATED_IN", graph, &ids.organisation, &ids.place, no_props)?;
    print_done(&format!("  IS_LOCATED_IN (Org->Place):            {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&static_dir.join("tag_hasType_tagclass_0_0.csv"), "HAS_TYPE", graph, &ids.tag, &ids.tagclass, no_props)?;
    print_done(&format!("  HAS_TYPE (Tag->TagClass):              {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&static_dir.join("tagclass_isSubclassOf_tagclass_0_0.csv"), "IS_SUBCLASS_OF", graph, &ids.tagclass, &ids.tagclass, no_props)?;
    print_done(&format!("  IS_SUBCLASS_OF (TagClass->TagClass):   {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    // --- Dynamic edges ---

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("person_isLocatedIn_place_0_0.csv"), "IS_LOCATED_IN", graph, &ids.person, &ids.place, no_props)?;
    print_done(&format!("  IS_LOCATED_IN (Person->Place):         {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("person_knows_person_0_0.csv"), "KNOWS", graph, &ids.person, &ids.person, props_creation_date)?;
    print_done(&format!("  KNOWS (Person->Person):                {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("person_hasInterest_tag_0_0.csv"), "HAS_INTEREST", graph, &ids.person, &ids.tag, no_props)?;
    print_done(&format!("  HAS_INTEREST (Person->Tag):            {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("person_studyAt_organisation_0_0.csv"), "STUDY_AT", graph, &ids.person, &ids.organisation, props_class_year)?;
    print_done(&format!("  STUDY_AT (Person->Org):                {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("person_workAt_organisation_0_0.csv"), "WORK_AT", graph, &ids.person, &ids.organisation, props_work_from)?;
    print_done(&format!("  WORK_AT (Person->Org):                 {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("person_likes_post_0_0.csv"), "LIKES", graph, &ids.person, &ids.post, props_creation_date)?;
    print_done(&format!("  LIKES (Person->Post):                  {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("person_likes_comment_0_0.csv"), "LIKES", graph, &ids.person, &ids.comment, props_creation_date)?;
    print_done(&format!("  LIKES (Person->Comment):               {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("forum_hasModerator_person_0_0.csv"), "HAS_MODERATOR", graph, &ids.forum, &ids.person, no_props)?;
    print_done(&format!("  HAS_MODERATOR (Forum->Person):         {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("forum_hasMember_person_0_0.csv"), "HAS_MEMBER", graph, &ids.forum, &ids.person, props_join_date)?;
    print_done(&format!("  HAS_MEMBER (Forum->Person):            {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("forum_hasTag_tag_0_0.csv"), "HAS_TAG", graph, &ids.forum, &ids.tag, no_props)?;
    print_done(&format!("  HAS_TAG (Forum->Tag):                  {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("forum_containerOf_post_0_0.csv"), "CONTAINER_OF", graph, &ids.forum, &ids.post, no_props)?;
    print_done(&format!("  CONTAINER_OF (Forum->Post):            {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("post_hasCreator_person_0_0.csv"), "HAS_CREATOR", graph, &ids.post, &ids.person, no_props)?;
    print_done(&format!("  HAS_CREATOR (Post->Person):            {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("post_hasTag_tag_0_0.csv"), "HAS_TAG", graph, &ids.post, &ids.tag, no_props)?;
    print_done(&format!("  HAS_TAG (Post->Tag):                   {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("post_isLocatedIn_place_0_0.csv"), "IS_LOCATED_IN", graph, &ids.post, &ids.place, no_props)?;
    print_done(&format!("  IS_LOCATED_IN (Post->Place):           {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("comment_hasCreator_person_0_0.csv"), "HAS_CREATOR", graph, &ids.comment, &ids.person, no_props)?;
    print_done(&format!("  HAS_CREATOR (Comment->Person):         {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("comment_hasTag_tag_0_0.csv"), "HAS_TAG", graph, &ids.comment, &ids.tag, no_props)?;
    print_done(&format!("  HAS_TAG (Comment->Tag):                {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("comment_isLocatedIn_place_0_0.csv"), "IS_LOCATED_IN", graph, &ids.comment, &ids.place, no_props)?;
    print_done(&format!("  IS_LOCATED_IN (Comment->Place):        {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("comment_replyOf_comment_0_0.csv"), "REPLY_OF", graph, &ids.comment, &ids.comment, no_props)?;
    print_done(&format!("  REPLY_OF (Comment->Comment):           {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    let t = std::time::Instant::now();
    let n = load_edges(&dynamic_dir.join("comment_replyOf_post_0_0.csv"), "REPLY_OF", graph, &ids.comment, &ids.post, no_props)?;
    print_done(&format!("  REPLY_OF (Comment->Post):              {:>12} edges ({})", format_num(n), format_duration(t.elapsed())));
    total_edges += n;

    Ok(LoadResult { total_nodes, total_edges })
}
