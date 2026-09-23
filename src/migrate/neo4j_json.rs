//! Import a Neo4j graph from `apoc.export.json` output.
//!
//! # The format
//!
//! `apoc.export.json.all(file, {})` writes JSON Lines — one JSON object per
//! line, nodes and relationships interleaved:
//!
//! ```text
//! {"type":"node","id":"0","labels":["Person"],"properties":{"name":"Alice"}}
//! {"type":"relationship","id":"0","label":"KNOWS",
//!  "start":{"id":"0","labels":["Person"]},"end":{"id":"1","labels":["Person"]},
//!  "properties":{"since":2019}}
//! ```
//!
//! With `{jsonFormat:'JSON'}` it writes a single object holding two arrays
//! instead. Both are accepted, because which one a user has depends on an APOC
//! option they may not have chosen deliberately.
//!
//! # What this does not do
//!
//! **It does not guess types.** JSON has no date and no point. APOC writes a
//! `ZonedDateTime` as the ISO-8601 string `"2019-06-01T12:00:00Z"` and a point
//! as a map carrying a `crs` key, and both are indistinguishable from a string
//! or a map that a user actually stored. Converting on sight would silently
//! turn a real string property — a version number, an identifier, a log line —
//! into a date, and that damage is invisible until a query compares it.
//!
//! So values arrive with exactly the JSON type they had, and the report counts
//! the ones that *look* convertible under [`ImportReport::values_that_look_temporal`]
//! and [`ImportReport::values_that_look_spatial`]. That count is the finding: it
//! tells a migrating user how much of their data needs a deliberate `SET
//! n.prop = datetime(n.prop)` pass, rather than leaving them to discover it.
//!
//! # What it refuses to lose quietly
//!
//! Every input line falls into exactly one bucket in the report. A relationship
//! whose endpoints never appear is *dangling*, not dropped: APOC does not
//! promise nodes come first, so relationships are buffered and resolved at the
//! end, and only then can an endpoint be called missing. A property whose value
//! is JSON `null` is counted as dropped, because Neo4j cannot store a null and
//! a null in the file means the exporter, not the graph, produced it.

use crate::graph::{GraphStore, NodeId, PropertyValue};
use serde_json::Value as Json;
use std::collections::HashMap;

/// What an import did, in terms a migrating user can check against their source.
///
/// Every count here is written by the import itself, not re-derived afterwards
/// from the resulting store — a count taken from the destination cannot notice
/// anything that failed to arrive.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ImportReport {
    /// Lines read, including any that were neither a node nor a relationship.
    pub records_read: usize,
    pub nodes_created: usize,
    pub edges_created: usize,
    pub node_properties_set: usize,
    pub edge_properties_set: usize,
    pub labels_applied: usize,
    /// Relationships whose `start` or `end` id never appeared as a node.
    ///
    /// These are the ones a partial export produces — `apoc.export.json.query`
    /// over a subgraph exports relationships whose far end was not selected.
    pub dangling_edges: usize,
    /// The Neo4j ids of the endpoints that were never defined, deduplicated and
    /// sorted. Named rather than counted, because "3 edges dangled" does not
    /// tell a user which part of their export was short.
    pub missing_endpoints: Vec<String>,
    /// Properties whose value was JSON `null`.
    pub null_properties_dropped: usize,
    /// String values matching an ISO-8601 date, time or datetime.
    ///
    /// Imported as strings. See the module docs: this count is advice, not a
    /// defect.
    pub values_that_look_temporal: usize,
    /// Map values carrying a `crs` key, which is how APOC writes a `Point`.
    pub values_that_look_spatial: usize,
    /// Lines that parsed as JSON but carried a `type` this importer does not
    /// know, with the value seen.
    pub unknown_record_types: Vec<String>,
    /// Lines that were not valid JSON, as `(line number, message)`.
    pub unparseable_lines: Vec<(usize, String)>,
}

impl ImportReport {
    /// True when every line read turned into graph data.
    ///
    /// Deliberately strict: a dangling relationship or an unparseable line
    /// makes this false even though the import produced a usable graph. A
    /// migration that quietly lost an edge is the failure worth catching, and
    /// the individual counts say which kind of loss it was.
    pub fn lossless(&self) -> bool {
        self.dangling_edges == 0
            && self.null_properties_dropped == 0
            && self.unknown_record_types.is_empty()
            && self.unparseable_lines.is_empty()
    }
}

/// Import errors that stop the whole file, as opposed to the per-record
/// problems the report records.
#[derive(Debug)]
pub enum ImportError {
    /// The file was neither JSON Lines nor a single JSON object with `nodes`
    /// and `rels` arrays.
    UnrecognizedShape(String),
    Graph(crate::graph::GraphError),
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::UnrecognizedShape(s) => write!(
                f,
                "not an apoc.export.json file: {s}. Expected JSON Lines (apoc's default) \
                 or a single object with `nodes` and `rels` arrays (jsonFormat:'JSON')"
            ),
            ImportError::Graph(e) => write!(f, "graph error during import: {e}"),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<crate::graph::GraphError> for ImportError {
    fn from(e: crate::graph::GraphError) -> Self {
        ImportError::Graph(e)
    }
}

/// Import `apoc.export.json` text into `store`, appending to whatever it holds.
pub fn import_str(
    text: &str,
    store: &mut GraphStore,
    tenant_id: &str,
) -> Result<ImportReport, ImportError> {
    let mut report = ImportReport::default();
    let records = split_records(text, &mut report)?;

    // Neo4j id -> our id. Neo4j ids are strings in the JSON export even though
    // they are integers in the database, and a user may have rewritten them, so
    // they are kept as strings rather than parsed.
    let mut node_ids: HashMap<String, NodeId> = HashMap::new();
    // Relationships are buffered: APOC does not promise nodes are written
    // first, and a relationship read before its endpoints is not yet dangling.
    let mut pending: Vec<Json> = Vec::new();

    for record in records {
        match record.get("type").and_then(|t| t.as_str()) {
            Some("node") => import_node(&record, store, tenant_id, &mut node_ids, &mut report)?,
            Some("relationship") => pending.push(record),
            Some(other) => {
                let other = other.to_string();
                if !report.unknown_record_types.contains(&other) {
                    report.unknown_record_types.push(other);
                }
            }
            None => {
                if !report
                    .unknown_record_types
                    .iter()
                    .any(|t| t == "(no `type` field)")
                {
                    report
                        .unknown_record_types
                        .push("(no `type` field)".to_string());
                }
            }
        }
    }

    let mut missing: Vec<String> = Vec::new();
    for record in pending {
        import_relationship(&record, store, &node_ids, &mut report, &mut missing)?;
    }
    missing.sort();
    missing.dedup();
    report.missing_endpoints = missing;

    Ok(report)
}

/// Both APOC shapes, reduced to a list of records.
fn split_records(text: &str, report: &mut ImportReport) -> Result<Vec<Json>, ImportError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    // `jsonFormat:'JSON'`: one object, two arrays. Tried first because a
    // pretty-printed object is also multi-line and would fail line-by-line.
    if let Ok(Json::Object(map)) = serde_json::from_str::<Json>(trimmed) {
        let has_arrays = map.get("nodes").is_some_and(|v| v.is_array())
            || map.get("rels").is_some_and(|v| v.is_array())
            || map.get("relationships").is_some_and(|v| v.is_array());
        if has_arrays {
            let mut out = Vec::new();
            for key in ["nodes", "rels", "relationships"] {
                if let Some(Json::Array(items)) = map.get(key) {
                    for item in items {
                        let mut item = item.clone();
                        // The array form omits `type`, which the object form
                        // carries. Supply it so one code path handles both.
                        if item.get("type").is_none() {
                            if let Json::Object(o) = &mut item {
                                let t = if key == "nodes" {
                                    "node"
                                } else {
                                    "relationship"
                                };
                                o.insert("type".to_string(), Json::String(t.to_string()));
                            }
                        }
                        out.push(item);
                    }
                }
            }
            report.records_read = out.len();
            return Ok(out);
        }
    }

    // JSON Lines.
    let mut out = Vec::new();
    for (n, line) in trimmed.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        report.records_read += 1;
        match serde_json::from_str::<Json>(line) {
            Ok(v) => out.push(v),
            Err(e) => report.unparseable_lines.push((n + 1, e.to_string())),
        }
    }

    if out.is_empty() && !report.unparseable_lines.is_empty() {
        return Err(ImportError::UnrecognizedShape(format!(
            "no line parsed as JSON (first error on line {}: {})",
            report.unparseable_lines[0].0, report.unparseable_lines[0].1
        )));
    }
    Ok(out)
}

fn import_node(
    record: &Json,
    store: &mut GraphStore,
    tenant_id: &str,
    node_ids: &mut HashMap<String, NodeId>,
    report: &mut ImportReport,
) -> Result<(), ImportError> {
    let labels: Vec<String> = record
        .get("labels")
        .and_then(|l| l.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();

    // Neo4j allows a node with no labels, and so does the store —
    // `create_node_with_labels` takes an iterator that may be empty. Inventing
    // a placeholder label here would make the imported graph answer
    // `MATCH (n:Resource)` for nodes that never had it, which is the mistake
    // the RDF round trip made.
    let id = store.create_node_with_labels(labels.iter().map(|l| l.as_str().into()));
    report.nodes_created += 1;
    report.labels_applied += labels.len();

    if let Some(Json::Object(props)) = record.get("properties") {
        for (key, value) in props {
            match convert(value, report) {
                Some(v) => {
                    store.set_node_property(tenant_id, id, key.clone(), v)?;
                    report.node_properties_set += 1;
                }
                None => report.null_properties_dropped += 1,
            }
        }
    }

    if let Some(neo_id) = record.get("id").and_then(json_id) {
        node_ids.insert(neo_id, id);
    }
    Ok(())
}

fn import_relationship(
    record: &Json,
    store: &mut GraphStore,
    node_ids: &HashMap<String, NodeId>,
    report: &mut ImportReport,
    missing: &mut Vec<String>,
) -> Result<(), ImportError> {
    // APOC writes the endpoint as an object carrying the id and labels; some
    // versions and some hand-written files use a bare id.
    let endpoint = |key: &str| -> Option<String> {
        let v = record.get(key)?;
        v.get("id").and_then(json_id).or_else(|| json_id(v))
    };

    let (start, end) = (endpoint("start"), endpoint("end"));
    let resolved = |e: &Option<String>| e.as_ref().and_then(|id| node_ids.get(id).copied());

    match (resolved(&start), resolved(&end)) {
        (Some(s), Some(t)) => {
            // `label` in APOC's JSON; `type` is taken by the record kind.
            let edge_type = record
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("RELATED_TO");
            let edge = store.create_edge(s, t, edge_type)?;
            report.edges_created += 1;

            if let Some(Json::Object(props)) = record.get("properties") {
                for (key, value) in props {
                    match convert(value, report) {
                        Some(v) => {
                            store.set_edge_property(edge, key.clone(), v)?;
                            report.edge_properties_set += 1;
                        }
                        None => report.null_properties_dropped += 1,
                    }
                }
            }
        }
        _ => {
            report.dangling_edges += 1;
            for (side, seen) in [(&start, resolved(&start)), (&end, resolved(&end))] {
                if seen.is_none() {
                    missing.push(side.clone().unwrap_or_else(|| "(no id)".to_string()));
                }
            }
        }
    }
    Ok(())
}

/// An id as a string, whether the file wrote it quoted or bare.
fn json_id(v: &Json) -> Option<String> {
    match v {
        Json::String(s) => Some(s.clone()),
        Json::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// JSON value to `PropertyValue`, preserving the type the file carried.
///
/// `None` means the value was JSON `null`, which Neo4j cannot store: the
/// caller counts it as dropped rather than writing `PropertyValue::Null`,
/// because a stored null and an absent property answer `exists()` differently.
fn convert(value: &Json, report: &mut ImportReport) -> Option<PropertyValue> {
    Some(match value {
        Json::Null => return None,
        Json::Bool(b) => PropertyValue::Boolean(*b),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else {
                // `as_f64` is None only for an integer outside i64 and u64,
                // which no Neo4j property can hold; a string keeps the digits
                // rather than rounding them.
                match n.as_f64() {
                    Some(f) => PropertyValue::Float(f),
                    None => PropertyValue::String(n.to_string()),
                }
            }
        }
        Json::String(s) => {
            if looks_temporal(s) {
                report.values_that_look_temporal += 1;
            }
            PropertyValue::String(s.clone())
        }
        Json::Array(items) => PropertyValue::Array(
            items
                .iter()
                // A null inside an array is not a property being dropped — the
                // array itself is the property — so it becomes an explicit
                // Null element and is not counted.
                .map(|i| convert(i, report).unwrap_or(PropertyValue::Null))
                .collect(),
        ),
        Json::Object(map) => {
            if map.contains_key("crs") {
                report.values_that_look_spatial += 1;
            }
            PropertyValue::Map(
                map.iter()
                    .map(|(k, v)| (k.clone(), convert(v, report).unwrap_or(PropertyValue::Null)))
                    .collect(),
            )
        }
    })
}

/// Whether a string is shaped like an ISO-8601 date, time or datetime.
///
/// Shape only, and deliberately so — this decides what to *count*, never what
/// to convert. A false positive costs a slightly high number in an advisory
/// field; a false positive in a converter costs the user their data.
fn looks_temporal(s: &str) -> bool {
    let b = s.as_bytes();
    let digits_at =
        |i: usize, n: usize| b.len() > i + n - 1 && b[i..i + n].iter().all(|c| c.is_ascii_digit());
    // YYYY-MM-DD...
    if b.len() >= 10
        && digits_at(0, 4)
        && b[4] == b'-'
        && digits_at(5, 2)
        && b[7] == b'-'
        && digits_at(8, 2)
    {
        return true;
    }
    // HH:MM:SS...
    if b.len() >= 8
        && digits_at(0, 2)
        && b[2] == b':'
        && digits_at(3, 2)
        && b[5] == b':'
        && digits_at(6, 2)
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_temporal_is_shape_not_meaning() {
        assert!(looks_temporal("2019-06-01T12:00:00Z"));
        assert!(looks_temporal("2019-06-01"));
        assert!(looks_temporal("12:31:14.645876123"));
        // Version numbers and identifiers must not be mistaken for dates: this
        // is the check that keeps the count advisory rather than alarming.
        assert!(!looks_temporal("1.8.0"));
        assert!(!looks_temporal("2019"));
        assert!(!looks_temporal("v2019-06-01"));
        assert!(!looks_temporal(""));
    }

    #[test]
    fn a_null_property_is_dropped_not_stored() {
        let mut report = ImportReport::default();
        assert_eq!(convert(&Json::Null, &mut report), None);
        // Inside an array it is a value, not an absent property.
        let arr = serde_json::json!([1, null]);
        assert_eq!(
            convert(&arr, &mut report),
            Some(PropertyValue::Array(vec![
                PropertyValue::Integer(1),
                PropertyValue::Null
            ]))
        );
        assert_eq!(
            report.null_properties_dropped, 0,
            "the array is one property"
        );
    }
}
