//! Read-only dedup-key analysis for snapshot files (#1950).
//!
//! This deliberately reads nodes directly from the gzip JSONL stream. It does not
//! construct a [`GraphStore`](crate::graph::GraphStore), start a server, or import
//! any snapshot. The value index is limited to primitive values that snapshot
//! import itself accepts as dedup identifiers.

use crate::snapshot::codec::json_to_property;
use crate::snapshot::format::{SnapshotHeader, SnapshotNode};
use flate2::read::GzDecoder;
use rustc_hash::FxHashMap;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// Minimum per-snapshot key coverage for a recommendation.
pub const RECOMMENDED_MIN_COVERAGE: f64 = 0.50;
/// Minimum per-snapshot injectivity for a recommendation.
pub const RECOMMENDED_MIN_INJECTIVITY: f64 = 0.90;
/// Minimum share of distinct values that must be namespace-like to recommend.
pub const RECOMMENDED_MIN_NAMESPACE_SHAPE: f64 = 0.80;

/// Summary for one `(label, property)` pair across the supplied snapshots.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotKeyStat {
    pub label: String,
    pub property: String,
    /// One row per input snapshot, in input order.
    pub snapshots: Vec<SnapshotKeySourceStat>,
    /// Number of normalized values present in at least two input snapshots.
    pub cross_source_collisions: u64,
    /// The shared value carried by the most records, and its record count.
    pub largest_collision_group: Option<CollisionGroup>,
    /// `namespace_like`, `prose`, or `mixed`, based on observed distinct values.
    pub value_shape: String,
    /// Machine-readable explanation for recommendation or exclusion.
    pub recommendation: String,
}

/// Per-snapshot measurements for one key.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotKeySourceStat {
    pub file: String,
    pub label_nodes: u64,
    pub nodes_with_key: u64,
    pub distinct_values: u64,
    pub coverage: f64,
    pub injectivity: f64,
}

/// Largest shared value group for one key.
#[derive(Debug, Clone, Serialize)]
pub struct CollisionGroup {
    pub value: String,
    pub records: u64,
    pub snapshots: u64,
}

/// Whole report returned by [`analyze_snapshot_keys`].
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotKeyReport {
    pub files: Vec<String>,
    pub label_filter: Option<String>,
    pub keys: Vec<SnapshotKeyStat>,
    /// Can be passed directly to `--dedup-keys` as a comma-separated list.
    pub recommended_keys: Vec<String>,
    /// Successful merge operations predicted for the recommended keys, importing
    /// files in the order supplied into an empty store.
    pub predicted_merge_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct KeyId {
    label: String,
    property: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ValueId {
    key: KeyId,
    value: String,
}

#[derive(Default)]
struct ValueCounts {
    records: u64,
    source_bits: Vec<u64>,
    namespace_like: bool,
}

#[derive(Default)]
struct KeyCounts {
    nodes_with_key: Vec<u64>,
    namespace_values: u64,
    prose_values: u64,
}

/// Analyze candidate dedup keys without loading snapshots into a graph store.
///
/// Values use the same string normalization and primitive-value eligibility as
/// snapshot import. Files are processed in the given order both for measurement
/// and for the predicted import simulation.
pub fn analyze_snapshot_keys(
    files: &[PathBuf],
    label_filter: Option<&str>,
) -> Result<SnapshotKeyReport, Box<dyn std::error::Error>> {
    if files.is_empty() {
        return Err("provide at least one .sgsnap file".into());
    }

    let source_count = files.len();
    let words = source_count.div_ceil(64);
    let file_names: Vec<String> = files.iter().map(|p| p.display().to_string()).collect();
    let mut label_nodes: HashMap<(usize, String), u64> = HashMap::new();
    let mut key_counts: FxHashMap<KeyId, KeyCounts> = FxHashMap::default();
    let mut value_counts: FxHashMap<ValueId, ValueCounts> = FxHashMap::default();

    for (source, path) in files.iter().enumerate() {
        visit_snapshot_nodes(path, |node, version| {
            for label in &node.labels {
                if label_filter.is_some_and(|filter| filter != label) {
                    continue;
                }
                *label_nodes.entry((source, label.clone())).or_default() += 1;

                for (property, raw) in &node.props {
                    let Some(value) = dedup_value(raw, version) else {
                        continue;
                    };
                    let key = KeyId {
                        label: label.clone(),
                        property: property.clone(),
                    };
                    let counts = key_counts.entry(key.clone()).or_default();
                    if counts.nodes_with_key.len() != source_count {
                        counts.nodes_with_key.resize(source_count, 0);
                    }
                    counts.nodes_with_key[source] += 1;

                    let id = ValueId { key, value };
                    let entry = value_counts.entry(id).or_insert_with(|| ValueCounts {
                        records: 0,
                        source_bits: vec![0; words],
                        namespace_like: false,
                    });
                    entry.records += 1;
                    entry.source_bits[source / 64] |= 1u64 << (source % 64);
                    entry.namespace_like |= is_namespace_like(raw);
                }
            }
        })?;
    }

    let mut distinct_by_source: FxHashMap<KeyId, Vec<u64>> = FxHashMap::default();
    let mut shared_by_key: FxHashMap<KeyId, u64> = FxHashMap::default();
    let mut largest_by_key: FxHashMap<KeyId, CollisionGroup> = FxHashMap::default();
    for (id, counts) in &value_counts {
        let distinct = distinct_by_source
            .entry(id.key.clone())
            .or_insert_with(|| vec![0; source_count]);
        for source in 0..source_count {
            if counts.source_bits[source / 64] & (1u64 << (source % 64)) != 0 {
                distinct[source] += 1;
            }
        }
        let sources = counts
            .source_bits
            .iter()
            .map(|word| word.count_ones() as u64)
            .sum::<u64>();
        if sources >= 2 {
            *shared_by_key.entry(id.key.clone()).or_default() += 1;
            let candidate = CollisionGroup {
                value: id.value.clone(),
                records: counts.records,
                snapshots: sources,
            };
            let largest = largest_by_key
                .entry(id.key.clone())
                .or_insert_with(|| candidate.clone());
            if candidate.records > largest.records {
                *largest = candidate;
            }
        }
        let shape = key_counts.entry(id.key.clone()).or_default();
        if counts.namespace_like {
            shape.namespace_values += 1;
        } else {
            shape.prose_values += 1;
        }
    }

    let mut key_ids: Vec<KeyId> = key_counts.keys().cloned().collect();
    key_ids.sort();
    let mut recommended_keys = Vec::new();
    let mut keys = Vec::with_capacity(key_ids.len());

    for key in &key_ids {
        let counts = &key_counts[key];
        let distinct = distinct_by_source
            .get(key)
            .cloned()
            .unwrap_or_else(|| vec![0; source_count]);
        let shared_values = shared_by_key.get(key).copied().unwrap_or(0);
        let mut source_stats = Vec::with_capacity(source_count);
        let mut present_sources = 0usize;
        let mut min_coverage = 1.0f64;
        let mut min_injectivity = 1.0f64;

        for source in 0..source_count {
            let nodes_with_key = counts.nodes_with_key.get(source).copied().unwrap_or(0);
            let label_count = label_nodes
                .get(&(source, key.label.clone()))
                .copied()
                .unwrap_or(0);
            let distinct_values = distinct.get(source).copied().unwrap_or(0);
            let coverage = ratio(nodes_with_key, label_count);
            let injectivity = ratio(distinct_values, nodes_with_key);
            if nodes_with_key > 0 {
                present_sources += 1;
                min_coverage = min_coverage.min(coverage);
                min_injectivity = min_injectivity.min(injectivity);
            }
            source_stats.push(SnapshotKeySourceStat {
                file: file_names[source].clone(),
                label_nodes: label_count,
                nodes_with_key,
                distinct_values,
                coverage,
                injectivity,
            });
        }

        let total_shape = counts.namespace_values + counts.prose_values;
        let value_shape = if total_shape == 0 {
            "mixed"
        } else if counts.namespace_values as f64 / total_shape as f64
            >= RECOMMENDED_MIN_NAMESPACE_SHAPE
        {
            "namespace_like"
        } else if counts.prose_values as f64 / total_shape as f64 >= RECOMMENDED_MIN_NAMESPACE_SHAPE
        {
            "prose"
        } else {
            "mixed"
        };

        let recommendation = if present_sources < 2 {
            "single_source".to_string()
        } else if shared_values == 0 {
            "no_cross_source_collisions".to_string()
        } else if min_injectivity < RECOMMENDED_MIN_INJECTIVITY {
            "low_injectivity".to_string()
        } else if min_coverage < RECOMMENDED_MIN_COVERAGE {
            "low_coverage".to_string()
        } else if value_shape != "namespace_like" {
            "non_namespace_shape".to_string()
        } else {
            let rule = format!("{}.{}", key.label, key.property);
            recommended_keys.push(rule);
            "recommended".to_string()
        };

        keys.push(SnapshotKeyStat {
            label: key.label.clone(),
            property: key.property.clone(),
            snapshots: source_stats,
            cross_source_collisions: shared_values,
            largest_collision_group: largest_by_key.get(key).cloned(),
            value_shape: value_shape.to_string(),
            recommendation,
        });
    }

    let predicted_merge_count = if recommended_keys.is_empty() {
        0
    } else {
        predict_merges(files, &recommended_keys)?
    };

    Ok(SnapshotKeyReport {
        files: file_names,
        label_filter: label_filter.map(str::to_string),
        keys,
        recommended_keys,
        predicted_merge_count,
    })
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn dedup_value(raw: &Value, version: u32) -> Option<String> {
    match raw {
        Value::String(value) => Some(value.trim().to_lowercase()),
        Value::Number(value) if version >= 2 => Some(value.to_string()),
        Value::Number(value) => value.as_i64().map(|value| value.to_string()),
        _ => None,
    }
}

fn is_namespace_like(raw: &Value) -> bool {
    match raw {
        Value::Number(_) => true,
        Value::String(value) => {
            !value.trim().is_empty()
                && !value.chars().any(char::is_whitespace)
                && (value.chars().any(|c| c.is_ascii_digit())
                    || value
                        .chars()
                        .any(|c| matches!(c, ':' | '_' | '-' | '/' | '.')))
        }
        _ => false,
    }
}

fn visit_snapshot_nodes(
    path: &Path,
    mut visit: impl FnMut(SnapshotNode, u32),
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut lines = BufReader::new(GzDecoder::new(file)).lines();
    let header_line = lines
        .next()
        .ok_or("empty snapshot file: missing header")??;
    let header: SnapshotHeader = serde_json::from_str(&header_line)?;
    if header.format != "sgsnap" {
        return Err(format!("{} is not an sgsnap file", path.display()).into());
    }
    if header.version != 1 && header.version != 2 {
        return Err(format!(
            "{} has unsupported snapshot version {}",
            path.display(),
            header.version
        )
        .into());
    }

    for line in lines {
        let line = line?;
        let row: Value = serde_json::from_str(&line)?;
        if row.get("t").and_then(Value::as_str) != Some("n") {
            continue;
        }
        visit(serde_json::from_value(row)?, header.version);
    }
    Ok(())
}

#[derive(Default)]
struct SimEntity {
    properties: HashMap<String, Value>,
}

fn predict_merges(files: &[PathBuf], rules: &[String]) -> Result<u64, Box<dyn std::error::Error>> {
    let parsed_rules: Vec<(String, String)> = rules
        .iter()
        .map(|rule| {
            let (label, property) = rule
                .split_once('.')
                .expect("generated recommendation is qualified");
            (label.to_string(), property.to_string())
        })
        .collect();
    let mut index: FxHashMap<(String, String, String), usize> = FxHashMap::default();
    let mut entities: Vec<SimEntity> = Vec::new();
    let mut merges = 0u64;

    for path in files {
        visit_snapshot_nodes(path, |node, version| {
            let mut selected = HashMap::new();
            for (label, property) in &parsed_rules {
                if node
                    .labels
                    .iter()
                    .any(|incoming_label| incoming_label == label)
                {
                    if let Some(value) = node
                        .props
                        .get(property)
                        .and_then(|raw| dedup_value(raw, version))
                    {
                        selected.insert((label.clone(), property.clone()), value);
                    }
                }
            }

            let mut matched = None;
            'find: for (label, property) in &parsed_rules {
                if let Some(value) = selected.get(&(label.clone(), property.clone())) {
                    let rule = format!("{label}.{property}");
                    for incoming_label in &node.labels {
                        if let Some(&entity) =
                            index.get(&(incoming_label.clone(), rule.clone(), value.clone()))
                        {
                            matched = Some(entity);
                            break 'find;
                        }
                    }
                }
            }

            let conflict = matched.is_some_and(|entity| {
                parsed_rules.iter().any(|(label, property)| {
                    if !node
                        .labels
                        .iter()
                        .any(|incoming_label| incoming_label == label)
                    {
                        return false;
                    }
                    let Some(incoming) = node.props.get(property).filter(|value| !value.is_null())
                    else {
                        return false;
                    };
                    let Some(existing) = entities[entity]
                        .properties
                        .get(property)
                        .filter(|value| !value.is_null())
                    else {
                        return false;
                    };
                    match (existing, incoming) {
                        (Value::String(a), Value::String(b)) => {
                            a.trim().to_lowercase() != b.trim().to_lowercase()
                        }
                        _ => existing != incoming,
                    }
                })
            });

            let entity = if let Some(entity) = matched.filter(|_| !conflict) {
                merges += 1;
                entity
            } else {
                entities.push(SimEntity::default());
                entities.len() - 1
            };
            let record = &mut entities[entity];
            for (_, property) in &parsed_rules {
                if let Some(value) = node.props.get(property).filter(|value| !value.is_null()) {
                    let stored = record
                        .properties
                        .entry(property.clone())
                        .or_insert(Value::Null);
                    if stored.is_null() {
                        *stored = json_to_property(value).to_json();
                    }
                }
            }

            // Import inserts keys for newly created nodes. Properties/labels added
            // by a merge are intentionally not inserted into the dedup index.
            if matched.is_none() || conflict {
                for (label, property) in &parsed_rules {
                    if !node
                        .labels
                        .iter()
                        .any(|incoming_label| incoming_label == label)
                    {
                        continue;
                    }
                    let Some(value) = selected.get(&(label.clone(), property.clone())) else {
                        continue;
                    };
                    let rule = format!("{label}.{property}");
                    for incoming_label in &node.labels {
                        index.insert(
                            (incoming_label.clone(), rule.clone(), value.clone()),
                            entity,
                        );
                    }
                }
            }
        })?;
    }
    Ok(merges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn fixture(nodes: &[Value]) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut gzip = GzEncoder::new(file.as_file(), Compression::default());
        writeln!(
            gzip,
            "{}",
            serde_json::json!({
                "format": "sgsnap",
                "version": 2,
                "tenant": "fixture",
                "node_count": nodes.len(),
                "edge_count": 0,
                "labels": ["Gene", "Protein"],
                "edge_types": [],
                "created_at": "2026-01-01T00:00:00Z",
                "samyama_version": "1.13.0"
            })
        )
        .unwrap();
        for node in nodes {
            writeln!(gzip, "{node}").unwrap();
        }
        gzip.finish().unwrap();
        file
    }

    fn source_nodes() -> Vec<Value> {
        vec![
            serde_json::json!({"t":"n", "id":1, "labels":["Gene"], "props":{"symbol":"TP53", "description":"tumor protein"}}),
            serde_json::json!({"t":"n", "id":2, "labels":["Gene"], "props":{"symbol":"BRCA1", "description":"breast cancer gene"}}),
            serde_json::json!({"t":"n", "id":3, "labels":["Protein"], "props":{"symbol":"P53", "accession":"P00001"}}),
            serde_json::json!({"t":"n", "id":4, "labels":["Protein"], "props":{"symbol":"P53", "accession":"P00002"}}),
        ]
    }

    #[test]
    fn recommends_scoped_identifiers_and_predicts_actual_import_merges() {
        let first = fixture(&source_nodes());
        let second = fixture(&source_nodes());
        let files = vec![first.path().to_path_buf(), second.path().to_path_buf()];
        let report = analyze_snapshot_keys(&files, None).unwrap();

        assert_eq!(
            report.recommended_keys,
            ["Gene.symbol", "Protein.accession"]
        );
        assert_eq!(report.predicted_merge_count, 4);
        let protein_symbol = report
            .keys
            .iter()
            .find(|key| key.label == "Protein" && key.property == "symbol")
            .unwrap();
        assert_eq!(protein_symbol.recommendation, "low_injectivity");
        assert_eq!(protein_symbol.snapshots[0].injectivity, 0.5);
        let gene_description = report
            .keys
            .iter()
            .find(|key| key.label == "Gene" && key.property == "description")
            .unwrap();
        assert_eq!(gene_description.value_shape, "prose");
        assert_eq!(gene_description.recommendation, "non_namespace_shape");

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["recommended_keys"][0], "Gene.symbol");
        assert_eq!(json["recommended_keys"][1], "Protein.accession");

        let mut store = crate::graph::GraphStore::new();
        let mut actual_merges = 0;
        for path in &files {
            let stats = crate::snapshot::import_tenant_with_dedup(
                &mut store,
                File::open(path).unwrap(),
                &report
                    .recommended_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            actual_merges += stats.merged_count;
        }
        assert_eq!(report.predicted_merge_count, actual_merges);
    }

    #[test]
    fn reports_a_key_present_in_only_one_snapshot() {
        let file = fixture(&source_nodes());
        let empty = fixture(&[]);
        let report = analyze_snapshot_keys(
            &[file.path().to_path_buf(), empty.path().to_path_buf()],
            None,
        )
        .unwrap();
        assert!(report.keys.iter().any(|key| {
            key.label == "Gene" && key.property == "symbol" && key.recommendation == "single_source"
        }));
    }
}
