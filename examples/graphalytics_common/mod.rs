//! LDBC Graphalytics edge-list dataset loader.
//!
//! Parses the standard Graphalytics file format:
//!   - Vertex file:  one vertex ID per line
//!   - Edge file:    "source|target" or "source|target|weight" per line
//!     (also supports space-delimited, as used by official example datasets)
//!
//! Builds a `samyama_graph_algorithms::GraphView` directly without going through
//! the main GraphStore, so the benchmark measures pure algorithm time.

use samyama_graph_algorithms::GraphView;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Metadata about a loaded graph.
pub struct GraphInfo {
    pub name: String,
    pub directed: bool,
    pub vertex_count: usize,
    pub edge_count: usize,
}

/// Result of loading a Graphalytics dataset.
pub struct LoadedGraph {
    pub view: GraphView,
    pub info: GraphInfo,
}

/// Algorithm parameters parsed from a `.properties` file.
pub struct DatasetProperties {
    pub directed: Option<bool>,
    pub bfs_source: Option<u64>,
    pub sssp_source: Option<u64>,
    pub pr_damping: Option<f64>,
    pub pr_iterations: Option<usize>,
    pub cdlp_max_iterations: Option<usize>,
}

impl Default for DatasetProperties {
    fn default() -> Self {
        Self {
            directed: None,
            bfs_source: None,
            sssp_source: None,
            pr_damping: None,
            pr_iterations: None,
            cdlp_max_iterations: None,
        }
    }
}

/// Parse a Graphalytics `.properties` file for algorithm parameters.
pub fn parse_properties(path: &Path) -> DatasetProperties {
    let mut props = DatasetProperties::default();

    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return props,
    };

    let reader = BufReader::new(file);
    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        let line = line.trim().to_string();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim();

            if key.ends_with(".directed") {
                props.directed = value.parse().ok();
            } else if key.ends_with(".bfs.source-vertex") {
                props.bfs_source = value.parse().ok();
            } else if key.ends_with(".sssp.source-vertex") {
                props.sssp_source = value.parse().ok();
            } else if key.ends_with(".pr.damping-factor") {
                props.pr_damping = value.parse().ok();
            } else if key.ends_with(".pr.num-iterations") {
                props.pr_iterations = value.parse().ok();
            } else if key.ends_with(".cdlp.max-iterations") {
                props.cdlp_max_iterations = value.parse().ok();
            }
        }
    }

    props
}

/// Load expected validation output for an algorithm.
/// Returns a map from vertex ID to the expected value string.
pub fn load_validation(
    data_dir: &Path,
    dataset: &str,
    algo: &str,
) -> Option<HashMap<u64, String>> {
    // Try: <data_dir>/<dataset>/<dataset>-<ALGO>
    let dataset_dir = data_dir.join(dataset);
    let path = dataset_dir.join(format!("{}-{}", dataset, algo));

    if !path.exists() {
        return None;
    }

    let file = File::open(&path).ok()?;
    let reader = BufReader::new(file);
    let mut result = HashMap::new();

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(id) = parts[0].parse::<u64>() {
                result.insert(id, parts[1].to_string());
            }
        }
    }

    if result.is_empty() { None } else { Some(result) }
}

/// Load a Graphalytics dataset from vertex and edge files.
///
/// The vertex file contains one vertex ID (u64) per line.
/// The edge file contains rows: `source|target[|weight]` or `source target [weight]`.
///
/// If `directed` is false, each edge is stored in both directions.
pub fn load_graphalytics_dataset(
    vertex_path: &Path,
    edge_path: &Path,
    directed: bool,
    dataset_name: &str,
) -> Result<LoadedGraph, Box<dyn std::error::Error>> {
    // ── Phase 1: Load vertices ──────────────────────────────────────────
    let mut node_ids: Vec<u64> = Vec::new();

    if vertex_path.exists() {
        let file = File::open(vertex_path)?;
        let reader = BufReader::new(file);
        for line_result in reader.lines() {
            let line = line_result?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('%') {
                continue;
            }
            let id: u64 = line.parse().map_err(|e| {
                format!("Failed to parse vertex ID '{}': {}", line, e)
            })?;
            node_ids.push(id);
        }
    }

    // ── Phase 2: Load edges ─────────────────────────────────────────────
    struct RawEdge {
        source: u64,
        target: u64,
        weight: f64,
    }

    let mut raw_edges: Vec<RawEdge> = Vec::new();
    let mut has_weights = false;

    if edge_path.exists() {
        let file = File::open(edge_path)?;
        let reader = BufReader::new(file);
        for line_result in reader.lines() {
            let line = line_result?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('%') {
                continue;
            }

            // Try pipe delimiter first, then whitespace
            let parts: Vec<&str> = if line.contains('|') {
                line.split('|').collect()
            } else {
                line.split_whitespace().collect()
            };

            if parts.len() < 2 {
                continue;
            }

            let source: u64 = parts[0].parse().map_err(|e| {
                format!("Failed to parse source ID '{}': {}", parts[0], e)
            })?;
            let target: u64 = parts[1].parse().map_err(|e| {
                format!("Failed to parse target ID '{}': {}", parts[1], e)
            })?;

            let weight = if parts.len() >= 3 {
                has_weights = true;
                parts[2].parse::<f64>().unwrap_or(1.0)
            } else {
                1.0
            };

            raw_edges.push(RawEdge { source, target, weight });
        }
    }

    // ── Phase 3: Discover all vertex IDs ────────────────────────────────
    // If vertex file was empty or missing, infer vertices from edges.
    if node_ids.is_empty() {
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for e in &raw_edges {
            seen.insert(e.source);
            seen.insert(e.target);
        }
        node_ids = seen.into_iter().collect();
        node_ids.sort();
    }

    // ── Phase 4: Build index mappings ───────────────────────────────────
    let node_count = node_ids.len();
    let mut node_to_index: HashMap<u64, usize> = HashMap::with_capacity(node_count);
    let mut index_to_node: Vec<u64> = Vec::with_capacity(node_count);

    for (idx, &nid) in node_ids.iter().enumerate() {
        node_to_index.insert(nid, idx);
        index_to_node.push(nid);
    }

    // ── Phase 5: Build adjacency lists ──────────────────────────────────
    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); node_count];
    let mut incoming: Vec<Vec<usize>> = vec![Vec::new(); node_count];
    let mut weights_out: Vec<Vec<f64>> = if has_weights {
        vec![Vec::new(); node_count]
    } else {
        Vec::new()
    };

    let mut edge_count = 0usize;

    for e in &raw_edges {
        let Some(&src_idx) = node_to_index.get(&e.source) else { continue };
        let Some(&tgt_idx) = node_to_index.get(&e.target) else { continue };

        outgoing[src_idx].push(tgt_idx);
        incoming[tgt_idx].push(src_idx);
        if has_weights {
            weights_out[src_idx].push(e.weight);
        }
        edge_count += 1;

        if !directed {
            // Add reverse edge for undirected graphs
            outgoing[tgt_idx].push(src_idx);
            incoming[src_idx].push(tgt_idx);
            if has_weights {
                weights_out[tgt_idx].push(e.weight);
            }
            edge_count += 1;
        }
    }

    // ── Phase 6: Build GraphView via from_adjacency_list ────────────────
    let weight_vecs = if has_weights { Some(weights_out) } else { None };

    let view = GraphView::from_adjacency_list(
        node_count,
        index_to_node,
        node_to_index,
        outgoing,
        incoming,
        weight_vecs,
    );

    let info = GraphInfo {
        name: dataset_name.to_string(),
        directed,
        vertex_count: node_count,
        edge_count,
    };

    Ok(LoadedGraph { view, info })
}

/// Attempt to auto-detect whether a dataset is directed or undirected
/// based on the dataset name convention or properties file.
pub fn is_directed(dataset_name: &str) -> bool {
    let lower = dataset_name.to_lowercase();
    if lower.contains("undirected") {
        false
    } else if lower.contains("directed") {
        true
    } else {
        // Default to directed
        true
    }
}

/// Find the vertex, edge, and properties file paths for a given dataset.
///
/// Looks for standard Graphalytics naming:
///   - `<dataset>.v`  or `<dataset>.vertices`
///   - `<dataset>.e`  or `<dataset>.edges`
///   - `<dataset>.properties`
pub fn find_dataset_files(
    data_dir: &Path,
    dataset: &str,
) -> Result<(std::path::PathBuf, std::path::PathBuf), Box<dyn std::error::Error>> {
    let dataset_dir = data_dir.join(dataset);

    // Try inside a subdirectory named after the dataset
    let candidates_v = [
        dataset_dir.join(format!("{}.v", dataset)),
        dataset_dir.join(format!("{}.vertices", dataset)),
        // Also try directly in data_dir
        data_dir.join(format!("{}.v", dataset)),
        data_dir.join(format!("{}.vertices", dataset)),
    ];

    let candidates_e = [
        dataset_dir.join(format!("{}.e", dataset)),
        dataset_dir.join(format!("{}.edges", dataset)),
        data_dir.join(format!("{}.e", dataset)),
        data_dir.join(format!("{}.edges", dataset)),
    ];

    let vertex_path = candidates_v
        .iter()
        .find(|p| p.exists())
        .cloned()
        .ok_or_else(|| {
            format!(
                "Vertex file not found. Tried:\n  {}",
                candidates_v
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join("\n  ")
            )
        })?;

    let edge_path = candidates_e
        .iter()
        .find(|p| p.exists())
        .cloned()
        .ok_or_else(|| {
            format!(
                "Edge file not found. Tried:\n  {}",
                candidates_e
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join("\n  ")
            )
        })?;

    Ok((vertex_path, edge_path))
}

/// Find the properties file for a dataset (if it exists).
pub fn find_properties_file(data_dir: &Path, dataset: &str) -> Option<std::path::PathBuf> {
    let candidates = [
        data_dir.join(dataset).join(format!("{}.properties", dataset)),
        data_dir.join(format!("{}.properties", dataset)),
    ];
    candidates.iter().find(|p| p.exists()).cloned()
}

// ============================================================================
// FORMATTING HELPERS
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
    if secs < 0.001 {
        format!("{:.0}us", secs * 1_000_000.0)
    } else if secs < 1.0 {
        format!("{:.2}ms", secs * 1000.0)
    } else {
        format!("{:.3}s", secs)
    }
}
