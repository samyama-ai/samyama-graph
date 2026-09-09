//! Snapshot format types for `.sgsnap` files.
//!
//! Format: gzip-compressed JSON-lines (one JSON object per line).
//! Line 0 is the header, lines 1..N are nodes, lines N+1..M are edges.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Header line (line 0) of a .sgsnap file
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub format: String,           // Always "sgsnap"
    pub version: u32,             // Format version: 1 (legacy), 2 (with CSR + ColumnStore)
    pub tenant: String,           // Tenant ID that was exported
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub edge_types: Vec<String>,
    pub created_at: String,       // ISO 8601
    pub samyama_version: String,
}

/// Current snapshot format version.
/// v1: JSON nodes + JSON edges (from Edge arena)
/// v2: JSON nodes (with ColumnStore props merged) + stub edges (from adjacency lists)
pub const SNAPSHOT_VERSION: u32 = 2;

/// A node record in the snapshot
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotNode {
    pub t: String,                // Always "n"
    pub id: u64,                  // Original NodeId
    pub labels: Vec<String>,
    pub props: HashMap<String, serde_json::Value>,
    /// Creation timestamp, milliseconds since the epoch (#1124).
    ///
    /// Additive, in the same sense as `SnapshotHierarchyIndex` below: a snapshot
    /// written before this field simply lacks it and defaults to 0, which is the
    /// value import produced for every node until now, so old files load
    /// unchanged and the format version does not move. Older readers ignore the
    /// extra key.
    ///
    /// Zero means "not carried", not "created at the epoch". Import leaves the
    /// node's own timestamp alone rather than writing 0 over it, so a v1 snapshot
    /// does not stamp every node with a false creation time.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub created_at: i64,
    /// Last-update timestamp, milliseconds since the epoch. See `created_at`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub updated_at: i64,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

/// An edge record in the snapshot
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotEdge {
    pub t: String,                // Always "e"
    pub id: u64,                  // Original EdgeId
    pub src: u64,                 // Source NodeId
    pub tgt: u64,                 // Target NodeId
    #[serde(rename = "type")]
    pub edge_type: String,
    pub props: HashMap<String, serde_json::Value>,
}

/// Stats returned from export
#[derive(Debug)]
pub struct ExportStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub edge_types: Vec<String>,
    pub bytes_written: u64,
}

/// A hierarchy index **declaration** in the snapshot (ADR-035 §6).
///
/// Only the declaration travels, not the built structure. Interval arrays and Fenwick
/// trees serialize to many times their in-memory size as JSON, and rebuilding on import is
/// a single O(n + m) pass — a rounding error next to importing the graph those nodes and
/// edges came from. What must survive a round-trip is the *intent*: which edge types form
/// a hierarchy, which property is its measure, and which monoids were asked for.
///
/// This is an additive line type. Readers built before ADR-035 dispatch on `"t":"n"` and
/// `"t":"e"` and ignore everything else, so a snapshot carrying these records still loads
/// on an older build — which is why the format version does not move.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotHierarchyIndex {
    pub t: String,                        // Always "h"
    pub name: String,
    pub edge_types: Vec<String>,
    #[serde(default)]
    pub reverse: bool,
    #[serde(default)]
    pub measure_label: Option<String>,
    #[serde(default)]
    pub measure_property: Option<String>,
    #[serde(default)]
    pub ops: Vec<String>,
}

/// Stats returned from import
#[derive(Debug)]
pub struct ImportStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub merged_count: u64,
    pub labels: Vec<String>,
    pub edge_types: Vec<String>,
    /// Hierarchy indexes rebuilt from declarations in the snapshot.
    pub hierarchy_count: u64,
}
