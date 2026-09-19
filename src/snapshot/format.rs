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
    /// What this export did not carry (INT-06).
    ///
    /// In the file as well as in the return value, so that whoever finds the
    /// snapshot later -- which is usually not whoever wrote it -- can read the
    /// losses off the artifact itself. Additive: `default` keeps older files
    /// loading and the format version where it is.
    #[serde(default)]
    pub dropped: Vec<Dropped>,
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
    /// What the format did not carry (INT-06).
    pub dropped: Vec<Dropped>,
}

/// One thing an export did not carry.
///
/// INT-06 asks for full-fidelity export **plus an explicit loss report**. The
/// snapshot has always dropped things -- index declarations, edge timestamps,
/// version history -- and a user had no way to learn that except by comparing
/// the two graphs afterwards and noticing. An export that is silent about its
/// losses is the shape of a backup somebody discovers is incomplete during a
/// restore.
///
/// A row is emitted **only when there was something to lose**: a graph with no
/// vector index produces no vector-index row, so the report is a list of what
/// happened to this graph rather than a standing disclaimer nobody reads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Dropped {
    /// A stable identifier a client can branch on, e.g. `property_indexes`.
    pub what: String,
    /// How many of them.
    pub count: u64,
    /// What it means for the restored graph, in one sentence.
    pub detail: String,
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
