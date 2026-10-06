//! Snapshot format types for `.sgsnap` files.
//!
//! Format: gzip-compressed JSON-lines (one JSON object per line).
//! Line 0 is the header, then the index catalog (`"t":"i"`, #1506) and any
//! hierarchy declarations (`"t":"h"`), then nodes, then edges.

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
    /// The query catalog published beside this snapshot (#1154).
    ///
    /// The catalog is a sidecar, not a section: templates are edited far more
    /// often than the data changes, and rebuilding a multi-GB artifact to fix
    /// one Cypher string is not a trade worth making. The digest is what keeps
    /// the pair honest anyway -- `samyama verify` refuses a catalog whose bytes
    /// do not match it. Additive: absent means "no catalog is promised", and is
    /// not written at all, so a snapshot without one is byte-for-byte what it
    /// was before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queries: Option<QueriesRef>,
    /// The publisher's promise that this snapshot is served without writes
    /// (#1158). Only a read-only snapshot may carry materialized results: an
    /// answer baked into a file is true only of the graph it was computed on.
    /// Set after export with `samyama snapshot-read-only`. Additive, and not
    /// written when false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    /// The materialized results published beside this snapshot (`.sgresults`,
    /// #1158), by name and SHA-256 like the catalog. A sidecar for the same
    /// reason the catalog is one: the body format does not change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub results: Option<QueriesRef>,
}

/// Where a snapshot's sidecar -- its query catalog (#1154) or its materialized
/// results (#1158) -- lives, and what its bytes hash to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueriesRef {
    /// File name of the catalog, resolved against the snapshot's own directory.
    /// A bare name rather than a path, because the pair is published together
    /// (`gh release upload`, an S3 prefix) and a path would name the machine
    /// that built it.
    pub file: String,
    /// Lowercase hex SHA-256 of the catalog file exactly as published, so
    /// `sha256sum` on the release asset gives the same string.
    pub sha256: String,
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

/// The index catalog in the snapshot (#1506): every property index, unique
/// constraint, full-text index and vector index the exporting store declared.
///
/// One line, written right after the header, and written **even when the list
/// is empty**. Presence is the signal: a file with this line says exactly which
/// indexes exist, including "none", so import declares those and nothing else.
/// A file without it (written before #1506, or by another tool) says nothing,
/// and import keeps the old behaviour of rediscovering vector indexes from the
/// Vector properties.
///
/// Declarations only, the same `IndexDefinition` the RocksDB catalog persists
/// (#1477). Contents are rebuilt from the imported rows, which cannot disagree
/// with them; a posting list carried in the file could.
///
/// Additive in the same way as `SnapshotHierarchyIndex`: readers that predate it
/// skip the `"t":"i"` line, and so behave exactly as they did before, which is
/// why the format version does not move.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotIndexCatalog {
    pub t: String, // Always "i"
    #[serde(default)]
    pub definitions: Vec<crate::index::catalog::IndexDefinition>,
}

/// Stats returned from import
#[derive(Debug)]
pub struct ImportStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub merged_count: u64,
    /// How many distinct nodes absorbed at least one dedup merge (#1808).
    ///
    /// `merged_count` alone cannot tell a good key from a bad one: 300 merges
    /// may be 300 duplicate pairs or 300 records collapsed onto one node. A
    /// `--dedup-keys` list containing `symbol` reported 531,984 merges and read
    /// as a better run than the 104,080 of identifier-only keys, while
    /// destroying 77% of UniProt. The group count is what separates them.
    pub merge_groups: u64,
    /// Records collapsed into one node in the largest group, counting the node
    /// that first claimed the value. 2 is a duplicate pair; a large value is the
    /// signature of a key that is not an entity identifier.
    pub largest_merge_group: u64,
    /// Merges attributed to the dedup key that matched, highest first (#1808).
    ///
    /// The matching key is the first in `dedup_keys` whose value is *found*, not
    /// the first given, so an identifier listed ahead of a free-text key does not
    /// shield it: orthologues differ in accession, so the accession never matches
    /// and the lookup falls through to `symbol`. This names the key that actually
    /// did the merging.
    pub merges_by_key: Vec<(String, u64)>,
    pub labels: Vec<String>,
    pub edge_types: Vec<String>,
    /// Hierarchy indexes rebuilt from declarations in the snapshot.
    pub hierarchy_count: u64,
    /// Index definitions re-declared from the snapshot's catalog (#1506).
    ///
    /// `None` when the file carries no catalog line, which is the signal for a
    /// caller to fall back to rediscovering vector indexes. `Some` -- even with
    /// every count zero -- means the file said which indexes exist and import
    /// declared exactly those.
    pub indexes: Option<crate::index::catalog::RestoredIndexes>,
    /// Definitions in the snapshot that collided with a *different* definition
    /// already on the target (same vector key at another dimension, metric,
    /// quantization or name; same full-text name over another label or
    /// property). The target's definition is kept and the snapshot's is skipped:
    /// the rows already there were indexed under it, and replacing it would
    /// change answers for data this import did not bring.
    pub index_conflicts: u64,
}
