//! Materialized results: a catalog's answers, published beside a read-only
//! snapshot so a restore can serve them without executing anything (#1158).
//!
//! # Shape
//!
//! A sidecar, `<kg>.sgresults`, named by SHA-256 in the snapshot header like
//! the catalog (ADR-022). Not a section of the `.sgsnap` body: the body format
//! does not change, and a snapshot without results is byte-for-byte what it
//! was.
//!
//! # The constraints, and where each is enforced
//!
//! 1. **Bound to the graph's epoch.** [`Materialized::bind`] records the
//!    store's epoch after the restore; the first write bumps it, and from then
//!    on [`Materialized::lookup`] answers nothing, says so once, and stays
//!    dropped. The same coarse rule as the result cache: no dependency tracking.
//! 2. **Read-only snapshots only.** [`build`] refuses a snapshot whose header
//!    does not say `read_only`, and [`Materialized::load`] refuses to serve
//!    for one.
//! 3. **Capped, and reported.** A per-result byte cap and a total cap, the
//!    lesser of a byte count and a fraction of the snapshot's own size. The
//!    smallest results are admitted first, so scalars and aggregates go in
//!    before row sets. What did not fit is listed with the reason.
//! 4. **Re-verified.** [`build`] executes every entry and admits it only if
//!    the rows and hash agree with the catalog; `samyama verify` re-executes
//!    every stored result and fails on any disagreement; and [`Materialized::
//!    load`] refuses a file whose stored rows no longer hash to what it says.
//! 5. **Exact-match only, bypassable, disclosed.** An entry answers only its
//!    own Cypher with exactly its sample values; a caller can ask for
//!    execution instead; and every answer says whether it was `materialized`
//!    or `computed`.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::graph::GraphStore;
use crate::query::RecordBatch;
use crate::snapshot::format::{QueriesRef, SnapshotHeader};
use crate::snapshot::verify::{canonical_hash, queries_sha256, run_bound, ParamSpec, QueryCatalog};

/// The format string of a results file.
pub const RESULTS_FORMAT: &str = "samyama.results/1";

/// Default cap on one stored result, in bytes of its rows as JSON.
pub const DEFAULT_MAX_RESULT_BYTES: u64 = 64 * 1024;

/// Default cap on all stored results together, as a percentage of the
/// snapshot file's size. Results spend against the footprint budget
/// (PERF-10), so the default is small.
pub const DEFAULT_MAX_TOTAL_PCT: f64 = 5.0;

/// A results file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ResultsFile {
    pub format: String,
    pub generated_by: String,
    /// SHA-256 of the catalog these are the answers to. A results file is
    /// meaningless without the questions, and answers to an edited catalog are
    /// answers to different questions.
    pub catalog_sha256: String,
    /// The caps this file was built under, and what it used.
    pub caps: Caps,
    pub entries: Vec<StoredResult>,
    /// Entries that were not stored, and why.
    #[serde(default)]
    pub skipped: Vec<Skipped>,
}

/// The caps a results file was built under (#1158 constraint 3).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Caps {
    pub max_result_bytes: u64,
    /// The total cap actually applied: the lesser of the byte cap given and
    /// the percentage of the snapshot.
    pub max_total_bytes: u64,
    pub max_total_pct: f64,
    pub snapshot_bytes: u64,
    /// What the stored results came to.
    pub used_bytes: u64,
}

/// One stored answer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct StoredResult {
    pub id: String,
    pub cypher: String,
    /// The entry's parameters as declared. This answer is for their samples
    /// and no other values; the declared types are kept so re-verification
    /// binds exactly what the build bound.
    pub params: Vec<ParamSpec>,
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    /// The catalog's row count and canonical hash, which the build checked
    /// execution against.
    pub row_count: usize,
    pub hash: String,
    /// SHA-256 of `rows` as serialized here, so a stored row edited after the
    /// build is caught at load rather than served.
    pub rows_sha256: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Skipped {
    pub id: String,
    pub reason: String,
}

/// A result batch rendered as JSON objects, one per row, keyed by column.
pub fn rows_json(batch: &RecordBatch) -> Vec<serde_json::Map<String, serde_json::Value>> {
    batch
        .records
        .iter()
        .map(|rec| {
            batch
                .columns
                .iter()
                .map(|c| {
                    (
                        c.clone(),
                        rec.get(c).map_or(serde_json::Value::Null, cell_json),
                    )
                })
                .collect()
        })
        .collect()
}

/// One cell as JSON. Nodes and edges are their ids, as the agent tools and
/// the CLI render them; a stored answer cannot carry a live reference.
pub fn cell_json(v: &crate::query::executor::record::Value) -> serde_json::Value {
    use crate::query::executor::record::Value as V;
    use serde_json::json;
    match v {
        V::Null => serde_json::Value::Null,
        V::Property(p) => p.to_json(),
        V::List(items) => serde_json::Value::Array(items.iter().map(cell_json).collect()),
        V::Map(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), cell_json(v)))
                .collect(),
        ),
        V::Node(id, _) | V::NodeRef(id) => json!({ "node_id": id.as_u64() }),
        V::Edge(id, _) | V::EdgeRef(id, _, _, _) => json!({ "edge_id": id.as_u64() }),
        V::Path { nodes, edges } => json!({
            "nodes": nodes.iter().map(|n| n.as_u64()).collect::<Vec<_>>(),
            "edges": edges.iter().map(|e| e.as_u64()).collect::<Vec<_>>(),
        }),
    }
}

fn rows_sha256(rows: &[serde_json::Map<String, serde_json::Value>]) -> (String, u64) {
    let bytes = serde_json::to_vec(rows).unwrap_or_default();
    let digest = Sha256::digest(&bytes);
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    (hex, bytes.len() as u64)
}

/// The values an answer stored for `params` is an answer to: their samples.
pub fn sample_values(params: &[ParamSpec]) -> BTreeMap<String, serde_json::Value> {
    params
        .iter()
        .map(|p| (p.name.clone(), p.sample.clone()))
        .collect()
}

/// The caps a build is asked for (#1158 constraint 3).
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_result_bytes: u64,
    /// An absolute total cap, applied together with the percentage.
    pub max_total_bytes: Option<u64>,
    pub max_total_pct: f64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            max_total_bytes: None,
            max_total_pct: DEFAULT_MAX_TOTAL_PCT,
        }
    }
}

/// Build the results for `catalog` (whose file bytes hash to
/// `catalog_sha256`) against `store`, restored from a snapshot with `header`
/// whose file is `snapshot_bytes` long.
///
/// Refuses unless the header says `read_only`. Executes every entry with its
/// samples and refuses the whole build if any disagrees with the catalog:
/// the catalog and the snapshot are a pair, and a disagreement means one of
/// them is not what it claims, which no results file should paper over.
pub fn build(
    store: &GraphStore,
    header: &SnapshotHeader,
    snapshot_bytes: u64,
    catalog: &QueryCatalog,
    catalog_sha256: &str,
    limits: Limits,
) -> Result<ResultsFile, String> {
    let Limits {
        max_result_bytes,
        max_total_bytes,
        max_total_pct,
    } = limits;
    if !header.read_only {
        return Err(
            "the snapshot is not marked read-only. A materialized answer is true only of \
             the graph it was computed on, so results are built only for a snapshot its \
             publisher has promised not to write to: `samyama snapshot-read-only <snapshot>`."
                .to_string(),
        );
    }
    if !(0.0..=100.0).contains(&max_total_pct) {
        return Err(format!(
            "--max-total-pct {max_total_pct} is not a percentage"
        ));
    }

    let mut candidates = Vec::with_capacity(catalog.entries.len());
    for e in &catalog.entries {
        let batch = run_bound(store, &e.cypher, &e.params)
            .map_err(|err| format!("{}: failed while building results: {err}", e.id))?;
        let hash = canonical_hash(&batch);
        if batch.records.len() != e.rows || hash != e.hash {
            return Err(format!(
                "{}: execution gives {} rows ({hash}) but the catalog records {} ({}). \
                 The catalog is not this snapshot's; no results are written.",
                e.id,
                batch.records.len(),
                e.rows,
                e.hash
            ));
        }
        let rows = rows_json(&batch);
        let (rows_sha256, bytes) = rows_sha256(&rows);
        candidates.push(StoredResult {
            id: e.id.clone(),
            cypher: e.cypher.clone(),
            params: e.params.clone(),
            columns: batch.columns.clone(),
            rows,
            row_count: e.rows,
            hash,
            rows_sha256,
            bytes,
        });
    }

    let pct_cap = (snapshot_bytes as f64 * max_total_pct / 100.0) as u64;
    let total_cap = max_total_bytes.map_or(pct_cap, |b| b.min(pct_cap));
    // Smallest first: a scalar or an aggregate is the cheapest answer to store
    // and usually the most asked, and admitting by catalog order would let one
    // large row set crowd out every answer after it.
    candidates.sort_by(|a, b| (a.row_count, a.bytes, &a.id).cmp(&(b.row_count, b.bytes, &b.id)));

    let (mut entries, mut skipped, mut used) = (Vec::new(), Vec::new(), 0u64);
    for c in candidates {
        if c.bytes > max_result_bytes {
            skipped.push(Skipped {
                reason: format!(
                    "{} bytes, over the per-result cap of {max_result_bytes}",
                    c.bytes
                ),
                id: c.id,
            });
        } else if used + c.bytes > total_cap {
            skipped.push(Skipped {
                reason: format!(
                    "{} bytes would bring the total to {}, over the total cap of {total_cap}",
                    c.bytes,
                    used + c.bytes
                ),
                id: c.id,
            });
        } else {
            used += c.bytes;
            entries.push(c);
        }
    }
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    skipped.sort_by(|a, b| a.id.cmp(&b.id));

    Ok(ResultsFile {
        format: RESULTS_FORMAT.to_string(),
        generated_by: format!("samyama {}", crate::VERSION),
        catalog_sha256: catalog_sha256.to_ascii_lowercase(),
        caps: Caps {
            max_result_bytes,
            max_total_bytes: total_cap,
            max_total_pct,
            snapshot_bytes,
            used_bytes: used,
        },
        entries,
        skipped,
    })
}

/// Re-execute every stored result against `store` and report each that
/// disagrees (#1158 constraint 4). Empty when every one agrees.
pub fn reverify(store: &GraphStore, file: &ResultsFile) -> Vec<String> {
    let mut problems = Vec::new();
    for r in &file.entries {
        match run_bound(store, &r.cypher, &r.params) {
            Err(e) => problems.push(format!("{}: failed to re-execute: {e}", r.id)),
            Ok(batch) => {
                let hash = canonical_hash(&batch);
                let rows = rows_json(&batch);
                if batch.records.len() != r.row_count || hash != r.hash || rows != r.rows {
                    problems.push(format!(
                        "{}: the stored answer ({} rows, {}) disagrees with execution \
                         ({} rows, {hash})",
                        r.id,
                        r.row_count,
                        r.hash,
                        batch.records.len()
                    ));
                }
            }
        }
    }
    problems
}

/// Where an answer came from (TRUST-06).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Materialized,
    Computed,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Materialized => "materialized",
            Source::Computed => "computed",
        }
    }
}

/// Stored results, loaded, checked, and bound to one store's epoch.
pub struct Materialized {
    by_key: BTreeMap<(String, String), StoredResult>,
    epoch: Option<u64>,
    dropped: std::sync::atomic::AtomicBool,
}

impl Materialized {
    /// Load a results file, given its bytes and the snapshot header that
    /// links it. Every check [`read_checked`] makes applies.
    pub fn load(bytes: &[u8], header: &SnapshotHeader) -> Result<Self, String> {
        let file = read_checked(bytes, header)?;
        let by_key = file
            .entries
            .into_iter()
            .map(|r| ((r.cypher.clone(), params_key(&sample_values(&r.params))), r))
            .collect();
        Ok(Self {
            by_key,
            epoch: None,
            dropped: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Bind to `store` as it is now, which must be the store the snapshot was
    /// restored into, before anything writes to it.
    pub fn bind(mut self, store: &GraphStore) -> Self {
        self.epoch = Some(store.epoch());
        self
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Whether the results were dropped because the store was written to.
    pub fn is_dropped(&self) -> bool {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The stored answer to exactly `cypher` with exactly `params`, if there
    /// is one and the store has not been written to since [`bind`].
    ///
    /// [`bind`]: Materialized::bind
    pub fn lookup(
        &self,
        store: &GraphStore,
        cypher: &str,
        params: &BTreeMap<String, serde_json::Value>,
    ) -> Option<&StoredResult> {
        if self.epoch != Some(store.epoch()) {
            if !self
                .dropped
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                tracing::warn!(
                    "materialized results dropped: the store was written to after the \
                     read-only snapshot was restored (#1158)"
                );
            }
            return None;
        }
        if self.is_dropped() {
            return None;
        }
        self.by_key.get(&(cypher.to_string(), params_key(params)))
    }
}

/// Read a results file and check it against the snapshot header that links
/// it.
///
/// Refuses -- rather than serving fewer answers -- when anything does not line
/// up: a snapshot not marked read-only, a file whose bytes are not the ones
/// the header names, results for a different catalog, or a stored row edited
/// after the build. Every one of those would be a fast, confident answer to
/// some other question.
pub fn read_checked(bytes: &[u8], header: &SnapshotHeader) -> Result<ResultsFile, String> {
    if !header.read_only {
        return Err("the snapshot is not marked read-only, so its results are not served".into());
    }
    let Some(link) = &header.results else {
        return Err("the snapshot header names no results file".into());
    };
    check_ref(link, bytes)?;
    let file: ResultsFile =
        serde_json::from_slice(bytes).map_err(|e| format!("cannot read {}: {e}", link.file))?;
    if file.format != RESULTS_FORMAT {
        return Err(format!(
            "{} is format {:?}; this build reads {RESULTS_FORMAT:?}",
            link.file, file.format
        ));
    }
    match &header.queries {
        Some(q) if q.sha256.eq_ignore_ascii_case(&file.catalog_sha256) => {}
        Some(q) => {
            return Err(format!(
                "{} answers the catalog with sha256 {}, but the snapshot's catalog is {}",
                link.file, file.catalog_sha256, q.sha256
            ))
        }
        None => {
            return Err(format!(
                "{} answers a catalog, and the snapshot names none",
                link.file
            ))
        }
    }
    for r in &file.entries {
        let (sha, _) = rows_sha256(&r.rows);
        if sha != r.rows_sha256 || r.rows.len() != r.row_count {
            return Err(format!(
                "{}: the stored rows do not hash to the recorded {} -- the file was \
                 edited after it was built",
                r.id, r.rows_sha256
            ));
        }
    }
    Ok(file)
}

/// The parameters as a canonical string, so two maps with the same entries
/// match and `1` does not match `1.0` or `"1"`.
fn params_key(params: &BTreeMap<String, serde_json::Value>) -> String {
    serde_json::to_string(params).unwrap_or_default()
}

fn check_ref(link: &QueriesRef, bytes: &[u8]) -> Result<(), String> {
    let actual = queries_sha256(bytes);
    if actual.eq_ignore_ascii_case(&link.sha256) {
        Ok(())
    } else {
        Err(format!(
            "{}'s sha256 is {actual}, but the snapshot header records {}. These are not \
             the results the snapshot was published with.",
            link.file, link.sha256
        ))
    }
}
