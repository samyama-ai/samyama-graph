//! A snapshot that cannot reproduce its own answers is a failed restore (#1157).
//!
//! ADR-022 records that `.sgsnap` has no checksum on the body: "a truncated
//! upload decompresses cleanly to a partial graph and silently imports." A
//! checksum would only prove the file arrived. It cannot prove the graph is the
//! graph, and every snapshot defect this codebase has shipped was of the second
//! kind:
//!
//! * #303/#420 — imported graphs had empty `node.properties` and no property statistics
//! * #1130 — an imported node has an empty row copy, so `SET` then restart loses
//!   every property the update did not touch
//! * #332 — a published snapshot was a degraded subset missing an edge type and half the labels
//! * #1096 — export writes `edge_type:""` rather than failing, losing those edges silently
//! * #1124 — a round trip resets `created_at`/`updated_at` to 0
//! * #199 — import reported "unexpected end of file" and loaded the data anyway
//!
//! Each is a silent partial restore, and each would be caught by running real
//! queries against the restored graph and comparing row counts and values.
//!
//! ## The trap
//!
//! A catalog whose queries return zero rows passes trivially. That has happened
//! here: the OSS benchmark's SF1 defaults referenced person ids absent from the
//! dataset and "returned 0 rows for every complex read and reported 21/21
//! passed in 32 ms" (#449). So a zero-row expectation is refused at build time
//! unless the entry is explicitly marked unanswerable, and a verify run in
//! which *every* entry returns zero rows fails whatever the expectations say.

use std::collections::BTreeMap;

use crate::graph::GraphStore;
use crate::query::{QueryEngine, RecordBatch};

/// Format identifier written into a catalog, so a reader can refuse a shape it
/// does not understand rather than misinterpreting it.
pub const CATALOG_FORMAT: &str = "samyama.queries/1";

/// One catalog entry: a query and what it produced when the snapshot was built.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CatalogEntry {
    pub id: String,
    pub cypher: String,
    /// Rows the query returned against the snapshot this catalog describes.
    pub rows: usize,
    /// Order-canonical hash of the rendered rows. Sorted before hashing, so a
    /// query without `ORDER BY` does not fail on row order -- which would be a
    /// false alarm about the restore rather than a finding.
    pub hash: String,
    /// A query that legitimately returns nothing. KG-08 asks for these
    /// deliberately, and they are the only entries allowed a zero-row
    /// expectation.
    #[serde(default)]
    pub unanswerable: bool,
}

/// A shipped query catalog.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryCatalog {
    pub format: String,
    pub generated_by: String,
    pub entries: Vec<CatalogEntry>,
}

/// What a mismatch most likely means. Named rather than diffed, because a diff
/// of two large result sets tells the reader what changed and not what broke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Expected rows, got none.
    EmptyResult,
    /// Got some rows, but fewer than expected.
    PartialResult,
    /// Got more rows than expected.
    ExtraRows,
    /// Right number of rows, different values.
    ValuesDiffer,
    /// The query itself failed.
    QueryError,
}

impl FailureClass {
    /// The probable cause, in the vocabulary of the defects this guards against.
    pub fn explain(self) -> &'static str {
        match self {
            FailureClass::EmptyResult => {
                "no rows where the catalog expects some: a label or edge type is \
                 missing from the restored graph, or the body was truncated \
                 (ADR-022 has no body checksum, so a partial file imports cleanly)"
            }
            FailureClass::PartialResult => {
                "fewer rows than the catalog expects: nodes or edges are missing, \
                 the shape of a degraded subset being published as if complete (#332)"
            }
            FailureClass::ExtraRows => {
                "more rows than the catalog expects: the snapshot was imported \
                 into a store that already held data, or an import ran twice"
            }
            FailureClass::ValuesDiffer => {
                "the right number of rows with the wrong values: properties lost or \
                 reset by the round trip (#303/#420 empty properties, #1130 the row \
                 copy, #1124 timestamps reset to 0) -- or, for an aggregate, a \
                 changed graph behind an unchanged row count, since `count(*)` \
                 returns one row whatever it counted"
            }
            FailureClass::QueryError => "the query failed against the restored graph",
        }
    }
}

/// One entry's outcome.
#[derive(Debug, Clone)]
pub struct EntryResult {
    pub id: String,
    pub expected_rows: usize,
    pub actual_rows: usize,
    pub failure: Option<FailureClass>,
    pub detail: String,
}

/// The whole run.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub results: Vec<EntryResult>,
    /// Every entry returned zero rows. Treated as a failure on its own, because
    /// it is what a catalog run against an empty graph looks like and it would
    /// otherwise be indistinguishable from success for an all-unanswerable
    /// catalog.
    pub everything_empty: bool,
}

impl VerifyReport {
    pub fn failed(&self) -> impl Iterator<Item = &EntryResult> {
        self.results.iter().filter(|r| r.failure.is_some())
    }

    pub fn is_ok(&self) -> bool {
        !self.everything_empty && self.failed().count() == 0
    }
}

/// Render a result deterministically, then hash it order-independently.
///
/// Rows are rendered, sorted, and hashed. Sorting is what makes an unordered
/// query stable across restores; without it this check would fail on row order
/// and report a restore defect that is not one.
pub fn canonical_hash(batch: &RecordBatch) -> String {
    let mut rows: Vec<String> = Vec::with_capacity(batch.records.len());
    for rec in &batch.records {
        let mut cells: BTreeMap<&str, String> = BTreeMap::new();
        for col in &batch.columns {
            cells.insert(col.as_str(), format!("{:?}", rec.get(col)));
        }
        rows.push(
            cells.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("\u{1f}"),
        );
    }
    rows.sort();

    // FNV-1a over the joined rows. A cryptographic digest would be a new
    // dependency for no gain: this detects accidental corruption, and a
    // snapshot able to forge a hash could forge the rows too.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in rows.join("\u{1e}").as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{h:016x}")
}

/// Build a catalog from queries run against a store.
///
/// Refuses a zero-row entry unless it is marked unanswerable. An entry that
/// returns nothing at build time will return nothing after any restore,
/// including a restore that lost the entire graph, so it cannot detect
/// anything and its presence would inflate the count of passing checks.
pub fn build_catalog(
    store: &GraphStore,
    queries: &[(String, String)],
    unanswerable: &[String],
) -> Result<QueryCatalog, String> {
    let engine = QueryEngine::new();
    let mut entries = Vec::with_capacity(queries.len());
    for (id, cypher) in queries {
        let batch = engine
            .execute(cypher, store)
            .map_err(|e| format!("{id}: query failed while building the catalog: {e}"))?;
        let rows = batch.records.len();
        let is_unanswerable = unanswerable.contains(id);
        if rows == 0 && !is_unanswerable {
            return Err(format!(
                "{id}: returned 0 rows. A zero-row entry passes against any graph, \
                 including an empty one, so it detects nothing. Fix the query, or \
                 mark it unanswerable if returning nothing is the point."
            ));
        }
        entries.push(CatalogEntry {
            id: id.clone(),
            cypher: cypher.clone(),
            rows,
            hash: canonical_hash(&batch),
            unanswerable: is_unanswerable,
        });
    }
    Ok(QueryCatalog {
        format: CATALOG_FORMAT.to_string(),
        generated_by: format!("samyama {}", crate::VERSION),
        entries,
    })
}

/// Run a catalog against a restored store.
pub fn verify(store: &GraphStore, catalog: &QueryCatalog) -> Result<VerifyReport, String> {
    if catalog.format != CATALOG_FORMAT {
        return Err(format!(
            "catalog format {:?} is not {CATALOG_FORMAT}; refusing to guess at its shape",
            catalog.format
        ));
    }
    let engine = QueryEngine::new();
    let mut results = Vec::with_capacity(catalog.entries.len());

    for e in &catalog.entries {
        let (actual_rows, failure, detail) = match engine.execute(&e.cypher, store) {
            Err(err) => (0, Some(FailureClass::QueryError), err.to_string()),
            Ok(batch) => {
                let rows = batch.records.len();
                let hash = canonical_hash(&batch);
                if rows == e.rows && hash == e.hash {
                    (rows, None, String::new())
                } else if rows == 0 && e.rows > 0 {
                    (rows, Some(FailureClass::EmptyResult), String::new())
                } else if rows < e.rows {
                    (rows, Some(FailureClass::PartialResult),
                     format!("{} of {} rows", rows, e.rows))
                } else if rows > e.rows {
                    (rows, Some(FailureClass::ExtraRows),
                     format!("{} rows, expected {}", rows, e.rows))
                } else {
                    (rows, Some(FailureClass::ValuesDiffer),
                     format!("hash {} != {}", hash, e.hash))
                }
            }
        };
        results.push(EntryResult {
            id: e.id.clone(),
            expected_rows: e.rows,
            actual_rows,
            failure,
            detail,
        });
    }

    // Guard against the #449 shape: everything answered, nothing returned.
    let everything_empty = !results.is_empty() && results.iter().all(|r| r.actual_rows == 0);
    Ok(VerifyReport { results, everything_empty })
}
