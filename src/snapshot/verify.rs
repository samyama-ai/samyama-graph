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

/// Text that betrays a template filled by string substitution rather than by
/// binding a parameter (#1156).
///
/// A catalog assembled by interpolation is a Cypher-injection surface that
/// ships with the data. The parser has real parameters (`$name`,
/// `Expression::Parameter`), so the safe form is available and the unsafe one
/// is merely easier to write.
///
/// Note that a bare `{` is *not* a marker: Cypher uses it for map literals and
/// inline property patterns, so refusing it would refuse ordinary queries.
pub const INTERPOLATION_MARKERS: &[&str] = &["{{", "${", "%s", "%d", "#{", "<<", "}}"];

/// A declared parameter.
///
/// The declared type is checked against the value that actually reaches the
/// query, and the sample is executed at build time. That last part is what
/// catches the failure this guards against: a string parameter compared to an
/// integer property returns **zero rows and no error**, so the caller reads
/// "no results" where the truth is "wrong type". Measured, not assumed --
/// `n.age = $x` with `$x` as the string "30" against `age = 30` returns 0 rows,
/// while the float 30.0 returns 1, so numeric coercion works and only the
/// string case is silent.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParamSpec {
    pub name: String,
    /// "int" | "float" | "string" | "bool"
    #[serde(rename = "type")]
    pub kind: String,
    /// A value that must produce rows at build time.
    pub sample: serde_json::Value,
    /// Permitted values, when the parameter is meant to be one of a set.
    #[serde(default)]
    pub enum_values: Option<Vec<serde_json::Value>>,
}

impl ParamSpec {
    /// The type name of the sample as it is actually written.
    fn sample_kind(&self) -> &'static str {
        match &self.sample {
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => "int",
            serde_json::Value::Number(_) => "float",
            serde_json::Value::String(_) => "string",
            _ => "other",
        }
    }

    fn to_property(&self) -> Result<crate::graph::PropertyValue, String> {
        use crate::graph::PropertyValue as P;
        Ok(match (&self.sample, self.kind.as_str()) {
            (serde_json::Value::Bool(b), "bool") => P::Boolean(*b),
            (serde_json::Value::Number(n), "int") => P::Integer(
                n.as_i64().ok_or_else(|| format!("{}: sample is not an integer", self.name))?,
            ),
            (serde_json::Value::Number(n), "float") => P::Float(
                n.as_f64().ok_or_else(|| format!("{}: sample is not a float", self.name))?,
            ),
            (serde_json::Value::String(v), "string") => P::String(v.clone()),
            _ => {
                return Err(format!(
                    "{}: declared type {:?} but the sample is a {}. A mis-declared \
                     type is not caught at execution -- a string against an integer \
                     property returns zero rows and no error.",
                    self.name, self.kind, self.sample_kind()
                ))
            }
        })
    }
}

/// Every `$name` the query references, in order of first appearance.
///
/// A hand scan rather than a regex: the grammar is `"$" ~ (ALPHANUMERIC | "_")+`,
/// which is small enough to read directly and avoids a dependency.
pub fn referenced_params(cypher: &str) -> Vec<String> {
    let b = cypher.as_bytes();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        if b[i] == b'$' {
            let start = i + 1;
            let mut j = start;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
            if j > start {
                let name = cypher[start..j].to_string();
                if !out.contains(&name) {
                    out.push(name);
                }
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

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
    /// Declared parameters. Every `$name` in the query must appear here and
    /// every entry here must be referenced by the query (#1156).
    #[serde(default)]
    pub params: Vec<ParamSpec>,
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

/// Run a catalog query with its declared parameters **bound**, never substituted.
///
/// Goes through `QueryExecutor::with_params`, so values reach the engine as
/// `Expression::Parameter` bindings. Nothing here builds a query string from a
/// value, which is the property #1156 asks for and the reason this helper
/// exists rather than each caller formatting its own.
pub fn run_bound(
    store: &GraphStore,
    cypher: &str,
    params: &[ParamSpec],
) -> Result<RecordBatch, String> {
    let parsed = crate::query::parse_query(cypher).map_err(|e| e.to_string())?;
    let mut bound = std::collections::HashMap::new();
    for p in params {
        bound.insert(p.name.clone(), p.to_property()?);
    }
    crate::query::QueryExecutor::new(store)
        .with_params(bound)
        .execute(&parsed)
        .map_err(|e| e.to_string())
}

/// Refuse a query that is assembled rather than parameterized, and a parameter
/// set that does not match the query (#1156).
///
/// Checked at both build and load. Checking only at build would leave a
/// hand-edited catalog -- which is the form a published `.sgqueries` file takes
/// once it leaves us -- unchecked at the point it is executed.
pub fn validate_entry_shape(id: &str, cypher: &str, params: &[ParamSpec]) -> Result<(), String> {
    for m in INTERPOLATION_MARKERS {
        if cypher.contains(m) {
            return Err(format!(
                "{id}: query contains {m:?}, which is a template filled by string \
                 substitution. A catalog assembled that way is a Cypher-injection \
                 surface that ships with the data. Use a bound parameter ($name)."
            ));
        }
    }
    let referenced = referenced_params(cypher);
    for name in &referenced {
        if !params.iter().any(|p| &p.name == name) {
            return Err(format!(
                "{id}: query references ${name} but declares no type for it. An \
                 undeclared parameter is one whose value nothing checks."
            ));
        }
    }
    for p in params {
        if !referenced.contains(&p.name) {
            return Err(format!(
                "{id}: declares parameter {:?}, which the query never uses. A spec \
                 that binds nothing gives false assurance about what is checked.",
                p.name
            ));
        }
        p.to_property()?;
        if let Some(allowed) = &p.enum_values {
            if !allowed.contains(&p.sample) {
                return Err(format!(
                    "{}: the sample value is not in the declared enum", p.name
                ));
            }
        }
    }
    Ok(())
}

/// Build a catalog from queries run against a store.
///
/// Refuses a zero-row entry unless it is marked unanswerable. An entry that
/// returns nothing at build time will return nothing after any restore,
/// including a restore that lost the entire graph, so it cannot detect
/// anything and its presence would inflate the count of passing checks.
/// A query offered for inclusion in a catalog.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct QuerySpec {
    pub id: String,
    pub cypher: String,
    #[serde(default)]
    pub unanswerable: bool,
    #[serde(default)]
    pub params: Vec<ParamSpec>,
}

pub fn build_catalog(
    store: &GraphStore,
    queries: &[QuerySpec],
    unanswerable: &[String],
) -> Result<QueryCatalog, String> {
    let mut entries = Vec::with_capacity(queries.len());
    for q in queries {
        let (id, cypher) = (&q.id, &q.cypher);
        validate_entry_shape(id, cypher, &q.params)?;
        let batch = run_bound(store, cypher, &q.params)
            .map_err(|e| format!("{id}: query failed while building the catalog: {e}"))?;
        let rows = batch.records.len();
        let is_unanswerable = q.unanswerable || unanswerable.contains(id);
        if rows == 0 && !is_unanswerable {
            return Err(format!(
                "{id}: returned 0 rows. A zero-row entry passes against any graph, \
                 including an empty one, so it detects nothing. Fix the query, or \
                 mark it unanswerable if returning nothing is the point.{}",
                if q.params.is_empty() { "" } else {
                    " With parameters, the usual cause is a type mismatch: a string \
                     compared to an integer property matches nothing and raises no \
                     error."
                }
            ));
        }
        entries.push(CatalogEntry {
            id: id.clone(),
            cypher: cypher.clone(),
            rows,
            hash: canonical_hash(&batch),
            unanswerable: is_unanswerable,
            params: q.params.clone(),
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
    let mut results = Vec::with_capacity(catalog.entries.len());

    // Shape is re-checked here, not only at build. A published catalog is a
    // file that leaves us and can be edited; executing it is the point at which
    // an interpolated template would do harm.
    for e in &catalog.entries {
        validate_entry_shape(&e.id, &e.cypher, &e.params)?;
    }

    for e in &catalog.entries {
        let (actual_rows, failure, detail) = match run_bound(store, &e.cypher, &e.params) {
            Err(err) => (0, Some(FailureClass::QueryError), err),
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
