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
use crate::query::RecordBatch;

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
    /// The natural-language question, which is what KG-08 and DX-08 are about.
    /// Empty is allowed so a purely structural catalog stays valid, but the
    /// KG-08 conformance check requires it.
    #[serde(default)]
    pub question: String,
    /// Other phrasings of the same question.
    #[serde(default)]
    pub paraphrases: Vec<String>,
    /// "easy" | "medium" | "hard". KG-08 asks for difficulty labels.
    #[serde(default)]
    pub difficulty: String,
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
    /// Rows every operator produced, summed, when the entry ran with its
    /// sample values at build time (#1156). `run_template` refuses a call that
    /// does more than `work_ceiling(work)`. Absent in a catalog built before
    /// it was recorded, and `run_template` refuses such an entry rather than
    /// run it unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<u64>,
}

/// How many times the recorded work a call may do before it is refused.
///
/// Stated, not tuned: a template blessed against its samples may legitimately
/// see values that match ten times as much, and a value that matches far more
/// than that has turned the template into a different query -- the index
/// lookup it was written as into the scan it was not.
pub const WORK_CEILING_FACTOR: u64 = 10;

/// The smallest ceiling a template gets, whatever its recorded work.
///
/// A sample that matches three rows would otherwise give a ceiling of thirty,
/// refusing ordinary values for no reason. Ten thousand rows is cheap on any
/// graph we publish, so a floor this size removes the false refusals without
/// letting through anything that threatens PERF-19's five-second ceiling.
pub const MIN_WORK_CEILING: u64 = 10_000;

/// The ceiling a call is held to, from the work recorded at build time.
pub fn work_ceiling(recorded: u64) -> u64 {
    recorded
        .saturating_mul(WORK_CEILING_FACTOR)
        .max(MIN_WORK_CEILING)
}

/// A shipped query catalog.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryCatalog {
    pub format: String,
    pub generated_by: String,
    /// Whether the questions were written by us or drawn from traffic (#1159).
    ///
    /// Required rather than defaulted, so the safe answer is never the silent
    /// one. An observed catalog is user text and may not be published without
    /// an explicit flag.
    #[serde(default = "default_provenance")]
    pub provenance: crate::snapshot::publish_gate::Provenance,
    /// The tenant the source snapshot was exported from, as its header records
    /// it (#1159). Informational: publishability is never inferred from it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// Whether this catalog was built for release (#1159). `catalog-build`
    /// writes `false` unless given `--release`, so a catalog is private by
    /// default; `catalog-gate` refuses anything but `true`. Absent in a catalog
    /// that predates the field, and refused as such (see
    /// `publish_gate::release_refusal`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publishable: Option<bool>,
    pub entries: Vec<CatalogEntry>,
}

/// A catalog with no stated provenance is treated as observed.
///
/// The unsafe reading is the conservative one here: an authored catalog
/// mislabelled observed costs a flag, while an observed catalog mislabelled
/// authored publishes someone's questions.
fn default_provenance() -> crate::snapshot::publish_gate::Provenance {
    crate::snapshot::publish_gate::Provenance::Observed
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
    run_metered(store, cypher, params, None).map(|(batch, _)| batch)
}

/// `run_bound`, charging every row every operator produces to a meter that
/// refuses past `ceiling`. Returns the rows and the work the run did.
fn run_metered(
    store: &GraphStore,
    cypher: &str,
    params: &[ParamSpec],
    ceiling: Option<u64>,
) -> Result<(RecordBatch, u64), String> {
    let parsed = crate::query::parse_query(cypher).map_err(|e| e.to_string())?;
    let mut bound = std::collections::HashMap::new();
    for p in params {
        bound.insert(p.name.clone(), p.to_property()?);
    }
    let meter = crate::query::executor::budget::WorkMeter::new(ceiling);
    let batch = crate::query::QueryExecutor::new(store)
        .with_params(bound)
        .with_work_meter(std::sync::Arc::clone(&meter))
        .execute(&parsed)
        .map_err(|e| e.to_string())?;
    Ok((batch, meter.produced()))
}

/// Run a catalog entry with caller-supplied parameter values (#1156).
///
/// This is the path by which a published template is *used*, as opposed to
/// verified, so it is where the declared contract is enforced:
///
/// * a value for a parameter the entry does not declare is refused;
/// * a value of the wrong type is refused, before the query runs, because the
///   engine answers a string compared to an integer property with zero rows
///   and no error;
/// * a value outside a declared enum is refused;
/// * an omitted parameter takes its declared sample;
/// * the run is held to `work_ceiling(entry.work)` rows across all operators,
///   and refused, naming the values, if it goes past it -- so a legal value
///   that turns an index lookup into a scan of most of the graph cannot defeat
///   PERF-19's five-second ceiling.
///
/// An entry with no recorded work predates the ceiling and is refused rather
/// than run unbounded. Rebuild the catalog to record it.
pub fn run_template(
    store: &GraphStore,
    entry: &CatalogEntry,
    values: &BTreeMap<String, serde_json::Value>,
) -> Result<RecordBatch, String> {
    validate_entry_shape(&entry.id, &entry.cypher, &entry.params)?;
    let Some(recorded) = entry.work else {
        return Err(format!(
            "{}: the catalog records no work for this entry, so a call cannot be \
             held to a ceiling. It was built before #1156; rebuild it with \
             `samyama catalog-build`.",
            entry.id
        ));
    };
    for name in values.keys() {
        if !entry.params.iter().any(|p| &p.name == name) {
            return Err(format!(
                "{}: no parameter named {name:?}. Declared: {}",
                entry.id,
                entry
                    .params
                    .iter()
                    .map(|p| p.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    let mut bound = Vec::with_capacity(entry.params.len());
    for p in &entry.params {
        let mut spec = p.clone();
        if let Some(v) = values.get(&p.name) {
            spec.sample = v.clone();
        }
        // `to_property` checks the value against the declared type.
        spec.to_property()
            .map_err(|e| format!("{}: {e}", entry.id))?;
        if let Some(allowed) = &p.enum_values {
            if !allowed.contains(&spec.sample) {
                return Err(format!(
                    "{}: {} = {} is not one of the declared values {}",
                    entry.id,
                    p.name,
                    spec.sample,
                    serde_json::Value::Array(allowed.clone())
                ));
            }
        }
        bound.push(spec);
    }
    let ceiling = work_ceiling(recorded);
    run_metered(store, &entry.cypher, &bound, Some(ceiling))
        .map(|(batch, _)| batch)
        .map_err(|e| {
            if e.contains(crate::query::error_code::TEMPLATE_COST_EXCEEDED) {
                let given = bound
                    .iter()
                    .filter(|p| values.contains_key(&p.name))
                    .map(|p| format!("{} = {}", p.name, p.sample))
                    .collect::<Vec<_>>();
                format!(
                    "{}: refused -- with {} this template does more than {ceiling} rows \
                     of work, the ceiling for it ({WORK_CEILING_FACTOR}x the {recorded} \
                     rows it did with its sample values, at least {MIN_WORK_CEILING}). \
                     A value that matches far more of the graph than the samples turns \
                     the template into a different query. Narrow the value, or run the \
                     Cypher directly where the engine's own limits apply. [{}]",
                    entry.id,
                    if given.is_empty() {
                        "its sample values".to_string()
                    } else {
                        given.join(", ")
                    },
                    crate::query::error_code::TEMPLATE_COST_EXCEEDED
                )
            } else {
                format!("{}: {e}", entry.id)
            }
        })
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
    // `LIMIT $k` and `SKIP $k` are Cypher, and the engine binds them. A
    // published catalog must still not hand whoever fills its parameters an
    // unbounded row-count slot (#1156). The parser used to refuse them, which
    // is what kept this unreachable; the catalog now refuses them itself.
    if crate::query::parse_query(cypher).is_ok_and(|q| q.has_deferred_row_counts()) {
        return Err(format!(
            "{id}: SKIP/LIMIT takes a parameter. A catalog query fixes its own row \
             count; a parameter there is an unbounded slot for whoever fills it."
        ));
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
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub paraphrases: Vec<String>,
    #[serde(default)]
    pub difficulty: String,
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
        let (batch, work) = run_metered(store, cypher, &q.params, None)
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
            question: q.question.clone(),
            paraphrases: q.paraphrases.clone(),
            difficulty: q.difficulty.clone(),
            cypher: cypher.clone(),
            rows,
            hash: canonical_hash(&batch),
            unanswerable: is_unanswerable,
            params: q.params.clone(),
            work: Some(work),
        });
    }
    Ok(QueryCatalog {
        format: CATALOG_FORMAT.to_string(),
        generated_by: format!("samyama {}", crate::VERSION),
        provenance: crate::snapshot::publish_gate::Provenance::Authored,
        tenant: None,
        // Private by default (#1159): only `catalog-build --release` says otherwise.
        publishable: Some(false),
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


/// Whether a catalog meets what KG-08 and DX-08 already require (#1154).
///
/// Neither is a new requirement. KG-08 asks for "≥30 queries with gold answers,
/// difficulty labels, and unanswerable items" per KG; DX-08 for "5 example
/// questions and expected outputs". Both are recorded as partially met today
/// because the queries live as prose in READMEs where nothing executes them.
/// This is the check that makes them true or false rather than aspirational.
pub fn kg08_conformance(catalog: &QueryCatalog) -> Vec<String> {
    const MIN_ENTRIES: usize = 30;
    const DIFFICULTIES: &[&str] = &["easy", "medium", "hard"];
    let mut problems = Vec::new();

    if catalog.entries.len() < MIN_ENTRIES {
        problems.push(format!(
            "KG-08 asks for at least {MIN_ENTRIES} queries; this catalog has {}",
            catalog.entries.len()
        ));
    }
    if !catalog.entries.iter().any(|e| e.unanswerable) {
        problems.push(
            "KG-08 asks for unanswerable items and there are none. They are not \
             padding: refusing to answer is a correctness behaviour, and a suite \
             without them cannot tell a model that declines from one that guesses."
                .to_string(),
        );
    }
    for e in &catalog.entries {
        if e.question.trim().is_empty() {
            problems.push(format!("{}: no question text; DX-08 is about the question", e.id));
        }
        if !DIFFICULTIES.contains(&e.difficulty.as_str()) {
            problems.push(format!(
                "{}: difficulty {:?} is not one of {DIFFICULTIES:?}", e.id, e.difficulty
            ));
        }
    }
    problems
}

/// Digest of a catalog's content, as `catalog-gate` prints it.
///
/// The catalog ships beside the `.sgsnap` rather than inside it: templates are
/// schema-bound and get edited far more often than the data changes, and
/// rebuilding a multi-GB artifact to fix a Cypher string is not a good trade
/// (export runs at ~0.77 MB/s and is CPU-bound, #314). A digest is what makes
/// a mismatched pair detectable anyway; the one the snapshot header records is
/// `queries_sha256`, over the file's bytes rather than this re-serialisation.
pub fn catalog_digest(serialized: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in serialized.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{h:016x}")
}

/// SHA-256 of a catalog file's bytes, as lowercase hex, for the snapshot
/// header's `queries.sha256` (#1154).
///
/// Over the bytes as published rather than a re-serialisation, unlike
/// `catalog_digest`: the header is checked against the release asset, and the
/// check a downloader can run by hand is `sha256sum` on that asset. A
/// cryptographic digest here, not FNV, because this one is compared against a
/// file that crossed a network and may have been edited on the way.
pub fn queries_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Check a catalog file against the reference in its snapshot's header.
///
/// A mismatch is refused rather than reported: a catalog that is not the one
/// the snapshot was published with checks some other graph's answers, so its
/// passing or failing says nothing about this restore.
pub fn check_queries_ref(
    link: &crate::snapshot::format::QueriesRef,
    bytes: &[u8],
) -> Result<(), String> {
    let actual = queries_sha256(bytes);
    if actual.eq_ignore_ascii_case(&link.sha256) {
        return Ok(());
    }
    Err(format!(
        "the catalog's sha256 is {actual}, but the snapshot header records {} for {:?}. \
         This is not the catalog the snapshot was published with, so its results would \
         say nothing about this restore. Use the catalog the header names, or rebuild \
         the pair with `samyama catalog-build ... --link`.",
        link.sha256, link.file
    ))
}

/// The string sample and enum values in `entries` that are excerpts of rows
/// the graph says may not leave (#1159, TRUST-03).
///
/// A sample value is drawn from the data so the build has something to run,
/// which makes it an excerpt, and an excerpt of a row marked
/// `__redistributable = false` -- or derived, through `DERIVED_FROM`, from one
/// -- fails the check an export of that row fails. One finding per
/// (entry, parameter, value), naming the node it was found on.
///
/// Strings only. A string sample is the name, title or identifier the issue is
/// about; a number or a boolean matches thousands of rows by coincidence (a
/// year, a tier, `true`), and refusing on that would refuse every catalog.
/// `__`-prefixed properties are the provenance itself, not data, and are not
/// compared. A value some other row also carries is not refused: it is not an
/// excerpt of the withheld row. Unmarked rows are not withheld: absent means
/// unknown, and the policy for unknown is the caller's
/// (see [`crate::provenance`]).
pub fn withheld_samples(store: &GraphStore, entries: &[CatalogEntry]) -> Vec<String> {
    use crate::graph::PropertyValue;
    use crate::provenance::{derivation_path, redistributable, Redistributable};
    use std::collections::{HashMap, HashSet};

    // value -> the (entry, parameter) places it is used.
    let mut wanted: HashMap<&str, Vec<(&str, &str)>> = HashMap::new();
    for e in entries {
        for p in &e.params {
            let enums = p.enum_values.iter().flatten();
            for v in std::iter::once(&p.sample).chain(enums) {
                if let serde_json::Value::String(v) = v {
                    let at = wanted.entry(v.as_str()).or_default();
                    if !at.contains(&(e.id.as_str(), p.name.as_str())) {
                        at.push((e.id.as_str(), p.name.as_str()));
                    }
                }
            }
        }
    }
    if wanted.is_empty() {
        return Vec::new();
    }

    // value -> the first withheld row carrying it, and whether any row that
    // is not withheld carries it too.
    let mut withheld_on: HashMap<&str, (u64, String)> = HashMap::new();
    let mut public: HashSet<&str> = HashSet::new();
    for node in store.all_nodes() {
        let props = store.node_properties_merged(node.id);
        let mut is_withheld: Option<bool> = None;
        for (key, value) in props.iter() {
            if key.starts_with("__") {
                continue;
            }
            let PropertyValue::String(text) = value else {
                continue;
            };
            let Some((&value, _)) = wanted.get_key_value(text.as_str()) else {
                continue;
            };
            let withheld = *is_withheld.get_or_insert_with(|| {
                redistributable(store, node.id) == Redistributable::No
                    || derivation_path(store, node.id)
                        .iter()
                        .any(|d| d.redistributable == Redistributable::No)
            });
            if withheld {
                withheld_on
                    .entry(value)
                    .or_insert_with(|| (node.id.as_u64(), key.to_string()));
            } else {
                public.insert(value);
            }
        }
    }

    let mut found: Vec<String> = Vec::new();
    for (value, (node, key)) in &withheld_on {
        // A value some redistributable or unmarked row also carries is not an
        // excerpt of the withheld one: it could have come from either, and a
        // country or a status shared with public rows identifies nothing.
        if public.contains(value) {
            continue;
        }
        for (entry, param) in &wanted[value] {
            found.push(format!(
                "{entry}.params.{param}: {value:?} appears only on rows that may not \
                 be redistributed (the {key} of node {node}, marked \
                 __redistributable = false or derived from a row that is). A sample \
                 is a data excerpt; declare a value a redistributable row carries."
            ));
        }
    }
    found.sort();
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::PropertyValue;
    use serde_json::json;

    fn spec(name: &str, kind: &str, sample: serde_json::Value) -> ParamSpec {
        ParamSpec {
            name: name.into(),
            kind: kind.into(),
            sample,
            enum_values: None,
        }
    }

    fn people() -> GraphStore {
        let mut store = GraphStore::new();
        for (name, active) in [("Ann", true), ("Bob", false), ("Cy", true)] {
            let n = store.create_node("Person");
            store.set_node_property("default", n, "name", name).unwrap();
            store
                .set_node_property("default", n, "active", active)
                .unwrap();
        }
        store
    }

    #[test]
    fn a_sample_that_contradicts_its_declared_type_is_named() {
        let cases = [
            (spec("p", "int", json!("30")), "sample is a string"),
            (spec("p", "string", json!(true)), "sample is a bool"),
            (spec("p", "string", json!(1.5)), "sample is a float"),
            (spec("p", "bool", json!(3)), "sample is a int"),
            (spec("p", "int", json!(null)), "sample is a other"),
            (spec("p", "int", json!(1.5)), "p: sample is not an integer"),
        ];
        for (p, want) in cases {
            let err = p.to_property().unwrap_err();
            assert!(err.contains(want), "{err}");
        }
        assert_eq!(
            spec("p", "bool", json!(false)).to_property(),
            Ok(PropertyValue::Boolean(false))
        );
        assert_eq!(
            spec("p", "float", json!(2)).to_property(),
            Ok(PropertyValue::Float(2.0))
        );
    }

    #[test]
    fn a_bare_dollar_is_not_a_parameter() {
        assert_eq!(
            referenced_params("RETURN $ + $a_1, $a_1, $b"),
            vec!["a_1", "b"]
        );
        assert!(referenced_params("RETURN '$'").is_empty());
    }

    #[test]
    fn a_catalog_without_provenance_is_treated_as_observed() {
        let cat: QueryCatalog = serde_json::from_value(json!({
            "format": CATALOG_FORMAT,
            "generated_by": "hand",
            "entries": []
        }))
        .unwrap();
        assert_eq!(
            cat.provenance,
            crate::snapshot::publish_gate::Provenance::Observed
        );
    }

    #[test]
    fn a_catalog_from_before_the_release_stamp_still_parses_and_reserialises_unchanged() {
        let old = json!({
            "format": CATALOG_FORMAT,
            "generated_by": "hand",
            "provenance": "authored",
            "entries": []
        });
        let cat: QueryCatalog = serde_json::from_value(old.clone()).unwrap();
        assert_eq!(cat.publishable, None);
        assert_eq!(cat.tenant, None);
        // Absent stays absent, so `catalog_digest` of an old catalog is what it was.
        assert_eq!(serde_json::to_value(&cat).unwrap(), old);
    }

    #[test]
    fn a_built_catalog_is_private_until_stamped_for_release() {
        let cat = build_catalog(&GraphStore::new(), &[], &[]).unwrap();
        assert_eq!(cat.publishable, Some(false));
        let v = serde_json::to_value(&cat).unwrap();
        assert_eq!(v["publishable"], json!(false));
        assert!(v.get("tenant").is_none());
    }

    #[test]
    fn every_failure_class_explains_itself() {
        for (class, word) in [
            (FailureClass::EmptyResult, "no rows"),
            (FailureClass::PartialResult, "fewer rows"),
            (FailureClass::ExtraRows, "more rows"),
            (FailureClass::ValuesDiffer, "wrong values"),
            (FailureClass::QueryError, "query failed"),
        ] {
            assert!(class.explain().contains(word), "{class:?}");
        }
    }

    #[test]
    fn a_sample_outside_its_declared_enum_is_refused() {
        let mut p = spec("s", "string", json!("x"));
        p.enum_values = Some(vec![json!("a"), json!("b")]);
        let err =
            validate_entry_shape("q1", "MATCH (n) WHERE n.k = $s RETURN n", &[p]).unwrap_err();
        assert!(err.contains("not in the declared enum"), "{err}");
    }

    #[test]
    fn verify_classifies_errors_and_row_count_changes() {
        let store = people();
        let queries: Vec<QuerySpec> = serde_json::from_value(json!([
        { "id": "all", "cypher": "MATCH (p:Person) RETURN p.name AS name" },
        { "id": "active", "cypher": "MATCH (p:Person) WHERE p.active = $a RETURN p.name AS name",
          "params": [{ "name": "a", "type": "bool", "sample": true }] }
    ]))
    .unwrap();
        let mut catalog = build_catalog(&store, &queries, &[]).unwrap();
        assert!(verify(&store, &catalog).unwrap().is_ok());

        // Pretend the snapshot had fewer and more rows than it does now.
        catalog.entries[0].rows = 5;
        catalog.entries[1].rows = 1;
        let report = verify(&store, &catalog).unwrap();
        let classes: Vec<Option<FailureClass>> = report.results.iter().map(|r| r.failure).collect();
        assert_eq!(
            classes,
            vec![
                Some(FailureClass::PartialResult),
                Some(FailureClass::ExtraRows)
            ]
        );
        assert_eq!(report.results[0].detail, "3 of 5 rows");
        assert_eq!(report.results[1].detail, "2 rows, expected 1");

        // A query that no longer parses is a query error, not a crash.
        catalog.entries[0].cypher = "MATCH (p:Person RETURN p".into();
        let report = verify(&store, &catalog).unwrap();
        assert_eq!(report.results[0].failure, Some(FailureClass::QueryError));
        assert_eq!(report.failed().count(), 2);
        assert!(!report.is_ok());
    }

    #[test]
    fn verify_refuses_an_unknown_catalog_format() {
        let catalog = QueryCatalog {
            format: "other/9".into(),
            generated_by: String::new(),
            provenance: crate::snapshot::publish_gate::Provenance::Authored,
            tenant: None,
            publishable: None,
            entries: vec![],
        };
        let err = verify(&GraphStore::new(), &catalog).unwrap_err();
        assert!(err.contains("refusing to guess"), "{err}");
    }

    #[test]
    fn a_catalog_is_checked_against_the_digest_its_snapshot_records() {
        use crate::snapshot::format::QueriesRef;
        let bytes = br#"{"format":"samyama.queries/1"}"#;
        // The digest `sha256sum` gives for the same bytes.
        assert_eq!(
            queries_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let link = QueriesRef {
            file: "kg.sgqueries".into(),
            sha256: queries_sha256(bytes),
        };
        assert_eq!(check_queries_ref(&link, bytes), Ok(()));

        let err = check_queries_ref(&link, br#"{"format":"samyama.queries/2"}"#).unwrap_err();
        assert!(
            err.contains("not the catalog the snapshot was published with"),
            "{err}"
        );
        assert!(err.contains("kg.sgqueries"), "{err}");
    }
}
