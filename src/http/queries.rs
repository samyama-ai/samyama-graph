//! A graph's query catalog (`.sgqueries`) over HTTP (#2026).
//!
//! The CLI could list and run a catalog's templates (`samyama catalog`); a
//! client of the server could not. These endpoints load a catalog for a graph
//! and list, search, describe and run its entries:
//!
//! | endpoint | needs |
//! |---|---|
//! | `POST /api/queries/import` (multipart `file`) | write |
//! | `GET /api/queries?q=&difficulty=` | read |
//! | `GET /api/queries/:id` | read |
//! | `POST /api/queries/:id/run` `{params}` | read |
//!
//! A run goes through `run_template_in` in `RunMode::Api`: values are bound,
//! never substituted; every declared parameter needs a value; types and enums
//! are checked before the query runs; and the run is held to the entry's work
//! ceiling. Refusals carry a `code` and the fields that name what was wrong.
//!
//! A catalog is uploaded content that anyone who may read can then run, so an
//! entry whose query writes is refused at import.
//!
//! v1 keeps catalogs in memory, one per graph: a restart drops them, and a
//! client must handle `no_catalog`. Importing a snapshot replaces the graph's
//! data and so drops its catalog too (`restore_snapshot_handler`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::extract::{Json, Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{json, Value as J};

use crate::http::server::AppState;
use crate::snapshot::verify::{
    queries_sha256, refuse_writing_entry, run_template_in, validate_entry_shape, work_ceiling,
    CatalogEntry, QueryCatalog, RunMode, TemplateError,
};

/// The largest catalog `POST /api/queries/import` accepts. The published
/// surveillance catalog, 34 entries, is under 40 KB; this leaves room for
/// catalogs hundreds of times that size without letting an upload hold
/// unbounded memory per graph.
pub const MAX_CATALOG_BYTES: usize = 8 << 20;

/// The most rows one `run` answers with. The work ceiling bounds how much a
/// run does, not how much it returns: a template that legitimately lists a
/// million nodes does a million rows of work and is under a ceiling built for
/// it. Past this the answer is cut and says so.
pub const MAX_RUN_ROWS: usize = 10_000;

/// A catalog as loaded for one graph.
#[derive(Debug, Clone)]
pub struct LoadedCatalog {
    pub catalog: QueryCatalog,
    /// SHA-256 of the bytes as uploaded, as `queries_sha256` gives it.
    pub sha256: String,
}

/// Every graph's loaded catalog, by graph name.
pub type Catalogs = Arc<std::sync::RwLock<HashMap<String, LoadedCatalog>>>;

fn default_graph() -> String {
    "default".to_string()
}

/// A refusal: `{"error", "code", ...fields}`.
pub(crate) fn refusal(
    status: StatusCode,
    code: &str,
    error: String,
    fields: serde_json::Map<String, J>,
) -> Response {
    let mut body = serde_json::Map::new();
    body.insert("error".into(), J::String(error));
    body.insert("code".into(), J::String(code.into()));
    body.extend(fields);
    (status, Json(J::Object(body))).into_response()
}

fn plain(status: StatusCode, code: &str, error: String) -> Response {
    refusal(status, code, error, serde_json::Map::new())
}

fn no_catalog(graph: &str) -> Response {
    plain(
        StatusCode::NOT_FOUND,
        "no_catalog",
        format!(
            "graph '{graph}' has no query catalog loaded. Load one with \
             POST /api/queries/import; catalogs are held in memory and a restart drops them."
        ),
    )
}

/// The HTTP status a template refusal is answered with.
fn status_of(e: &TemplateError) -> StatusCode {
    match e {
        TemplateError::ParamMissing { .. }
        | TemplateError::ParamUnknown { .. }
        | TemplateError::ParamType { .. }
        | TemplateError::ParamNotAllowed { .. } => StatusCode::BAD_REQUEST,
        TemplateError::WorkCeiling { .. } | TemplateError::Entry { .. } => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        TemplateError::Query { .. } => StatusCode::BAD_REQUEST,
    }
}

/// Parse and check a catalog file: JSON in the `QueryCatalog` shape, unique
/// ids, every entry parameterized rather than assembled, and none that writes.
///
/// Every problem is found before anything is loaded, so a refused upload
/// leaves the graph's current catalog in place.
pub fn parse_catalog(bytes: &[u8]) -> Result<QueryCatalog, String> {
    let catalog: QueryCatalog = serde_json::from_slice(bytes)
        .map_err(|e| format!("not a query catalog (.sgqueries): {e}"))?;
    let mut seen = std::collections::HashSet::new();
    for entry in &catalog.entries {
        if !seen.insert(entry.id.as_str()) {
            return Err(format!(
                "{}: the id appears twice; an id names one entry",
                entry.id
            ));
        }
        validate_entry_shape(&entry.id, &entry.cypher, &entry.params)?;
        refuse_writing_entry(entry)?;
    }
    Ok(catalog)
}

/// Replace `graph`'s catalog. Returns the import summary.
pub fn install(catalogs: &Catalogs, graph: &str, bytes: &[u8], catalog: QueryCatalog) -> J {
    let sha256 = queries_sha256(bytes);
    let mut by: BTreeMap<&str, usize> = [("easy", 0), ("medium", 0), ("hard", 0)].into();
    for e in &catalog.entries {
        if let Some(n) = by.get_mut(e.difficulty.as_str()) {
            *n += 1;
        }
    }
    let summary = json!({
        "loaded": catalog.entries.len(),
        "sha256": sha256,
        "by_difficulty": by,
        "with_params": catalog.entries.iter().filter(|e| !e.params.is_empty()).count(),
    });
    let replaced = catalogs
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .insert(graph.to_string(), LoadedCatalog { catalog, sha256 })
        .is_some();
    let mut summary = summary;
    summary["replaced"] = J::Bool(replaced);
    summary
}

/// Drop `graph`'s catalog, as a snapshot import must. Returns whether one was
/// loaded.
pub fn drop_catalog(catalogs: &Catalogs, graph: &str) -> bool {
    catalogs
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .remove(graph)
        .is_some()
}

/// An entry as the list shows it: enough to draw its parameter form.
fn summary(e: &CatalogEntry) -> J {
    json!({
        "id": e.id,
        "question": e.question,
        "paraphrases": e.paraphrases,
        "difficulty": e.difficulty,
        "unanswerable": e.unanswerable,
        "params": e.params.iter().map(|p| json!({
            "name": p.name,
            "type": p.kind,
            "sample": p.sample,
            "enum_values": p.enum_values,
        })).collect::<Vec<_>>(),
    })
}

/// Does every word of `q` appear in `e`'s question, or every word in one of
/// its paraphrases? Each is a whole phrasing of the question, so words are not
/// pooled across them: "cases" from the question and "country" from a
/// paraphrase do not together make a match.
fn matches(e: &CatalogEntry, q: &str) -> bool {
    let words: Vec<String> = q.split_whitespace().map(str::to_lowercase).collect();
    std::iter::once(&e.question)
        .chain(&e.paraphrases)
        .map(|s| s.to_lowercase())
        .any(|text| words.iter().all(|w| text.contains(w.as_str())))
}

#[derive(Debug, Deserialize)]
pub struct GraphParam {
    #[serde(default = "default_graph")]
    pub graph: String,
}

/// `POST /api/queries/import`: load a `.sgqueries` file for a graph.
pub async fn import_handler(
    State(state): State<AppState>,
    Query(p): Query<GraphParam>,
    mut multipart: Multipart,
) -> Response {
    let mut graph = p.graph;
    let mut file: Option<Vec<u8>> = None;
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                return plain(
                    e.status(),
                    "bad_upload",
                    format!("failed to read the upload: {e}. A catalog may be at most {MAX_CATALOG_BYTES} bytes."),
                )
            }
        };
        match field.name().unwrap_or("") {
            "file" => match field.bytes().await {
                Ok(b) => file = Some(b.to_vec()),
                Err(e) => {
                    return plain(
                        e.status(),
                        "bad_upload",
                        format!("failed to read file: {e}"),
                    )
                }
            },
            "graph" => match field.text().await {
                Ok(t) => graph = t.trim().to_string(),
                Err(e) => {
                    return plain(
                        e.status(),
                        "bad_upload",
                        format!("failed to read graph: {e}"),
                    )
                }
            },
            _ => {}
        }
    }
    if let Some(r) = super::handler::reject_foreign_graph(&graph) {
        return r;
    }
    let Some(bytes) = file else {
        return plain(
            StatusCode::BAD_REQUEST,
            "bad_upload",
            "no file field in the multipart request".into(),
        );
    };
    if bytes.len() > MAX_CATALOG_BYTES {
        return plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "bad_upload",
            format!(
                "the catalog is {} bytes; at most {MAX_CATALOG_BYTES} are accepted",
                bytes.len()
            ),
        );
    }
    match parse_catalog(&bytes) {
        Ok(catalog) => {
            Json(install(&state.query_catalogs, &graph, &bytes, catalog)).into_response()
        }
        Err(e) => plain(StatusCode::BAD_REQUEST, "invalid_catalog", e),
    }
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub difficulty: Option<String>,
    #[serde(default = "default_graph")]
    pub graph: String,
}

/// `GET /api/queries`: list the graph's entries, filtered by keyword and
/// difficulty.
pub async fn list_handler(State(state): State<AppState>, Query(p): Query<ListParams>) -> Response {
    if let Some(r) = super::handler::reject_foreign_graph(&p.graph) {
        return r;
    }
    let catalogs = state
        .query_catalogs
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let Some(loaded) = catalogs.get(&p.graph) else {
        return no_catalog(&p.graph);
    };
    let results: Vec<J> = loaded
        .catalog
        .entries
        .iter()
        .filter(|e| {
            p.difficulty
                .as_deref()
                .is_none_or(|d| e.difficulty.eq_ignore_ascii_case(d))
        })
        .filter(|e| p.q.as_deref().is_none_or(|q| matches(e, q)))
        .map(summary)
        .collect();
    Json(json!({ "total": results.len(), "sha256": loaded.sha256, "results": results }))
        .into_response()
}

fn find<'a>(loaded: &'a LoadedCatalog, id: &str) -> Option<&'a CatalogEntry> {
    loaded.catalog.entries.iter().find(|e| e.id == id)
}

fn not_found(id: &str) -> Response {
    plain(
        StatusCode::NOT_FOUND,
        "query_not_found",
        format!("the catalog has no entry with id {id:?}"),
    )
}

/// `GET /api/queries/:id`: one entry, with its Cypher and work ceiling.
pub async fn get_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<GraphParam>,
) -> Response {
    if let Some(r) = super::handler::reject_foreign_graph(&p.graph) {
        return r;
    }
    let catalogs = state
        .query_catalogs
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let Some(loaded) = catalogs.get(&p.graph) else {
        return no_catalog(&p.graph);
    };
    let Some(e) = find(loaded, &id) else {
        return not_found(&id);
    };
    let mut body = summary(e);
    body["cypher"] = J::String(e.cypher.clone());
    body["work"] = json!(e.work);
    body["work_ceiling"] = json!(e.work.map(work_ceiling));
    Json(body).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    #[serde(default)]
    pub params: BTreeMap<String, J>,
    #[serde(default = "default_graph")]
    pub graph: String,
}

/// `POST /api/queries/:id/run`: run an entry with bound values.
pub async fn run_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<RunRequest>,
) -> Response {
    if let Some(r) = super::handler::reject_foreign_graph(&req.graph) {
        return r;
    }
    // Cloned out so the catalog lock is not held across the store's.
    let entry = {
        let catalogs = state
            .query_catalogs
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let Some(loaded) = catalogs.get(&req.graph) else {
            return no_catalog(&req.graph);
        };
        match find(loaded, &id) {
            Some(e) => e.clone(),
            None => return not_found(&id),
        }
    };
    let store = crate::store_lock::read(&state.store).await;
    let batch = match run_template_in(&store, &entry, &req.params, RunMode::Api) {
        Ok(b) => b,
        Err(e) => return refusal(status_of(&e), e.code(), e.to_string(), e.fields()),
    };
    drop(store);
    let full_props = HashMap::new();
    let mut nodes = HashMap::new();
    let mut edges = HashMap::new();
    let row_count = batch.records.len();
    let rows: Vec<Vec<J>> = batch
        .records
        .iter()
        .take(MAX_RUN_ROWS)
        .map(|r| {
            batch
                .columns
                .iter()
                .map(|c| {
                    super::handler::render_value(
                        r.get(c).unwrap_or(&crate::query::Value::Null),
                        &full_props,
                        &mut nodes,
                        &mut edges,
                    )
                })
                .collect()
        })
        .collect();
    Json(json!({
        "columns": batch.columns,
        "rows": rows,
        "row_count": row_count,
        "truncated": row_count > MAX_RUN_ROWS,
        "work": entry.work,
        "work_ceiling": entry.work.map(work_ceiling),
        "source": "computed",
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, cypher: &str) -> J {
        json!({"id": id, "question": "q", "cypher": cypher, "rows": 1, "hash": "h", "work": 1})
    }

    fn catalog(entries: Vec<J>) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "format": "samyama.queries/1",
            "generated_by": "test",
            "provenance": "authored",
            "entries": entries,
        }))
        .unwrap()
    }

    #[test]
    fn a_writing_entry_is_refused_at_import() {
        for q in [
            "CREATE (n:X) RETURN n",
            "MATCH (n) SET n.x = 1 RETURN n",
            "MATCH (n) DETACH DELETE n",
            "MERGE (n:X {id: 1}) RETURN n",
            "MATCH (n) REMOVE n.x RETURN n",
        ] {
            let err = parse_catalog(&catalog(vec![entry("w", q)])).unwrap_err();
            assert!(err.contains("writes"), "{q}: {err}");
        }
    }

    #[test]
    fn a_read_naming_a_write_keyword_is_not_refused() {
        let q = "MATCH (n) WHERE n.created = 'DELETE' RETURN n.created AS c";
        parse_catalog(&catalog(vec![entry("r", q)])).unwrap();
    }

    #[test]
    fn a_duplicate_id_is_refused() {
        let q = "MATCH (n) RETURN count(n) AS c";
        let err = parse_catalog(&catalog(vec![entry("a", q), entry("a", q)])).unwrap_err();
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn install_reports_replacement_and_drop_clears() {
        let c: Catalogs = Default::default();
        let bytes = catalog(vec![entry("a", "MATCH (n) RETURN count(n) AS c")]);
        let first = install(&c, "default", &bytes, parse_catalog(&bytes).unwrap());
        assert_eq!(first["replaced"], json!(false));
        assert_eq!(first["loaded"], json!(1));
        let again = install(&c, "default", &bytes, parse_catalog(&bytes).unwrap());
        assert_eq!(again["replaced"], json!(true));
        assert!(drop_catalog(&c, "default"));
        assert!(!drop_catalog(&c, "default"));
    }

    #[test]
    fn search_needs_every_word_in_the_question_or_a_paraphrase() {
        let e: CatalogEntry = serde_json::from_value(json!({
            "id": "a", "question": "Cases by region", "paraphrases": ["outbreaks per country"],
            "cypher": "RETURN 1 AS x", "rows": 1, "hash": "h",
        }))
        .unwrap();
        assert!(matches(&e, "cases REGION"));
        assert!(matches(&e, "outbreaks country"));
        assert!(!matches(&e, "cases country"));
    }
}
