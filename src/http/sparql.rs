//! `POST /api/sparql`: SPARQL over the graph as it is now (#2100, part 3).
//!
//! The query reads the store through [`GraphAsRdf`] under the read lock, so it
//! sees the data a Cypher query would, copies nothing, and writes nothing. The
//! answer is the W3C SPARQL 1.1 Query Results JSON format, which SPARQL
//! clients read without an adapter.
//!
//! Two request bodies are accepted:
//!
//! * `Content-Type: application/sparql-query` with the query as the body: the
//!   SPARQL 1.1 Protocol's direct POST, which existing SPARQL tools send.
//! * JSON `{"query": "...", "graph": "default", "base": "..."}`, as
//!   `/api/query` takes its statement.
//!
//! The IRIs are the ones `samyama convert` exports under the same base, so a
//! query written against an exported file runs here unchanged. The base
//! defaults to that tool's default and can be named per request.

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::http::server::{AppState, Subject};
use crate::rdf::MappingConfig;
use crate::sparql::{SparqlEngine, SparqlError};

/// The base IRI `samyama convert` exports under when none is configured.
pub const DEFAULT_BASE_IRI: &str = "https://samyama.ai/rdf/";

const SPARQL_QUERY: &str = "application/sparql-query";
const SPARQL_RESULTS_JSON: &str = "application/sparql-results+json";

#[derive(Deserialize)]
struct JsonRequest {
    query: String,
    #[serde(default = "default_graph")]
    graph: String,
    #[serde(default)]
    base: Option<String>,
}

fn default_graph() -> String {
    "default".to_string()
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": message.into() }))).into_response()
}

/// The query, graph and base a request names, or why it names none.
fn read_request(headers: &HeaderMap, body: &Bytes) -> Result<JsonRequest, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if content_type == SPARQL_QUERY {
        let query = std::str::from_utf8(body).map_err(|_| "the query is not UTF-8".to_string())?;
        return Ok(JsonRequest { query: query.to_string(), graph: default_graph(), base: None });
    }
    serde_json::from_slice(body).map_err(|e| {
        format!("send the query as `{SPARQL_QUERY}`, or as JSON {{\"query\": \"...\"}}: {e}")
    })
}

/// `POST /api/sparql`.
pub async fn sparql_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let req = match read_request(&headers, &body) {
        Ok(r) => r,
        Err(why) => return error(StatusCode::BAD_REQUEST, why),
    };
    // One graph per build, refused by name as `/api/query` refuses it (#366):
    // an ignored argument would answer from a graph the caller did not name.
    if req.graph != default_graph() {
        return error(
            StatusCode::BAD_REQUEST,
            format!(
                "This build serves a single graph ('default'); the requested graph '{}' does not exist.",
                req.graph
            ),
        );
    }
    // A SPARQL query here only reads; UPDATE is refused by the engine.
    if let Some(Extension(Subject(user))) = &subject {
        if let Err(e) = user.authorize_statement(&req.graph, Ok::<bool, ()>(false)) {
            return error(StatusCode::FORBIDDEN, e);
        }
    }
    let base = req.base.unwrap_or_else(|| DEFAULT_BASE_IRI.to_string());

    let store = crate::store_lock::read(&state.store).await;
    let answer = SparqlEngine::over_graph(&store, MappingConfig::new(base)).query(&req.query);
    drop(store);

    match answer {
        Ok(results) => match results.to_json() {
            Ok(doc) => ([(header::CONTENT_TYPE, SPARQL_RESULTS_JSON)], doc.to_string()).into_response(),
            Err(e) => error(StatusCode::BAD_REQUEST, e),
        },
        // Not evaluated is a refusal, never an empty 200: an empty answer to a
        // question the engine did not understand reads as "no matches".
        Err(SparqlError::Parse(e)) => error(StatusCode::BAD_REQUEST, format!("SPARQL parse error: {e}")),
        Err(e) => error(StatusCode::BAD_REQUEST, e.to_string()),
    }
}
