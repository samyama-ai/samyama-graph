//! `GET /api/icons` and `PUT /api/icons`: the graph's node style (#2029).
//!
//! The style is the one the last snapshot import loaded from its header, or
//! the last one a client put. It lives on the store, so the next
//! `POST /api/snapshot/export` writes it into the header and it travels with
//! the file.

use axum::{
    body::Bytes,
    extract::{Extension, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::graph::GraphStore;
use crate::http::handler::reject_foreign_graph;
use crate::http::server::{AppState, Subject};
use crate::snapshot::icons::{IconStyle, FORMAT, MAX_STYLE_BYTES};

/// Largest `PUT /api/icons` body the router reads before the handler sees it;
/// the handler refuses anything over [`MAX_STYLE_BYTES`] with its own code.
pub(crate) const PUT_BODY_LIMIT: usize = 4 * MAX_STYLE_BYTES;

#[derive(Deserialize, Default)]
pub struct IconsQuery {
    #[serde(default)]
    pub graph: Option<String>,
}

fn refusal(status: StatusCode, code: &str, error: String, extra: Value) -> Response {
    let mut body = json!({ "code": code, "error": error });
    if let (Some(b), Value::Object(extra)) = (body.as_object_mut(), extra) {
        b.extend(extra);
    }
    (status, Json(body)).into_response()
}

fn schema(store: &GraphStore) -> (Vec<String>, Vec<String>) {
    let mut labels: Vec<String> = store
        .all_labels()
        .into_iter()
        .map(|l| l.as_str().to_string())
        .collect();
    labels.sort_unstable();
    let edge_types = store
        .all_edge_types()
        .into_iter()
        .map(|t| t.as_str().to_string())
        .collect();
    (labels, edge_types)
}

/// The style as served: empty maps when the graph has none.
fn body(store: &GraphStore) -> Value {
    let empty = IconStyle::empty();
    let style = store.icon_style().unwrap_or(&empty);
    json!({
        "format": FORMAT,
        "nodes": style.nodes,
        "edges": style.edges,
        "source": store.icon_style_source(),
    })
}

fn authorize(subject: &Option<Extension<Subject>>, graph: &str, write: bool) -> Option<Response> {
    let Some(Extension(Subject(user))) = subject else {
        return None;
    };
    user.authorize_statement::<&str>(graph, Ok(write))
        .err()
        .map(|e| (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response())
}

/// `GET /api/icons?graph=`
pub async fn get_icons_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Query(q): Query<IconsQuery>,
) -> Response {
    let graph = q.graph.unwrap_or_else(|| "default".to_string());
    if let Some(resp) = authorize(&subject, &graph, false) {
        return resp;
    }
    if let Some(resp) = reject_foreign_graph(&graph) {
        return resp;
    }
    let store = state.store.read().await;
    Json(body(&store)).into_response()
}

/// `PUT /api/icons?graph=`: replaces the style with the body, a
/// `samyama.icons/1` object (`format` may be left out). Unlike an import, a
/// mistake refuses the whole body: the caller is there to fix it. An empty
/// style clears it.
pub async fn put_icons_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Query(q): Query<IconsQuery>,
    bytes: Bytes,
) -> Response {
    let graph = q.graph.unwrap_or_else(|| "default".to_string());
    if let Some(resp) = authorize(&subject, &graph, true) {
        return resp;
    }
    if let Some(resp) = reject_foreign_graph(&graph) {
        return resp;
    }
    if bytes.len() > MAX_STYLE_BYTES {
        return refusal(
            StatusCode::PAYLOAD_TOO_LARGE,
            "style_too_large",
            format!(
                "the style is {} bytes; at most {MAX_STYLE_BYTES} are accepted",
                bytes.len()
            ),
            json!({ "limit": MAX_STYLE_BYTES }),
        );
    }
    let raw: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => {
            return refusal(
                StatusCode::BAD_REQUEST,
                "invalid_style",
                format!("the body is not JSON: {e}"),
                json!({ "problems": [e.to_string()] }),
            )
        }
    };
    let style = match IconStyle::strict(&raw) {
        Ok(s) => s,
        Err(problems) => {
            return refusal(
                StatusCode::BAD_REQUEST,
                "invalid_style",
                format!(
                    "the style has {} problem(s); nothing was changed",
                    problems.len()
                ),
                json!({ "problems": problems }),
            )
        }
    };

    let mut store = state.store.write().await;
    let (labels, edge_types) = schema(&store);
    // Kept as given, labels the graph does not have yet included: a client may
    // style a label before its data arrives. The report says which they are.
    let mut report = style.report("api", &labels, &edge_types);
    if style.is_empty() {
        report.source = "none";
    }
    store.set_icon_style(Some(style), "api");
    let mut out = body(&store);
    out["icons"] = serde_json::to_value(&report).unwrap_or(Value::Null);
    Json(out).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_body_of_a_graph_without_a_style_has_empty_maps() {
        let store = GraphStore::new();
        let b = body(&store);
        assert_eq!(b["format"], FORMAT);
        assert_eq!(b["nodes"], json!({}));
        assert_eq!(b["edges"], json!({}));
        assert_eq!(b["source"], "none");
    }
}
