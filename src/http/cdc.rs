//! `GET /api/cdc/events`: what changed, after an offset (#2025).
//!
//! `GET /api/changes` (#1771) answers *whether* the graph changed since a
//! token, from memory, for a view deciding whether to re-run its query. This
//! answers *what* changed: the committed change events of one tenant, in
//! order, from the durable feed `crate::persistence::cdc` writes beside the
//! rows. A consumer keeps the `next` offset of its last page and asks for the
//! events after it, across restarts of either side.

use crate::http::server::{AppState, Subject};
use crate::persistence::{cdc, ChangeReadError};
use axum::extract::{Extension, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

/// Longest a request may wait for the next event, in seconds.
pub const MAX_WAIT_SECS: u64 = 30;

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({ "error": message.into() })),
    )
        .into_response()
}

/// A query parameter as a whole number, `default` when absent.
fn number(params: &HashMap<String, String>, name: &str, default: u64) -> Result<u64, Response> {
    match params.get(name) {
        None => Ok(default),
        Some(v) => v.parse::<u64>().map_err(|_| {
            error(
                StatusCode::BAD_REQUEST,
                format!("`{name}` must be a whole number, got {v:?}"),
            )
        }),
    }
}

/// `GET /api/cdc/events?after=<offset>&limit=<n>[&graph=<tenant>][&wait=<s>]`
///
/// The tenant's committed change events with offsets above `after`, oldest
/// first, at most `limit` (default 100, at most 1000). `after` defaults to 0,
/// the beginning. `wait` holds the request open up to that many seconds (at
/// most 30) when there is nothing yet, and answers as soon as an event is
/// committed: a long poll, so a consumer need not poll in a loop.
///
/// The parameters are read by hand rather than by `Query<T>`, whose rejection
/// is a `text/plain` 400 that the OpenAPI document does not promise.
pub async fn events_handler(
    State(state): State<AppState>,
    subject: Option<Extension<Subject>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let graph = params
        .get("graph")
        .cloned()
        .unwrap_or_else(|| "default".to_string());
    // The tenant binding, as on `/api/query`: a credential bound to one
    // tenant reads that tenant's feed and no other.
    if let Some(Extension(Subject(user))) = &subject {
        if let Err(e) = user.authorize_graph(&graph) {
            return error(StatusCode::FORBIDDEN, e);
        }
    }
    let after = match number(&params, "after", 0) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let limit = match number(&params, "limit", cdc::DEFAULT_LIMIT as u64) {
        Ok(0) => {
            return error(StatusCode::BAD_REQUEST, "`limit` must be at least 1");
        }
        Ok(v) => v.min(cdc::MAX_LIMIT as u64) as usize,
        Err(r) => return r,
    };
    let wait = match number(&params, "wait", 0) {
        Ok(v) => Duration::from_secs(v.min(MAX_WAIT_SECS)),
        Err(r) => return r,
    };
    let Some(pm) = state.persistence.clone() else {
        return error(
            StatusCode::NOT_FOUND,
            "change data capture needs a data directory; this server keeps nothing on disk",
        );
    };

    let deadline = tokio::time::Instant::now() + wait;
    let page = loop {
        // Registered before the read, so an event committed between the read
        // and the wait still wakes it.
        let notified = pm.change_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let page = match pm.read_changes(&graph, after, limit) {
            Ok(page) => page,
            Err(e @ ChangeReadError::Disabled) => {
                return error(StatusCode::NOT_FOUND, e.to_string())
            }
            Err(ChangeReadError::Trimmed {
                after,
                oldest,
                latest,
            }) => {
                return (
                    StatusCode::GONE,
                    [(header::CACHE_CONTROL, "no-store")],
                    Json(json!({
                        "error": ChangeReadError::Trimmed { after, oldest, latest }.to_string(),
                        "after": after,
                        "oldest": oldest,
                        "latest": latest,
                    })),
                )
                    .into_response();
            }
            Err(e @ ChangeReadError::Storage(_)) => {
                return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        };
        if !page.events.is_empty() || tokio::time::Instant::now() >= deadline {
            break page;
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            break page;
        }
    };

    let next = page
        .events
        .last()
        .and_then(|e| e["offset"].as_u64())
        .unwrap_or(after);
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "graph": graph,
            "events": page.events,
            "next": next,
            "oldest": page.oldest,
            "latest": page.latest,
            "more": next < page.latest,
        })),
    )
        .into_response()
}
