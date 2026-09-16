//! BEGIN / COMMIT / ROLLBACK over HTTP (#1200 step 6b, LANG-07).
//!
//! `POST /api/tx/begin` takes the store's write lock and opens a session
//! transaction in the store (step 6a). The guard is kept in `AppState` under
//! a transaction id, so the lock outlives the request. `POST /api/query` with
//! `"tx": <id>` runs the statement against that guard; `POST /api/tx/:id/commit`
//! and `POST /api/tx/:id/rollback` end it and release the lock.
//!
//! No other request can read or write while a transaction is open, because
//! the lock is held. That is what makes a transaction's writes invisible to
//! everyone else without a buffer in the executor. It is also why a transaction
//! must not be left open: each one is rolled back after
//! `GraphStore::session_transaction_timeout()`.
//!
//! Persistence follows the transaction: the write log opens at BEGIN, is
//! applied at COMMIT, and is dropped at ROLLBACK, when nothing it recorded
//! should reach disk.

use crate::graph::GraphStore;
use crate::http::server::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, OwnedRwLockWriteGuard};

/// One open HTTP transaction: the writer's lock, and when it expires.
pub struct HttpTxn {
    pub guard: OwnedRwLockWriteGuard<GraphStore>,
    pub deadline: Instant,
}

/// Open transactions by id.
pub type TxnSessions = Arc<Mutex<HashMap<String, HttpTxn>>>;

/// Undo everything the transaction did, drop what it logged for persistence,
/// and release the lock.
pub fn roll_back(mut txn: HttpTxn) {
    if let Err(e) = txn.guard.rollback_session_transaction() {
        tracing::warn!("rollback of an HTTP transaction failed: {e}");
    }
    let _ = txn.guard.take_write_log();
}

fn error(status: StatusCode, message: String) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

pub fn not_open(id: &str) -> Response {
    error(
        StatusCode::NOT_FOUND,
        format!(
            "no open transaction '{id}': it was committed, rolled back, or timed out \
             and was rolled back"
        ),
    )
}

pub async fn begin_handler(State(state): State<AppState>) -> Response {
    let mut guard = Arc::clone(&state.store).write_owned().await;
    let version = match guard.begin_session_transaction() {
        Ok(v) => v,
        Err(e) => return error(StatusCode::CONFLICT, e.to_string()),
    };
    if state.persistence.is_some() {
        guard.enable_write_log();
    }
    let id = uuid::Uuid::new_v4().to_string();
    let limit = GraphStore::session_transaction_timeout();
    state
        .transactions
        .lock()
        .await
        .insert(id.clone(), HttpTxn { guard, deadline: Instant::now() + limit });

    // An abandoned transaction would hold the lock for good.
    let sessions = Arc::clone(&state.transactions);
    let expiring = id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(limit).await;
        let txn = sessions.lock().await.remove(&expiring);
        if let Some(txn) = txn {
            tracing::warn!("transaction {expiring} was open for {}s; rolled back", limit.as_secs());
            roll_back(txn);
        }
    });

    Json(json!({ "tx": id, "version": version, "timeout_seconds": limit.as_secs() })).into_response()
}

pub async fn commit_handler(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(mut txn) = state.transactions.lock().await.remove(&id) else {
        return not_open(&id);
    };
    if Instant::now() > txn.deadline {
        roll_back(txn);
        return error(StatusCode::CONFLICT, format!("transaction '{id}' timed out and was rolled back"));
    }
    let version = match txn.guard.commit_session_transaction() {
        Ok(v) => v,
        Err(e) => return error(StatusCode::CONFLICT, e.to_string()),
    };
    if let Some(pm) = &state.persistence {
        let mutations = txn.guard.take_write_log();
        if let Err(e) = pm.apply_mutations("default", &txn.guard, &mutations) {
            tracing::warn!("failed to persist a committed transaction ({} mutations): {e}", mutations.len());
        }
    }
    Json(json!({ "tx": id, "committed": true, "version": version })).into_response()
}

pub async fn rollback_handler(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(txn) = state.transactions.lock().await.remove(&id) else {
        return not_open(&id);
    };
    roll_back(txn);
    Json(json!({ "tx": id, "rolled_back": true })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::handler::query_handler;
    use crate::query::QueryEngine;
    use axum::{body::Body, http::Request, routing::post, Router};
    use http_body_util::BodyExt;
    use tokio::sync::RwLock;
    use tower::util::ServiceExt;

    fn app() -> (Router, AppState) {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
            persistence: None,
            transactions: Default::default(),
        };
        let router = Router::new()
            .route("/api/query", post(query_handler))
            .route("/api/tx/begin", post(begin_handler))
            .route("/api/tx/:id/commit", post(commit_handler))
            .route("/api/tx/:id/rollback", post(rollback_handler))
            .with_state(state.clone());
        (router, state)
    }

    async fn post_json(app: &Router, uri: &str, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
        let response = app
            .clone()
            .oneshot(
                Request::post(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
    }

    async fn begin(app: &Router) -> String {
        let (status, body) = post_json(app, "/api/tx/begin", json!({})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["tx"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn a_rolled_back_transaction_leaves_nothing() {
        let (app, state) = app();
        let tx = begin(&app).await;
        let (status, body) =
            post_json(&app, "/api/query", json!({ "query": "CREATE (:T {x: 1})", "tx": tx })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, _) = post_json(&app, &format!("/api/tx/{tx}/rollback"), json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(state.store.read().await.node_count(), 0, "a rolled-back CREATE survived");
    }

    #[tokio::test]
    async fn a_committed_transaction_keeps_its_writes_and_reads_them_inside() {
        let (app, state) = app();
        let tx = begin(&app).await;
        post_json(&app, "/api/query", json!({ "query": "CREATE (:T {x: 1})", "tx": tx })).await;
        let (_, inside) = post_json(
            &app,
            "/api/query",
            json!({ "query": "MATCH (t:T) RETURN count(t) AS n", "tx": tx }),
        )
        .await;
        assert_eq!(inside["records"][0][0], json!(1), "the transaction cannot read its own write: {inside}");
        let (status, body) = post_json(&app, &format!("/api/tx/{tx}/commit"), json!({})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(state.store.read().await.node_count(), 1);
    }

    #[tokio::test]
    async fn nobody_else_can_read_while_a_transaction_is_open() {
        let (app, state) = app();
        let tx = begin(&app).await;
        assert!(state.store.try_read().is_err(), "a reader got in while a transaction held the lock");
        post_json(&app, &format!("/api/tx/{tx}/rollback"), json!({})).await;
        assert!(state.store.try_read().is_ok(), "the lock was not released");
    }

    #[tokio::test]
    async fn an_unknown_or_finished_transaction_is_refused() {
        let (app, _) = app();
        let (status, _) = post_json(&app, &format!("/api/tx/{}/commit", "nope"), json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let tx = begin(&app).await;
        post_json(&app, &format!("/api/tx/{tx}/commit"), json!({})).await;
        let (status, _) = post_json(&app, &format!("/api/tx/{tx}/rollback"), json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "a committed transaction was rolled back");
        let (status, _) =
            post_json(&app, "/api/query", json!({ "query": "CREATE (:T)", "tx": tx })).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "a query ran in a finished transaction");
    }
}
