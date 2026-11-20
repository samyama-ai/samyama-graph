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
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedRwLockWriteGuard};

/// One open HTTP transaction: the writer's lock, and when it expires.
pub struct HttpTxn {
    pub guard: OwnedRwLockWriteGuard<GraphStore>,
    pub deadline: Instant,
    pub limit: Duration,
}

/// How many timed-out transaction ids are remembered, so a late COMMIT,
/// ROLLBACK or query can be told its transaction expired rather than that it
/// never existed (#1518). Bounded: the oldest are forgotten first, and a
/// forgotten one gets the 404 it would have got before.
const EXPIRED_REMEMBERED: usize = 1024;

/// Open transactions by id, and the ids of the ones that timed out.
#[derive(Default)]
pub struct Sessions {
    open: HashMap<String, HttpTxn>,
    expired: VecDeque<(String, Duration)>,
}

impl Sessions {
    pub fn insert(&mut self, id: String, txn: HttpTxn) {
        self.open.insert(id, txn);
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut HttpTxn> {
        self.open.get_mut(id)
    }

    pub fn remove(&mut self, id: &str) -> Option<HttpTxn> {
        self.open.remove(id)
    }

    /// Take a transaction out because it timed out, leaving a tombstone.
    /// Returns it to be rolled back; `None` if it had already finished.
    pub fn expire(&mut self, id: &str, limit: Duration) -> Option<HttpTxn> {
        let txn = self.open.remove(id)?;
        self.remember_expired(id, limit);
        Some(txn)
    }

    fn remember_expired(&mut self, id: &str, limit: Duration) {
        if self.expired.len() == EXPIRED_REMEMBERED {
            self.expired.pop_front();
        }
        self.expired.push_back((id.to_string(), limit));
    }

    /// The response for an id that is not open: 409 if it timed out, else 404.
    pub fn refused(&self, id: &str) -> Response {
        match self.expired.iter().find(|(e, _)| e == id) {
            Some((_, limit)) => timed_out(id, *limit),
            None => not_open(id),
        }
    }
}

pub type TxnSessions = Arc<Mutex<Sessions>>;

/// Undo everything the transaction did, drop what it logged for persistence,
/// and release the lock.
pub fn roll_back(mut txn: HttpTxn) {
    if let Err(e) = txn.guard.rollback_session_transaction() {
        tracing::warn!("rollback of an HTTP transaction failed: {e}");
    }
    let _ = txn.guard.take_write_log();
}

/// The idempotency slot a committed transaction is recorded under (#2176).
/// The space keeps it apart from every client key: `validate_key` refuses one.
fn committed_slot(id: &str) -> String {
    format!("tx {id}")
}

/// What a COMMIT is, for a key's fingerprint: any two COMMITs are the same
/// request, and a COMMIT is never the same request as an autocommit query.
fn commit_fingerprint() -> String {
    crate::idempotency::fingerprint("COMMIT", "default", Some("tx"), &Default::default())
}

/// The earlier reply to a COMMIT of `id`, if it committed and is remembered.
fn committed_reply(guard: &GraphStore, id: &str) -> Option<Response> {
    let record = guard
        .idempotency()
        .get(
            "default",
            &committed_slot(id),
            crate::idempotency::now_ms(),
            crate::idempotency::ttl(),
        )?
        .clone();
    super::handler::answer_seen(crate::idempotency::Lookup::Replay(record))
}

fn error(status: StatusCode, message: String) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

pub fn not_open(id: &str) -> Response {
    error(
        StatusCode::NOT_FOUND,
        format!("no open transaction '{id}': it was committed, rolled back, or never begun"),
    )
}

fn timed_out(id: &str, limit: Duration) -> Response {
    error(
        StatusCode::CONFLICT,
        format!(
            "transaction '{id}' was open longer than {}s and was rolled back",
            limit.as_secs()
        ),
    )
}

/// Roll back a transaction that outlived `limit`, remembering that it did.
pub async fn expire(sessions: &TxnSessions, id: &str, limit: Duration) {
    let txn = sessions.lock().await.expire(id, limit);
    if let Some(txn) = txn {
        tracing::warn!("transaction {id} was open for {}s; rolled back", limit.as_secs());
        roll_back(txn);
    }
}

pub async fn begin_handler(State(state): State<AppState>) -> Response {
    let mut guard = Arc::clone(&state.store).write_owned().await;
    let version = match guard.begin_session_transaction() {
        Ok(v) => v,
        Err(e) => return error(StatusCode::CONFLICT, e.to_string()),
    };
    if let Some(pm) = &state.persistence {
        guard.enable_write_log();
        // Set once for the whole transaction: the quota counter does not move
        // until the commit, so the store's own tally is what bounds it (#1483).
        guard.set_write_admission(pm.write_admission("default"));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let limit = GraphStore::session_transaction_timeout();
    state
        .transactions
        .lock()
        .await
        .insert(id.clone(), HttpTxn { guard, deadline: Instant::now() + limit, limit });

    // An abandoned transaction would hold the lock for good.
    let sessions = Arc::clone(&state.transactions);
    let expiring = id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(limit).await;
        expire(&sessions, &expiring, limit).await;
    });

    Json(json!({ "tx": id, "version": version, "timeout_seconds": limit.as_secs() })).into_response()
}

pub async fn commit_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    // A malformed key is refused before the transaction is touched, so it
    // can still be committed (#2176).
    let key = match super::handler::idempotency_key(&headers) {
        Ok(key) => key,
        Err(refused) => return refused,
    };
    let mut txn = {
        let mut sessions = state.transactions.lock().await;
        let past_deadline = match sessions.get_mut(&id) {
            None => {
                // Not open. If it committed, answer what the commit did: a
                // client asking again is one whose first reply was lost.
                let refused = sessions.refused(&id);
                drop(sessions);
                let guard = state.store.read().await;
                return committed_reply(&guard, &id).unwrap_or(refused);
            }
            Some(txn) => (Instant::now() > txn.deadline).then_some(txn.limit),
        };
        // Past the deadline but not yet reaped: expire it here, the same way
        // the background task would have.
        if let Some(limit) = past_deadline {
            if let Some(txn) = sessions.expire(&id, limit) {
                roll_back(txn);
            }
            return sessions.refused(&id);
        }
        // A key already used is decided before the transaction is taken out,
        // so a key used for something else leaves it open to be committed
        // without the header.
        if let Some(key) = &key {
            let txn = sessions.get_mut(&id).expect("checked above");
            let seen = txn.guard.idempotency().lookup(
                "default",
                key,
                &commit_fingerprint(),
                crate::idempotency::now_ms(),
                crate::idempotency::ttl(),
            );
            match seen {
                crate::idempotency::Lookup::Miss => {}
                crate::idempotency::Lookup::Conflict => {
                    return super::handler::answer_seen(seen).expect("a conflict has a reply");
                }
                // This transaction re-runs one that committed under the key:
                // its writes would be the second copy. Roll them back and give
                // the first commit's reply.
                crate::idempotency::Lookup::Replay(_) => {
                    let txn = sessions.remove(&id).expect("checked above");
                    roll_back(txn);
                    return super::handler::answer_seen(seen).expect("a replay has a reply");
                }
            }
        }
        sessions.remove(&id).expect("checked above")
    };

    // The reply is built before the commit so it can be written with it.
    let Some(version) = txn.guard.session_transaction_version() else {
        return error(
            StatusCode::CONFLICT,
            format!("transaction '{id}' is not open in the store"),
        );
    };
    let body = json!({ "tx": id, "committed": true, "version": version });
    let record = crate::idempotency::Record {
        fingerprint: commit_fingerprint(),
        status: StatusCode::OK.as_u16(),
        body: serde_json::to_vec(&body).unwrap_or_default(),
        version,
        created_ms: crate::idempotency::now_ms(),
    };
    let mut slots = vec![committed_slot(&id)];
    slots.extend(key);

    // Persisted before the reply, in the same write as the records; refused
    // and rolled back if it cannot be (#1274). Remembered in memory only once
    // on disk, so a failed commit is never replayed as a committed one.
    let committed = match &state.persistence {
        Some(pm) => {
            let keys = crate::idempotency::PendingWrites {
                puts: slots
                    .iter()
                    .map(|slot| ("default".to_string(), slot.clone(), record.clone()))
                    .collect(),
                deletes: Vec::new(),
            };
            pm.commit_session_transaction_keyed("default", &mut txn.guard, &keys)
                .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))
        }
        None => txn
            .guard
            .commit_session_transaction()
            .map_err(|e| error(StatusCode::CONFLICT, e.to_string())),
    };
    if let Err(refused) = committed {
        return refused;
    }
    let log = txn.guard.idempotency_mut();
    for slot in &slots {
        log.record("default", slot, record.clone(), crate::idempotency::ttl());
    }
    // Already written with the commit above.
    let _ = log.take_pending();
    Json(body).into_response()
}

pub async fn rollback_handler(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let txn = {
        let mut sessions = state.transactions.lock().await;
        match sessions.remove(&id) {
            Some(txn) => txn,
            None => {
                // A committed transaction cannot be rolled back; saying "no
                // such transaction" would hide that it committed (#2176).
                let refused = sessions.refused(&id);
                drop(sessions);
                let guard = state.store.read().await;
                if committed_reply(&guard, &id).is_some() {
                    return error(
                        StatusCode::CONFLICT,
                        format!("transaction '{id}' was committed; it cannot be rolled back"),
                    );
                }
                return refused;
            }
        }
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
            snapshot_key: None,
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
        // 409, not 404: "no such transaction" would hide that it committed (#2176).
        assert_eq!(status, StatusCode::CONFLICT, "a committed transaction was rolled back");
        let (status, _) =
            post_json(&app, "/api/query", json!({ "query": "CREATE (:T)", "tx": tx })).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "a query ran in a finished transaction");
    }

    /// #1518: the background task used to remove a timed-out transaction
    /// without a trace, so a late COMMIT got the 404 meant for an id that never
    /// existed, and the 409 branch was reachable only in a microsecond race.
    /// The expiry is driven directly rather than by waiting out a real timeout.
    #[tokio::test]
    async fn a_timed_out_transaction_is_reported_as_timed_out_not_unknown() {
        let (app, state) = app();
        let tx = begin(&app).await;
        post_json(&app, "/api/query", json!({ "query": "CREATE (:T)", "tx": tx })).await;
        expire(&state.transactions, &tx, Duration::from_secs(7)).await;

        assert_eq!(state.store.read().await.node_count(), 0, "the expired CREATE survived");
        let (status, body) = post_json(&app, &format!("/api/tx/{tx}/commit"), json!({})).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        let message = body["error"].as_str().unwrap();
        assert!(message.contains("longer than 7s and was rolled back"), "{message}");
        let (status, _) = post_json(&app, &format!("/api/tx/{tx}/rollback"), json!({})).await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, _) =
            post_json(&app, "/api/query", json!({ "query": "MATCH (n) RETURN n", "tx": tx })).await;
        assert_eq!(status, StatusCode::CONFLICT);

        // An id that never existed is still 404.
        let (status, _) = post_json(&app, "/api/tx/nope/commit", json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// A commit that arrives after the deadline but before the background task
    /// has run is expired on the spot and answered the same way.
    #[tokio::test]
    async fn a_commit_past_the_deadline_is_refused_as_timed_out() {
        let (app, state) = app();
        let tx = begin(&app).await;
        post_json(&app, "/api/query", json!({ "query": "CREATE (:T)", "tx": tx })).await;
        state.transactions.lock().await.get_mut(&tx).unwrap().deadline =
            Instant::now() - Duration::from_millis(1);

        let (status, body) = post_json(&app, &format!("/api/tx/{tx}/commit"), json!({})).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(state.store.read().await.node_count(), 0, "a late commit kept its write");
        assert!(state.store.try_read().is_ok(), "the lock was not released");
    }

    #[test]
    fn the_tombstones_are_bounded() {
        let mut sessions = Sessions::default();
        for i in 0..EXPIRED_REMEMBERED + 10 {
            sessions.remember_expired(&i.to_string(), Duration::from_secs(1));
        }
        assert_eq!(sessions.expired.len(), EXPIRED_REMEMBERED);
        assert_eq!(sessions.refused("0").status(), StatusCode::NOT_FOUND, "the oldest was kept");
        let newest = (EXPIRED_REMEMBERED + 9).to_string();
        assert_eq!(sessions.refused(&newest).status(), StatusCode::CONFLICT);
    }

    /// #1274: a commit that could not be persisted used to answer
    /// `{"committed": true}`. It must be refused and rolled back.
    ///
    /// The failure is injected with `fail_next_apply_for_test`, not with a
    /// `max_nodes: Some(0)` quota as it was. A quota is no longer a way to make
    /// a persist fail — it is checked before the row is created, so the CREATE
    /// below would be refused and the commit would have nothing to fail on
    /// (#1483). Using a quota to stand in for a disk failure was always testing
    /// the wrong thing; the hook exists for exactly this.
    #[tokio::test]
    async fn a_commit_that_cannot_be_persisted_is_refused_and_rolled_back() {
        let dir = tempfile::TempDir::new().unwrap();
        let pm = Arc::new(crate::persistence::PersistenceManager::new(dir.path()).unwrap());
        let (_, mut state) = app();
        state.persistence = Some(Arc::clone(&pm));
        let app = Router::new()
            .route("/api/query", post(query_handler))
            .route("/api/tx/begin", post(begin_handler))
            .route("/api/tx/:id/commit", post(commit_handler))
            .with_state(state.clone());

        let tx = begin(&app).await;
        post_json(&app, "/api/query", json!({ "query": "CREATE (:T)", "tx": tx })).await;
        pm.fail_next_apply_for_test();
        let (status, body) = post_json(&app, &format!("/api/tx/{tx}/commit"), json!({})).await;
        assert_ne!(status, StatusCode::OK, "a commit that was not persisted reported success: {body}");
        let guard = state.store.read().await;
        assert_eq!(guard.node_count(), 0, "the refused CREATE is still in memory");
        assert!(guard.session_transaction_version().is_none(), "the transaction is still open");
    }
}
