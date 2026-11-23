//! The persisted write path for writers inside the process (#2241).
//!
//! A Cypher write reaches disk because the RESP and HTTP query paths wrap it:
//! they take the writer's lock, turn on the write log, hand the store the
//! tenant's quota, run the statement, drain the log and pass it to
//! [`PersistenceManager::apply_mutations`] under the statement's tenant. A
//! writer that calls `GraphStore` directly skips every one of those steps. The
//! plan executor's telemetry did: its rows were either never persisted, or,
//! because the log stays on between statements, saved by the next unrelated
//! write under *that* write's tenant.
//!
//! [`PersistedWriter::write`] is those steps in one place, for the plan
//! executor, agent memory and any background writer that comes after them. It
//! differs from the query path in one respect, on purpose: the body runs as a
//! session transaction (the `GRAPH.BEGIN` / `GRAPH.COMMIT` mechanism) and is
//! persisted *before* it is committed in memory, through
//! [`PersistenceManager::commit_session_transaction_detailed`]. So a write of
//! several rows lands whole or not at all:
//!
//! - the body fails: everything it wrote is rolled back and nothing reaches
//!   disk;
//! - the persist fails: the transaction is rolled back and the disk repaired,
//!   and the caller gets an error. Memory and disk then agree again, so the
//!   process is not marked degraded -- the reason the transaction path never
//!   needed that flag (`persistence::health`). It is marked degraded when the
//!   rollback or the repair fails as well, because then they do not agree;
//! - the process is already degraded: the write is refused before it starts,
//!   as a Cypher write would be.
//!
//! The caller names the tenant on every call. There is no default, because a
//! default is how telemetry ended up under somebody else's tenant.

use crate::graph::GraphStore;
use crate::persistence::{health, PersistenceManager};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Why [`PersistedWriter::write`] did not keep a write.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PersistedWriteError {
    /// Nothing was attempted: an earlier write did not reach disk, or the
    /// store could not open a transaction for this one.
    #[error("{0}")]
    Refused(String),
    /// The body returned an error. What it wrote was rolled back.
    #[error("{0}")]
    Body(String),
    /// The body succeeded, the write could not be persisted, and it was
    /// rolled back.
    #[error("{0}")]
    NotPersisted(String),
}

/// A handle that writes to a store the way a Cypher write does: journalled,
/// admitted against the tenant's quota, and persisted before it is kept.
///
/// Without a [`PersistenceManager`] it still runs the body as one transaction,
/// so a failed body leaves nothing behind; there is just no disk to reach.
#[derive(Clone)]
pub struct PersistedWriter {
    store: Arc<RwLock<GraphStore>>,
    persistence: Option<Arc<PersistenceManager>>,
}

impl PersistedWriter {
    /// A writer for `store`, persisting through `persistence` when there is
    /// one. Pass the server's manager: one that is not the server's writes a
    /// different directory.
    pub fn new(
        store: Arc<RwLock<GraphStore>>,
        persistence: Option<Arc<PersistenceManager>>,
    ) -> Self {
        Self { store, persistence }
    }

    /// A writer for a store with nothing on disk.
    pub fn in_memory(store: Arc<RwLock<GraphStore>>) -> Self {
        Self::new(store, None)
    }

    /// The store, for reading.
    pub fn store(&self) -> &Arc<RwLock<GraphStore>> {
        &self.store
    }

    /// The persistence manager writes go through, if any.
    pub fn persistence(&self) -> Option<&Arc<PersistenceManager>> {
        self.persistence.as_ref()
    }

    /// Run `body` against the store as one write for `tenant`, and return its
    /// value only once what it wrote is on disk.
    ///
    /// `body` must write with the journalled `GraphStore` methods
    /// (`try_create_node_with_properties`, `set_node_property`,
    /// `create_edge_with_properties`, ...), not through `get_node_mut`: only a
    /// journalled write is persisted, and only a journalled write is undone by
    /// the rollback. A node it creates must be admitted first with
    /// [`GraphStore::admit_node`], because `create_node*` cannot refuse;
    /// `create_edge*` admits on its own.
    pub async fn write<T, E: std::fmt::Display>(
        &self,
        tenant: &str,
        body: impl FnOnce(&mut GraphStore) -> Result<T, E>,
    ) -> Result<T, PersistedWriteError> {
        use tracing::Instrument;
        // As on the query path (#1274): once a write has not reached disk,
        // every later one widens the gap.
        if self.persistence.is_some() && health::is_degraded() {
            return Err(PersistedWriteError::Refused(health::refusal()));
        }
        // Through the helper, so the lock probes see this wait too (#2219).
        let mut store = crate::store_lock::write(&self.store)
            .instrument(tracing::debug_span!("samyama.lock.acquire", mode = "write"))
            .await;
        write_locked(&mut store, self.persistence.as_deref(), tenant, body)
    }
}

fn write_locked<T, E: std::fmt::Display>(
    store: &mut GraphStore,
    persistence: Option<&PersistenceManager>,
    tenant: &str,
    body: impl FnOnce(&mut GraphStore) -> Result<T, E>,
) -> Result<T, PersistedWriteError> {
    // `GRAPH.BEGIN` and `/api/tx/begin` hold the writer's lock for as long as
    // their transaction is open, so with the lock in hand none should be. If
    // one is, it is not ours to commit.
    store.begin_session_transaction().map_err(|e| {
        PersistedWriteError::Refused(format!("a transaction is already open on the store: {e}"))
    })?;
    if let Some(pm) = persistence {
        store.enable_write_log();
        // Before the body runs, so a breach is a refused write rather than
        // rows in memory that are not on disk (#1483).
        store.set_write_admission(pm.write_admission(tenant));
    }

    let value = match body(store) {
        Ok(value) => value,
        Err(e) => {
            let rolled_back = store.rollback_session_transaction();
            // None of it reached disk, so none of it is persisted later by a
            // write that happens to drain the log next.
            let _ = store.take_write_log();
            if let Err(rb) = rolled_back {
                let message = format!("the write failed ({e}) and its rollback failed: {rb}");
                if persistence.is_some() {
                    health::mark_degraded(message.clone());
                }
                return Err(PersistedWriteError::Body(message));
            }
            return Err(PersistedWriteError::Body(e.to_string()));
        }
    };

    match persistence {
        Some(pm) => {
            let _persist = tracing::debug_span!("samyama.storage.persist", tenant).entered();
            if let Err(refusal) = pm.commit_session_transaction_detailed(tenant, store) {
                tracing::warn!(
                    "in-process write for tenant {tenant} not persisted: {}",
                    refusal.message
                );
                if !refusal.disk_matches_memory {
                    health::mark_degraded(refusal.message.clone());
                }
                return Err(PersistedWriteError::NotPersisted(refusal.message));
            }
        }
        None => {
            store
                .commit_session_transaction()
                .map_err(|e| PersistedWriteError::Body(e.to_string()))?;
        }
    }
    // What `GraphStore::atomically` does after a statement commits: nothing
    // reads at the version before it any more.
    store.gc_auto();
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Label, PropertyValue};

    fn writer() -> PersistedWriter {
        PersistedWriter::in_memory(Arc::new(RwLock::new(GraphStore::new())))
    }

    #[tokio::test]
    async fn a_body_that_succeeds_is_kept_and_its_value_returned() {
        let w = writer();
        let id = w
            .write("default", |store| {
                store.admit_node()?;
                let id = store.create_node("N");
                store.set_node_property("default", id, "k", "v")?;
                Ok::<_, crate::graph::GraphError>(id)
            })
            .await
            .unwrap();
        let g = w.store().read().await;
        assert_eq!(g.node_count(), 1);
        assert_eq!(
            g.node_property(id, "k"),
            Some(PropertyValue::String("v".into()))
        );
        assert!(
            g.session_transaction_version().is_none(),
            "the transaction was left open"
        );
    }

    /// The whole body is one unit: a failure after the first row undoes it.
    #[tokio::test]
    async fn a_body_that_fails_part_way_leaves_nothing_behind() {
        let w = writer();
        let err = w
            .write("default", |store| {
                let a = store.create_node("A");
                store.create_node("B");
                store.create_edge(a, crate::graph::NodeId::new(999_999), "R")?;
                Ok::<_, crate::graph::GraphError>(())
            })
            .await
            .unwrap_err();
        assert!(matches!(err, PersistedWriteError::Body(_)), "{err:?}");
        let g = w.store().read().await;
        assert_eq!(g.node_count(), 0, "the nodes before the failure were kept");
        assert!(g.get_nodes_by_label(&Label::new("A")).is_empty());
        assert!(
            g.session_transaction_version().is_none(),
            "the transaction was left open"
        );
    }

    #[tokio::test]
    async fn an_open_transaction_is_not_joined() {
        let w = writer();
        w.store().write().await.begin_session_transaction().unwrap();
        let err = w
            .write("default", |store| {
                store.create_node("N");
                Ok::<_, String>(())
            })
            .await
            .unwrap_err();
        assert!(matches!(err, PersistedWriteError::Refused(_)), "{err:?}");
        assert_eq!(w.store().read().await.node_count(), 0);
    }
}
