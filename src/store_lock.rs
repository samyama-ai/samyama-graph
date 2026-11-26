//! The one place the store's lock is taken (#2219).
//!
//! Every request reads or writes the graph through one
//! `tokio::sync::RwLock<GraphStore>`, so a long writer (an explicit
//! transaction holds it until commit) queues every reader behind it. To see
//! that queue, the waits are reported by two USDT probes, `lock__wait_start`
//! and `lock__acquired` (`src/usdt.rs`), fired here and nowhere else: the
//! call sites take the lock through these functions instead of calling
//! `read()`/`write()` themselves, so a new site does not need probes of its own
//! (Naresh's design on #2219).
//!
//! Detached, the probes cost a load and a branch each, and no clock is read.
//!
//! The probes carry the mode, not a tenant: the lock is one lock shared by
//! every tenant, and a tenant argument would attribute a global wait to
//! whichever tenant happened to ask.

use std::sync::Arc;

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::graph::GraphStore;
use crate::usdt::LockMode;

/// Take the store's lock for reading.
pub async fn read(lock: &RwLock<GraphStore>) -> RwLockReadGuard<'_, GraphStore> {
    let since = crate::usdt::lock_wait_start(LockMode::Read);
    let guard = lock.read().await;
    crate::usdt::lock_acquired(LockMode::Read, since);
    guard
}

/// Take the store's lock for writing.
pub async fn write(lock: &RwLock<GraphStore>) -> RwLockWriteGuard<'_, GraphStore> {
    let since = crate::usdt::lock_wait_start(LockMode::Write);
    let guard = lock.write().await;
    crate::usdt::lock_acquired(LockMode::Write, since);
    guard
}

/// [`read`], with a guard that owns its `Arc` and so can outlive the borrow.
pub async fn read_owned(lock: Arc<RwLock<GraphStore>>) -> OwnedRwLockReadGuard<GraphStore> {
    let since = crate::usdt::lock_wait_start(LockMode::Read);
    let guard = lock.read_owned().await;
    crate::usdt::lock_acquired(LockMode::Read, since);
    guard
}

/// [`write`], with an owned guard: what an explicit transaction holds until
/// it commits or rolls back.
pub async fn write_owned(lock: Arc<RwLock<GraphStore>>) -> OwnedRwLockWriteGuard<GraphStore> {
    let since = crate::usdt::lock_wait_start(LockMode::Write);
    let guard = lock.write_owned().await;
    crate::usdt::lock_acquired(LockMode::Write, since);
    guard
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn each_helper_takes_the_lock_it_names() {
        let lock = Arc::new(RwLock::new(GraphStore::new()));
        {
            let _r1 = read(&lock).await;
            let _r2 = read(&lock).await;
            assert!(lock.try_write().is_err(), "a reader is held");
        }
        {
            let mut w = write(&lock).await;
            w.create_node("Seen");
            assert!(lock.try_read().is_err(), "a writer is held");
        }
        let owned = write_owned(Arc::clone(&lock)).await;
        assert!(lock.try_read().is_err(), "the owned writer is held");
        drop(owned);
        let r = read_owned(Arc::clone(&lock)).await;
        assert_eq!(r.node_count(), 1);
    }

    #[test]
    fn detached_probes_read_no_clock() {
        // Nothing attaches in a test run, so the wait is never timed.
        assert!(crate::usdt::lock_wait_start(LockMode::Read).is_none());
        assert!(crate::usdt::lock_wait_start(LockMode::Write).is_none());
    }
}
