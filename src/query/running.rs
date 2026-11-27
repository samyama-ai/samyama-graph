//! The graph algorithms running right now: how far each has got, and a way to
//! stop one (ALGO-12, #1693).
//!
//! #2041 gave long algorithms a progress hook and stopped them at the query
//! deadline. Two things were still missing: a caller could not *watch* a long
//! algorithm -- nothing outside the query could see "40,000 of 120,000 nodes" --
//! and could not stop one except by waiting for its deadline. This is both.
//!
//! An algorithm call registers itself for exactly as long as it runs: [`enter`]
//! returns a guard, and dropping the guard (the call returned, failed or
//! panicked) removes the entry, so the list never shows a run that ended. The
//! progress hook the algorithm already calls writes `phase`, `done` and `total`
//! into the entry with atomics, and reads `cancel` from it; another thread --
//! the HTTP handler -- reads and flags the same entry. Nothing here takes the
//! store lock, so the list answers while an algorithm holds it.
//!
//! Process-wide, like `slowlog`: one engine serves every tenant.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// What a running algorithm is doing, in the units its `done`/`total` count.
pub mod phase {
    /// Before any progress has been reported.
    pub const STARTING: &str = "starting";
    /// Reading the store into the algorithm's graph view; counts nodes.
    pub const BUILDING_VIEW: &str = "building graph view";
    /// The algorithm's own loop; counts its iterations, sweeps or passes.
    pub const RUNNING: &str = "running";
}

/// One algorithm call in progress.
struct Entry {
    id: u64,
    algorithm: String,
    started: Instant,
    started_ms: u64,
    phase: Mutex<&'static str>,
    done: AtomicU64,
    total: AtomicU64,
    cancel: AtomicBool,
}

fn registry() -> &'static Mutex<BTreeMap<u64, Arc<Entry>>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<u64, Arc<Entry>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn next_id() -> u64 {
    // Monotonic, never reused, so a cancel sent for a run that has since ended
    // cannot land on a different run that took its number.
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

thread_local! {
    /// The algorithm call running on this thread, which the progress hook
    /// reports into. An algorithm runs on the executor's thread from start to
    /// end, so the thread identifies the call.
    static CURRENT: RefCell<Option<Arc<Entry>>> = const { RefCell::new(None) };
}

/// Registers an algorithm call until the guard is dropped.
pub struct Running {
    entry: Arc<Entry>,
    previous: Option<Arc<Entry>>,
}

/// Start tracking `algorithm` on this thread. Keep the guard alive for the
/// whole call.
pub fn enter(algorithm: &str) -> Running {
    let entry = Arc::new(Entry {
        id: next_id(),
        algorithm: algorithm.to_string(),
        started: Instant::now(),
        started_ms: SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0),
        phase: Mutex::new(phase::STARTING),
        done: AtomicU64::new(0),
        total: AtomicU64::new(0),
        cancel: AtomicBool::new(false),
    });
    registry().lock().unwrap_or_else(|e| e.into_inner()).insert(entry.id, entry.clone());
    let previous = CURRENT.with(|c| c.replace(Some(entry.clone())));
    Running { entry, previous }
}

impl Running {
    /// This call's id, as `GET /api/algorithms/running` reports it.
    pub fn id(&self) -> u64 {
        self.entry.id
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        registry().lock().unwrap_or_else(|e| e.into_inner()).remove(&self.entry.id);
        let previous = self.previous.take();
        CURRENT.with(|c| *c.borrow_mut() = previous);
    }
}

/// Record progress for the call on this thread; answers whether it has been
/// cancelled. A thread with no registered call reports nowhere and is never
/// cancelled.
pub fn report(phase: &'static str, done: usize, total: usize) -> bool {
    CURRENT.with(|c| match c.borrow().as_ref() {
        Some(e) => {
            *e.phase.lock().unwrap_or_else(|p| p.into_inner()) = phase;
            e.done.store(done as u64, Ordering::Relaxed);
            e.total.store(total as u64, Ordering::Relaxed);
            e.cancel.load(Ordering::Relaxed)
        }
        None => false,
    })
}

/// Has the call on this thread been cancelled?
pub fn cancelled() -> bool {
    CURRENT.with(|c| c.borrow().as_ref().is_some_and(|e| e.cancel.load(Ordering::Relaxed)))
}

/// A running algorithm, as `GET /api/algorithms/running` lists it.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RunningAlgorithm {
    pub id: u64,
    /// The name it was called by, e.g. `algo.pageRank`.
    pub algorithm: String,
    /// When it started, milliseconds since the Unix epoch.
    pub started_ms: u64,
    pub elapsed_ms: u64,
    /// [`phase`]: what `done` and `total` count.
    pub phase: String,
    pub done: u64,
    pub total: u64,
    /// A cancel was requested; it takes effect at the algorithm's next report.
    pub cancel_requested: bool,
}

/// Every algorithm call running now, oldest first.
pub fn list() -> Vec<RunningAlgorithm> {
    registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .values()
        .map(|e| RunningAlgorithm {
            id: e.id,
            algorithm: e.algorithm.clone(),
            started_ms: e.started_ms,
            elapsed_ms: e.started.elapsed().as_millis() as u64,
            phase: e.phase.lock().unwrap_or_else(|p| p.into_inner()).to_string(),
            done: e.done.load(Ordering::Relaxed),
            total: e.total.load(Ordering::Relaxed),
            cancel_requested: e.cancel.load(Ordering::Relaxed),
        })
        .collect()
}

/// Ask the call `id` to stop. `false` when no such call is running -- it never
/// existed, or it has already ended, which a caller cannot tell apart and need
/// not: either way there is nothing left to stop.
pub fn cancel(id: u64) -> bool {
    match registry().lock().unwrap_or_else(|e| e.into_inner()).get(&id) {
        Some(e) => {
            e.cancel.store(true, Ordering::Relaxed);
            true
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_lives_exactly_as_long_as_its_guard() {
        let guard = enter("algo.test");
        let id = guard.id();
        assert!(list().iter().any(|r| r.id == id && r.algorithm == "algo.test" && r.phase == phase::STARTING));
        drop(guard);
        assert!(!list().iter().any(|r| r.id == id), "the entry outlived its call");
        assert!(!cancel(id), "an ended call was reported as cancellable");
    }

    #[test]
    fn progress_is_recorded_and_a_cancel_is_seen_at_the_next_report() {
        let guard = enter("algo.test");
        let id = guard.id();
        assert!(!report(phase::BUILDING_VIEW, 10, 40));
        let row = list().into_iter().find(|r| r.id == id).unwrap();
        assert_eq!((row.phase.as_str(), row.done, row.total, row.cancel_requested), (phase::BUILDING_VIEW, 10, 40, false));
        assert!(cancel(id));
        assert!(cancelled());
        assert!(report(phase::RUNNING, 1, 5), "the cancel was not seen at the next report");
        assert!(list().into_iter().find(|r| r.id == id).unwrap().cancel_requested);
    }

    #[test]
    fn a_thread_with_no_call_reports_nowhere_and_is_never_cancelled() {
        std::thread::spawn(|| {
            assert!(!report(phase::RUNNING, 1, 2));
            assert!(!cancelled());
        })
        .join()
        .unwrap();
    }

    #[test]
    fn a_nested_call_restores_the_outer_one() {
        let outer = enter("algo.outer");
        {
            let _inner = enter("algo.inner");
            report(phase::RUNNING, 7, 9);
        }
        cancel(outer.id());
        assert!(cancelled(), "after the inner call ended, reports went nowhere");
    }
}
