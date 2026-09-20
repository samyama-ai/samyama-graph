//! A write that does not reach disk is not reported as success (REL-03, #1274).
//!
//! A single write statement persisted *after* the in-memory commit, and a
//! failure was a `warn!` line. The client was told the write succeeded, the
//! store kept it, the disk did not, and a restart threw it away — which is the
//! durability failure REL-03 exists to prevent, arriving silently.
//!
//! The transaction path was already right: it persists before committing in
//! memory, rolls back when persistence fails and repairs the disk. A statement
//! has no rollback, so the rows it already wrote stay in memory. What changes
//! here is that the client is **told**, and that nothing further is accepted —
//! once the store is ahead of the disk, every later write widens the gap, and
//! a restart replays a prefix that does not include the first failure.
//!
//! Tested with an injected failure rather than a real disk fault:
//! `PersistenceManager::fail_next_apply_for_test`. A read-only directory does
//! not do it — RocksDB buffers, so the failure surfaces somewhere else or not
//! at all — and the hook is a method rather than an environment variable
//! because a switch that turns off durability should not be reachable by a
//! stray variable on a production host.

use samyama::persistence::{health, PersistenceManager};

/// Every test here reads and writes one process-global flag, so they cannot
/// run at the same time: `a_clean_process_is_not_degraded` asserts the flag is
/// clear, and any other test in this binary setting it in between would fail
/// it for a reason that has nothing to do with the code.
///
/// The same trap as the latency histogram's tests, and the same answer: a test
/// whose result depends on what else is running is not a test. Serialising
/// them here is cheaper than giving the flag an injectable instance, and the
/// flag is global because the condition it describes is.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn guard() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn manager() -> (PersistenceManager, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let pm = PersistenceManager::new(dir.path()).expect("persistence");
    (pm, dir)
}

#[test]
fn the_injected_failure_actually_fails_one_apply_and_only_one() {
    let _serial = guard();
    // The hook is the whole basis of the tests below. If it did nothing, they
    // would pass by not exercising anything.
    let (pm, _dir) = manager();
    let mut store = samyama::graph::GraphStore::new();
    store.enable_write_log();
    store.create_node("N");
    let mutations = store.take_write_log();

    pm.fail_next_apply_for_test();
    assert!(
        pm.apply_mutations("default", &store, &mutations).is_err(),
        "the hook did not make the write fail"
    );
    assert!(
        pm.apply_mutations("default", &store, &mutations).is_ok(),
        "the hook must fire once, or every later test in the process fails"
    );
}

#[test]
fn a_failed_persist_marks_the_process_degraded_and_says_why() {
    let _serial = guard();
    health::reset_for_test();
    let (pm, _dir) = manager();
    let mut store = samyama::graph::GraphStore::new();
    store.enable_write_log();
    store.create_node("N");
    let mutations = store.take_write_log();

    pm.fail_next_apply_for_test();
    let err = pm
        .apply_mutations("default", &store, &mutations)
        .expect_err("injected");
    health::mark_degraded(err.to_string());

    assert!(health::is_degraded());
    let refusal = health::refusal();
    assert!(
        refusal.contains("injected failure"),
        "the refusal must carry the original cause, not a generic message: {refusal}"
    );
    assert!(
        refusal.contains("restart"),
        "and it must say what to do: {refusal}"
    );
    health::reset_for_test();
}

#[test]
fn a_clean_process_is_not_degraded() {
    let _serial = guard();
    // The half that stops the flag being "always on". A test suite where
    // everything is refused would pass every assertion about refusal.
    health::reset_for_test();
    assert!(!health::is_degraded());
    assert_eq!(health::reason(), None);
    let (pm, _dir) = manager();
    let mut store = samyama::graph::GraphStore::new();
    store.enable_write_log();
    store.create_node("N");
    let mutations = store.take_write_log();
    pm.apply_mutations("default", &store, &mutations)
        .expect("an ordinary write must succeed");
    assert!(
        !health::is_degraded(),
        "a successful write must not mark the process degraded"
    );
}

#[test]
fn a_transaction_that_cannot_be_persisted_is_rolled_back_in_memory() {
    let _serial = guard();
    // The path that was already right, pinned so it stays that way. This is
    // the difference between a transaction and a statement: the undo log lets
    // the store be put back, so memory and disk still agree afterwards.
    health::reset_for_test();
    let (pm, _dir) = manager();
    let mut store = samyama::graph::GraphStore::new();
    store.enable_write_log();

    store.begin_session_transaction().expect("begin");
    store.create_node("Doomed");
    let before_commit = store.all_nodes().len();
    assert_eq!(before_commit, 1, "the transaction wrote a node");

    pm.fail_next_apply_for_test();
    let err = pm
        .commit_session_transaction("default", &mut store)
        .expect_err("a commit that cannot be persisted must not report success");
    assert!(
        err.contains("rolled back"),
        "the error must say the transaction was undone, not merely that it failed: {err}"
    );
    assert_eq!(
        store.all_nodes().len(),
        0,
        "the node must be gone from memory too, or memory and disk disagree"
    );
}

#[test]
fn the_reason_kept_is_the_first_one() {
    let _serial = guard();
    // A disk that fills reports "no space left" for every write after the
    // first. The first is the one that says when it started.
    health::reset_for_test();
    health::mark_degraded("the original cause");
    health::mark_degraded("a later, less interesting one");
    assert!(health::refusal().contains("the original cause"));
    assert!(!health::refusal().contains("less interesting"));
    health::reset_for_test();
}
