//! Whether what is in memory is still what is on disk (REL-03, #1274).
//!
//! A single write statement persisted *after* the in-memory commit, and a
//! failure was a `warn!` line. The client was told the write succeeded, the
//! store kept it, and the disk did not — so memory and disk disagreed until a
//! restart threw the write away. A client told "committed" that loses the
//! write on restart is the durability failure REL-03 exists to prevent.
//!
//! # Why the server stops accepting writes
//!
//! Returning an error for the failed statement is necessary and not
//! sufficient. Once one write has not reached disk, the store is ahead of the
//! disk, and every later write compounds the divergence: a restart replays a
//! prefix that does not include the first failure and may not include anything
//! after it either. Continuing to accept writes after that point is how a
//! disk-full incident turns into a graph nobody can reason about.
//!
//! So the first failure marks the process **degraded**, and writes are refused
//! from then on with a message naming the original failure. Reads continue —
//! the in-memory graph is still the most complete thing anyone has, and taking
//! it away helps nobody.
//!
//! The transaction path does not need this: it persists *before* committing in
//! memory, rolls the transaction back when persistence fails, and repairs the
//! disk (`PersistenceManager::commit_session_transaction`). A statement has no
//! rollback — the engine has no statement-level undo (LANG-07) — so the rows
//! it already wrote stay in memory, and refusing what comes next is the only
//! remaining lever.
//!
//! # Clearing it
//!
//! Only a restart, which reloads from disk and makes memory and disk agree
//! again by discarding what never landed. There is deliberately no "resume"
//! command: nothing in this process knows what was lost, so a command to carry
//! on would be a command to guess.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

static DEGRADED: AtomicBool = AtomicBool::new(false);
static REASON: Mutex<Option<String>> = Mutex::new(None);

/// Record that a write did not reach disk.
///
/// The **first** reason is kept. The one after a disk fills is "no space left"
/// and so are the next thousand; the first one is the one that says what
/// happened.
pub fn mark_degraded(reason: impl Into<String>) {
    let reason = reason.into();
    let mut held = REASON.lock().unwrap_or_else(|e| e.into_inner());
    if held.is_none() {
        *held = Some(reason);
    }
    DEGRADED.store(true, Ordering::SeqCst);
}

/// Has a write failed to reach disk since this process started?
pub fn is_degraded() -> bool {
    DEGRADED.load(Ordering::SeqCst)
}

/// What went wrong the first time, if anything has.
pub fn reason() -> Option<String> {
    REASON
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// The error a refused write gets.
///
/// Names the original failure and says what to do about it. "Server is
/// read-only" tells an operator to restart something; it does not tell them
/// their disk is full, and that is the part they can act on.
pub fn refusal() -> String {
    match reason() {
        Some(r) => format!(
            "writes are refused: an earlier write did not reach disk ({r}). \
             The in-memory graph is ahead of the on-disk one, so accepting more writes \
             would widen the gap. Reads still work. Fix the storage and restart, which \
             reloads from disk and discards what never landed."
        ),
        None => "writes are refused: persistence is degraded".to_string(),
    }
}

/// Clear the flag. Tests only — a process cannot know what it lost.
#[doc(hidden)]
pub fn reset_for_test() {
    *REASON.lock().unwrap_or_else(|e| e.into_inner()) = None;
    DEGRADED.store(false, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_reason_is_the_one_kept() {
        reset_for_test();
        mark_degraded("No space left on device");
        mark_degraded("No space left on device (again)");
        mark_degraded("something else entirely");
        assert_eq!(reason().as_deref(), Some("No space left on device"));
        reset_for_test();
    }

    #[test]
    fn the_refusal_names_the_cause_and_the_remedy() {
        reset_for_test();
        mark_degraded("No space left on device");
        let r = refusal();
        assert!(r.contains("No space left on device"), "{r}");
        assert!(r.contains("restart"), "the operator needs to be told what to do: {r}");
        assert!(r.contains("Reads still work"), "{r}");
        reset_for_test();
    }
}
