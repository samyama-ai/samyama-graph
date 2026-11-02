//! Progress and cancellation for long algorithms (ALGO-12, #1693).
//!
//! An algorithm that runs in steps -- PageRank's iterations, say -- reports
//! each finished step to a [`Progress`] it was handed, and stops when that
//! answers `false`. The algorithm does not know why it was stopped: the caller
//! decides, from a deadline, a cancel request or anything else, and the
//! algorithm returns [`Stopped`] instead of a partial answer.
//!
//! The plain entry points (`page_rank`, ...) pass [`NoProgress`], so they
//! behave exactly as before.

/// Told how far an algorithm has got; answers whether it should go on.
pub trait Progress {
    /// `done` of `total` steps are finished. `step(0, total)` comes before the
    /// first one, so a caller that is already out of time stops the run before
    /// any work. Return `false` to stop the algorithm.
    fn step(&self, done: usize, total: usize) -> bool;
}

/// Reports nowhere and never stops anything.
pub struct NoProgress;

impl Progress for NoProgress {
    fn step(&self, _done: usize, _total: usize) -> bool {
        true
    }
}

/// An algorithm was stopped by its [`Progress`] before it finished. There is no
/// partial result: half-iterated scores are not an answer to anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stopped {
    /// Steps finished when it stopped.
    pub done: usize,
    /// Steps the run was allowed.
    pub total: usize,
}

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stopped after {} of {} steps", self.done, self.total)
    }
}

impl std::error::Error for Stopped {}
