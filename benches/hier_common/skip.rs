//! Corpus skips that expire (samyama-graph#444).
//!
//! A skip used to be a string: "engine gap: ...". That is a claim about the engine at the
//! moment it was written, stored in data nothing exercises — so when the engine gained the
//! capability, the skip stayed, and the benchmark reported 108/108 while it was 108 of 112.
//!
//! A skip is now a **probe**. It names the failure it expects:
//!
//! ```json
//! "skip": {"reason": "why this cannot run today", "error": "substring of the engine error"}
//! ```
//!
//! and the query is run anyway. Three outcomes:
//!
//! - it fails with an error containing `error` — the skip is still justified;
//! - it **succeeds** — the capability has arrived and the skip is stale;
//! - it fails with some **other** error — the skip's stated reason is no longer the reason,
//!   which is itself a finding.
//!
//! Only the first is a skip. The other two fail the run (the benchmark harness) and the test
//! suite (`tests/hier_corpus_skips_expire.rs`), so a skip cannot outlive its cause silently.
//!
//! A bare-string skip is rejected at load time: it has no signature, so it cannot expire.
//!
//! This file has no dependency on the engine so the classifier can be unit-tested with a
//! fake probe outcome. It is shared by `path` include, like the rest of `hier_common`.

#![allow(dead_code)]

/// A skip entry: why the query cannot run, and the error that proves it still cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skip {
    /// Human-readable reason, reported alongside the skip.
    pub reason: String,
    /// A substring the engine's error must contain for the skip to hold.
    pub error: String,
}

/// What running a skipped query says about its skip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipVerdict {
    /// Failed with the expected error: the skip is justified.
    StillBlocked,
    /// The query ran. The capability exists now; remove the skip.
    Stale,
    /// Failed, but not for the stated reason. Carries the actual error.
    WrongReason(String),
}

impl SkipVerdict {
    /// True when the skip may stand.
    pub fn holds(&self) -> bool {
        matches!(self, SkipVerdict::StillBlocked)
    }
}

/// Parse a corpus entry's `skip` field. `Ok(None)` when there is none.
///
/// Rejects a bare string, and an object missing a non-empty `reason` or `error`: those are
/// the shapes that cannot expire.
pub fn parse_skip(id: &str, v: &serde_json::Value) -> Result<Option<Skip>, String> {
    match v {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => Err(format!(
            "{id}: `skip` is a bare string ({s:?}). A skip must name the error that proves \
             the gap still exists: {{\"reason\": \"...\", \"error\": \"<substring of the \
             engine error>\"}} (samyama-graph#444)"
        )),
        serde_json::Value::Object(m) => {
            let field = |k: &str| -> Result<String, String> {
                match m.get(k).and_then(|x| x.as_str()) {
                    Some(s) if !s.trim().is_empty() => Ok(s.to_string()),
                    _ => Err(format!("{id}: `skip.{k}` must be a non-empty string")),
                }
            };
            Ok(Some(Skip {
                reason: field("reason")?,
                error: field("error")?,
            }))
        }
        other => Err(format!("{id}: `skip` must be an object, got {other}")),
    }
}

/// Classify a skipped query by what happened when it was run.
///
/// `outcome` is `Ok(())` if the query executed, or `Err(message)` with the engine's error.
pub fn classify(skip: &Skip, outcome: Result<(), String>) -> SkipVerdict {
    match outcome {
        Ok(()) => SkipVerdict::Stale,
        Err(e) if e.contains(&skip.error) => SkipVerdict::StillBlocked,
        Err(e) => SkipVerdict::WrongReason(e),
    }
}

/// One line describing a skip that no longer holds, for a failure report.
pub fn describe_failure(id: &str, skip: &Skip, verdict: &SkipVerdict) -> Option<String> {
    match verdict {
        SkipVerdict::StillBlocked => None,
        SkipVerdict::Stale => Some(format!(
            "{id}: STALE SKIP — the query now runs, so \"{}\" is no longer true. Remove the \
             skip from benchmarks/hier/generate_corpus.py and regenerate.",
            skip.reason
        )),
        SkipVerdict::WrongReason(actual) => Some(format!(
            "{id}: skip expects an error containing {:?} but the engine said {:?}. The \
             stated reason is no longer the reason.",
            skip.error, actual
        )),
    }
}
