//! What model calls cost, as Prometheus counters (COST-07, #1790).
//!
//! `/metrics` described queries and the graph, and nothing about the model
//! calls behind `/api/nlq`, agents and enrichment, which are the part of a
//! request that is billed per token. Every call through
//! [`crate::nlq::client::NLQClient::complete`] lands here.
//!
//! Token counters only move by what a provider reported. A call whose provider
//! reported nothing is counted in `samyama_llm_calls_without_usage_total`
//! instead, so a token total is never read as complete when it is not.

use super::client::TokenUsage;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static CALLS: AtomicU64 = AtomicU64::new(0);
static FAILED_CALLS: AtomicU64 = AtomicU64::new(0);
static CALLS_WITHOUT_USAGE: AtomicU64 = AtomicU64::new(0);
static PROMPT_TOKENS: AtomicU64 = AtomicU64::new(0);
static COMPLETION_TOKENS: AtomicU64 = AtomicU64::new(0);
static DURATION_MICROS: AtomicU64 = AtomicU64::new(0);
const MICROS_PER_SECOND: f64 = 1_000_000.0;

/// Count a call that returned an answer.
pub fn record_call(latency: Duration, usage: &TokenUsage) {
    CALLS.fetch_add(1, Ordering::Relaxed);
    add_duration(latency);
    if !usage.is_reported() {
        CALLS_WITHOUT_USAGE.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(n) = usage.prompt_tokens {
        PROMPT_TOKENS.fetch_add(n, Ordering::Relaxed);
    }
    if let Some(n) = usage.completion_tokens {
        COMPLETION_TOKENS.fetch_add(n, Ordering::Relaxed);
    }
}

/// Count a call that failed. Its time is still spent, so it is still added.
pub fn record_failure(latency: Duration) {
    CALLS.fetch_add(1, Ordering::Relaxed);
    FAILED_CALLS.fetch_add(1, Ordering::Relaxed);
    add_duration(latency);
}

fn add_duration(latency: Duration) {
    let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
    DURATION_MICROS.fetch_add(micros, Ordering::Relaxed);
}

/// The counters at one moment.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Snapshot {
    pub calls: u64,
    pub failed_calls: u64,
    pub calls_without_usage: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// Kept in whole microseconds, as counted, so two snapshots compare
    /// exactly; seconds are for rendering only.
    pub duration_micros: u64,
}

impl Snapshot {
    pub fn duration_seconds(&self) -> f64 {
        self.duration_micros as f64 / MICROS_PER_SECOND
    }
}

pub fn snapshot() -> Snapshot {
    Snapshot {
        calls: CALLS.load(Ordering::Relaxed),
        failed_calls: FAILED_CALLS.load(Ordering::Relaxed),
        calls_without_usage: CALLS_WITHOUT_USAGE.load(Ordering::Relaxed),
        prompt_tokens: PROMPT_TOKENS.load(Ordering::Relaxed),
        completion_tokens: COMPLETION_TOKENS.load(Ordering::Relaxed),
        duration_micros: DURATION_MICROS.load(Ordering::Relaxed),
    }
}

/// The counters in Prometheus text format.
pub fn render() -> String {
    render_snapshot(&snapshot())
}

fn render_snapshot(s: &Snapshot) -> String {
    let counters: [(&str, &str, String); 6] = [
        (
            "samyama_llm_calls_total",
            "Model calls made for NLQ, agents and enrichment, failed ones included.",
            s.calls.to_string(),
        ),
        (
            "samyama_llm_calls_failed_total",
            "Model calls that returned an error.",
            s.failed_calls.to_string(),
        ),
        (
            "samyama_llm_calls_without_usage_total",
            "Model calls that answered without reporting tokens; the token counters exclude them.",
            s.calls_without_usage.to_string(),
        ),
        (
            "samyama_llm_prompt_tokens_total",
            "Prompt tokens the providers reported.",
            s.prompt_tokens.to_string(),
        ),
        (
            "samyama_llm_completion_tokens_total",
            "Completion tokens the providers reported.",
            s.completion_tokens.to_string(),
        ),
        (
            "samyama_llm_call_duration_seconds_total",
            "Wall-clock time spent waiting on model calls.",
            s.duration_seconds().to_string(),
        ),
    ];
    let mut out = String::new();
    for (name, help, value) in counters {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // The counters are process-global and other tests in this binary make
    // model calls, so these tests check the rendering of a fixed snapshot and,
    // for the recorders, that a counter moved by at least what was added.

    #[test]
    fn every_counter_is_rendered_with_help_and_type() {
        let s = Snapshot {
            calls: 3,
            failed_calls: 1,
            calls_without_usage: 1,
            prompt_tokens: 412,
            completion_tokens: 17,
            duration_micros: 1_500_000,
        };
        let out = render_snapshot(&s);
        for (name, value) in [
            ("samyama_llm_calls_total", "3"),
            ("samyama_llm_calls_failed_total", "1"),
            ("samyama_llm_calls_without_usage_total", "1"),
            ("samyama_llm_prompt_tokens_total", "412"),
            ("samyama_llm_completion_tokens_total", "17"),
            ("samyama_llm_call_duration_seconds_total", "1.5"),
        ] {
            assert!(out.contains(&format!("# TYPE {name} counter\n")), "{name}");
            assert!(out.contains(&format!("# HELP {name} ")), "{name}");
            assert!(
                out.contains(&format!("\n{name} {value}\n")),
                "{name}: {out}"
            );
        }
    }

    #[test]
    fn a_reported_call_adds_its_tokens_and_time() {
        let before = snapshot();
        let usage = TokenUsage {
            prompt_tokens: Some(412),
            completion_tokens: Some(17),
            total_tokens: Some(429),
        };
        record_call(Duration::from_millis(250), &usage);
        let after = snapshot();
        assert!(after.calls > before.calls);
        assert!(after.prompt_tokens >= before.prompt_tokens + 412);
        assert!(after.completion_tokens >= before.completion_tokens + 17);
        // Compared in whole microseconds: in seconds, as floats, `x + 0.25`
        // can round past what was counted, and did on an ARM runner.
        assert!(after.duration_micros >= before.duration_micros + 250_000);
    }

    #[test]
    fn a_call_without_usage_is_counted_as_such() {
        let before = snapshot();
        record_call(Duration::ZERO, &TokenUsage::default());
        assert!(snapshot().calls_without_usage > before.calls_without_usage);
    }

    #[test]
    fn a_failed_call_is_counted_and_its_time_kept() {
        let before = snapshot();
        record_failure(Duration::from_millis(100));
        let after = snapshot();
        assert!(after.failed_calls > before.failed_calls);
        assert!(after.calls > before.calls);
        assert!(after.duration_micros >= before.duration_micros + 100_000);
    }
}
