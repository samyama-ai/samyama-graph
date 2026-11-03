//! Query latency, as a Prometheus histogram (REL-10).
//!
//! REL-10 asks for observability "sufficient to diagnose a slow query without
//! vendor help". `/metrics` exposed node and edge counts, index bytes and
//! cache hits — every one of them a fact about the graph and none of them a
//! fact about a query. A dashboard over those cannot show that anything got
//! slower, which makes it an artifact that satisfies a checklist rather than a
//! thing an operator uses at 3am.
//!
//! The latency was already measured: `QueryEngine::execute` times every query
//! and hands the duration to the slow-query log, which drops it unless it
//! crossed a threshold. So one number per query was being computed and thrown
//! away.
//!
//! # Why a histogram and not a mean
//!
//! A mean latency is the statistic that hides the problem. One query in twenty
//! over the agent turn budget (PERF-19) moves a mean by nothing and a p95 by
//! everything, and "p95" cannot be computed from a mean and a count. Buckets
//! can be aggregated across instances and quantiles read off them, which is
//! what `histogram_quantile` in a dashboard does.
//!
//! # Cost
//!
//! One `fetch_add` on a `Relaxed` atomic per bucket crossed, plus two more for
//! the count and the sum — on a path that has just run a query. There is no
//! lock and no allocation. The buckets are a fixed array, so the hot path does
//! a linear scan of 14 `f64` comparisons; a binary search would be fewer
//! instructions and less obvious, and 14 predictable comparisons next to a
//! query execution is not where the time goes.

use std::sync::atomic::{AtomicU64, Ordering};

/// Bucket upper bounds in seconds, ascending.
///
/// Chosen around the numbers this engine is judged by rather than a generic
/// decade scale: 1 ms and 5 ms are where the short reads live, 100 ms is
/// PERF-19's interactive band, 5 s is the agent turn budget it is measured
/// against, and 120 s is the benchmark harness's per-query timeout — a query
/// past that bucket was killed rather than slow.
pub const BUCKETS: [f64; 14] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 120.0,
];

/// Cumulative counts, one per bucket, plus the count and the sum.
///
/// `+Inf` is `count`, as Prometheus defines it, so it is not stored twice.
struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    /// Total seconds, as microseconds, so the sum stays an integer and two
    /// readers of `/metrics` cannot see a torn float.
    sum_micros: AtomicU64,
}

impl Histogram {
    const fn new() -> Self {
        #[allow(clippy::declare_interior_mutable_const)]
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self {
            buckets: [ZERO; BUCKETS.len()],
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }

    fn observe(&self, seconds: f64) {
        for (i, upper) in BUCKETS.iter().enumerate() {
            if seconds <= *upper {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add((seconds * 1_000_000.0) as u64, Ordering::Relaxed);
    }
}

static QUERY_LATENCY: Histogram = Histogram::new();
static QUERIES_TOTAL: AtomicU64 = AtomicU64::new(0);
static QUERIES_FAILED: AtomicU64 = AtomicU64::new(0);
static SLOW_QUERIES: AtomicU64 = AtomicU64::new(0);

/// Record one executed query.
///
/// `slow` is whatever the engine's own slow-query threshold decided, rather
/// than a second threshold defined here: an operator who tunes `SLOW_QUERY_MS`
/// and sees the log and the metric disagree has two numbers and trusts
/// neither.
pub fn record_query(elapsed: std::time::Duration, slow: bool, failed: bool) {
    QUERY_LATENCY.observe(elapsed.as_secs_f64());
    QUERIES_TOTAL.fetch_add(1, Ordering::Relaxed);
    if slow {
        SLOW_QUERIES.fetch_add(1, Ordering::Relaxed);
    }
    if failed {
        QUERIES_FAILED.fetch_add(1, Ordering::Relaxed);
    }
}

/// A snapshot, for `/metrics` and for tests.
pub struct Snapshot {
    /// Cumulative count per bucket, aligned with [`BUCKETS`].
    pub buckets: Vec<u64>,
    /// Queries observed.
    pub count: u64,
    /// Total time, in seconds.
    pub sum_seconds: f64,
    /// Queries executed since start.
    pub total: u64,
    /// Queries that returned an error.
    pub failed: u64,
    /// Queries past the slow-query threshold.
    pub slow: u64,
}

/// Read the counters.
///
/// Not atomic as a set: a query landing between two loads can leave the sum
/// ahead of the count. That is the ordinary behaviour of a Prometheus client
/// and the alternative is a lock on the query path, which is a real cost to
/// remove a discrepancy no dashboard can see.
pub fn snapshot() -> Snapshot {
    Snapshot {
        buckets: QUERY_LATENCY
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect(),
        count: QUERY_LATENCY.count.load(Ordering::Relaxed),
        sum_seconds: QUERY_LATENCY.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        total: QUERIES_TOTAL.load(Ordering::Relaxed),
        failed: QUERIES_FAILED.load(Ordering::Relaxed),
        slow: SLOW_QUERIES.load(Ordering::Relaxed),
    }
}

/// The histogram in Prometheus text format.
pub fn render() -> String {
    let s = snapshot();
    let mut out = String::new();
    out.push_str(
        "# HELP samyama_query_duration_seconds How long each executed query took.\n\
         # TYPE samyama_query_duration_seconds histogram\n",
    );
    for (i, upper) in BUCKETS.iter().enumerate() {
        out.push_str(&format!(
            "samyama_query_duration_seconds_bucket{{le=\"{upper}\"}} {}\n",
            s.buckets[i]
        ));
    }
    out.push_str(&format!(
        "samyama_query_duration_seconds_bucket{{le=\"+Inf\"}} {}\n",
        s.count
    ));
    out.push_str(&format!(
        "samyama_query_duration_seconds_sum {}\n",
        s.sum_seconds
    ));
    out.push_str(&format!(
        "samyama_query_duration_seconds_count {}\n",
        s.count
    ));
    out.push_str(&format!(
        "# HELP samyama_queries_total Queries executed since start.\n\
         # TYPE samyama_queries_total counter\n\
         samyama_queries_total {}\n",
        s.total
    ));
    out.push_str(&format!(
        "# HELP samyama_queries_failed_total Queries that returned an error.\n\
         # TYPE samyama_queries_failed_total counter\n\
         samyama_queries_failed_total {}\n",
        s.failed
    ));
    out.push_str(&format!(
        "# HELP samyama_slow_queries_total Queries past SLOW_QUERY_MS, the same threshold the slow-query log uses.\n\
         # TYPE samyama_slow_queries_total counter\n\
         samyama_slow_queries_total {}\n",
        s.slow
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn an_observation_lands_in_every_bucket_at_or_above_it() {
        // Prometheus buckets are *cumulative*: a 1 ms query is counted in
        // `le="0.001"` and in every larger bucket. Getting this wrong makes
        // `histogram_quantile` return nonsense rather than fail, which is the
        // worst way for a dashboard to be wrong.
        //
        // Tested on a **local** histogram, not the process-global one. The
        // first version asserted a delta on the global and passed alone and
        // failed under `cargo test`: every other test in this binary runs
        // queries, and each one moves the same counters. A test whose result
        // depends on what else is running is not a test.
        let h = Histogram::new();
        h.observe(0.001);
        for (i, upper) in BUCKETS.iter().enumerate() {
            let got = h.buckets[i].load(Ordering::Relaxed);
            let expected = u64::from(0.001 <= *upper);
            assert_eq!(got, expected, "bucket le={upper} holds {got}, expected {expected}");
        }
        assert_eq!(h.count.load(Ordering::Relaxed), 1);
        assert_eq!(h.sum_micros.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn observations_accumulate_across_buckets() {
        let h = Histogram::new();
        for s in [0.0001, 0.01, 2.0, 1000.0] {
            h.observe(s);
        }
        assert_eq!(h.count.load(Ordering::Relaxed), 4);
        // The smallest bucket holds only the smallest observation; the largest
        // holds everything at or below 120 s, which is three of the four. The
        // 1000 s one is past every bucket and appears only in the count, which
        // is what `+Inf` means.
        assert_eq!(h.buckets[0].load(Ordering::Relaxed), 1);
        assert_eq!(h.buckets[BUCKETS.len() - 1].load(Ordering::Relaxed), 3);
    }

    #[test]
    fn the_buckets_are_ascending() {
        // A bucket list out of order silently breaks the cumulative property
        // above, and nothing else would notice.
        assert!(BUCKETS.windows(2).all(|w| w[0] < w[1]), "{BUCKETS:?}");
    }

    #[test]
    fn the_global_counters_only_ever_go_up() {
        // The globals are shared with every other test in this binary, so the
        // only thing assertable about them is monotonicity — and that is worth
        // asserting, because a counter that resets makes every `rate()` in a
        // dashboard spike.
        let before = snapshot();
        record_query(Duration::from_millis(1), true, false);
        record_query(Duration::from_millis(1), false, true);
        let after = snapshot();
        assert!(after.total >= before.total + 2);
        assert!(after.slow >= before.slow + 1);
        assert!(after.failed >= before.failed + 1);
        assert!(after.count >= before.count + 2);
        assert!(after.sum_seconds >= before.sum_seconds);
    }

    #[test]
    fn the_rendered_text_has_an_inf_bucket_and_a_count_that_agree() {
        record_query(Duration::from_millis(2), false, false);
        let text = render();
        let inf = text
            .lines()
            .find(|l| l.contains("le=\"+Inf\""))
            .expect("an +Inf bucket");
        let count = text
            .lines()
            .find(|l| l.starts_with("samyama_query_duration_seconds_count"))
            .expect("a count");
        let n = |l: &str| l.rsplit(' ').next().unwrap().to_string();
        assert_eq!(
            n(inf),
            n(count),
            "Prometheus defines +Inf as the count; a dashboard reads both"
        );
    }
}
