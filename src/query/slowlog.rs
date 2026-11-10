//! The most recent slow queries, kept in memory and readable over HTTP (#2079).
//!
//! `samyama_slow_queries_total` on `/metrics` says slow queries happened, and
//! the `slow query` log line says which -- on the server's stderr, which an
//! operator, a workbench or an agent watching the counter often cannot reach.
//! Redis and FalkorDB answer the same question with `SLOWLOG GET`, PostgreSQL
//! with `pg_stat_statements`. This is that: a bounded ring of the last
//! `SAMYAMA_SLOWLOG_MAX_LEN` (default 128) queries past `SLOW_QUERY_MS`,
//! filled from the same place the log line is written, so the two never
//! disagree about what was slow.
//!
//! Process-wide, like the counters in `metrics`: one engine serves every
//! tenant, and the entry does not yet carry the tenant it ran for.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use serde::Serialize;

/// Entries kept when `SAMYAMA_SLOWLOG_MAX_LEN` is unset or unreadable.
pub const DEFAULT_MAX_LEN: usize = 128;

/// Characters of query text kept per entry, as in the log line: a generated
/// query can be megabytes, and a ring of 128 of them is a memory problem.
const QUERY_CHARS: usize = 512;

/// One query that ran past the slow-query threshold.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SlowQuery {
    /// Monotonic per process, so a poller can ask for what is newer than the
    /// last id it saw, and a reset does not reuse ids.
    pub id: u64,
    /// When it finished, milliseconds since the Unix epoch.
    pub timestamp_ms: u64,
    /// The query text, cut at 512 characters.
    pub query: String,
    pub elapsed_ms: u64,
    /// The threshold in force when it was recorded.
    pub threshold_ms: u64,
    pub rows: usize,
    pub failed: bool,
}

struct Ring {
    entries: Mutex<VecDeque<SlowQuery>>,
    max_len: usize,
    next_id: AtomicU64,
}

fn ring() -> &'static Ring {
    static RING: OnceLock<Ring> = OnceLock::new();
    RING.get_or_init(|| {
        let max_len = max_len_from(std::env::var("SAMYAMA_SLOWLOG_MAX_LEN").ok().as_deref());
        Ring {
            entries: Mutex::new(VecDeque::with_capacity(max_len)),
            max_len,
            next_id: AtomicU64::new(1),
        }
    })
}

/// The ring's size from `SAMYAMA_SLOWLOG_MAX_LEN`: a positive integer, else the
/// default. `0` is read as "use the default", not "keep nothing": a slow-query
/// log that silently records nothing is the gap this exists to close, and
/// `SLOW_QUERY_MS=0` already switches slow-query recording off.
pub fn max_len_from(value: Option<&str>) -> usize {
    value
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MAX_LEN)
}

/// Record one slow query, dropping the oldest when the ring is full.
///
/// The id and the timestamp are taken while the lock is held. Taken before it,
/// two queries finishing together could take ids 5 and 6 and then enter the
/// ring as 6, 5, so "newest first" would not be in id order.
pub fn record(query: &str, elapsed_ms: u64, threshold_ms: u64, rows: usize, failed: bool) {
    let r = ring();
    let query: String = query.chars().take(QUERY_CHARS).collect();
    let mut entries = r.entries.lock().unwrap_or_else(|e| e.into_inner());
    let entry = SlowQuery {
        id: r.next_id.fetch_add(1, Ordering::Relaxed),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64),
        query,
        elapsed_ms,
        threshold_ms,
        rows,
        failed,
    };
    if entries.len() == r.max_len {
        entries.pop_front();
    }
    entries.push_back(entry);
}

/// The most recent entries, newest first, at most `limit` of them.
pub fn recent(limit: usize) -> Vec<SlowQuery> {
    let entries = ring().entries.lock().unwrap_or_else(|e| e.into_inner());
    entries.iter().rev().take(limit).cloned().collect()
}

/// Entries currently held.
pub fn len() -> usize {
    ring().entries.lock().unwrap_or_else(|e| e.into_inner()).len()
}

/// The ring's capacity.
pub fn max_len() -> usize {
    ring().max_len
}

/// Empty the ring; returns how many entries it held. Ids keep counting.
pub fn reset() -> usize {
    let mut entries = ring().entries.lock().unwrap_or_else(|e| e.into_inner());
    let n = entries.len();
    entries.clear();
    n
}
