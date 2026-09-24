//! `ANALYZE` — recompute the planner's statistics and report them (LANG-13).
//!
//! The planner estimates cardinality from [`GraphStatistics`], which
//! `GraphStore` builds lazily and caches.
//!
//! # What this is, and what it is not
//!
//! The obvious justification for `ANALYZE` is "the cache can go stale". On
//! this engine it cannot, and that was checked rather than assumed: every
//! mutating path calls `invalidate_statistics_cache` — 22 call sites, including
//! the bulk-load `create_node_stub` and `create_edge_stub` that a snapshot
//! import uses. Removing the invalidation below fails no test, because no
//! public API can produce a populated-but-wrong cache.
//!
//! So this is **a diagnostic that also forces recomputation**, not a repair:
//!
//! - it reports the numbers the planner is actually working from, which is
//!   otherwise only visible by reading the source or restarting;
//! - it guarantees the next plan uses statistics computed now, which is what a
//!   reader diagnosing a bad plan wants to be able to assume;
//! - the invalidation is insurance against a future write path that forgets to
//!   invalidate — cheap, and stated here as insurance rather than sold as a fix
//!   for a bug that exists.
//!
//! Saying that plainly matters more than the feature: a statement documented as
//! fixing a staleness that cannot happen would be a claim nothing backs.
//!
//! # It reports what it did
//!
//! `ANALYZE` returns a row rather than nothing. A statement whose only effect
//! is invisible gives the caller no way to tell "recomputed" from "did
//! nothing". The row carries the node and edge counts the statistics were
//! computed over, the number of labels and edge types, and whether a cached set
//! was replaced — the statistics' own subject, so a reader can see at a glance
//! whether they describe the graph they think they do.
//!
//! # Why this is a read
//!
//! It touches a cache, not the graph. `invalidate_statistics_cache` and
//! `compute_statistics` both take `&self` — the cache is behind an `RwLock` —
//! so `ANALYZE` runs on the read executor. Reporting it as a write would make
//! it unavailable to exactly the person diagnosing a slow read.

use crate::graph::{GraphStore, PropertyValue};

use super::operator::{OperatorDescription, PhysicalOperator};
use super::record::{Record, Value};
use super::ExecutionResult;

/// Columns `ANALYZE` returns.
pub fn analyze_columns() -> Vec<String> {
    vec![
        "nodes".to_string(),
        "edges".to_string(),
        "labels".to_string(),
        "edge_types".to_string(),
        "cache_was_stale".to_string(),
    ]
}

/// Recomputes [`GraphStatistics`] and reports what it computed them over.
#[derive(Debug, Default)]
pub struct AnalyzeOperator {
    done: bool,
}

impl AnalyzeOperator {
    pub fn new() -> Self {
        Self { done: false }
    }
}

impl PhysicalOperator for AnalyzeOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Was there a cached set before this ran? Asked before invalidating,
        // because afterwards the answer is always "no" and the row would say
        // the same thing on every call -- a field that cannot vary is not
        // reporting anything. This is also the only part of the row a test can
        // distinguish: see the module docs on why the invalidation itself is
        // not observable through the public API.
        let was_cached = store.has_cached_statistics();

        store.invalidate_statistics_cache();
        let stats = store.statistics();

        let mut record = Record::new();
        record.bind(
            "nodes".to_string(),
            Value::Property(PropertyValue::Integer(stats.total_nodes as i64)),
        );
        record.bind(
            "edges".to_string(),
            Value::Property(PropertyValue::Integer(stats.total_edges as i64)),
        );
        record.bind(
            "labels".to_string(),
            Value::Property(PropertyValue::Integer(stats.label_counts.len() as i64)),
        );
        record.bind(
            "edge_types".to_string(),
            Value::Property(PropertyValue::Integer(stats.edge_type_counts.len() as i64)),
        );
        // `true` means the cache held a set that this call replaced. `false`
        // means there was nothing cached and the statistics would have been
        // computed on the next plan anyway.
        record.bind(
            "cache_was_stale".to_string(),
            Value::Property(PropertyValue::Boolean(was_cached)),
        );
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.done = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Analyze".to_string(),
            details: "recompute planner statistics".to_string(),
            children: Vec::new(),
        }
    }
}
