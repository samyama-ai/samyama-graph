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
//! # `ANALYZE :Label`
//!
//! Recomputes exactly what `ANALYZE` does -- the store caches one set of
//! statistics for the whole graph, so there is no per-label refresh to be had,
//! and pretending otherwise would be a claim the code does not back. What the
//! label changes is the row: it reports the part of the statistics that
//! describes that label -- how many nodes carry it and how many of its
//! properties have selectivity statistics -- which is what a reader looking at
//! one bad estimate wants to see. A label no node carries reports zero rather
//! than an error: statistics about an absent label are a legitimate answer,
//! and the planner estimates from the same zero.
//!
//! # Why this is a read
//!
//! It touches a cache, not the graph. `invalidate_statistics_cache` and
//! `compute_statistics` both take `&self` — the cache is behind an `RwLock` —
//! so `ANALYZE` runs on the read executor. Reporting it as a write would make
//! it unavailable to exactly the person diagnosing a slow read.

use crate::graph::{GraphStore, Label, PropertyValue};

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

/// Columns `ANALYZE :Label` returns.
pub fn analyze_label_columns() -> Vec<String> {
    vec![
        "label".to_string(),
        "nodes".to_string(),
        "properties".to_string(),
        "cache_was_stale".to_string(),
    ]
}

/// The columns of `ANALYZE`, or of `ANALYZE :Label` when a label is given.
pub fn columns_for(label: Option<&Label>) -> Vec<String> {
    match label {
        Some(_) => analyze_label_columns(),
        None => analyze_columns(),
    }
}

/// Recomputes [`GraphStatistics`] and reports what it computed them over.
#[derive(Debug, Default)]
pub struct AnalyzeOperator {
    done: bool,
    /// `ANALYZE :Label`: report this label's part of the statistics.
    label: Option<Label>,
}

impl AnalyzeOperator {
    pub fn new() -> Self {
        Self {
            done: false,
            label: None,
        }
    }

    /// `ANALYZE :Label` -- the same recomputation, reported for one label.
    pub fn for_label(label: Option<Label>) -> Self {
        Self { done: false, label }
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
        if let Some(label) = &self.label {
            let nodes = stats.label_counts.get(label).copied().unwrap_or(0);
            let properties = stats
                .property_stats
                .keys()
                .filter(|(l, _)| l == label)
                .count();
            record.bind(
                "label".to_string(),
                Value::Property(PropertyValue::String(label.as_str().to_string())),
            );
            record.bind(
                "nodes".to_string(),
                Value::Property(PropertyValue::Integer(nodes as i64)),
            );
            record.bind(
                "properties".to_string(),
                Value::Property(PropertyValue::Integer(properties as i64)),
            );
            record.bind(
                "cache_was_stale".to_string(),
                Value::Property(PropertyValue::Boolean(was_cached)),
            );
            return Ok(Some(record));
        }
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
        let details = match &self.label {
            Some(l) => format!("recompute planner statistics, report :{}", l.as_str()),
            None => "recompute planner statistics".to_string(),
        };
        OperatorDescription {
            name: "Analyze".to_string(),
            details,
            children: Vec::new(),
        }
    }
}

#[cfg(test)]
#[path = "analyze_ops_cov_tests.rs"]
mod cov_tests;
