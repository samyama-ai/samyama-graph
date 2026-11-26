//! Per-label retention: how long nodes of a label are kept, and how many
//! (HW-03 / NDS-03, #2217).
//!
//! An edge gateway writes a reading every second and never stops. Without a
//! bound the graph grows until the process runs out of memory, so a label can
//! carry a rule — `CALL db.retention.set('Reading', {maxAge: '1h',
//! maxRecords: 100000})` — and a background sweep deletes what the rule no
//! longer covers.
//!
//! # What a sweep removes
//!
//! A node is *governed* only when **every** one of its labels has a rule. That
//! is how a reading is kept: adding `:Anomaly` to it, a label with no rule,
//! takes it out of every sweep. A governed node is removed when, under any of
//! its labels' rules,
//!
//! * it is older than `maxAge` (by `created_at`), or
//! * it is not among the newest `maxRecords` governed nodes of that label.
//!
//! A node whose creation time was never recorded (`created_at == 0`, the bulk
//! loader's stubs) has no age, so `maxAge` leaves it alone; under `maxRecords`
//! it counts as the oldest.
//!
//! # Where the rules live
//!
//! In the index catalog, as [`IndexDefinition::Retention`] records, so a rule
//! is exactly as durable as an index on the same data directory and travels in
//! a snapshot next to the indexes.
//!
//! [`IndexDefinition::Retention`]: crate::index::catalog::IndexDefinition::Retention

use std::collections::{BTreeMap, HashSet};

use super::{GraphStore, Label, NodeId};

/// One label's bounds. At least one of the two is set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionRule {
    /// Nodes older than this many milliseconds are removed.
    pub max_age_ms: Option<u64>,
    /// At most this many governed nodes of the label are kept, newest first.
    pub max_records: Option<u64>,
}

/// Every label's rule, by label name.
pub type RetentionRules = BTreeMap<String, RetentionRule>;

/// Parse a duration: an integer number of milliseconds, or a number followed
/// by `ms`, `s`, `m`, `h` or `d` (`'500ms'`, `'90s'`, `'15m'`, `'1h'`, `'7d'`).
pub fn parse_duration(text: &str) -> Result<u64, String> {
    let t = text.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (digits, unit) = t.split_at(split);
    let n: u64 = digits.parse().map_err(|_| {
        format!("`{text}` is not a duration; write e.g. '90s', '15m', '1h' or '7d'")
    })?;
    let scale: u64 = match unit.trim() {
        "" | "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        other => {
            return Err(format!(
                "`{text}`: unknown unit `{other}`; use ms, s, m, h or d"
            ))
        }
    };
    n.checked_mul(scale)
        .filter(|ms| *ms > 0)
        .ok_or_else(|| format!("`{text}` must be a positive duration"))
}

/// The nodes a sweep at `now_ms` removes, in ascending id order.
///
/// Read-only, so the choice can be tested without deleting anything and the
/// deletion can happen under the same write lock that made the choice.
pub fn expired(store: &GraphStore, rules: &RetentionRules, now_ms: i64) -> Vec<NodeId> {
    let governed = |id: NodeId| {
        store.get_node(id).is_some_and(|n| {
            !n.labels.is_empty() && n.labels.iter().all(|l| rules.contains_key(l.as_str()))
        })
    };
    let mut out: HashSet<NodeId> = HashSet::new();
    for (label, rule) in rules {
        // (created_at, id) of every governed node of the label, oldest first.
        let mut members: Vec<(i64, NodeId)> = store
            .get_nodes_by_label(&Label::new(label))
            .into_iter()
            .filter(|n| governed(n.id))
            .map(|n| (n.created_at, n.id))
            .collect();
        members.sort_unstable_by_key(|(at, id)| (*at, id.as_u64()));

        if let Some(age) = rule.max_age_ms {
            let cutoff = now_ms.saturating_sub(i64::try_from(age).unwrap_or(i64::MAX));
            out.extend(
                members
                    .iter()
                    .filter(|(at, _)| *at > 0 && *at < cutoff)
                    .map(|(_, id)| *id),
            );
        }
        if let Some(max) = rule.max_records {
            let max = usize::try_from(max).unwrap_or(usize::MAX);
            let over = members.len().saturating_sub(max);
            out.extend(members.iter().take(over).map(|(_, id)| *id));
        }
    }
    let mut out: Vec<NodeId> = out.into_iter().collect();
    out.sort_unstable_by_key(|id| id.as_u64());
    out
}

/// One sweep: delete what the rules no longer cover and persist the
/// deletions through `persistence`, as a statement's writes are (#2348).
/// Returns how many nodes went.
///
/// Does nothing while the persistence layer is degraded: a deletion that
/// cannot be journalled would come back on the next restart, and the writes
/// that are refused in that state are what a sweep is making room for.
pub async fn sweep_once(
    store: &tokio::sync::RwLock<GraphStore>,
    persistence: Option<&crate::persistence::PersistenceManager>,
    tenant: &str,
) -> usize {
    if crate::store_lock::read(store).await.retention_rules().is_empty() {
        return 0;
    }
    if persistence.is_some() && crate::persistence::health::is_degraded() {
        return 0;
    }
    let mut guard = crate::store_lock::write(store).await;
    if persistence.is_some() {
        guard.enable_write_log();
    }
    let removed = guard.sweep_retention(tenant, chrono::Utc::now().timestamp_millis());
    if let Some(pm) = persistence {
        let journal = guard.take_write_log();
        if let Err(e) = pm.apply_mutations(tenant, &guard, &journal) {
            tracing::warn!("retention sweep removed {removed} nodes but could not persist it: {e}");
            crate::persistence::health::mark_degraded(e.to_string());
        }
    }
    if removed > 0 {
        tracing::debug!("retention sweep removed {removed} nodes");
    }
    removed
}

/// Run [`sweep_once`] every `every`, for as long as the process lives.
pub fn spawn_sweeper(
    store: std::sync::Arc<tokio::sync::RwLock<GraphStore>>,
    persistence: Option<std::sync::Arc<crate::persistence::PersistenceManager>>,
    tenant: String,
    every: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            sweep_once(&store, persistence.as_deref(), &tenant).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse_with_and_without_a_unit() {
        assert_eq!(parse_duration("500"), Ok(500));
        assert_eq!(parse_duration("500ms"), Ok(500));
        assert_eq!(parse_duration("90s"), Ok(90_000));
        assert_eq!(parse_duration("15m"), Ok(900_000));
        assert_eq!(parse_duration(" 1h "), Ok(3_600_000));
        assert_eq!(parse_duration("7d"), Ok(604_800_000));
    }

    #[test]
    fn durations_that_mean_nothing_are_refused() {
        for bad in ["", "h", "0s", "1y", "-5s", "1.5h", "99999999999999999999d"] {
            assert!(parse_duration(bad).is_err(), "{bad:?} parsed");
        }
    }
}
