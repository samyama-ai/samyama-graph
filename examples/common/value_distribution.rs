//! Value-distribution checks for KG loaders.
//!
//! Row counts cannot see the defect class that #1609 and #1815 record: a
//! loader writes the right number of nodes, with the right labels, and the
//! snapshot loads -- but a column holds the wrong values, or no values at all.
//!
//! Three shapes, all measured per label and property rather than per row:
//!
//! * **A column shift.** WHO SPAR capacity C1 is the only capacity whose name
//!   contains a comma, so a naive `split(',')` moved `year` into `score` for
//!   exactly those 562 rows. `score` then ranged 2021..=2023 where every other
//!   capacity ranged 0..=100, and `year` was absent (#1609).
//! * **Structure with no content.** `Country.income_level` and
//!   `Country.who_region` were present on all 233 nodes and non-empty on none.
//!   That is worse than an absent column: `exists(c.who_region)` returns true
//!   and grouping by region yields one empty bucket (#1815).
//! * **One value standing in for all of them.** `SideEffect.name` in SIDER has
//!   5,805 values and 2 distinct ones, `PT` and `LLT` -- the MedDRA term-type
//!   column written where the term belongs (#1815). Same shape, caught by the
//!   same measurement: distinct values against node count.
//!
//! The checks return the violations they find rather than panicking, so a
//! caller can report all of them at once.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use samyama_sdk::{GraphStore, Label, PropertyValue};

/// How one property is distributed over the nodes of one label.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyStats {
    /// Nodes carrying the key at all.
    pub present: usize,
    /// Nodes whose value is not an empty or whitespace-only string.
    pub non_empty: usize,
    /// Distinct rendered values, empty ones included.
    pub distinct: usize,
}

fn render(v: &PropertyValue) -> String {
    match v {
        PropertyValue::String(s) => s.clone(),
        other => format!("{:?}", other),
    }
}

fn numeric(v: &PropertyValue) -> Option<f64> {
    match v {
        PropertyValue::Integer(i) => Some(*i as f64),
        PropertyValue::Float(f) => Some(*f),
        _ => None,
    }
}

/// Count, for every property seen on `label`, how many nodes carry it, how
/// many carry a non-empty value, and how many distinct values there are.
///
/// Returns `(node_count, stats_by_key)`.
pub fn property_stats(graph: &GraphStore, label: &str) -> (usize, BTreeMap<String, KeyStats>) {
    let l: Label = label.into();
    let mut nodes = 0usize;
    let mut present: BTreeMap<String, usize> = BTreeMap::new();
    let mut non_empty: BTreeMap<String, usize> = BTreeMap::new();
    let mut values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for node in graph.get_nodes_by_label(&l) {
        nodes += 1;
        for (k, v) in node.properties.iter() {
            *present.entry(k.clone()).or_default() += 1;
            let rendered = render(v);
            if !rendered.trim().is_empty() {
                *non_empty.entry(k.clone()).or_default() += 1;
            }
            values.entry(k.clone()).or_default().insert(rendered);
        }
    }
    let stats = present
        .into_iter()
        .map(|(k, p)| {
            let s = KeyStats {
                present: p,
                non_empty: non_empty.get(&k).copied().unwrap_or(0),
                distinct: values.get(&k).map(|v| v.len()).unwrap_or(0),
            };
            (k, s)
        })
        .collect();
    (nodes, stats)
}

/// Every property written for *every* node of a label must be non-empty for at
/// least one of them. A column present on all and empty on all carries no
/// information while reading as if it does (#1815).
pub fn empty_on_every_node(graph: &GraphStore, label: &str) -> Vec<String> {
    let (nodes, stats) = property_stats(graph, label);
    if nodes == 0 {
        return vec![];
    }
    stats
        .into_iter()
        .filter(|(_, s)| s.present == nodes && s.non_empty == 0)
        .map(|(k, s)| {
            format!(
                "{label}.{k}: present on {}/{} nodes, non-empty on 0",
                s.present, nodes
            )
        })
        .collect()
}

/// Properties whose distinct-value count is at or below `max_distinct` while
/// being written for more than `min_nodes` nodes. SIDER's `SideEffect.name` is
/// 5,805 values and 2 distinct ones (#1815).
pub fn too_few_distinct_values(
    graph: &GraphStore,
    label: &str,
    min_nodes: usize,
    max_distinct: usize,
) -> Vec<String> {
    let (_, stats) = property_stats(graph, label);
    stats
        .into_iter()
        .filter(|(_, s)| s.present > min_nodes && s.distinct <= max_distinct)
        .map(|(k, s)| format!("{label}.{k}: {} values, {} distinct", s.present, s.distinct))
        .collect()
}

/// Nodes of `label` that do not carry `key` at all.
pub fn missing_property(graph: &GraphStore, label: &str, key: &str) -> usize {
    let l: Label = label.into();
    graph
        .get_nodes_by_label(&l)
        .iter()
        .filter(|n| n.get_property(key).is_none())
        .count()
}

/// Observed `[min, max]` of a numeric property over the nodes of `label` whose
/// `group_key` equals `group`, or `None` when no node qualifies.
///
/// A range per group is what separates a column shift from a wide-but-valid
/// distribution: SPAR C01 read 2021..=2023 while every sibling read 0..=100.
pub fn numeric_range_for_group(
    graph: &GraphStore,
    label: &str,
    key: &str,
    group_key: &str,
    group: &str,
) -> Option<(f64, f64)> {
    let l: Label = label.into();
    let mut range: Option<(f64, f64)> = None;
    for node in graph.get_nodes_by_label(&l) {
        let matches = match node.get_property(group_key) {
            Some(PropertyValue::String(s)) => s == group,
            _ => false,
        };
        if !matches {
            continue;
        }
        if let Some(v) = node.get_property(key).and_then(numeric) {
            range = Some(match range {
                None => (v, v),
                Some((lo, hi)) => (lo.min(v), hi.max(v)),
            });
        }
    }
    range
}

/// Observed `[min, max]` of a numeric property over every node of `label`.
pub fn numeric_range(graph: &GraphStore, label: &str, key: &str) -> Option<(f64, f64)> {
    let l: Label = label.into();
    let mut range: Option<(f64, f64)> = None;
    for node in graph.get_nodes_by_label(&l) {
        if let Some(v) = node.get_property(key).and_then(numeric) {
            range = Some(match range {
                None => (v, v),
                Some((lo, hi)) => (lo.min(v), hi.max(v)),
            });
        }
    }
    range
}
