//! `EXPLAIN (FORMAT JSON)` and `PROFILE (FORMAT JSON)` (#2013).
//!
//! A workbench drawing the plan as a diagram read EXPLAIN's text with regular
//! expressions -- the `+- ` indent, the `[estimated rows=… cost=…]` suffix,
//! the `--- Planner Diagnostics ---` header -- so any change to how the text
//! is laid out broke the diagram without failing anything. This builds the
//! same plan as a map, from the same trees the text is printed from, so the
//! two cannot disagree and the layout of the text is no longer a contract.
//!
//! One row, column `plan`. Operators are numbered in pre-order from 0, the
//! order PROFILE instruments them in. A number the engine does not have is
//! `null`, never `0`: an operator with no row model has no estimate, which is
//! not an estimate of nothing.

use std::time::Duration;

use crate::graph::{GraphStore, PropertyValue};

use super::operator::OperatorBox;
use super::planner::PlanDiagnostics;
use super::profile::ProfileNode;
use super::{estimate_node, EstimatedNode, Record, RecordBatch, Value};

/// Bumped when a field changes meaning or is removed; adding one does not.
pub const FORMAT_VERSION: i64 = 1;

fn map(entries: Vec<(&str, PropertyValue)>) -> PropertyValue {
    PropertyValue::Map(entries.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn number(v: Option<f64>) -> PropertyValue {
    v.map(PropertyValue::Float).unwrap_or(PropertyValue::Null)
}

fn millis(d: Duration) -> PropertyValue {
    PropertyValue::Float(d.as_secs_f64() * 1000.0)
}

fn text(s: &str) -> PropertyValue {
    PropertyValue::String(s.to_string())
}

/// The structural hash as `/api/query` reports it, so the two can be compared.
fn hash(root: &OperatorBox) -> PropertyValue {
    text(&format!("{:016x}", root.describe().structural_hash()))
}

fn estimated(node: &EstimatedNode, next_id: &mut i64) -> PropertyValue {
    let id = *next_id;
    *next_id += 1;
    let children = node.children.iter().map(|c| estimated(c, next_id)).collect();
    map(vec![
        ("id", PropertyValue::Integer(id)),
        ("operator", text(&node.name)),
        ("details", text(&node.details)),
        ("estimated_rows", number(node.rows)),
        ("estimated_cost", number(node.cost)),
        ("children", PropertyValue::Array(children)),
    ])
}

/// The planner's candidates, numbered as `CYPHER plan=N` takes them. Only the
/// graph-native planner records them; under the default planner the list is
/// empty and `candidates_evaluated` is null.
fn candidates(diag: Option<PlanDiagnostics>) -> Vec<(&'static str, PropertyValue)> {
    let Some(diag) = diag else {
        return vec![
            ("candidates_evaluated", PropertyValue::Null),
            ("candidate_plans", PropertyValue::Array(vec![])),
        ];
    };
    let plans = diag
        .candidate_costs
        .iter()
        .enumerate()
        .map(|(i, (description, cost))| {
            map(vec![
                ("plan", PropertyValue::Integer(i as i64 + 1)),
                ("cost", PropertyValue::Float(*cost)),
                ("chosen", PropertyValue::Boolean(i == diag.chosen)),
                ("description", text(description)),
            ])
        })
        .collect();
    vec![
        ("candidates_evaluated", PropertyValue::Integer(diag.candidates_evaluated as i64)),
        ("candidate_plans", PropertyValue::Array(plans)),
    ]
}

/// The per-operator row budget in force; `null` when it is disabled.
fn budget(row_budget: u64) -> PropertyValue {
    match row_budget {
        0 => PropertyValue::Null,
        n => PropertyValue::Integer(n as i64),
    }
}

/// The `EXPLAIN (FORMAT JSON)` document.
pub(super) fn explain(
    root: &mut OperatorBox,
    store: &GraphStore,
    row_budget: u64,
    diag: Option<PlanDiagnostics>,
) -> PropertyValue {
    let plan_hash = hash(root);
    let tree = estimate_node(root, store);
    let mut fields = vec![
        ("format_version", PropertyValue::Integer(FORMAT_VERSION)),
        ("kind", text("explain")),
        ("plan_hash", plan_hash),
        ("total_estimated_rows", number(tree.rows)),
        ("total_estimated_cost", number(tree.cost)),
        ("root", estimated(&tree, &mut 0)),
        ("row_budget", budget(row_budget)),
    ];
    fields.extend(candidates(diag));
    map(fields)
}

/// What PROFILE measured, alongside the plan it measured.
pub(super) struct Measured<'a> {
    pub nodes: &'a [ProfileNode],
    /// The instrumented run, whose per-operator times the tree reports.
    pub instrumented: Duration,
    /// The plain run, which is the query's real execution time.
    pub plain: Duration,
    pub rows: usize,
}

/// The `PROFILE (FORMAT JSON)` document. `plan_hash` is taken before the plan
/// is instrumented, since instrumenting it consumes it.
pub(super) fn profile(
    plan_hash: PropertyValue,
    measured: Measured<'_>,
    row_budget: u64,
    diag: Option<PlanDiagnostics>,
) -> PropertyValue {
    let nodes = measured.nodes;
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for (i, node) in nodes.iter().enumerate() {
        if let Some(parent) = node.parent {
            children[parent].push(i);
        }
    }
    // Cost is the rows an operator and everything under it produce, as
    // EXPLAIN computes it; unknown when any estimate in the subtree is.
    let mut cost: Vec<Option<f64>> = vec![None; nodes.len()];
    for i in (0..nodes.len()).rev() {
        let total = nodes[i].estimated.and_then(|rows| {
            children[i].iter().try_fold(rows, |acc, &c| cost[c].map(|cc| acc + cc))
        });
        cost[i] = total;
    }
    fn build(i: usize, nodes: &[ProfileNode], children: &[Vec<usize>], cost: &[Option<f64>]) -> PropertyValue {
        let node = &nodes[i];
        let below: Duration = children[i].iter().map(|&c| nodes[c].inclusive()).sum();
        map(vec![
            ("id", PropertyValue::Integer(i as i64)),
            ("operator", text(&node.name)),
            ("details", text(&node.details)),
            ("estimated_rows", number(node.estimated)),
            ("estimated_cost", number(cost[i])),
            ("actual_rows", PropertyValue::Integer(node.rows() as i64)),
            ("calls", PropertyValue::Integer(node.calls() as i64)),
            ("time_ms", millis(node.inclusive())),
            ("self_time_ms", millis(node.inclusive().saturating_sub(below))),
            ("q_error", number(node.q_error())),
            (
                "children",
                PropertyValue::Array(children[i].iter().map(|&c| build(c, nodes, children, cost)).collect()),
            ),
        ])
    }
    let root = match nodes.first() {
        Some(_) => build(0, nodes, &children, &cost),
        None => PropertyValue::Null,
    };
    let mut fields = vec![
        ("format_version", PropertyValue::Integer(FORMAT_VERSION)),
        ("kind", text("profile")),
        ("plan_hash", plan_hash),
        ("total_estimated_rows", number(nodes.first().and_then(|n| n.estimated))),
        ("total_estimated_cost", number(cost.first().copied().flatten())),
        ("total_actual_rows", PropertyValue::Integer(measured.rows as i64)),
        ("execution_time_ms", millis(measured.plain)),
        ("instrumented_time_ms", millis(measured.instrumented)),
        ("root", root),
        ("row_budget", budget(row_budget)),
    ];
    fields.extend(candidates(diag));
    map(fields)
}

/// The one-row, one-column result both documents are returned in.
pub(super) fn batch(document: PropertyValue, plan_hash: Option<u64>) -> RecordBatch {
    let mut record = Record::new();
    record.bind("plan".to_string(), Value::Property(document));
    RecordBatch { records: vec![record], columns: vec!["plan".to_string()], plan_hash }
}

/// The hash of `root`, for the PROFILE path, which must take it before
/// instrumenting the plan.
pub(super) fn plan_hash_of(root: &OperatorBox) -> PropertyValue {
    hash(root)
}

#[cfg(test)]
#[path = "plan_json_tests.rs"]
mod tests;
