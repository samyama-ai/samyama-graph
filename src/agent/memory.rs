//! Graph-native decision trace primitives for agent memory (#2099).

use crate::graph::{EdgeType, GraphStore, Label, NodeId, PropertyValue};

const TENANT: &str = "default";
const DECISION_LABEL: &str = "AgentDecision";
const CAUSED_BY: &str = "CAUSED_BY";
const EVIDENCED_BY: &str = "EVIDENCED_BY";

/// A decision recorded as one step in an inspectable trace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecisionStep {
    pub node_id: NodeId,
    pub session_id: String,
    pub trace_id: String,
    pub step_order: u64,
    pub parent_step_id: Option<NodeId>,
    pub action: String,
    pub rationale: String,
    pub evidence_node_ids: Vec<NodeId>,
    pub outcome: String,
}

/// A trace is ordered for reading and retains graph IDs for causal inspection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecisionTrace {
    pub trace_id: String,
    pub steps: Vec<DecisionStep>,
}

/// Record one decision and its causal/evidence edges in the graph.
///
/// Passing no trace ID starts a new trace. `parent_step_id` links this step to
/// an earlier decision in the same trace; evidence IDs may refer to any graph
/// nodes and are checked before the trace is changed.
pub fn record_decision(
    store: &mut GraphStore,
    session_id: &str,
    trace_id: Option<&str>,
    parent_step_id: Option<NodeId>,
    action: &str,
    rationale: &str,
    evidence_node_ids: &[NodeId],
    outcome: &str,
) -> Result<DecisionStep, String> {
    if session_id.trim().is_empty() {
        return Err("session_id must not be empty".into());
    }
    if action.trim().is_empty() {
        return Err("action must not be empty".into());
    }
    for &evidence_id in evidence_node_ids {
        if !store.has_node(evidence_id) {
            return Err(format!("evidence node {evidence_id} does not exist"));
        }
    }

    let trace_id = trace_id
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    if trace_id.trim().is_empty() {
        return Err("trace_id must not be empty".into());
    }

    if let Some(parent_id) = parent_step_id {
        let Some(parent) = read_step(store, parent_id) else {
            return Err(format!("parent decision {parent_id} does not exist"));
        };
        if parent.trace_id != trace_id {
            return Err("parent decision must belong to the same trace".into());
        }
    }

    let step_order = trace_steps(store, &trace_id)
        .iter()
        .map(|step| step.step_order)
        .max()
        .map_or(0, |last| last.saturating_add(1));
    let node_id = store.create_node(DECISION_LABEL);
    let fields = [
        ("session_id", PropertyValue::String(session_id.to_owned())),
        ("trace_id", PropertyValue::String(trace_id.clone())),
        ("step_order", PropertyValue::Integer(step_order as i64)),
        ("action", PropertyValue::String(action.to_owned())),
        ("rationale", PropertyValue::String(rationale.to_owned())),
        ("outcome", PropertyValue::String(outcome.to_owned())),
    ];
    for (key, value) in fields {
        store
            .set_node_property(TENANT, node_id, key, value)
            .map_err(|error| format!("could not store decision field `{key}`: {error}"))?;
    }

    if let Some(parent_id) = parent_step_id {
        store
            .create_edge(node_id, parent_id, EdgeType::new(CAUSED_BY))
            .map_err(|error| format!("could not link parent decision: {error}"))?;
    }
    for &evidence_id in evidence_node_ids {
        store
            .create_edge(node_id, evidence_id, EdgeType::new(EVIDENCED_BY))
            .map_err(|error| format!("could not link evidence node: {error}"))?;
    }

    Ok(DecisionStep {
        node_id,
        session_id: session_id.to_owned(),
        trace_id,
        step_order,
        parent_step_id,
        action: action.to_owned(),
        rationale: rationale.to_owned(),
        evidence_node_ids: evidence_node_ids.to_vec(),
        outcome: outcome.to_owned(),
    })
}

/// Read all decisions for a trace in insertion order, including their graph links.
pub fn inspect_trace(store: &GraphStore, trace_id: &str) -> DecisionTrace {
    DecisionTrace {
        trace_id: trace_id.to_owned(),
        steps: trace_steps(store, trace_id),
    }
}

fn trace_steps(store: &GraphStore, trace_id: &str) -> Vec<DecisionStep> {
    let Some(nodes) = store.nodes_with_label(&Label::new(DECISION_LABEL)) else {
        return Vec::new();
    };
    let mut steps = nodes
        .iter()
        .filter_map(|&node_id| read_step(store, node_id))
        .filter(|step| step.trace_id == trace_id)
        .collect::<Vec<_>>();
    steps.sort_by_key(|step| (step.step_order, step.node_id));
    steps
}

fn read_step(store: &GraphStore, node_id: NodeId) -> Option<DecisionStep> {
    let properties = store.node_properties_merged(node_id);
    let string = |key: &str| match properties.get(key) {
        Some(PropertyValue::String(value)) => Some(value.clone()),
        _ => None,
    };
    let session_id = string("session_id")?;
    let trace_id = string("trace_id")?;
    let action = string("action")?;
    let rationale = string("rationale")?;
    let outcome = string("outcome")?;
    let step_order = match properties.get("step_order") {
        Some(PropertyValue::Integer(value)) if *value >= 0 => *value as u64,
        _ => return None,
    };

    let mut parent_step_id = None;
    let mut evidence_node_ids = Vec::new();
    for (_, source, target, edge_type) in store.get_outgoing_edge_targets_owned(node_id) {
        if source != node_id {
            continue;
        }
        match edge_type.as_str() {
            CAUSED_BY => parent_step_id = Some(target),
            EVIDENCED_BY => evidence_node_ids.push(target),
            _ => {}
        }
    }
    evidence_node_ids.sort_unstable();

    Some(DecisionStep {
        node_id,
        session_id,
        trace_id,
        step_order,
        parent_step_id,
        action,
        rationale,
        evidence_node_ids,
        outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_inspects_a_causal_trace_with_evidence() {
        let mut store = GraphStore::new();
        let evidence = store.create_node("Observation");
        let first = record_decision(
            &mut store,
            "session-a",
            Some("trace-a"),
            None,
            "search",
            "Need supporting evidence",
            &[evidence],
            "found one observation",
        )
        .unwrap();
        let second = record_decision(
            &mut store,
            "session-b",
            Some("trace-a"),
            Some(first.node_id),
            "answer",
            "Use the retrieved observation",
            &[evidence],
            "answered",
        )
        .unwrap();

        let trace = inspect_trace(&store, "trace-a");
        assert_eq!(trace.trace_id, "trace-a");
        assert_eq!(trace.steps.len(), 2);
        assert_eq!(trace.steps[0].step_order, 0);
        assert_eq!(trace.steps[0].evidence_node_ids, vec![evidence]);
        assert_eq!(trace.steps[1].step_order, 1);
        assert_eq!(trace.steps[1].parent_step_id, Some(first.node_id));
        assert_eq!(trace.steps[1].session_id, "session-b");
        assert_eq!(trace.steps[1].node_id, second.node_id);
    }

    #[test]
    fn refuses_missing_evidence_and_cross_trace_parents_without_mutating() {
        let mut store = GraphStore::new();
        let parent = record_decision(
            &mut store,
            "session-a",
            Some("trace-a"),
            None,
            "observe",
            "first step",
            &[],
            "done",
        )
        .unwrap();
        let before = store.node_count();
        assert!(record_decision(
            &mut store,
            "session-a",
            Some("trace-a"),
            None,
            "answer",
            "missing evidence",
            &[NodeId::new(999_999)],
            "done",
        )
        .unwrap_err()
        .contains("evidence node"));
        assert!(record_decision(
            &mut store,
            "session-a",
            Some("trace-b"),
            Some(parent.node_id),
            "answer",
            "wrong trace",
            &[],
            "done",
        )
        .unwrap_err()
        .contains("same trace"));
        assert_eq!(store.node_count(), before);
    }

    #[test]
    fn creates_a_unique_trace_when_none_is_supplied() {
        let mut store = GraphStore::new();
        let step = record_decision(
            &mut store,
            "session-a",
            None,
            None,
            "inspect",
            "start trace",
            &[],
            "done",
        )
        .unwrap();
        assert!(!step.trace_id.is_empty());
        assert_eq!(inspect_trace(&store, &step.trace_id).steps, vec![step]);
    }
}
