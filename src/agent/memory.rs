//! Graph-native decision trace primitives for agent memory (#2099).

use crate::graph::{EdgeType, GraphStore, Label, NodeId, PropertyValue};
use crate::persistence::PersistedWriter;

/// `AgentDecision` is agent memory: it is in the single memory label list
/// (`memory_labels`, #2242), so export leaves it out by default and the publish
/// gate refuses it -- a decision's `rationale` is free text a tenant would not
/// want in a published snapshot.
const DECISION_LABEL: &str = super::memory_labels::AGENT_DECISION;
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

/// One decision to record. A struct rather than eight positional parameters:
/// `Some("trace-a"), None, "search", "Need evidence"` reads as four
/// interchangeable arguments at a call site and the compiler cannot tell a
/// transposed pair from a correct one.
#[derive(Clone, Copy, Debug)]
pub struct DecisionInput<'a> {
    pub session_id: &'a str,
    /// `None` starts a new trace.
    pub trace_id: Option<&'a str>,
    /// Links this step to an earlier decision in the same trace.
    pub parent_step_id: Option<NodeId>,
    pub action: &'a str,
    pub rationale: &'a str,
    /// May refer to any graph nodes, not only memory ones.
    pub evidence_node_ids: &'a [NodeId],
    pub outcome: &'a str,
}

/// Record one decision and its causal/evidence edges in the graph.
///
/// Written through `writer` as one transaction under `tenant` (#2241): the
/// rows persist under that tenant, the same tenant names the index events
/// they publish, and the decision node and its edges land together or not at
/// all. A failure part-way -- an edge that cannot be created, a write that
/// does not reach disk -- leaves nothing behind.
pub async fn record_decision(
    writer: &PersistedWriter,
    tenant: &str,
    input: DecisionInput<'_>,
) -> Result<DecisionStep, String> {
    writer
        .write(tenant, |store| record_decision_in(store, tenant, input))
        .await
        .map_err(|error| error.to_string())
}

/// The writes of [`record_decision`], run inside its transaction.
///
/// Inputs are checked before the first node is created, so a missing evidence
/// node or a parent in another trace is refused without writing anything; the
/// transaction is what undoes a failure after that point.
fn record_decision_in(
    store: &mut GraphStore,
    tenant: &str,
    input: DecisionInput<'_>,
) -> Result<DecisionStep, String> {
    let DecisionInput {
        session_id,
        trace_id,
        parent_step_id,
        action,
        rationale,
        evidence_node_ids,
        outcome,
    } = input;
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
    store
        .admit_node()
        .map_err(|error| format!("could not store decision: {error}"))?;
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
            .set_node_property(tenant, node_id, key, value)
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

/// Every `AgentDecision` node in the store is read and then filtered by trace,
/// so this costs the whole label, not one trace. `record_decision` calls it to
/// pick the next `step_order`, which makes recording the Nth decision O(all
/// decisions recorded so far). Fine for a first slice and for the sizes the
/// tests use; the memory benchmark harness (#2243) is where it will show, and
/// a per-trace index is the fix when it does.
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
    use crate::graph::event::IndexEvent;

    /// The tenant used to be a `const "default"`, which published every
    /// tenant's decision writes under `default` to the index, auto-embed and
    /// agent-trigger subscribers. Asserting the event proves the parameter is
    /// carried rather than merely accepted.
    #[test]
    fn the_tenant_argument_reaches_the_index_event() {
        let (mut store, mut rx) = GraphStore::with_async_indexing();
        record_decision_in(
            &mut store,
            "tenant-b",
            DecisionInput {
                session_id: "session-a",
                trace_id: Some("trace-a"),
                parent_step_id: None,
                action: "observe",
                rationale: "first step",
                evidence_node_ids: &[],
                outcome: "done",
            },
        )
        .unwrap();

        let mut seen = None;
        while let Ok(event) = rx.try_recv() {
            if let IndexEvent::PropertySet { tenant_id, .. } = event {
                seen = Some(tenant_id);
                break;
            }
        }
        assert_eq!(seen.as_deref(), Some("tenant-b"));
    }

    #[test]
    fn records_and_inspects_a_causal_trace_with_evidence() {
        let mut store = GraphStore::new();
        let evidence = store.create_node("Observation");
        let first = record_decision_in(
            &mut store,
            "default",
            DecisionInput {
                session_id: "session-a",
                trace_id: Some("trace-a"),
                parent_step_id: None,
                action: "search",
                rationale: "Need supporting evidence",
                evidence_node_ids: &[evidence],
                outcome: "found one observation",
            },
        )
        .unwrap();
        let second = record_decision_in(
            &mut store,
            "default",
            DecisionInput {
                session_id: "session-b",
                trace_id: Some("trace-a"),
                parent_step_id: Some(first.node_id),
                action: "answer",
                rationale: "Use the retrieved observation",
                evidence_node_ids: &[evidence],
                outcome: "answered",
            },
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
        let parent = record_decision_in(
            &mut store,
            "default",
            DecisionInput {
                session_id: "session-a",
                trace_id: Some("trace-a"),
                parent_step_id: None,
                action: "observe",
                rationale: "first step",
                evidence_node_ids: &[],
                outcome: "done",
            },
        )
        .unwrap();
        let before = store.node_count();
        assert!(record_decision_in(
            &mut store,
            "default",
            DecisionInput {
                session_id: "session-a",
                trace_id: Some("trace-a"),
                parent_step_id: None,
                action: "answer",
                rationale: "missing evidence",
                evidence_node_ids: &[NodeId::new(999_999)],
                outcome: "done",
            },
        )
        .unwrap_err()
        .contains("evidence node"));
        assert!(record_decision_in(
            &mut store,
            "default",
            DecisionInput {
                session_id: "session-a",
                trace_id: Some("trace-b"),
                parent_step_id: Some(parent.node_id),
                action: "answer",
                rationale: "wrong trace",
                evidence_node_ids: &[],
                outcome: "done",
            },
        )
        .unwrap_err()
        .contains("same trace"));
        assert_eq!(store.node_count(), before);
    }

    #[tokio::test]
    async fn record_decision_writes_through_the_writer_as_one_transaction() {
        let store = std::sync::Arc::new(tokio::sync::RwLock::new(GraphStore::new()));
        let writer = PersistedWriter::in_memory(store.clone());
        let step = record_decision(
            &writer,
            "default",
            DecisionInput {
                session_id: "session-a",
                trace_id: Some("trace-a"),
                parent_step_id: None,
                action: "observe",
                rationale: "first step",
                evidence_node_ids: &[],
                outcome: "done",
            },
        )
        .await
        .unwrap();
        let g = store.read().await;
        assert!(
            g.session_transaction_version().is_none(),
            "the transaction was left open"
        );
        assert_eq!(inspect_trace(&g, "trace-a").steps, vec![step]);
        drop(g);

        let err = record_decision(
            &writer,
            "default",
            DecisionInput {
                session_id: "session-a",
                trace_id: Some("trace-a"),
                parent_step_id: None,
                action: "answer",
                rationale: "missing evidence",
                evidence_node_ids: &[NodeId::new(999_999)],
                outcome: "done",
            },
        )
        .await
        .unwrap_err();
        assert!(err.contains("evidence node"), "{err}");
        assert_eq!(store.read().await.node_count(), 1);
    }

    #[test]
    fn creates_a_unique_trace_when_none_is_supplied() {
        let mut store = GraphStore::new();
        let step = record_decision_in(
            &mut store,
            "default",
            DecisionInput {
                session_id: "session-a",
                trace_id: None,
                parent_step_id: None,
                action: "inspect",
                rationale: "start trace",
                evidence_node_ids: &[],
                outcome: "done",
            },
        )
        .unwrap();
        assert!(!step.trace_id.is_empty());
        assert_eq!(inspect_trace(&store, &step.trace_id).steps, vec![step]);
    }
}
