//! Plan executor — runs a `ToolPlan` against registered `Tool`s, writing
//! one `(:Question)-[:USED_TOOL]->(:Tool)` edge per call with timing and
//! cost telemetry. The same Question node is reused across runs sharing
//! the same prompt (qid = sha256(prompt) prefix).

use crate::agent::planner::{PlanRunResult, ToolCall, ToolCallRecord, ToolPlan};
use crate::agent::{AgentError, AgentResult, Tool};
use crate::graph::{GraphStore, Label, NodeId, PropertyValue};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Owns the registered tools + a graph handle for telemetry writes.
pub struct PlanExecutor {
    tools: HashMap<String, Arc<dyn Tool>>,
    store: Arc<RwLock<GraphStore>>,
}

impl PlanExecutor {
    pub fn new(
        tools: HashMap<String, Arc<dyn Tool>>,
        store: Arc<RwLock<GraphStore>>,
    ) -> Self {
        Self { tools, store }
    }

    /// Stable identifier for a question — sha256 prefix, hex-encoded.
    pub fn question_id(prompt: &str) -> String {
        let digest = Sha256::digest(prompt.as_bytes());
        hex_prefix(&digest, 16)
    }

    /// Execute a plan. Each parallel group runs concurrently; groups
    /// run sequentially. After execution, telemetry edges are written
    /// in plan order under a single write lock.
    pub async fn execute(
        &self,
        prompt: &str,
        plan: &ToolPlan,
    ) -> AgentResult<PlanRunResult> {
        let qid = Self::question_id(prompt);
        let groups = plan.parallel_groups();
        let mut records: Vec<Option<ToolCallRecord>> = vec![None; plan.calls.len()];

        let t0 = Instant::now();
        for group in &groups {
            // Run group concurrently. Each future returns (idx, ToolCallRecord).
            let mut futures = Vec::with_capacity(group.len());
            for &idx in group {
                let call = plan.calls[idx].clone();
                let tool = self.tools.get(&call.tool).cloned();
                futures.push(async move {
                    let rec = run_one(idx, call, tool).await;
                    (idx, rec)
                });
            }
            let results = futures::future::join_all(futures).await;
            for (idx, rec) in results {
                records[idx] = Some(rec);
            }
        }
        let total_latency_ms = t0.elapsed().as_millis() as u64;

        let records: Vec<ToolCallRecord> = records
            .into_iter()
            .map(|r| r.expect("every slot populated by join_all"))
            .collect();
        let total_token_cost: u64 = records.iter().map(|r| r.token_cost).sum();

        // Write telemetry. Holds a single write lock for atomicity within a run.
        write_telemetry(&self.store, &qid, prompt, &records).await?;

        Ok(PlanRunResult {
            question_id: qid,
            records,
            total_latency_ms,
            total_token_cost,
        })
    }
}

async fn run_one(
    _idx: usize,
    call: ToolCall,
    tool: Option<Arc<dyn Tool>>,
) -> ToolCallRecord {
    let started = Instant::now();
    let args = call.args.clone();
    let (result, error, hit_rate) = match tool {
        None => (
            None,
            Some(format!("tool '{}' not registered", call.tool)),
            0.0,
        ),
        Some(t) => match t.execute(args.clone()).await {
            Ok(v) => {
                let h = score_hit_rate(&v);
                (Some(v), None, h)
            }
            Err(e) => (None, Some(e.to_string()), 0.0),
        },
    };
    let latency_ms = started.elapsed().as_millis() as u64;
    let token_cost = estimate_tokens(&args, result.as_ref());
    ToolCallRecord {
        tool: call.tool,
        args,
        latency_ms,
        token_cost,
        hit_rate,
        result,
        error,
    }
}

/// Heuristic 0..1 score: 1.0 for non-empty Ok, 0.5 for empty Ok, 0.0 for
/// errors. Refine when AGE has a downstream answer-synthesizer that can
/// signal whether the tool's output was actually used.
fn score_hit_rate(v: &serde_json::Value) -> f64 {
    use serde_json::Value;
    match v {
        Value::Null => 0.0,
        Value::Bool(_) | Value::Number(_) => 1.0,
        Value::String(s) => if s.is_empty() { 0.5 } else { 1.0 },
        Value::Array(a) => if a.is_empty() { 0.5 } else { 1.0 },
        Value::Object(o) => {
            // Common shape: {"records": [...], ...} — empty records = miss.
            if let Some(Value::Array(records)) = o.get("records") {
                return if records.is_empty() { 0.5 } else { 1.0 };
            }
            if o.is_empty() { 0.5 } else { 1.0 }
        }
    }
}

/// Coarse byte→token estimate (4 bytes/token is industry rule of thumb
/// for English text; close enough for telemetry).
fn estimate_tokens(args: &serde_json::Value, result: Option<&serde_json::Value>) -> u64 {
    let mut bytes = serde_json::to_vec(args).map(|v| v.len()).unwrap_or(0);
    if let Some(r) = result {
        bytes += serde_json::to_vec(r).map(|v| v.len()).unwrap_or(0);
    }
    ((bytes as f64) / 4.0).ceil() as u64
}

async fn write_telemetry(
    store: &RwLock<GraphStore>,
    qid: &str,
    prompt: &str,
    records: &[ToolCallRecord],
) -> AgentResult<()> {
    let mut guard = store.write().await;

    // Find-or-create Question node by qid.
    let q_node = find_node_by_property(&guard, "Question", "qid", qid)
        .unwrap_or_else(|| {
            let nid = guard.create_node("Question");
            if let Some(node) = guard.get_node_mut(nid) {
                node.set_property("qid", qid);
                node.set_property("text", prompt);
            }
            nid
        });

    // Find-or-create one Tool node per distinct tool referenced.
    let mut tool_nodes: HashMap<String, NodeId> = HashMap::new();
    for rec in records {
        if tool_nodes.contains_key(&rec.tool) { continue; }
        let nid = find_node_by_property(&guard, "Tool", "tid", &rec.tool)
            .unwrap_or_else(|| {
                let nid = guard.create_node("Tool");
                if let Some(node) = guard.get_node_mut(nid) {
                    node.set_property("tid", rec.tool.as_str());
                }
                nid
            });
        tool_nodes.insert(rec.tool.clone(), nid);
    }

    // Append one USED_TOOL edge per call, in plan order.
    let now_ms = chrono::Utc::now().timestamp_millis();
    for (slot, rec) in records.iter().enumerate() {
        let tnode = tool_nodes[&rec.tool];
        let eid = guard
            .create_edge(q_node, tnode, "USED_TOOL")
            .map_err(|e| AgentError::ExecutionError(format!("create_edge: {e}")))?;
        guard.set_edge_property(eid, "latency_ms", rec.latency_ms as i64)
            .map_err(|e| AgentError::ExecutionError(format!("set_edge_property: {e}")))?;
        guard.set_edge_property(eid, "token_cost", rec.token_cost as i64)
            .map_err(|e| AgentError::ExecutionError(format!("set_edge_property: {e}")))?;
        guard.set_edge_property(eid, "hit_rate", rec.hit_rate)
            .map_err(|e| AgentError::ExecutionError(format!("set_edge_property: {e}")))?;
        guard.set_edge_property(eid, "slot", slot as i64)
            .map_err(|e| AgentError::ExecutionError(format!("set_edge_property: {e}")))?;
        guard.set_edge_property(eid, "ts_ms", now_ms)
            .map_err(|e| AgentError::ExecutionError(format!("set_edge_property: {e}")))?;
        if let Some(err) = &rec.error {
            guard.set_edge_property(eid, "error", err.as_str())
                .map_err(|e| AgentError::ExecutionError(format!("set_edge_property: {e}")))?;
        }
    }

    Ok(())
}

fn find_node_by_property(
    store: &GraphStore,
    label: &str,
    key: &str,
    value: &str,
) -> Option<NodeId> {
    let lbl = Label::new(label);
    for node in store.get_nodes_by_label(&lbl) {
        if let Some(PropertyValue::String(s)) = node.get_property(key) {
            if s == value { return Some(node.id); }
        }
    }
    None
}

fn hex_prefix(bytes: &[u8], n_chars: usize) -> String {
    let mut s = String::with_capacity(n_chars);
    for b in bytes {
        if s.len() >= n_chars { break; }
        s.push_str(&format!("{:02x}", b));
    }
    s.truncate(n_chars);
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn question_id_is_stable() {
        let a = PlanExecutor::question_id("hello");
        let b = PlanExecutor::question_id("hello");
        assert_eq!(a, b);
        assert_ne!(a, PlanExecutor::question_id("world"));
        assert_eq!(a.len(), 16);
    }

    #[test]
    fn hit_rate_heuristic_distinguishes_outcomes() {
        assert_eq!(score_hit_rate(&json!(null)), 0.0);
        assert_eq!(score_hit_rate(&json!([])), 0.5);
        assert_eq!(score_hit_rate(&json!([1, 2, 3])), 1.0);
        assert_eq!(score_hit_rate(&json!({"records": []})), 0.5);
        assert_eq!(score_hit_rate(&json!({"records": [["x"]]})), 1.0);
    }

    #[test]
    fn token_estimate_is_proportional_to_payload() {
        let small = estimate_tokens(&json!({"q": "x"}), Some(&json!("y")));
        let large = estimate_tokens(
            &json!({"q": "x".repeat(400)}),
            Some(&json!("y".repeat(400))),
        );
        assert!(large > small * 10, "expected large>>small, got {small} vs {large}");
    }
}
