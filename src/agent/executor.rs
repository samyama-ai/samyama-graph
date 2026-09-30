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
        if let Some(PropertyValue::String(s)) = store.node_property(node.id, key) {
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

    #[test]
    fn hit_rate_scores_scalars_strings_and_plain_objects() {
        assert_eq!(score_hit_rate(&json!(true)), 1.0);
        assert_eq!(score_hit_rate(&json!(0)), 1.0);
        assert_eq!(score_hit_rate(&json!("")), 0.5);
        assert_eq!(score_hit_rate(&json!("x")), 1.0);
        assert_eq!(score_hit_rate(&json!({})), 0.5);
        assert_eq!(score_hit_rate(&json!({"a": 1})), 1.0);
        // A non-array `records` field is not the records shape.
        assert_eq!(score_hit_rate(&json!({"records": 3})), 1.0);
    }

    #[test]
    fn token_estimate_without_result_counts_args_only() {
        // `{"q":"x"}` is 9 bytes -> ceil(9/4) = 3.
        assert_eq!(estimate_tokens(&json!({"q": "x"}), None), 3);
        assert_eq!(estimate_tokens(&json!(null), Some(&json!(null))), 2);
    }

    #[test]
    fn hex_prefix_truncates_to_requested_length() {
        assert_eq!(hex_prefix(&[0xab, 0xcd, 0xef], 3), "abc");
        assert_eq!(hex_prefix(&[0x01], 8), "01");
        assert_eq!(hex_prefix(&[], 4), "");
    }

    struct Echo;
    #[async_trait::async_trait]
    impl Tool for Echo {
        fn name(&self) -> &str { "echo" }
        fn description(&self) -> &str { "echo args" }
        fn parameters(&self) -> serde_json::Value { json!({}) }
        async fn execute(&self, args: serde_json::Value) -> AgentResult<serde_json::Value> {
            Ok(json!({"records": [args]}))
        }
    }

    struct Fails;
    #[async_trait::async_trait]
    impl Tool for Fails {
        fn name(&self) -> &str { "fails" }
        fn description(&self) -> &str { "always fails" }
        fn parameters(&self) -> serde_json::Value { json!({}) }
        async fn execute(&self, _args: serde_json::Value) -> AgentResult<serde_json::Value> {
            Err(AgentError::ToolError("nope".into()))
        }
    }

    fn executor() -> (PlanExecutor, Arc<RwLock<GraphStore>>) {
        let mut tools: HashMap<String, Arc<dyn Tool>> = HashMap::new();
        tools.insert("echo".into(), Arc::new(Echo));
        tools.insert("fails".into(), Arc::new(Fails));
        let store = Arc::new(RwLock::new(GraphStore::new()));
        (PlanExecutor::new(tools, store.clone()), store)
    }

    fn call(tool: &str, parallel: bool) -> ToolCall {
        ToolCall { tool: tool.into(), args: json!({"t": tool}), parallel_with_prev: parallel }
    }

    #[tokio::test]
    async fn execute_records_results_errors_and_missing_tools_in_plan_order() {
        let (exec, store) = executor();
        let plan = ToolPlan {
            calls: vec![call("echo", false), call("fails", true), call("ghost", false)],
        };
        let run = exec.execute("what?", &plan).await.unwrap();
        assert_eq!(run.question_id, PlanExecutor::question_id("what?"));
        let tools: Vec<&str> = run.records.iter().map(|r| r.tool.as_str()).collect();
        assert_eq!(tools, vec!["echo", "fails", "ghost"]);

        let echo = &run.records[0];
        assert_eq!(echo.result, Some(json!({"records": [{"t": "echo"}]})));
        assert_eq!(echo.error, None);
        assert_eq!(echo.hit_rate, 1.0);
        assert!(echo.token_cost > 0);

        let fails = &run.records[1];
        assert_eq!(fails.result, None);
        assert_eq!(fails.error.as_deref(), Some("Tool error: nope"));
        assert_eq!(fails.hit_rate, 0.0);

        let ghost = &run.records[2];
        assert_eq!(ghost.error.as_deref(), Some("tool 'ghost' not registered"));
        assert_eq!(run.total_token_cost, run.records.iter().map(|r| r.token_cost).sum::<u64>());

        // Telemetry: one Question, three Tool nodes, one USED_TOOL edge per call.
        let g = store.read().await;
        let q = find_node_by_property(&g, "Question", "qid", &run.question_id).expect("question node");
        assert_eq!(g.node_property(q, "text"), Some(PropertyValue::String("what?".into())));
        assert_eq!(g.get_nodes_by_label(&Label::new("Tool")).len(), 3);
        let mut edges = g.get_outgoing_edges(q);
        edges.sort_by_key(|e| match e.properties.get("slot") {
            Some(PropertyValue::Integer(i)) => *i,
            _ => -1,
        });
        assert_eq!(edges.len(), 3);
        assert!(edges.iter().all(|e| e.edge_type.as_str() == "USED_TOOL"));
        assert_eq!(edges[0].properties.get("error"), None);
        assert_eq!(
            edges[1].properties.get("error"),
            Some(&PropertyValue::String("Tool error: nope".into()))
        );
        assert_eq!(edges[0].properties.get("hit_rate"), Some(&PropertyValue::Float(1.0)));
    }

    #[tokio::test]
    async fn repeated_prompts_reuse_question_and_tool_nodes() {
        let (exec, store) = executor();
        let plan = ToolPlan { calls: vec![call("echo", false), call("echo", false)] };
        exec.execute("same", &plan).await.unwrap();
        exec.execute("same", &plan).await.unwrap();
        let g = store.read().await;
        assert_eq!(g.get_nodes_by_label(&Label::new("Question")).len(), 1);
        assert_eq!(g.get_nodes_by_label(&Label::new("Tool")).len(), 1);
        let q = find_node_by_property(&g, "Question", "qid", &PlanExecutor::question_id("same")).unwrap();
        assert_eq!(g.get_outgoing_edges(q).len(), 4);
    }

    #[tokio::test]
    async fn empty_plan_creates_only_the_question() {
        let (exec, store) = executor();
        let run = exec.execute("nothing", &ToolPlan::default()).await.unwrap();
        assert!(run.records.is_empty());
        assert_eq!(run.total_token_cost, 0);
        let g = store.read().await;
        assert_eq!(g.get_nodes_by_label(&Label::new("Question")).len(), 1);
        assert!(g.get_nodes_by_label(&Label::new("Tool")).is_empty());
        assert!(find_node_by_property(&g, "Question", "qid", "no-such-qid").is_none());
    }

    #[tokio::test]
    async fn distinct_prompts_get_distinct_question_nodes() {
        let (exec, store) = executor();
        assert_eq!(Echo.description(), "echo args");
        assert_eq!(Echo.parameters(), json!({}));
        assert_eq!(Fails.description(), "always fails");
        assert_eq!(Fails.parameters(), json!({}));
        exec.execute("first", &ToolPlan::default()).await.unwrap();
        exec.execute("second", &ToolPlan::default()).await.unwrap();
        exec.execute("second", &ToolPlan::default()).await.unwrap();
        let g = store.read().await;
        assert_eq!(g.get_nodes_by_label(&Label::new("Question")).len(), 2);
        let q1 = find_node_by_property(&g, "Question", "qid", &PlanExecutor::question_id("first"));
        let q2 = find_node_by_property(&g, "Question", "qid", &PlanExecutor::question_id("second"));
        assert!(q1.is_some() && q2.is_some() && q1 != q2);
    }
}
