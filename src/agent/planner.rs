//! Tool-call planning data structures and LLM-output parsing.
//!
//! A plan is a flat sequence of tool calls; consecutive calls flagged
//! `parallel_with_prev = true` form a parallel group. The executor
//! (see `super::executor`) realises this with `tokio::join_all` per group.

use crate::agent::AgentError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One step in a plan.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub tool: String,
    #[serde(default)]
    pub args: Value,
    /// If true, this call runs concurrently with the previous call (and
    /// any earlier consecutive `parallel_with_prev` calls in the same
    /// group). The first call in a plan ignores this flag.
    #[serde(default)]
    pub parallel_with_prev: bool,
}

/// A complete plan — a flat ordered list of tool calls.
#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct ToolPlan {
    pub calls: Vec<ToolCall>,
}

impl ToolPlan {
    /// Parse an LLM-emitted plan. Accepts either:
    ///  - bare JSON `{"calls": [...]}` or `[...]`
    ///  - JSON wrapped in ```json ... ``` markdown fences (LLM noise)
    pub fn from_llm_json(s: &str) -> Result<Self, AgentError> {
        let cleaned = strip_fences(s);
        // Try object form first, then array form.
        if let Ok(plan) = serde_json::from_str::<ToolPlan>(&cleaned) {
            return Ok(plan);
        }
        if let Ok(calls) = serde_json::from_str::<Vec<ToolCall>>(&cleaned) {
            return Ok(ToolPlan { calls });
        }
        Err(AgentError::ExecutionError(format!(
            "could not parse plan as ToolPlan or [ToolCall]; first 200 chars: {:?}",
            &cleaned.chars().take(200).collect::<String>()
        )))
    }

    /// Group consecutive `parallel_with_prev` calls together. Returned as
    /// `Vec<Vec<index>>` of original positions.
    pub fn parallel_groups(&self) -> Vec<Vec<usize>> {
        let mut groups: Vec<Vec<usize>> = Vec::new();
        for (i, call) in self.calls.iter().enumerate() {
            if i == 0 || !call.parallel_with_prev {
                groups.push(vec![i]);
            } else {
                groups.last_mut().unwrap().push(i);
            }
        }
        groups
    }
}

/// Telemetry record for a single tool call (one per plan slot, in plan order).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub tool: String,
    pub args: Value,
    pub latency_ms: u64,
    pub token_cost: u64,
    pub hit_rate: f64,
    pub result: Option<Value>,
    pub error: Option<String>,
}

/// Result of executing a plan end-to-end.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanRunResult {
    pub question_id: String,
    pub records: Vec<ToolCallRecord>,
    /// Total wall-clock latency including parallel overlap.
    pub total_latency_ms: u64,
    /// Sum of per-call token cost estimates.
    pub total_token_cost: u64,
}

fn strip_fences(s: &str) -> String {
    let trimmed = s.trim();
    if let Some(rest) = trimmed.strip_prefix("```json") {
        return rest.trim_start().trim_end_matches("```").trim().to_string();
    }
    if let Some(rest) = trimmed.strip_prefix("```") {
        return rest.trim_start().trim_end_matches("```").trim().to_string();
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_object_form() {
        let s = r#"{"calls": [{"tool": "cypher", "args": {"q": "MATCH (n) RETURN n"}}]}"#;
        let p = ToolPlan::from_llm_json(s).unwrap();
        assert_eq!(p.calls.len(), 1);
        assert_eq!(p.calls[0].tool, "cypher");
    }

    #[test]
    fn parses_array_form() {
        let s = r#"[{"tool": "a"}, {"tool": "b", "parallel_with_prev": true}]"#;
        let p = ToolPlan::from_llm_json(s).unwrap();
        assert_eq!(p.calls.len(), 2);
        assert!(p.calls[1].parallel_with_prev);
    }

    #[test]
    fn strips_markdown_fences() {
        let s = "```json\n[{\"tool\": \"x\"}]\n```";
        let p = ToolPlan::from_llm_json(s).unwrap();
        assert_eq!(p.calls[0].tool, "x");
    }

    #[test]
    fn parallel_groups_partitions_correctly() {
        let plan = ToolPlan {
            calls: vec![
                ToolCall { tool: "a".into(), args: json!(null), parallel_with_prev: false },
                ToolCall { tool: "b".into(), args: json!(null), parallel_with_prev: true },
                ToolCall { tool: "c".into(), args: json!(null), parallel_with_prev: true },
                ToolCall { tool: "d".into(), args: json!(null), parallel_with_prev: false },
            ],
        };
        let groups = plan.parallel_groups();
        assert_eq!(groups, vec![vec![0, 1, 2], vec![3]]);
    }

    #[test]
    fn rejects_garbage() {
        assert!(ToolPlan::from_llm_json("not json").is_err());
    }
}
