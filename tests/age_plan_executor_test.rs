//! AGE plan-executor contract tests.
//!
//! Wiring UC5 into the agent runtime: register a real CypherTool, run a
//! ToolPlan with sequential and parallel calls, verify that
//! (:Question)-[:USED_TOOL]->(:Tool) telemetry edges land in the store
//! with latency_ms, token_cost, hit_rate, slot, ts_ms.

use samyama::agent::tools::CypherTool;
use samyama::agent::{AgentRuntime, ToolCall, ToolPlan};
use samyama::graph::{GraphStore, Label};
use samyama::persistence::tenant::{AgentConfig, LLMProvider};
use samyama::query::QueryEngine;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

fn mock_agent_config() -> AgentConfig {
    AgentConfig {
        enabled: true,
        provider: LLMProvider::Mock,
        model: "mock".into(),
        api_key: None,
        api_base_url: None,
        system_prompt: None,
        tools: vec![],
        policies: std::collections::HashMap::new(),
    }
}

async fn new_runtime_with_fixture() -> (Arc<RwLock<GraphStore>>, AgentRuntime) {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    {
        let mut g = store.write().await;
        for name in ["Alice", "Bob", "Charlie"] {
            let nid = g.create_node("Person");
            if let Some(n) = g.get_node_mut(nid) {
                n.set_property("name", name);
            }
        }
    }
    let engine = Arc::new(QueryEngine::new());
    let mut rt = AgentRuntime::new(mock_agent_config()).with_store(store.clone());
    rt.register_tool(Arc::new(CypherTool::new(engine, store.clone())));
    (store, rt)
}

#[tokio::test]
async fn executes_sequential_plan_and_writes_telemetry() {
    let (store, rt) = new_runtime_with_fixture().await;
    let prompt = "count people";
    let plan = ToolPlan {
        calls: vec![
            ToolCall {
                tool: "cypher".into(),
                args: json!({"query": "MATCH (n:Person) RETURN count(n) AS c"}),
                parallel_with_prev: false,
            },
            ToolCall {
                tool: "cypher".into(),
                args: json!({"query": "MATCH (n:Person) RETURN n.name AS name"}),
                parallel_with_prev: false,
            },
        ],
    };

    let result = rt.execute_plan(prompt, &plan).await.expect("execute");

    assert_eq!(result.records.len(), 2);
    for rec in &result.records {
        assert_eq!(rec.tool, "cypher");
        assert!(rec.error.is_none(), "unexpected error: {:?}", rec.error);
        assert_eq!(rec.hit_rate, 1.0, "non-empty cypher result should score 1.0");
        assert!(rec.token_cost > 0);
    }

    // Telemetry edges are in the graph: 1 Question, 1 Tool, 2 USED_TOOL edges
    // sharing the same Question (same prompt).
    let g = store.read().await;
    let questions = g.get_nodes_by_label(&Label::new("Question"));
    let tools = g.get_nodes_by_label(&Label::new("Tool"));
    assert_eq!(questions.len(), 1, "expected 1 Question node");
    assert_eq!(tools.len(), 1, "expected 1 Tool node (cypher)");

    let edges = g.get_edges_by_type(&samyama::graph::EdgeType::new("USED_TOOL"));
    assert_eq!(edges.len(), 2, "expected 2 USED_TOOL edges");
    for e in &edges {
        assert!(e.get_property("latency_ms").is_some());
        assert!(e.get_property("token_cost").is_some());
        assert!(e.get_property("hit_rate").is_some());
        assert!(e.get_property("slot").is_some());
        assert!(e.get_property("ts_ms").is_some());
    }
}

#[tokio::test]
async fn parallel_group_shares_wall_time() {
    let (_store, rt) = new_runtime_with_fixture().await;
    let plan = ToolPlan {
        calls: vec![
            ToolCall {
                tool: "cypher".into(),
                args: json!({"query": "MATCH (n:Person) RETURN count(n)"}),
                parallel_with_prev: false,
            },
            ToolCall {
                tool: "cypher".into(),
                args: json!({"query": "MATCH (n:Person) RETURN count(n)"}),
                parallel_with_prev: true,
            },
            ToolCall {
                tool: "cypher".into(),
                args: json!({"query": "MATCH (n:Person) RETURN count(n)"}),
                parallel_with_prev: true,
            },
        ],
    };
    let r = rt.execute_plan("parallel probe", &plan).await.unwrap();
    let sum: u64 = r.records.iter().map(|x| x.latency_ms).sum();
    // Parallel total wall should be < sum of individual latencies
    // for a 3-way parallel group. We accept equality on very fast runs
    // (everything rounds to 0 ms) — the important property is <=.
    assert!(
        r.total_latency_ms <= sum,
        "parallel wall {} should be <= serial sum {}",
        r.total_latency_ms,
        sum
    );
}

#[tokio::test]
async fn same_prompt_reuses_question_node() {
    let (store, rt) = new_runtime_with_fixture().await;
    let plan = ToolPlan {
        calls: vec![ToolCall {
            tool: "cypher".into(),
            args: json!({"query": "MATCH (n:Person) RETURN count(n)"}),
            parallel_with_prev: false,
        }],
    };
    rt.execute_plan("identical", &plan).await.unwrap();
    rt.execute_plan("identical", &plan).await.unwrap();
    rt.execute_plan("different", &plan).await.unwrap();

    let g = store.read().await;
    let questions = g.get_nodes_by_label(&Label::new("Question"));
    assert_eq!(
        questions.len(),
        2,
        "expected 2 distinct Question nodes (identical dedups, different is new)"
    );
    let edges = g.get_edges_by_type(&samyama::graph::EdgeType::new("USED_TOOL"));
    assert_eq!(edges.len(), 3);
}

#[tokio::test]
async fn unknown_tool_records_error_without_panicking() {
    let (store, rt) = new_runtime_with_fixture().await;
    let plan = ToolPlan {
        calls: vec![ToolCall {
            tool: "not_registered".into(),
            args: json!({}),
            parallel_with_prev: false,
        }],
    };
    let r = rt.execute_plan("bad tool", &plan).await.unwrap();
    assert_eq!(r.records.len(), 1);
    assert!(r.records[0].error.is_some());
    assert_eq!(r.records[0].hit_rate, 0.0);

    let g = store.read().await;
    let edges = g.get_edges_by_type(&samyama::graph::EdgeType::new("USED_TOOL"));
    assert_eq!(edges.len(), 1);
    assert!(edges[0].get_property("error").is_some());
}

#[tokio::test]
async fn runtime_without_store_refuses_execute_plan() {
    let engine = Arc::new(QueryEngine::new());
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let mut rt = AgentRuntime::new(mock_agent_config());
    rt.register_tool(Arc::new(CypherTool::new(engine, store)));
    let plan = ToolPlan {
        calls: vec![ToolCall {
            tool: "cypher".into(),
            args: json!({"query": "MATCH (n) RETURN n"}),
            parallel_with_prev: false,
        }],
    };
    let err = rt.execute_plan("no store", &plan).await.unwrap_err();
    assert!(format!("{err}").contains("no store"));
}
