//! Agentic Enrichment
//!
//! Implements agents that can use tools to enrich the graph.

pub mod enrich;
pub mod executor;
pub mod planner;
pub mod signature;
pub mod tools;

use crate::graph::GraphStore;
use crate::nlq::client::NLQClient;
use crate::persistence::tenant::{AgentConfig, NLQConfig};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

pub use executor::PlanExecutor;
pub use planner::{PlanRunResult, ToolCall, ToolCallRecord, ToolPlan};

#[derive(Error, Debug)]
pub enum AgentError {
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Tool error: {0}")]
    ToolError(String),
    #[error("LLM error: {0}")]
    LLMError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}

pub type AgentResult<T> = Result<T, AgentError>;

#[derive(Deserialize)]
struct StructuredToolPlan {
    calls: Vec<StructuredToolCall>,
}

#[derive(Deserialize)]
struct StructuredToolCall {
    tool: String,
    args_json: String,
    parallel_with_prev: bool,
}

/// Trait for agent tools
#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters(&self) -> Value;
    async fn execute(&self, args: Value) -> AgentResult<Value>;
}

/// Runtime for executing agents
pub struct AgentRuntime {
    config: AgentConfig,
    tools: HashMap<String, Arc<dyn Tool>>,
    /// Optional graph store for telemetry writes from plan execution.
    /// When set, `execute_plan` / `plan_and_execute` record each tool
    /// call as a `(:Question)-[:USED_TOOL]->(:Tool)` edge.
    store: Option<Arc<RwLock<GraphStore>>>,
}

impl AgentRuntime {
    pub fn new(config: AgentConfig) -> Self {
        Self {
            config,
            tools: HashMap::new(),
            store: None,
        }
    }

    /// Attach a graph store handle so plan execution emits telemetry edges.
    pub fn with_store(mut self, store: Arc<RwLock<GraphStore>>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn register_tool(&mut self, tool: Arc<dyn Tool>) {
        self.tools.insert(tool.name().to_string(), tool);
    }

    /// Convert AgentConfig to NLQConfig for reusing the NLQ client
    fn to_nlq_config(config: &AgentConfig) -> NLQConfig {
        NLQConfig {
            enabled: config.enabled,
            provider: config.provider.clone(),
            model: config.model.clone(),
            api_key: config.api_key.clone(),
            api_base_url: config.api_base_url.clone(),
            system_prompt: config.system_prompt.clone(),
        }
    }

    /// Process a trigger (e.g., "Enrich Company node X")
    pub async fn process_trigger(&self, prompt: &str, _context: &str) -> AgentResult<String> {
        let nlq_config = Self::to_nlq_config(&self.config);
        let client =
            NLQClient::new(&nlq_config).map_err(|e| AgentError::ConfigError(e.to_string()))?;
        let response = client
            .generate_cypher(prompt)
            .await
            .map_err(|e| AgentError::LLMError(e.to_string()))?;
        Ok(response)
    }

    /// Execute a pre-built plan against the registered tools, writing
    /// telemetry edges to the attached store.
    pub async fn execute_plan(&self, prompt: &str, plan: &ToolPlan) -> AgentResult<PlanRunResult> {
        let store = self.store.as_ref().ok_or_else(|| {
            AgentError::ConfigError(
                "AgentRuntime has no store; call with_store() before execute_plan".into(),
            )
        })?;
        let exec = PlanExecutor::new(self.tools.clone(), store.clone());
        exec.execute(prompt, plan).await
    }

    /// Ask the configured LLM to emit a plan given the available tools
    /// and then execute it, recording telemetry.
    pub async fn plan_and_execute(
        &self,
        prompt: &str,
        context: &str,
    ) -> AgentResult<PlanRunResult> {
        let nlq_config = Self::to_nlq_config(&self.config);
        let client =
            NLQClient::new(&nlq_config).map_err(|e| AgentError::ConfigError(e.to_string()))?;

        let tool_catalog: Vec<Value> = self
            .tools
            .values()
            .map(|t| {
                serde_json::json!({
                    "name": t.name(),
                    "description": t.description(),
                    "parameters": t.parameters(),
                })
            })
            .collect();
        let allowed_tools = tool_catalog
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_owned))
            .collect();
        let signature = signature::SignatureSpec {
            id: "smriti.tool_planner.v1",
            instruction: "Choose registered tools for the user request. Return each call's arguments as a JSON object encoded in args_json. Use parallel_with_prev only when independent of the previous call. Do not repeat a tool.".into(),
            input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({
                "type":"object",
                "properties":{"calls":{"type":"array","items":{"type":"object","properties":{
                    "tool":{"type":"string"},
                    "args_json":{"type":"string"},
                    "parallel_with_prev":{"type":"boolean"}
                },"required":["tool","args_json","parallel_with_prev"]}}},
                "required":["calls"]
            }),
            demos: vec![],
            invariants: vec![
                signature::OutputInvariant::RegisteredToolNamesOnly {
                    field: "calls".into(),
                    allowed: allowed_tools,
                },
                signature::OutputInvariant::ValidJsonObjectsInArray {
                    field: "calls".into(),
                    string_field: "args_json".into(),
                },
            ],
        };
        let runner = signature::SignatureRunner::default();
        let structured = runner
            .invoke_without_store::<_, StructuredToolPlan>(
                &client,
                &signature,
                &serde_json::json!({
                    "tools": tool_catalog,
                    "context": context,
                    "prompt": prompt,
                }),
            )
            .await?;
        let calls = structured
            .output
            .calls
            .into_iter()
            .map(|call| {
                let args = serde_json::from_str(&call.args_json).map_err(|error| {
                    AgentError::ExecutionError(format!(
                        "validated tool arguments could not be decoded: {error}"
                    ))
                })?;
                Ok(ToolCall {
                    tool: call.tool,
                    args,
                    parallel_with_prev: call.parallel_with_prev,
                })
            })
            .collect::<AgentResult<Vec<_>>>()?;
        let plan = ToolPlan { calls };
        self.execute_plan(prompt, &plan).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::tenant::LLMProvider;

    fn mock_agent_config() -> AgentConfig {
        AgentConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock-model".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
            tools: vec![],
            policies: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_agent_runtime_new() {
        let config = mock_agent_config();
        let runtime = AgentRuntime::new(config);
        assert!(runtime.tools.is_empty());
    }

    #[test]
    fn test_register_tool() {
        let config = mock_agent_config();
        let mut runtime = AgentRuntime::new(config);

        let tool = Arc::new(tools::WebSearchTool::new("test-key".to_string()));
        runtime.register_tool(tool);
        assert_eq!(runtime.tools.len(), 1);
        assert!(runtime.tools.contains_key("web_search"));
    }

    #[test]
    fn test_to_nlq_config() {
        let config = mock_agent_config();
        let nlq_config = AgentRuntime::to_nlq_config(&config);
        assert!(nlq_config.enabled);
        assert_eq!(nlq_config.provider, LLMProvider::Mock);
        assert_eq!(nlq_config.model, "mock-model");
    }

    #[tokio::test]
    async fn test_process_trigger_mock() {
        let config = mock_agent_config();
        let runtime = AgentRuntime::new(config);
        let result = runtime.process_trigger("Find all persons", "context").await;
        assert!(result.is_ok());
        let cypher = result.unwrap();
        assert!(cypher.contains("MATCH"));
    }

    use crate::nlq::test_http::{dead_base_url, MockHttp};

    fn ollama_config(base: &str) -> AgentConfig {
        AgentConfig {
            provider: LLMProvider::Ollama,
            api_base_url: Some(base.to_string()),
            ..mock_agent_config()
        }
    }

    fn ollama_says(answer: &str) -> (u16, String) {
        (200, serde_json::json!({ "response": answer }).to_string())
    }

    #[tokio::test]
    async fn process_trigger_maps_llm_failures_to_llm_error() {
        let runtime = AgentRuntime::new(ollama_config(&dead_base_url()));
        let err = runtime.process_trigger("p", "c").await.unwrap_err();
        assert!(
            matches!(err, AgentError::LLMError(ref m) if m.contains("Network error")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn execute_plan_without_store_is_a_config_error() {
        let runtime = AgentRuntime::new(mock_agent_config());
        let err = runtime
            .execute_plan("p", &ToolPlan::default())
            .await
            .unwrap_err();
        assert!(
            matches!(err, AgentError::ConfigError(ref m) if m.contains("with_store")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn execute_plan_runs_registered_tools_and_writes_telemetry() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let mut runtime = AgentRuntime::new(mock_agent_config()).with_store(store.clone());
        runtime.register_tool(Arc::new(tools::WebSearchTool::new("k".into())));
        let plan = ToolPlan {
            calls: vec![ToolCall {
                tool: "web_search".into(),
                args: serde_json::json!({"query": "graphs"}),
                parallel_with_prev: false,
            }],
        };
        let run = runtime.execute_plan("find graphs", &plan).await.unwrap();
        assert_eq!(run.records.len(), 1);
        assert!(run.records[0].error.is_none());
        assert!(run.records[0].result.as_ref().unwrap()["results"].is_array());
        assert_eq!(crate::store_lock::read(&store).await.edge_count(), 1);
    }

    #[tokio::test]
    async fn plan_and_execute_sends_tool_catalogue_and_runs_the_returned_plan() {
        let plan = r#"{"calls":[{"tool":"web_search","args_json":"{\"query\":\"samyama\"}","parallel_with_prev":false}]}"#;
        let srv = MockHttp::serve(vec![ollama_says(plan)]);
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let mut runtime = AgentRuntime::new(ollama_config(&srv.base_url)).with_store(store.clone());
        runtime.register_tool(Arc::new(tools::WebSearchTool::new("k".into())));

        let run = runtime
            .plan_and_execute("tell me about samyama", "ctx-123")
            .await
            .unwrap();
        assert_eq!(run.records.len(), 1);
        assert_eq!(run.records[0].tool, "web_search");
        assert_eq!(run.records[0].args, serde_json::json!({"query": "samyama"}));

        let sent = srv.requests()[0].json();
        let prompt = sent["prompt"].as_str().unwrap();
        assert!(
            prompt.contains("\"name\":\"web_search\""),
            "tool catalogue missing: {prompt}"
        );
        assert!(prompt.contains("\"context\":\"ctx-123\""));
        assert!(prompt.contains("\"prompt\":\"tell me about samyama\""));
        assert!(
            sent["format"]["properties"]["calls"]["items"]["properties"]["args_json"].is_object()
        );
    }

    #[tokio::test]
    async fn plan_and_execute_rejects_an_unparseable_plan() {
        let srv = MockHttp::serve(vec![ollama_says("I cannot help with that."); 3]);
        let runtime = AgentRuntime::new(ollama_config(&srv.base_url))
            .with_store(Arc::new(RwLock::new(GraphStore::new())));
        let err = runtime.plan_and_execute("p", "c").await.unwrap_err();
        assert!(
            matches!(err, AgentError::ExecutionError(ref m) if m.contains("failed after 3 attempt(s)")),
            "{err:?}"
        );
        assert_eq!(srv.requests().len(), 3);
    }

    #[tokio::test]
    async fn plan_and_execute_maps_llm_failures_to_llm_error() {
        let runtime = AgentRuntime::new(ollama_config(&dead_base_url()));
        let err = runtime.plan_and_execute("p", "c").await.unwrap_err();
        assert!(matches!(err, AgentError::LLMError(_)), "{err:?}");
    }

    #[test]
    fn agent_error_messages_name_their_kind() {
        assert_eq!(
            AgentError::ConfigError("a".into()).to_string(),
            "Configuration error: a"
        );
        assert_eq!(
            AgentError::ToolError("b".into()).to_string(),
            "Tool error: b"
        );
        assert_eq!(AgentError::LLMError("c".into()).to_string(), "LLM error: c");
        assert_eq!(
            AgentError::ExecutionError("d".into()).to_string(),
            "Execution error: d"
        );
    }
}
