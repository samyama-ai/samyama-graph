//! Agentic Enrichment
//!
//! Implements agents that can use tools to enrich the graph.

pub mod executor;
pub mod planner;
pub mod tools;

use crate::graph::GraphStore;
use crate::nlq::client::NLQClient;
use crate::persistence::tenant::{AgentConfig, NLQConfig};
use async_trait::async_trait;
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
    pub async fn execute_plan(
        &self,
        prompt: &str,
        plan: &ToolPlan,
    ) -> AgentResult<PlanRunResult> {
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
        let planner_prompt = format!(
            "You are a tool-call planner. Given the user prompt and a catalogue of tools, \
output ONLY a JSON plan matching `{{\"calls\": [{{\"tool\": string, \"args\": object, \"parallel_with_prev\": bool}}]}}`. \
Do not repeat a tool in a plan. Choose parallel_with_prev=true only when the call does not depend on the previous one.\n\n\
Tools: {}\n\nContext: {}\n\nPrompt: {}",
            serde_json::to_string(&tool_catalog).unwrap_or_default(),
            context,
            prompt,
        );

        let raw = client
            .generate_cypher(&planner_prompt)
            .await
            .map_err(|e| AgentError::LLMError(e.to_string()))?;
        let plan = ToolPlan::from_llm_json(&raw)?;
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
}
