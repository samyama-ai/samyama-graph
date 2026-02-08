//! Agentic Enrichment
//!
//! Implements agents that can use tools to enrich the graph.

pub mod tools;

use crate::persistence::tenant::{AgentConfig, NLQConfig};
use crate::nlq::client::NLQClient;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use async_trait::async_trait;

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
}

impl AgentRuntime {
    pub fn new(config: AgentConfig) -> Self {
        Self {
            config,
            tools: HashMap::new(),
        }
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
        let client = NLQClient::new(&nlq_config)
            .map_err(|e| AgentError::ConfigError(e.to_string()))?;
        let response = client.generate_cypher(prompt).await
            .map_err(|e| AgentError::LLMError(e.to_string()))?;
        Ok(response)
    }
}
