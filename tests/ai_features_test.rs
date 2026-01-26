use samyama::agent::{AgentRuntime, tools::WebSearchTool, Tool};
use samyama::persistence::tenant::{AgentConfig, LLMProvider, NLQConfig};
use samyama::nlq::NLQPipeline;
use std::sync::Arc;
use serde_json::json;

#[tokio::test]
async fn test_agent_runtime_tool_execution() {
    let mut runtime = AgentRuntime::new(AgentConfig {
        enabled: true,
        provider: LLMProvider::Mock, // Assuming Mock provider exists or we use dummy
        model: "mock".to_string(),
        api_key: None,
        api_base_url: None,
        system_prompt: None,
        tools: vec![],
        policies: std::collections::HashMap::new(),
    });

    // Register WebSearchTool with "mock" key to trigger mock mode
    runtime.register_tool(Arc::new(WebSearchTool::new("mock".to_string())));

    // Simulate LLM calling the tool (skipping LLM part for unit test, directly testing tool via runtime structure if possible, 
    // but runtime.process_trigger calls LLM. So we can't easily test process_trigger without mocking the LLM call inside runtime.)
    
    // Instead, let's test the Tool directly.
    let tool = WebSearchTool::new("mock".to_string());
    let args = json!({ "query": "Samyama Graph" });
    let result = tool.execute(args).await;
    
    assert!(result.is_ok());
    let value = result.unwrap();
    assert!(value.get("results").is_some());
}

#[tokio::test]
async fn test_nlq_pipeline_mock() {
    let config = NLQConfig {
        enabled: true,
        provider: LLMProvider::Mock,
        model: "mock".to_string(),
        api_key: Some("mock".to_string()),
        api_base_url: None,
        system_prompt: None,
    };

    let pipeline = NLQPipeline::new(config).unwrap();
    let cypher = pipeline.text_to_cypher("Show me nodes", "").await;

    assert!(cypher.is_ok());
    assert_eq!(cypher.unwrap(), "MATCH (n) RETURN n LIMIT 10");
}
