use samyama::agent::{AgentRuntime, tools::WebSearchTool};
use samyama::persistence::tenant::{AgentConfig, LLMProvider};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("--- Samyama Agentic Enrichment Demo ---");

    // 1. Configure Agent
    let agent_config = AgentConfig {
        enabled: true,
        provider: LLMProvider::Gemini,
        model: "gemini-2.0-flash-exp".to_string(),
        api_key: std::env::var("GEMINI_API_KEY").ok(),
        api_base_url: None,
        system_prompt: None,
        tools: vec![],
        policies: std::collections::HashMap::new(),
    };

    let mut runtime = AgentRuntime::new(agent_config);

    // 2. Register Web Search Tool
    // Using a placeholder API key for the tool since it's mocked
    runtime.register_tool(Arc::new(WebSearchTool::new("mock-key".to_string())));

    // 3. Simulate Trigger (e.g., Node Created: Company {name: "Samyama AI"})
    println!("\nTriggering agent for node context: Company {{name: 'Samyama AI'}}");
    let context = "Node: Company { name: 'Samyama AI', industry: 'Technology' }";
    let task = "Find the latest news about this company and update the 'news' property.";

    match runtime.process_trigger(task, context).await {
        Ok(result) => println!("Agent Result: {}", result),
        Err(e) => println!("Agent Error: {}", e),
    }
}
