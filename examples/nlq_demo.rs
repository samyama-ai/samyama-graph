use samyama::{GraphStore, PropertyValue, PersistenceManager, NLQConfig, LLMProvider, NLQPipeline};
use samyama::persistence::tenant::AutoEmbedConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    println!("--- Samyama NLQ Demo ---");

    // 1. Setup Tenant with NLQ Config
    let temp_dir = tempfile::TempDir::new().unwrap();
    let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
    let tenant_id = "nlq_tenant";
    persistence.tenants().create_tenant(tenant_id.to_string(), "NLQ Tenant".to_string(), None).unwrap();

    let api_key = std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY environment variable not set");

    let nlq_config = NLQConfig {
        enabled: true,
        provider: LLMProvider::Gemini,
        model: "gemini-2.5-flash".to_string(),
        api_key: Some(api_key),
        api_base_url: None,
        system_prompt: None,
    };

    persistence.tenants().update_nlq_config(tenant_id, Some(nlq_config.clone())).unwrap();

    // 2. Initialize Pipeline
    let pipeline = NLQPipeline::new(nlq_config).unwrap();

    // 3. Define Schema Summary (In real app, this would be auto-generated from stats)
    let schema_summary = "
    Nodes:
    - Person { name: String, age: Integer, city: String }
    - Movie { title: String, released: Integer }
    
    Edges:
    - (:Person)-[:KNOWS]->(:Person) { since: Integer }
    - (:Person)-[:LIKES]->(:Movie) { rating: Float }
    ";

    // 4. Test Queries
    let questions = vec![
        "Who knows Alice?",
        "What movies did Bob like?",
        "Find friends of friends of Charlie.",
        "List all people in New York older than 30."
    ];

    for q in questions {
        println!("\nUser: {}", q);
        match pipeline.text_to_cypher(q, schema_summary).await {
            Ok(cypher) => println!("Cypher: {}", cypher),
            Err(e) => println!("Error: {}", e),
        }
    }
}
