use samyama::{GraphStore, PropertyValue, PersistenceManager, ServerConfig, RespServer};
use samyama::persistence::tenant::{AutoEmbedConfig, LLMProvider, ResourceQuotas};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Duration;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    println!("--- Samyama Auto-Embed Demo ---");

    // 1. Initialize Persistence and Tenant
    let temp_dir = tempfile::TempDir::new().unwrap();
    let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
    
    let tenant_id = "ai_tenant";
    persistence.tenants().create_tenant(
        tenant_id.to_string(), 
        "AI Research Tenant".to_string(), 
        None
    ).unwrap();

    // 2. Configure Auto-Embed
    // Using Gemini as requested
    let api_key = std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY environment variable not set");
    
    let embed_config = AutoEmbedConfig {
        provider: LLMProvider::Gemini,
        embedding_model: "text-embedding-004".to_string(), 
        api_key: Some(api_key),
        api_base_url: None, // Will use default Gemini URL
        chunk_size: 100,
        chunk_overlap: 10,
        vector_dimension: 768, // Gemini text-embedding-004 dimension
        embedding_policies: HashMap::from([
            ("Document".to_string(), vec!["content".to_string()])
        ]),
    };

    persistence.tenants().update_embed_config(tenant_id, Some(embed_config)).unwrap();
    println!("Auto-Embed configured for tenant: {} using Gemini", tenant_id);

    // 3. Initialize GraphStore with Async Indexing
    let (mut graph, rx) = GraphStore::with_async_indexing();
    
    // Create vector index for the expected output (768 dimensions for Gemini)
    graph.create_vector_index("Document", "content", 768, samyama::vector::DistanceMetric::Cosine).unwrap();
    
    let store = Arc::new(RwLock::new(graph));
    
    // Start background indexer
    persistence.start_indexer(&*store.read().await, rx);

    // 4. Trigger Auto-Embed via property update
    println!("Creating document node...");
    let doc_id = {
        let mut g = store.write().await;
        g.create_node_with_properties(
            tenant_id,
            vec![samyama::graph::Label::new("Document")],
            HashMap::new()
        )
    };

    println!("Setting 'content' property (this should trigger background embedding)...");
    {
        let mut g = store.write().await;
        g.set_node_property(
            tenant_id,
            doc_id,
            "content",
            "Samyama is a high-performance graph database with native vector support."
        ).unwrap();
    }

    println!("Waiting for background processing (Gemini)...");
    // Wait a bit for Gemini to process
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 5. Verify embedding exists in vector index
    println!("Verifying vector index...");
    let results = {
        let g = store.read().await;
        // Search for similar content
        g.vector_search("Document", "content", &vec![0.1; 768], 1)
    };

    match results {
        Ok(matches) if !matches.is_empty() => {
            println!("✅ Auto-Embed Success! Found {} matches in vector index.", matches.len());
            for (id, score) in matches {
                println!("  - Node ID: {:?}, Score: {:.4}", id, score);
            }
        }
        _ => {
            println!("⚠️ No embeddings found yet. This is expected if Ollama is not running or slow.");
            println!("   Check logs for 'LLM API error' or 'Network error'.");
        }
    }
}
