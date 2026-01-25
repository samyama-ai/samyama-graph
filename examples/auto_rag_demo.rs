use samyama::{GraphStore, PropertyValue, PersistenceManager, ServerConfig, RespServer};
use samyama::persistence::tenant::{AutoRagConfig, LLMProvider, ResourceQuotas};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Duration;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    println!("--- Samyama Auto-RAG Demo ---");

    // 1. Initialize Persistence and Tenant
    let temp_dir = tempfile::TempDir::new().unwrap();
    let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
    
    let tenant_id = "ai_tenant";
    persistence.tenants().create_tenant(
        tenant_id.to_string(), 
        "AI Research Tenant".to_string(), 
        None
    ).unwrap();

    // 2. Configure Auto-RAG
    // Using Ollama for local testing (assumes Ollama is running)
    let rag_config = AutoRagConfig {
        provider: LLMProvider::Ollama,
        embedding_model: "llama3".to_string(), // Change to your local model
        api_key: None,
        api_base_url: Some("http://localhost:11434".to_string()),
        chunk_size: 100,
        chunk_overlap: 10,
        vector_dimension: 4096, // llama3 dimension
        embedding_policies: HashMap::from([
            ("Document".to_string(), vec!["content".to_string()])
        ]),
    };

    persistence.tenants().update_rag_config(tenant_id, Some(rag_config)).unwrap();
    println!("Auto-RAG configured for tenant: {}", tenant_id);

    // 3. Initialize GraphStore with Async Indexing
    let (mut graph, rx) = GraphStore::with_async_indexing();
    
    // Create vector index for the expected output
    graph.create_vector_index("Document", "content", 4096, samyama::vector::DistanceMetric::Cosine).unwrap();
    
    let store = Arc::new(RwLock::new(graph));
    
    // Start background indexer
    persistence.start_indexer(&*store.read().await, rx);

    // 4. Trigger Auto-RAG via property update
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

    println!("Waiting for background processing (Ollama)...");
    // Wait a bit for Ollama to process
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 5. Verify embedding exists in vector index
    println!("Verifying vector index...");
    let results = {
        let g = store.read().await;
        // Search for similar content
        g.vector_search("Document", "content", &vec![0.1; 4096], 1)
    };

    match results {
        Ok(matches) if !matches.is_empty() => {
            println!("✅ Auto-RAG Success! Found {} matches in vector index.", matches.len());
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
