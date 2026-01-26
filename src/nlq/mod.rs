//! Natural Language Querying (NLQ)
//! 
//! Implements Text-to-Cypher translation using LLMs.

pub mod client;

use thiserror::Error;
use crate::persistence::tenant::NLQConfig;

#[derive(Error, Debug)]
pub enum NLQError {
    #[error("LLM API error: {0}")]
    ApiError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
}

pub type NLQResult<T> = Result<T, NLQError>;

pub struct NLQPipeline {
    client: client::NLQClient,
}

impl NLQPipeline {
    pub fn new(config: NLQConfig) -> NLQResult<Self> {
        let client = client::NLQClient::new(&config)?;
        Ok(Self { client })
    }

    pub async fn text_to_cypher(&self, question: &str, schema_summary: &str) -> NLQResult<String> {
        // Construct prompt with schema
        let prompt = format!(
            "Given this graph schema:\n{}

Translate this question into a read-only OpenCypher query:\n\"{}\"\n
Return ONLY the Cypher query, no markdown, no explanations.",
            schema_summary,
            question
        );

        let cypher = self.client.generate_cypher(&prompt).await?;
        
        // Basic validation/sanitization
        let cleaned_cypher = cypher.trim()
            .trim_start_matches("```cypher")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        if self.is_safe_query(cleaned_cypher) {
            Ok(cleaned_cypher.to_string())
        } else {
            Err(NLQError::ValidationError("Generated query contains write operations or unsafe keywords".to_string()))
        }
    }

    fn is_safe_query(&self, query: &str) -> bool {
        let q = query.to_uppercase();
        !q.contains("CREATE") && 
        !q.contains("DELETE") && 
        !q.contains("SET") && 
        !q.contains("MERGE") &&
        !q.contains("DROP") &&
        !q.contains("REMOVE")
    }
}
