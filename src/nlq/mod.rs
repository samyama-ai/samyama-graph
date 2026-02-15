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

        // Extract Cypher from LLM response — handle markdown fences and explanations
        let cleaned_cypher = Self::extract_cypher(&cypher);

        if self.is_safe_query(&cleaned_cypher) {
            Ok(cleaned_cypher)
        } else {
            Err(NLQError::ValidationError("Generated query contains write operations or unsafe keywords".to_string()))
        }
    }

    /// Extract a Cypher query from an LLM response that may contain markdown
    /// fences, explanations, or multiple code blocks.
    fn extract_cypher(response: &str) -> String {
        let trimmed = response.trim();

        // If response contains a fenced code block, extract the first one
        if let Some(start) = trimmed.find("```") {
            let after_fence = &trimmed[start + 3..];
            // Skip language tag (e.g. "cypher\n")
            let code_start = after_fence.find('\n').map(|i| i + 1).unwrap_or(0);
            if let Some(end) = after_fence[code_start..].find("```") {
                return after_fence[code_start..code_start + end].trim().to_string();
            }
        }

        // No fences — take lines that look like Cypher (start with MATCH/RETURN/WITH/etc.)
        let cypher_keywords = ["MATCH", "RETURN", "WITH", "UNWIND", "CALL", "OPTIONAL"];
        let lines: Vec<&str> = trimmed.lines()
            .filter(|line| {
                let upper = line.trim().to_uppercase();
                cypher_keywords.iter().any(|kw| upper.starts_with(kw))
                    || upper.starts_with("WHERE")
                    || upper.starts_with("ORDER")
                    || upper.starts_with("LIMIT")
            })
            .collect();

        if !lines.is_empty() {
            return lines.join(" ");
        }

        // Fallback: strip outer fences and return as-is
        trimmed
            .trim_start_matches("```cypher")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .to_string()
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
