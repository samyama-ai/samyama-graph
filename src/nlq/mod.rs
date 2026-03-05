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
        let prompt = format!(
            "You are a Cypher query expert for a graph database. Given this schema:\n\n{}\n\n\
            Rules:\n\
            - Follow the Relationship Patterns EXACTLY — do not invent edges between labels that aren't listed\n\
            - When a question involves two unrelated labels (e.g. Country + DiseaseCategory), join them through a shared node (e.g. Trial)\n\
            - Use property names from the Key Properties section\n\
            - Use count(x) not COUNT(DISTINCT x) — DISTINCT inside aggregation is not supported\n\
            - Return ONLY the Cypher query, no markdown, no explanations\n\n\
            Question: \"{}\"",
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

    pub fn is_safe_query(&self, query: &str) -> bool {
        let trimmed = query.trim().to_uppercase();
        trimmed.starts_with("MATCH") ||
        trimmed.starts_with("RETURN") ||
        trimmed.starts_with("UNWIND") ||
        trimmed.starts_with("CALL") ||
        trimmed.starts_with("WITH")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::tenant::{NLQConfig, LLMProvider};

    fn make_pipeline() -> NLQPipeline {
        NLQPipeline::new(NLQConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        }).unwrap()
    }

    // --- is_safe_query tests (via pipeline) ---

    #[test]
    fn test_safe_read_queries() {
        let pipeline = make_pipeline();
        assert!(pipeline.is_safe_query("MATCH (n:Person) RETURN n.name"));
        assert!(pipeline.is_safe_query("MATCH (a)-[:KNOWS]->(b) RETURN a, b"));
        assert!(pipeline.is_safe_query("MATCH (n) WHERE n.age > 30 RETURN count(n)"));
        assert!(pipeline.is_safe_query("RETURN 1"));
        assert!(pipeline.is_safe_query("UNWIND [1,2,3] AS x RETURN x"));
        assert!(pipeline.is_safe_query("WITH 1 AS x RETURN x"));
        assert!(pipeline.is_safe_query("CALL db.labels()"));
        // Regression: property value containing write keyword must be safe
        assert!(pipeline.is_safe_query("MATCH (n:Person) WHERE n.name = 'SET' RETURN n"));
        assert!(pipeline.is_safe_query("MATCH (n) WHERE n.status = 'CREATED' RETURN n"));
        assert!(pipeline.is_safe_query("match (n) return n")); // lowercase
    }

    #[test]
    fn test_unsafe_write_queries() {
        let pipeline = make_pipeline();
        assert!(!pipeline.is_safe_query("CREATE (n:Person {name: 'Alice'})"));
        assert!(!pipeline.is_safe_query("DELETE n"));
        assert!(!pipeline.is_safe_query("SET n.name = 'Bob'"));
        assert!(!pipeline.is_safe_query("MERGE (n:Person {name: 'Alice'})"));
        assert!(!pipeline.is_safe_query("DROP INDEX my_index"));
        assert!(!pipeline.is_safe_query("REMOVE n.age"));
    }

    // --- extract_cypher tests ---

    #[test]
    fn test_extract_cypher_plain_query() {
        let input = "MATCH (n:Person) RETURN n.name";
        let result = NLQPipeline::extract_cypher(input);
        assert_eq!(result, "MATCH (n:Person) RETURN n.name");
    }

    #[test]
    fn test_extract_cypher_markdown_fenced() {
        let input = "Here is the query:\n```cypher\nMATCH (n:Person) RETURN n.name\n```\nHope this helps!";
        let result = NLQPipeline::extract_cypher(input);
        assert_eq!(result, "MATCH (n:Person) RETURN n.name");
    }

    #[test]
    fn test_extract_cypher_markdown_no_language_tag() {
        let input = "```\nMATCH (n) RETURN n\n```";
        let result = NLQPipeline::extract_cypher(input);
        assert_eq!(result, "MATCH (n) RETURN n");
    }

    #[test]
    fn test_extract_cypher_mixed_with_explanation() {
        let input = "To find all people, use this:\nMATCH (n:Person)\nWHERE n.age > 30\nRETURN n.name\nThis returns names of people over 30.";
        let result = NLQPipeline::extract_cypher(input);
        assert!(result.contains("MATCH (n:Person)"));
        assert!(result.contains("WHERE n.age > 30"));
        assert!(result.contains("RETURN n.name"));
        assert!(!result.contains("To find all people"));
    }

    #[test]
    fn test_extract_cypher_with_optional_match() {
        let input = "OPTIONAL MATCH (n:Person)-[:KNOWS]->(m)\nRETURN n, m";
        let result = NLQPipeline::extract_cypher(input);
        assert!(result.contains("OPTIONAL MATCH"));
        assert!(result.contains("RETURN"));
    }

    #[test]
    fn test_extract_cypher_with_order_and_limit() {
        let input = "MATCH (n:Person)\nRETURN n.name\nORDER BY n.name\nLIMIT 10";
        let result = NLQPipeline::extract_cypher(input);
        assert!(result.contains("MATCH"));
        assert!(result.contains("ORDER BY"));
        assert!(result.contains("LIMIT 10"));
    }

    #[test]
    fn test_extract_cypher_whitespace_trimming() {
        let input = "  \n  MATCH (n) RETURN n  \n  ";
        let result = NLQPipeline::extract_cypher(input);
        assert_eq!(result, "MATCH (n) RETURN n");
    }
}
