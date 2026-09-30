//! Natural Language Querying (NLQ)
//! 
//! Implements Text-to-Cypher translation using LLMs.

pub mod client;
#[cfg(test)] #[path = "test_http_cov.rs"] pub(crate) mod test_http;

use thiserror::Error;
use crate::persistence::tenant::{LLMProvider, NLQConfig};

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
        let prompt = Self::build_prompt(question, schema_summary);

        let cypher = self.client.generate_cypher(&prompt).await?;

        // Extract Cypher from LLM response — handle markdown fences and explanations
        let cleaned_cypher = Self::extract_cypher(&cypher);

        if self.is_safe_query(&cleaned_cypher) {
            Ok(cleaned_cypher)
        } else {
            Err(NLQError::ValidationError("Generated query contains write operations or unsafe keywords".to_string()))
        }
    }

    /// The prompt sent to the model for `question` over `schema_summary`.
    ///
    /// There used to be a rule here telling the model that `count(DISTINCT x)`
    /// is not supported and to write `count(x)` instead. It is supported, so
    /// the rule turned every "how many different ..." question into a count of
    /// rows with duplicates (#438).
    fn build_prompt(question: &str, schema_summary: &str) -> String {
        format!(
            "You are a Cypher query expert for a graph database. Given this schema:\n\n{}\n\n\
            Rules:\n\
            - Follow the Relationship Patterns EXACTLY — do not invent edges between labels that aren't listed\n\
            - When a question involves two unrelated labels (e.g. Country + DiseaseCategory), join them through a shared node (e.g. Trial)\n\
            - Use property names from the Key Properties section\n\
            - To filter on aggregated values, use WITH to compute the aggregation first, then WHERE, then RETURN — never place WHERE after RETURN\n\
            - Return ONLY the Cypher query, no markdown, no explanations\n\n\
            Question: \"{}\"",
            schema_summary,
            question
        )
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

    /// Whether a model-generated statement may be executed (AI-12).
    ///
    /// This used to be a prefix check on the uppercased text: a statement was
    /// safe if it began with MATCH, RETURN, UNWIND, CALL or WITH. Almost every
    /// destructive Cypher statement begins with MATCH, so
    /// `MATCH (n) DETACH DELETE n` passed, as did `MATCH (n) SET ...`,
    /// `MATCH ... MERGE ...` and `MATCH (n) REMOVE ...`. The tests did not
    /// catch it because every negative case *started* with a write keyword --
    /// they tested the check as written rather than the property it claims
    /// (#1156).
    ///
    /// The engine already had the right answer. `Query::is_write()` reads the
    /// parsed clause list, so it sees a write wherever it appears.
    ///
    /// Fails closed: a statement that does not parse is not safe. The old check
    /// would accept unparseable text beginning with MATCH, leaving the parser
    /// to reject it later -- which is a different component deciding a security
    /// question by accident.
    pub fn is_safe_query(&self, query: &str) -> bool {
        match crate::query::parse_query(query) {
            Ok(parsed) => !parsed.is_write(),
            Err(_) => false,
        }
    }
}

/// The environment variable a provider's API key is read from, or `None` for
/// a provider that takes no key.
///
/// Every NLQ path used to read `OPENAI_API_KEY` whatever the provider, so a
/// Gemini deployment had to put its Google key in a variable named for
/// OpenAI -- and an operator who set both sent the OpenAI key to Google (#438).
pub fn api_key_var(provider: &LLMProvider) -> Option<&'static str> {
    match provider {
        LLMProvider::OpenAI => Some("OPENAI_API_KEY"),
        LLMProvider::Gemini => Some("GEMINI_API_KEY"),
        LLMProvider::Anthropic => Some("ANTHROPIC_API_KEY"),
        LLMProvider::AzureOpenAI => Some("AZURE_OPENAI_API_KEY"),
        LLMProvider::Ollama | LLMProvider::ClaudeCode | LLMProvider::Mock => None,
    }
}

/// The NLQ configuration the process environment describes: `NLQ_PROVIDER`
/// (no default), `NLQ_MODEL` (default `gpt-4o`), the provider's own key
/// variable (see [`api_key_var`]) and optional `NLQ_API_BASE_URL`.
///
/// One reader for `POST /api/nlq`, `GRAPH.NLQ` and the enrichment worker, so
/// the three cannot disagree about which variable holds what.
pub fn config_from_env() -> Result<NLQConfig, String> {
    config_from(|var| std::env::var(var).ok())
}

/// As [`config_from_env`], reading variables through `get`.
///
/// Refuses a provider that is accepted by name but not implemented, and a
/// provider that needs a key when its variable is unset, naming the variable:
/// both used to surface only at the first request, as a generic error from
/// deep inside the client.
pub fn config_from(get: impl Fn(&str) -> Option<String>) -> Result<NLQConfig, String> {
    let provider = LLMProvider::parse(&get("NLQ_PROVIDER").unwrap_or_default())?;
    client::check_implemented(&provider).map_err(|e| e.to_string())?;
    let api_key = match api_key_var(&provider) {
        None => None,
        Some(var) => match get(var).filter(|k| !k.trim().is_empty()) {
            Some(key) => Some(key),
            None => {
                return Err(format!(
                    "NLQ_PROVIDER is {provider:?}, which needs an API key in {var}, and {var} is \
                     not set. Each provider reads its own variable; OPENAI_API_KEY is only \
                     read for OpenAI."
                ))
            }
        },
    };
    Ok(NLQConfig {
        enabled: true,
        provider,
        model: get("NLQ_MODEL").unwrap_or_else(|| "gpt-4o".to_string()),
        api_key,
        api_base_url: get("NLQ_API_BASE_URL"),
        system_prompt: None,
    })
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

    /// The gap the old prefix check left wide open (#1156).
    ///
    /// Every case here begins with MATCH, so the previous implementation --
    /// "safe if it starts with MATCH" -- returned true for all of them. These
    /// are not exotic: they are the ordinary way to write a destructive
    /// statement in Cypher, and the NLQ pipeline runs whatever the model
    /// returns once this says yes.
    #[test]
    fn a_write_that_begins_with_match_is_not_safe() {
        let pipeline = make_pipeline();
        for q in [
            "MATCH (n) DETACH DELETE n",
            "MATCH (n) DELETE n",
            "MATCH (n:Person) SET n.pwned = 1 RETURN n",
            "MATCH (a:Person) MERGE (b:Backdoor {x: 1}) RETURN a",
            "MATCH (n) REMOVE n.age RETURN n",
        ] {
            assert!(
                !pipeline.is_safe_query(q),
                "accepted a destructive statement because it begins with MATCH: {q}"
            );
        }
    }

    /// Fail closed. The old check accepted unparseable text that began with a
    /// permitted keyword and left the parser to refuse it later, which makes a
    /// security decision a side effect of parsing.
    #[test]
    fn text_that_does_not_parse_is_not_safe() {
        let pipeline = make_pipeline();
        for q in [
            "MATCH (((",
            "MATCH n RETURN",
            "WITH",
            "",
        ] {
            assert!(!pipeline.is_safe_query(q), "accepted unparseable text: {q:?}");
        }
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

    // ========== Coverage batch: additional NLQ pipeline tests ==========

    #[test]
    fn test_pipeline_creation_with_mock() {
        let pipeline = make_pipeline();
        // Just verify it was created successfully (constructor exercises NLQClient::new)
        assert!(pipeline.is_safe_query("MATCH (n) RETURN n"));
    }

    #[test]
    fn test_is_safe_query_call_prefix() {
        let pipeline = make_pipeline();
        assert!(pipeline.is_safe_query("CALL algo.pageRank({}) YIELD node"));
        assert!(pipeline.is_safe_query("CALL db.labels()"));
    }

    #[test]
    fn test_is_safe_query_with_prefix() {
        let pipeline = make_pipeline();
        assert!(pipeline.is_safe_query("WITH 1 AS x MATCH (n) RETURN n"));
    }

    #[test]
    fn test_is_safe_query_return_only() {
        let pipeline = make_pipeline();
        assert!(pipeline.is_safe_query("RETURN 42"));
        assert!(pipeline.is_safe_query("RETURN datetime()"));
    }

    #[test]
    fn test_is_safe_query_rejects_create() {
        let pipeline = make_pipeline();
        assert!(!pipeline.is_safe_query("CREATE (:Person {name: 'Eve'})"));
    }

    #[test]
    fn test_is_safe_query_rejects_drop() {
        let pipeline = make_pipeline();
        assert!(!pipeline.is_safe_query("DROP INDEX myIdx"));
    }

    #[test]
    fn test_is_safe_query_rejects_set_at_start() {
        let pipeline = make_pipeline();
        assert!(!pipeline.is_safe_query("SET n.name = 'test'"));
    }

    #[test]
    fn test_is_safe_query_rejects_remove_at_start() {
        let pipeline = make_pipeline();
        assert!(!pipeline.is_safe_query("REMOVE n.age"));
    }

    #[test]
    fn test_is_safe_query_whitespace_handling() {
        let pipeline = make_pipeline();
        assert!(pipeline.is_safe_query("  MATCH (n) RETURN n  "));
        assert!(pipeline.is_safe_query("  RETURN 1  "));
    }

    #[test]
    fn test_is_safe_query_empty_string() {
        let pipeline = make_pipeline();
        assert!(!pipeline.is_safe_query(""));
    }

    #[test]
    fn test_extract_cypher_multiple_fenced_blocks() {
        // extract_cypher should return the first fenced code block
        let input = "First block:\n```cypher\nMATCH (a) RETURN a\n```\nSecond:\n```cypher\nMATCH (b) RETURN b\n```";
        let result = NLQPipeline::extract_cypher(input);
        assert_eq!(result, "MATCH (a) RETURN a");
    }

    #[test]
    fn test_extract_cypher_fenced_without_closing() {
        // If there's no closing ```, the fallback should handle it
        let input = "Here:\n```cypher\nMATCH (n) RETURN n";
        let result = NLQPipeline::extract_cypher(input);
        // With no closing ```, fallback to line-based extraction
        assert!(result.contains("MATCH"));
        assert!(result.contains("RETURN"));
    }

    #[test]
    fn test_extract_cypher_only_non_cypher_text() {
        // No cypher keywords at line starts => fallback to trimmed input
        let input = "I think you should look at the data.";
        let result = NLQPipeline::extract_cypher(input);
        assert_eq!(result, "I think you should look at the data.");
    }

    #[test]
    fn test_extract_cypher_unwind_at_start() {
        let input = "UNWIND range(1, 10) AS i\nRETURN i";
        let result = NLQPipeline::extract_cypher(input);
        assert!(result.contains("UNWIND"));
        assert!(result.contains("RETURN"));
    }

    #[test]
    fn test_extract_cypher_call_at_start() {
        let input = "CALL db.labels()";
        let result = NLQPipeline::extract_cypher(input);
        assert!(result.contains("CALL"));
    }

    #[test]
    fn test_extract_cypher_with_clause_lines() {
        let input = "MATCH (n:Person)\nWITH n.city AS city\nRETURN city";
        let result = NLQPipeline::extract_cypher(input);
        assert!(result.contains("MATCH"));
        assert!(result.contains("WITH"));
        assert!(result.contains("RETURN"));
    }

    #[tokio::test]
    async fn test_text_to_cypher_with_mock() {
        let pipeline = make_pipeline();
        let schema = "Labels: Person, Company\nRelationships: WORKS_AT";
        let result = pipeline.text_to_cypher("Find all people", schema).await;
        // Mock returns "MATCH (n) RETURN n LIMIT 10", which starts with MATCH => safe
        assert!(result.is_ok());
        let cypher = result.unwrap();
        assert!(cypher.contains("MATCH"));
    }

    #[tokio::test]
    async fn test_text_to_cypher_validates_safety() {
        // The mock always returns "MATCH (n) RETURN n LIMIT 10" which is safe
        // We can only test that the pipeline works end-to-end with mock
        let pipeline = make_pipeline();
        let result = pipeline.text_to_cypher("test question", "schema").await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_extract_cypher_plain_fence_no_lang_tag() {
        let input = "```\nRETURN 42\n```";
        let result = NLQPipeline::extract_cypher(input);
        assert_eq!(result, "RETURN 42");
    }

    #[test]
    fn test_extract_cypher_mixed_case_keywords() {
        let input = "match (n:Person)\nwhere n.age > 30\nreturn n.name";
        let result = NLQPipeline::extract_cypher(input);
        // Keywords are checked case-insensitively (via to_uppercase)
        assert!(result.contains("match") || result.contains("MATCH"));
    }

    #[test]
    fn test_nlq_pipeline_new_with_different_providers() {
        // Test with OpenAI provider
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::OpenAI,
            model: "gpt-4".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: None,
            system_prompt: None,
        };
        let pipeline = NLQPipeline::new(config);
        assert!(pipeline.is_ok());
    }

    #[test]
    fn test_is_safe_query_unwind_prefix() {
        let pipeline = make_pipeline();
        assert!(pipeline.is_safe_query("UNWIND [1,2,3] AS x RETURN x"));
    }

    // --- prompt and environment config (#438) ---

    #[test]
    fn the_prompt_does_not_forbid_count_distinct_and_asks_for_with_before_where() {
        let prompt = NLQPipeline::build_prompt("how many cities?", "Labels: City");
        // count(DISTINCT x) is supported (tests/aggregate_identity_grouping.rs);
        // telling the model otherwise made it count duplicates.
        assert!(!prompt.contains("DISTINCT"), "{prompt}");
        assert!(!prompt.contains("not supported"), "{prompt}");
        assert!(
            prompt.contains("use WITH to compute the aggregation first, then WHERE, then RETURN"),
            "{prompt}"
        );
        assert!(
            prompt.contains("never place WHERE after RETURN"),
            "{prompt}"
        );
        assert!(prompt.contains("Labels: City") && prompt.contains("how many cities?"));
    }

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
    }

    #[test]
    fn each_provider_reads_its_own_key_variable() {
        let cases = [("openai", "OPENAI_API_KEY"), ("gemini", "GEMINI_API_KEY")];
        for (name, var) in cases {
            // Every key variable set, each to a different value: the one that
            // arrives in the config says which variable was read.
            let all = [
                ("NLQ_PROVIDER", name),
                ("OPENAI_API_KEY", "OPENAI_API_KEY"),
                ("GEMINI_API_KEY", "GEMINI_API_KEY"),
                ("ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"),
                ("AZURE_OPENAI_API_KEY", "AZURE_OPENAI_API_KEY"),
            ];
            let config = config_from(env(&all)).unwrap();
            assert_eq!(config.api_key.as_deref(), Some(var), "{name}");
        }
        for name in ["ollama", "claudecode", "mock"] {
            let config =
                config_from(env(&[("NLQ_PROVIDER", name), ("OPENAI_API_KEY", "sk")])).unwrap();
            assert_eq!(
                config.api_key, None,
                "{name} takes no key and must not be sent one"
            );
        }
        assert_eq!(
            api_key_var(&LLMProvider::Anthropic),
            Some("ANTHROPIC_API_KEY")
        );
        assert_eq!(
            api_key_var(&LLMProvider::AzureOpenAI),
            Some("AZURE_OPENAI_API_KEY")
        );
    }

    #[test]
    fn a_missing_key_is_refused_naming_the_variable() {
        // The Gemini case is the old defect: OPENAI_API_KEY set, and read.
        let err =
            config_from(env(&[("NLQ_PROVIDER", "gemini"), ("OPENAI_API_KEY", "sk")])).unwrap_err();
        assert!(err.contains("GEMINI_API_KEY"), "{err}");
        let err =
            config_from(env(&[("NLQ_PROVIDER", "openai"), ("OPENAI_API_KEY", " ")])).unwrap_err();
        assert!(err.contains("OPENAI_API_KEY"), "{err}");
    }

    #[test]
    fn the_rest_of_the_config_comes_from_the_environment() {
        let config = config_from(env(&[
            ("NLQ_PROVIDER", "ollama"),
            ("NLQ_MODEL", "llama3"),
            ("NLQ_API_BASE_URL", "http://127.0.0.1:1"),
        ]))
        .unwrap();
        assert_eq!(config.provider, LLMProvider::Ollama);
        assert_eq!(config.model, "llama3");
        assert_eq!(config.api_base_url.as_deref(), Some("http://127.0.0.1:1"));
        let unset = config_from(env(&[])).unwrap_err();
        assert!(unset.contains("NLQ_PROVIDER"), "{unset}");
    }

    #[test]
    fn an_unimplemented_provider_is_refused_before_its_key_is_asked_for() {
        for name in ["anthropic", "azure"] {
            let err = config_from(env(&[("NLQ_PROVIDER", name)])).unwrap_err();
            assert!(err.contains("not implemented"), "{name}: {err}");
            assert!(!err.contains("API_KEY"), "{name}: {err}");
        }
    }
}
