//! NLQ Client for LLM interactions

use crate::persistence::tenant::{NLQConfig, LLMProvider};
use crate::nlq::{NLQError, NLQResult};
use serde::{Deserialize, Serialize};
use reqwest::Client;
use std::time::Duration;

pub struct NLQClient {
    client: Client,
    config: NLQConfig,
    api_base_url: String,
}

impl NLQClient {
    pub fn new(config: &NLQConfig) -> NLQResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| NLQError::ConfigError(e.to_string()))?;

        let api_base_url = config.api_base_url.clone().unwrap_or_else(|| {
            match config.provider {
                LLMProvider::OpenAI => "https://api.openai.com/v1".to_string(),
                LLMProvider::Ollama => "http://localhost:11434".to_string(),
                LLMProvider::Gemini => "https://generativelanguage.googleapis.com/v1beta".to_string(),
                LLMProvider::AzureOpenAI => String::new(),
                LLMProvider::Anthropic => "https://api.anthropic.com/v1".to_string(),
                LLMProvider::ClaudeCode => String::new(),
                LLMProvider::Mock => String::new(),
            }
        });

        Ok(Self {
            client,
            config: config.clone(),
            api_base_url,
        })
    }

    pub async fn generate_cypher(&self, prompt: &str) -> NLQResult<String> {
        match self.config.provider {
            LLMProvider::OpenAI => self.openai_chat(prompt).await,
            LLMProvider::Ollama => self.ollama_chat(prompt).await,
            LLMProvider::Gemini => self.gemini_chat(prompt).await,
            LLMProvider::ClaudeCode => self.claude_code_generate(prompt).await,
            LLMProvider::Mock => Ok("MATCH (n) RETURN n LIMIT 10".to_string()),
            _ => Err(NLQError::ConfigError(format!("Provider {:?} not yet implemented", self.config.provider))),
        }
    }

    async fn openai_chat(&self, prompt: &str) -> NLQResult<String> {
        #[derive(Serialize)]
        struct Message {
            role: String,
            content: String,
        }

        #[derive(Serialize)]
        struct Request<'a> {
            model: &'a str,
            messages: Vec<Message>,
            temperature: f32,
        }

        #[derive(Deserialize)]
        struct Response {
            choices: Vec<Choice>,
        }

        #[derive(Deserialize)]
        struct Choice {
            message: MessageContent,
        }

        #[derive(Deserialize)]
        struct MessageContent {
            content: String,
        }

        let api_key = self.config.api_key.as_ref().ok_or_else(|| NLQError::ConfigError("OpenAI requires API key".to_string()))?;
        let system_prompt = self.config.system_prompt.clone().unwrap_or_else(|| "You are a Cypher expert.".to_string());

        let url = format!("{}/chat/completions", self.api_base_url);
        let resp = self.client.post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .json(&Request {
                model: &self.config.model,
                messages: vec![
                    Message { role: "system".to_string(), content: system_prompt },
                    Message { role: "user".to_string(), content: prompt.to_string() },
                ],
                temperature: 0.0,
            })
            .send()
            .await
            .map_err(|e| NLQError::NetworkError(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(NLQError::ApiError(format!("OpenAI error: {}", resp.status())));
        }

        let result: Response = resp.json().await.map_err(|e| NLQError::SerializationError(e.to_string()))?;
        Ok(result.choices.first().map(|c| c.message.content.clone()).unwrap_or_default())
    }

    async fn ollama_chat(&self, prompt: &str) -> NLQResult<String> {
        #[derive(Serialize)]
        struct Request<'a> {
            model: &'a str,
            prompt: String,
            system: String,
            stream: bool,
        }

        #[derive(Deserialize)]
        struct Response {
            response: String,
        }

        let system_prompt = self.config.system_prompt.clone().unwrap_or_else(|| "You are a Cypher expert.".to_string());
        
        let url = format!("{}/api/generate", self.api_base_url);
        let resp = self.client.post(&url)
            .json(&Request {
                model: &self.config.model,
                prompt: prompt.to_string(),
                system: system_prompt,
                stream: false,
            })
            .send()
            .await
            .map_err(|e| NLQError::NetworkError(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(NLQError::ApiError(format!("Ollama error: {}", resp.status())));
        }

        let result: Response = resp.json().await.map_err(|e| NLQError::SerializationError(e.to_string()))?;
        Ok(result.response)
    }

    async fn claude_code_generate(&self, prompt: &str) -> NLQResult<String> {
        let output = tokio::process::Command::new("claude")
            .arg("-p")
            .arg(prompt)
            .arg("--max-turns")
            .arg("2")
            .env_remove("CLAUDECODE")
            .output()
            .await
            .map_err(|e| NLQError::ApiError(format!("Claude Code CLI error: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(NLQError::ApiError(format!("Claude Code CLI failed: {}", stderr)));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    async fn gemini_chat(&self, prompt: &str) -> NLQResult<String> {
        #[derive(Serialize)]
        struct Request {
            contents: Vec<Content>,
            #[serde(rename = "generationConfig")]
            generation_config: GenerationConfig,
        }

        #[derive(Serialize, Deserialize)]
        struct Content {
            role: Option<String>,
            parts: Vec<Part>,
        }

        #[derive(Serialize, Deserialize)]
        struct Part {
            text: String,
        }

        #[derive(Serialize)]
        struct GenerationConfig {
            temperature: f32,
        }

        #[derive(Deserialize)]
        struct Response {
            candidates: Option<Vec<Candidate>>,
        }

        #[derive(Deserialize)]
        struct Candidate {
            content: Content,
        }

        let api_key = self.config.api_key.as_ref().ok_or_else(|| NLQError::ConfigError("Gemini requires API key".to_string()))?;
        let system_prompt = self.config.system_prompt.clone().unwrap_or_else(|| "You are a Cypher expert.".to_string());
        
        // Combine system prompt and user prompt because Gemini v1beta doesn't strictly have 'system' role in all endpoints
        // or effectively treats user/model turns.
        // A simple approach is prepending the system instruction.
        let full_prompt = format!("{}\n\nQuestion: {}", system_prompt, prompt);

        let url = format!("{}/models/{}:generateContent?key={}", self.api_base_url, self.config.model, api_key);
        
        let resp = self.client.post(&url)
            .json(&Request {
                contents: vec![
                    Content {
                        role: Some("user".to_string()),
                        parts: vec![Part { text: full_prompt }],
                    }
                ],
                generation_config: GenerationConfig { temperature: 0.0 },
            })
            .send()
            .await
            .map_err(|e| NLQError::NetworkError(e.to_string()))?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(NLQError::ApiError(format!("Gemini error: {}", text)));
        }

        let result: Response = resp.json().await.map_err(|e| NLQError::SerializationError(e.to_string()))?;
        
        if let Some(candidates) = result.candidates {
            if let Some(first) = candidates.first() {
                if let Some(part) = first.content.parts.first() {
                    return Ok(part.text.clone());
                }
            }
        }
        
        Ok(String::new())
    }
}

#[cfg(test)] #[path = "client_cov_tests.rs"] mod cov_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_config() -> NLQConfig {
        NLQConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock-model".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        }
    }

    #[test]
    fn test_nlq_client_new_mock() {
        let config = mock_config();
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_nlq_client_generate_cypher_mock() {
        let config = mock_config();
        let client = NLQClient::new(&config).unwrap();
        let result = client.generate_cypher("Who knows Alice?").await;
        assert!(result.is_ok());
        let cypher = result.unwrap();
        assert!(cypher.contains("MATCH")); // Mock returns "MATCH (n) RETURN n LIMIT 10"
    }

    #[test]
    fn test_nlq_client_new_openai() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::OpenAI,
            model: "gpt-4o".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: None,
            system_prompt: Some("You are a Cypher expert.".to_string()),
        };
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_nlq_client_new_ollama() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Ollama,
            model: "llama3".to_string(),
            api_key: None,
            api_base_url: Some("http://localhost:11434".to_string()),
            system_prompt: None,
        };
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_nlq_client_new_gemini() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Gemini,
            model: "gemini-pro".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: None,
            system_prompt: None,
        };
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_nlq_client_new_anthropic() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Anthropic,
            model: "claude-3".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: None,
            system_prompt: None,
        };
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_nlq_client_new_azure() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::AzureOpenAI,
            model: "gpt-4".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: Some("https://myendpoint.openai.azure.com".to_string()),
            system_prompt: None,
        };
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_nlq_client_new_claude_code() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::ClaudeCode,
            model: "claude".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        };
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_nlq_client_custom_base_url() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::OpenAI,
            model: "gpt-4".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: Some("https://custom.api.example.com/v1".to_string()),
            system_prompt: Some("Custom system prompt".to_string()),
        };
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_generate_cypher_unsupported_provider() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::AzureOpenAI,
            model: "gpt-4".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: Some("https://test.openai.azure.com".to_string()),
            system_prompt: None,
        };
        let client = NLQClient::new(&config).unwrap();
        let result = client.generate_cypher("test").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_generate_cypher_mock_returns_valid_cypher() {
        let config = mock_config();
        let client = NLQClient::new(&config).unwrap();
        let result = client.generate_cypher("Find all people").await.unwrap();
        assert_eq!(result, "MATCH (n) RETURN n LIMIT 10");
    }

    // ========== Coverage batch: additional NLQ client tests ==========

    #[test]
    fn test_nlq_client_default_base_urls() {
        // OpenAI default base URL
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::OpenAI,
            model: "gpt-4".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: None,
            system_prompt: None,
        };
        let client = NLQClient::new(&config).unwrap();
        assert_eq!(client.api_base_url, "https://api.openai.com/v1");

        // Ollama default base URL
        let config_ollama = NLQConfig {
            enabled: true,
            provider: LLMProvider::Ollama,
            model: "llama3".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        };
        let client_ollama = NLQClient::new(&config_ollama).unwrap();
        assert_eq!(client_ollama.api_base_url, "http://localhost:11434");

        // Gemini default base URL
        let config_gemini = NLQConfig {
            enabled: true,
            provider: LLMProvider::Gemini,
            model: "gemini-pro".to_string(),
            api_key: Some("key".to_string()),
            api_base_url: None,
            system_prompt: None,
        };
        let client_gemini = NLQClient::new(&config_gemini).unwrap();
        assert_eq!(client_gemini.api_base_url, "https://generativelanguage.googleapis.com/v1beta");

        // Anthropic default base URL
        let config_anthropic = NLQConfig {
            enabled: true,
            provider: LLMProvider::Anthropic,
            model: "claude-3".to_string(),
            api_key: Some("key".to_string()),
            api_base_url: None,
            system_prompt: None,
        };
        let client_anthropic = NLQClient::new(&config_anthropic).unwrap();
        assert_eq!(client_anthropic.api_base_url, "https://api.anthropic.com/v1");

        // AzureOpenAI default (empty)
        let config_azure = NLQConfig {
            enabled: true,
            provider: LLMProvider::AzureOpenAI,
            model: "gpt-4".to_string(),
            api_key: Some("key".to_string()),
            api_base_url: None,
            system_prompt: None,
        };
        let client_azure = NLQClient::new(&config_azure).unwrap();
        assert_eq!(client_azure.api_base_url, "");

        // ClaudeCode default (empty)
        let config_cc = NLQConfig {
            enabled: true,
            provider: LLMProvider::ClaudeCode,
            model: "claude".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        };
        let client_cc = NLQClient::new(&config_cc).unwrap();
        assert_eq!(client_cc.api_base_url, "");

        // Mock default (empty)
        let client_mock = NLQClient::new(&mock_config()).unwrap();
        assert_eq!(client_mock.api_base_url, "");
    }

    #[test]
    fn test_nlq_client_custom_base_url_overrides_default() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::OpenAI,
            model: "gpt-4".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: Some("https://custom.openai.proxy.com/v1".to_string()),
            system_prompt: None,
        };
        let client = NLQClient::new(&config).unwrap();
        assert_eq!(client.api_base_url, "https://custom.openai.proxy.com/v1");
    }

    #[tokio::test]
    async fn test_generate_cypher_mock_with_various_prompts() {
        let config = mock_config();
        let client = NLQClient::new(&config).unwrap();

        // Mock always returns the same regardless of prompt
        let r1 = client.generate_cypher("Find Alice").await.unwrap();
        let r2 = client.generate_cypher("Count all nodes").await.unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1, "MATCH (n) RETURN n LIMIT 10");
    }

    #[tokio::test]
    async fn test_generate_cypher_anthropic_not_implemented() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Anthropic,
            model: "claude-3".to_string(),
            api_key: Some("key".to_string()),
            api_base_url: None,
            system_prompt: None,
        };
        let client = NLQClient::new(&config).unwrap();
        let result = client.generate_cypher("test").await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.err().unwrap());
        assert!(err_msg.contains("not yet implemented"));
    }

    #[test]
    fn test_nlq_client_with_system_prompt() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: Some("You are a graph database expert specialized in medical data.".to_string()),
        };
        let client = NLQClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_nlq_error_display() {
        let e1 = NLQError::ApiError("test api error".to_string());
        assert!(format!("{}", e1).contains("LLM API error"));

        let e2 = NLQError::ConfigError("test config error".to_string());
        assert!(format!("{}", e2).contains("Configuration error"));

        let e3 = NLQError::NetworkError("test network error".to_string());
        assert!(format!("{}", e3).contains("Network error"));

        let e4 = NLQError::SerializationError("test serialization error".to_string());
        assert!(format!("{}", e4).contains("Serialization error"));

        let e5 = NLQError::ValidationError("test validation error".to_string());
        assert!(format!("{}", e5).contains("Validation error"));
    }
}
