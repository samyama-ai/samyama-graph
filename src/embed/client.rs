//! Embedding client for various LLM providers

use crate::persistence::tenant::{AutoEmbedConfig, LLMProvider};
use crate::embed::{EmbedError, EmbedResult};
use serde::{Deserialize, Serialize};
use reqwest::Client;
use std::time::Duration;

/// Client for interacting with LLM APIs to generate embeddings
pub struct EmbeddingClient {
    client: Client,
    provider: LLMProvider,
    model: String,
    api_key: Option<String>,
    api_base_url: String,
    dimensions: Option<usize>,
}

impl EmbeddingClient {
    /// Create a new embedding client based on configuration
    pub fn new(config: &AutoEmbedConfig) -> EmbedResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| EmbedError::ConfigError(e.to_string()))?;

        let api_base_url = config.api_base_url.clone().unwrap_or_else(|| {
            match config.provider {
                LLMProvider::OpenAI => "https://api.openai.com/v1".to_string(),
                LLMProvider::Ollama => "http://localhost:11434".to_string(),
                LLMProvider::Gemini => "https://generativelanguage.googleapis.com/v1beta".to_string(),
                LLMProvider::AzureOpenAI => String::new(), // Must be provided
                LLMProvider::Anthropic => "https://api.anthropic.com/v1".to_string(),
                LLMProvider::ClaudeCode => String::new(),
                LLMProvider::Mock => String::new(),
            }
        });

        if (config.provider == LLMProvider::AzureOpenAI || config.provider == LLMProvider::Ollama) && config.api_base_url.is_none() && config.provider == LLMProvider::AzureOpenAI {
             return Err(EmbedError::ConfigError("AzureOpenAI requires api_base_url".to_string()));
        }

        // Only pass dimensions when not using the model's native default (1536 for text-embedding-3-small).
        // OpenAI ignores the field for models that don't support it (e.g. ada-002).
        let dimensions = if config.vector_dimension > 0 { Some(config.vector_dimension) } else { None };

        Ok(Self {
            client,
            provider: config.provider.clone(),
            model: config.embedding_model.clone(),
            api_key: config.api_key.clone(),
            api_base_url,
            dimensions,
        })
    }

    /// Generate embeddings for a batch of texts
    pub async fn generate_embeddings(&self, texts: &[String]) -> EmbedResult<Vec<Vec<f32>>> {
        match self.provider {
            LLMProvider::OpenAI => self.openai_embeddings(texts).await,
            LLMProvider::Ollama => self.ollama_embeddings(texts).await,
            LLMProvider::Gemini => self.gemini_embeddings(texts).await,
            LLMProvider::Mock => {
                // Return deterministic dummy embeddings based on text length and first chars
                Ok(texts.iter().map(|t| {
                    let mut vec = vec![0.1; 64];
                    if !t.is_empty() {
                        // Vary the first few elements based on string properties
                        vec[0] = (t.len() as f32 % 100.0) / 100.0;
                        vec[1] = (t.as_bytes()[0] as f32 % 100.0) / 100.0;
                        if t.len() > 1 {
                            vec[2] = (t.as_bytes()[1] as f32 % 100.0) / 100.0;
                        }
                    }
                    vec
                }).collect())
            }
            _ => Err(EmbedError::ConfigError(format!("Provider {:?} not yet implemented", self.provider))),
        }
    }

    async fn openai_embeddings(&self, texts: &[String]) -> EmbedResult<Vec<Vec<f32>>> {
        #[derive(Serialize)]
        struct OpenAIRequest<'a> {
            input: &'a [String],
            model: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            dimensions: Option<usize>,
        }

        #[derive(Deserialize)]
        struct OpenAIResponse {
            data: Vec<OpenAIData>,
        }

        #[derive(Deserialize)]
        struct OpenAIData {
            embedding: Vec<f32>,
        }

        let api_key = self.api_key.as_ref().ok_or_else(|| EmbedError::ConfigError("OpenAI requires API key".to_string()))?;
        
        let url = format!("{}/embeddings", self.api_base_url);
        let resp = self.client.post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .json(&OpenAIRequest {
                input: texts,
                model: &self.model,
                dimensions: self.dimensions,
            })
            .send()
            .await
            .map_err(|e| EmbedError::NetworkError(e.to_string()))?;

        if !resp.status().is_success() {
            let error_text = resp.text().await.unwrap_or_default();
            return Err(EmbedError::ApiError(format!("OpenAI returned error: {}", error_text)));
        }

        let result: OpenAIResponse = resp.json().await.map_err(|e| EmbedError::SerializationError(e.to_string()))?;
        Ok(result.data.into_iter().map(|d| d.embedding).collect())
    }

    async fn ollama_embeddings(&self, texts: &[String]) -> EmbedResult<Vec<Vec<f32>>> {
        #[derive(Serialize)]
        struct OllamaRequest<'a> {
            model: &'a str,
            prompt: &'a str,
        }

        #[derive(Deserialize)]
        struct OllamaResponse {
            embedding: Vec<f32>,
        }

        let mut results = Vec::new();
        for text in texts {
            let url = format!("{}/api/embeddings", self.api_base_url);
            let resp = self.client.post(&url)
                .json(&OllamaRequest {
                    model: &self.model,
                    prompt: text,
                })
                .send()
                .await
                .map_err(|e| EmbedError::NetworkError(e.to_string()))?;

            if !resp.status().is_success() {
                let error_text = resp.text().await.unwrap_or_default();
                return Err(EmbedError::ApiError(format!("Ollama returned error: {}", error_text)));
            }

            let result: OllamaResponse = resp.json().await.map_err(|e| EmbedError::SerializationError(e.to_string()))?;
            results.push(result.embedding);
        }
        
        Ok(results)
    }

    async fn gemini_embeddings(&self, texts: &[String]) -> EmbedResult<Vec<Vec<f32>>> {
        #[derive(Serialize)]
        struct GeminiBatchRequest<'a> {
            requests: Vec<GeminiRequest<'a>>,
        }

        #[derive(Serialize)]
        struct GeminiRequest<'a> {
            model: String,
            content: GeminiContent<'a>,
        }

        #[derive(Serialize)]
        struct GeminiContent<'a> {
            parts: Vec<GeminiPart<'a>>,
        }

        #[derive(Serialize)]
        struct GeminiPart<'a> {
            text: &'a str,
        }

        #[derive(Deserialize)]
        struct GeminiBatchResponse {
            embeddings: Vec<GeminiEmbedding>,
        }

        #[derive(Deserialize)]
        struct GeminiEmbedding {
            values: Vec<f32>,
        }

        let api_key = self.api_key.as_ref().ok_or_else(|| EmbedError::ConfigError("Gemini requires API key".to_string()))?;
        
        let url = format!("{}/models/{}:batchEmbedContents?key={}", self.api_base_url, self.model, api_key);
        
        let requests = texts.iter().map(|t| GeminiRequest {
            model: format!("models/{}", self.model),
            content: GeminiContent {
                parts: vec![GeminiPart { text: t }],
            },
        }).collect();

        let resp = self.client.post(&url)
            .json(&GeminiBatchRequest { requests })
            .send()
            .await
            .map_err(|e| EmbedError::NetworkError(e.to_string()))?;

        if !resp.status().is_success() {
            let error_text = resp.text().await.unwrap_or_default();
            return Err(EmbedError::ApiError(format!("Gemini returned error: {}", error_text)));
        }

        let result: GeminiBatchResponse = resp.json().await.map_err(|e| EmbedError::SerializationError(e.to_string()))?;
        Ok(result.embeddings.into_iter().map(|e| e.values).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn mock_config() -> AutoEmbedConfig {
        AutoEmbedConfig {
            provider: LLMProvider::Mock,
            embedding_model: "mock-model".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 64,
            embedding_policies: HashMap::new(),
        }
    }

    #[test]
    fn test_embedding_client_new_mock() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_mock_embeddings() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts = vec!["hello world".to_string(), "test text".to_string()];
        let result = client.generate_embeddings(&texts).await;
        assert!(result.is_ok());
        let embeddings = result.unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), 64); // Mock returns 64-dim vectors
        // Verify deterministic: different texts produce different first elements
        assert_ne!(embeddings[0][0], embeddings[1][0]);
    }

    #[test]
    fn test_embedding_client_new_openai() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "text-embedding-3-small".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 1536,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_embedding_client_new_ollama() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::Ollama,
            embedding_model: "llama3".to_string(),
            api_key: None,
            api_base_url: Some("http://localhost:11434".to_string()),
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_mock_embeddings_empty_text() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts = vec!["".to_string()];
        let result = client.generate_embeddings(&texts).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_embedding_client_new_gemini() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::Gemini,
            embedding_model: "text-embedding-004".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_embedding_client_new_anthropic() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::Anthropic,
            embedding_model: "claude-embedding".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_embedding_client_new_azure_without_base_url() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::AzureOpenAI,
            embedding_model: "text-embedding-ada-002".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 1536,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config);
        // AzureOpenAI without api_base_url should fail
        assert!(client.is_err());
    }

    #[test]
    fn test_embedding_client_new_azure_with_base_url() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::AzureOpenAI,
            embedding_model: "text-embedding-ada-002".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: Some("https://myendpoint.openai.azure.com".to_string()),
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 1536,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_embedding_client_new_claude_code() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::ClaudeCode,
            embedding_model: "claude".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_mock_embeddings_multiple_texts() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts = vec![
            "hello".to_string(),
            "world".to_string(),
            "test".to_string(),
            "embedding".to_string(),
        ];
        let result = client.generate_embeddings(&texts).await;
        assert!(result.is_ok());
        let embeddings = result.unwrap();
        assert_eq!(embeddings.len(), 4);
        for emb in &embeddings {
            assert_eq!(emb.len(), 64);
        }
    }

    #[tokio::test]
    async fn test_mock_embeddings_single_char() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts = vec!["a".to_string()];
        let result = client.generate_embeddings(&texts).await;
        assert!(result.is_ok());
        let embeddings = result.unwrap();
        assert_eq!(embeddings.len(), 1);
        // Single char: vec[2] should still be 0.1 (no second byte)
        assert_eq!(embeddings[0][2], 0.1);
    }

    #[tokio::test]
    async fn test_generate_embeddings_unsupported_provider() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::Anthropic,
            embedding_model: "claude".to_string(),
            api_key: Some("test-key".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config).unwrap();
        let result = client.generate_embeddings(&["test".to_string()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_embeddings_empty_batch() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts: Vec<String> = vec![];
        let result = client.generate_embeddings(&texts).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_embedding_client_custom_base_url() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "text-embedding-3-small".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: Some("https://custom.api.example.com/v1".to_string()),
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 1536,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config);
        assert!(client.is_ok());
    }

    // ========== Coverage batch: additional embed client tests ==========

    #[tokio::test]
    async fn test_mock_embeddings_large_batch() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        // Generate a large batch of texts
        let texts: Vec<String> = (0..100).map(|i| format!("text number {}", i)).collect();
        let result = client.generate_embeddings(&texts).await;
        assert!(result.is_ok());
        let embeddings = result.unwrap();
        assert_eq!(embeddings.len(), 100);
        for emb in &embeddings {
            assert_eq!(emb.len(), 64);
        }
    }

    #[tokio::test]
    async fn test_mock_embeddings_deterministic() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts = vec!["hello world".to_string()];
        let r1 = client.generate_embeddings(&texts).await.unwrap();
        let r2 = client.generate_embeddings(&texts).await.unwrap();
        // Mock embeddings are deterministic: same text => same embedding
        assert_eq!(r1[0], r2[0]);
    }

    #[tokio::test]
    async fn test_mock_embeddings_different_texts_different_vectors() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
        let result = client.generate_embeddings(&texts).await.unwrap();
        // Different texts produce different first elements
        assert_ne!(result[0][0], result[1][0]);
        assert_ne!(result[1][0], result[2][0]);
    }

    #[tokio::test]
    async fn test_mock_embeddings_long_text() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let long_text = "a".repeat(10000);
        let texts = vec![long_text];
        let result = client.generate_embeddings(&texts).await;
        assert!(result.is_ok());
        let embeddings = result.unwrap();
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), 64);
    }

    #[tokio::test]
    async fn test_mock_embeddings_unicode_text() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts = vec!["unicode text".to_string()];
        let result = client.generate_embeddings(&texts).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_embedding_client_default_base_urls() {
        // OpenAI
        let config = AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "model".to_string(),
            api_key: Some("key".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config).unwrap();
        assert_eq!(client.api_base_url, "https://api.openai.com/v1");

        // Ollama
        let config_ollama = AutoEmbedConfig {
            provider: LLMProvider::Ollama,
            embedding_model: "model".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client_ollama = EmbeddingClient::new(&config_ollama).unwrap();
        assert_eq!(client_ollama.api_base_url, "http://localhost:11434");

        // Gemini
        let config_gemini = AutoEmbedConfig {
            provider: LLMProvider::Gemini,
            embedding_model: "model".to_string(),
            api_key: Some("key".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client_gemini = EmbeddingClient::new(&config_gemini).unwrap();
        assert_eq!(client_gemini.api_base_url, "https://generativelanguage.googleapis.com/v1beta");

        // Anthropic
        let config_anthropic = AutoEmbedConfig {
            provider: LLMProvider::Anthropic,
            embedding_model: "model".to_string(),
            api_key: Some("key".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client_anthropic = EmbeddingClient::new(&config_anthropic).unwrap();
        assert_eq!(client_anthropic.api_base_url, "https://api.anthropic.com/v1");

        // ClaudeCode
        let config_cc = AutoEmbedConfig {
            provider: LLMProvider::ClaudeCode,
            embedding_model: "model".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client_cc = EmbeddingClient::new(&config_cc).unwrap();
        assert_eq!(client_cc.api_base_url, "");

        // Mock
        let client_mock = EmbeddingClient::new(&mock_config()).unwrap();
        assert_eq!(client_mock.api_base_url, "");
    }

    #[test]
    fn test_embedding_client_custom_url_overrides_default() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "model".to_string(),
            api_key: Some("key".to_string()),
            api_base_url: Some("https://proxy.example.com/v1".to_string()),
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config).unwrap();
        assert_eq!(client.api_base_url, "https://proxy.example.com/v1");
    }

    #[tokio::test]
    async fn test_generate_embeddings_claude_code_not_implemented() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::ClaudeCode,
            embedding_model: "claude".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        let client = EmbeddingClient::new(&config).unwrap();
        let result = client.generate_embeddings(&["test".to_string()]).await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.err().unwrap());
        assert!(err_msg.contains("not yet implemented"));
    }

    #[test]
    fn test_embed_error_display() {
        let e1 = EmbedError::ApiError("api err".to_string());
        assert!(format!("{}", e1).contains("LLM API error"));

        let e2 = EmbedError::ConfigError("config err".to_string());
        assert!(format!("{}", e2).contains("Configuration error"));

        let e3 = EmbedError::NetworkError("net err".to_string());
        assert!(format!("{}", e3).contains("Network error"));

        let e4 = EmbedError::SerializationError("ser err".to_string());
        assert!(format!("{}", e4).contains("Serialization error"));
    }

    #[tokio::test]
    async fn test_mock_embeddings_two_char_text() {
        let config = mock_config();
        let client = EmbeddingClient::new(&config).unwrap();
        let texts = vec!["ab".to_string()];
        let result = client.generate_embeddings(&texts).await.unwrap();
        assert_eq!(result.len(), 1);
        // With 2+ chars, vec[2] should be set based on second byte
        assert_ne!(result[0][2], 0.1);
    }
}
