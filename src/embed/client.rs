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
            }
        });

        if (config.provider == LLMProvider::AzureOpenAI || config.provider == LLMProvider::Ollama) && config.api_base_url.is_none() && config.provider == LLMProvider::AzureOpenAI {
             return Err(EmbedError::ConfigError("AzureOpenAI requires api_base_url".to_string()));
        }

        Ok(Self {
            client,
            provider: config.provider.clone(),
            model: config.embedding_model.clone(),
            api_key: config.api_key.clone(),
            api_base_url,
        })
    }

    /// Generate embeddings for a batch of texts
    pub async fn generate_embeddings(&self, texts: &[String]) -> EmbedResult<Vec<Vec<f32>>> {
        match self.provider {
            LLMProvider::OpenAI => self.openai_embeddings(texts).await,
            LLMProvider::Ollama => self.ollama_embeddings(texts).await,
            LLMProvider::Gemini => self.gemini_embeddings(texts).await,
            _ => Err(EmbedError::ConfigError(format!("Provider {:?} not yet implemented", self.provider))),
        }
    }

    async fn openai_embeddings(&self, texts: &[String]) -> EmbedResult<Vec<Vec<f32>>> {
        #[derive(Serialize)]
        struct OpenAIRequest<'a> {
            input: &'a [String],
            model: &'a str,
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
