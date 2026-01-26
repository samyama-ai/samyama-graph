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
