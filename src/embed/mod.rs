//! Auto-Embed Pipelines
//!
//! Implements automatic embedding generation and text splitting for RAG applications.

pub mod client;

use serde::{Deserialize, Serialize};
use crate::persistence::tenant::AutoEmbedConfig;
use crate::graph::PropertyValue;
use thiserror::Error;

/// Embed errors
#[derive(Error, Debug)]
pub enum EmbedError {
    /// API error from LLM provider
    #[error("LLM API error: {0}")]
    ApiError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Network error
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Serialization/Deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),
}

pub type EmbedResult<T> = Result<T, EmbedError>;

/// A chunk of text with its embedding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextChunk {
    /// The text content
    pub text: String,
    /// The embedding vector
    pub embedding: Vec<f32>,
    /// Metadata about the chunk (e.g., offset, source)
    pub metadata: std::collections::HashMap<String, String>,
}

/// Pipeline for processing text into embeddings
pub struct EmbedPipeline {
    config: AutoEmbedConfig,
    client: client::EmbeddingClient,
}

impl EmbedPipeline {
    /// Create a new Embed pipeline from tenant config
    pub fn new(config: AutoEmbedConfig) -> EmbedResult<Self> {
        let client = client::EmbeddingClient::new(&config)?;
        Ok(Self { config, client })
    }

    /// Process text into one or more chunks with embeddings
    pub async fn process_text(&self, text: &str) -> EmbedResult<Vec<TextChunk>> {
        // 1. Split text into chunks
        let texts = self.split_text(text);
        
        // 2. Generate embeddings for chunks
        let embeddings = self.client.generate_embeddings(&texts).await?;
        
        // 3. Combine into TextChunks
        let mut chunks = Vec::new();
        for (i, (chunk_text, embedding)) in texts.into_iter().zip(embeddings.into_iter()).enumerate() {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("chunk_index".to_string(), i.to_string());
            
            chunks.push(TextChunk {
                text: chunk_text,
                embedding,
                metadata,
            });
        }
        
        Ok(chunks)
    }

    /// Simple character-based text splitter (place holder for more advanced recursive splitter)
    fn split_text(&self, text: &str) -> Vec<String> {
        if text.len() <= self.config.chunk_size {
            return vec![text.to_string()];
        }

        let mut chunks = Vec::new();
        let mut start = 0;
        
        while start < text.len() {
            let end = std::cmp::min(start + self.config.chunk_size, text.len());
            chunks.push(text[start..end].to_string());
            
            if end == text.len() {
                break;
            }
            
            start += self.config.chunk_size - self.config.chunk_overlap;
        }
        
        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn mock_config() -> AutoEmbedConfig {
        AutoEmbedConfig {
            provider: crate::persistence::tenant::LLMProvider::Mock,
            embedding_model: "mock".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 100,
            chunk_overlap: 20,
            vector_dimension: 64,
            embedding_policies: HashMap::new(),
            embedding_property: "embedding".to_string(),
        }
    }

    #[test]
    fn test_embed_pipeline_new() {
        let config = mock_config();
        let pipeline = EmbedPipeline::new(config);
        assert!(pipeline.is_ok());
    }

    #[test]
    fn test_split_text_short() {
        let config = mock_config();
        let pipeline = EmbedPipeline::new(config).unwrap();
        let chunks = pipeline.split_text("short text");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], "short text");
    }

    #[test]
    fn test_split_text_long() {
        let mut config = mock_config();
        config.chunk_size = 20;
        config.chunk_overlap = 5;
        let pipeline = EmbedPipeline::new(config).unwrap();
        let text = "This is a long text that should be split into multiple chunks for processing";
        let chunks = pipeline.split_text(text);
        assert!(chunks.len() > 1);
        // Each chunk should be <= chunk_size
        for chunk in &chunks {
            assert!(chunk.len() <= 20);
        }
    }

    #[tokio::test]
    async fn test_process_text_mock() {
        let config = mock_config();
        let pipeline = EmbedPipeline::new(config).unwrap();
        let result = pipeline.process_text("hello world").await;
        assert!(result.is_ok());
        let chunks = result.unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "hello world");
        assert_eq!(chunks[0].embedding.len(), 64);
        assert_eq!(chunks[0].metadata.get("chunk_index").unwrap(), "0");
    }

    #[test]
    fn test_text_chunk_struct() {
        let chunk = TextChunk {
            text: "hello".to_string(),
            embedding: vec![0.1, 0.2, 0.3],
            metadata: HashMap::from([("key".to_string(), "value".to_string())]),
        };
        assert_eq!(chunk.text, "hello");
        assert_eq!(chunk.embedding.len(), 3);
        assert_eq!(chunk.metadata.get("key").unwrap(), "value");
    }
}
