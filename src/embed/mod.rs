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
