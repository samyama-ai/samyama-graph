//! Request-building, response-parsing and error-path tests for the HTTP
//! embedding providers, against a canned server on 127.0.0.1.

use super::*;
use crate::embed::EmbedPipeline;
use crate::nlq::test_http::{dead_base_url, MockHttp};
use std::collections::HashMap;

fn cfg(
    provider: LLMProvider,
    base: Option<&str>,
    key: Option<&str>,
    dim: usize,
) -> AutoEmbedConfig {
    AutoEmbedConfig {
        provider,
        embedding_model: "emb-model".to_string(),
        api_key: key.map(str::to_string),
        api_base_url: base.map(str::to_string),
        chunk_size: 512,
        chunk_overlap: 64,
        vector_dimension: dim,
        embedding_policies: HashMap::new(),
        embedding_property: "embedding".to_string(),
    }
}

fn texts(xs: &[&str]) -> Vec<String> {
    xs.iter().map(|s| s.to_string()).collect()
}

#[test]
fn zero_vector_dimension_means_no_dimensions_field() {
    let c = EmbeddingClient::new(&cfg(LLMProvider::OpenAI, None, Some("k"), 0)).unwrap();
    assert_eq!(c.dimensions, None);
    let c = EmbeddingClient::new(&cfg(LLMProvider::OpenAI, None, Some("k"), 256)).unwrap();
    assert_eq!(c.dimensions, Some(256));
}

#[test]
fn azure_without_base_url_names_the_missing_setting() {
    let err = EmbeddingClient::new(&cfg(LLMProvider::AzureOpenAI, None, Some("k"), 8))
        .err()
        .expect("azure without a base url must be refused");
    assert!(
        matches!(err, EmbedError::ConfigError(ref m) if m.contains("api_base_url")),
        "{err:?}"
    );
}

#[test]
fn ollama_without_base_url_is_accepted_with_local_default() {
    let c = EmbeddingClient::new(&cfg(LLMProvider::Ollama, None, None, 8)).unwrap();
    assert_eq!(c.api_base_url, "http://localhost:11434");
    assert_eq!(c.model, "emb-model");
}

// ---------------------------------------------------------------- OpenAI

#[tokio::test]
async fn openai_sends_batch_with_dimensions_and_parses_vectors() {
    let srv = MockHttp::serve(vec![(
        200,
        r#"{"data": [{"embedding": [0.5, 1.0]}, {"embedding": [-1.0, 2.5]}]}"#.to_string(),
    )]);
    let c = EmbeddingClient::new(&cfg(
        LLMProvider::OpenAI,
        Some(&srv.base_url),
        Some("sk-1"),
        2,
    ))
    .unwrap();
    let out = c.generate_embeddings(&texts(&["a", "b"])).await.unwrap();
    assert_eq!(out, vec![vec![0.5, 1.0], vec![-1.0, 2.5]]);

    let r = &srv.requests()[0];
    assert_eq!(r.method, "POST");
    assert_eq!(r.path, "/embeddings");
    assert_eq!(r.header("authorization"), Some("Bearer sk-1"));
    let body = r.json();
    assert_eq!(body["model"], "emb-model");
    assert_eq!(body["input"], serde_json::json!(["a", "b"]));
    assert_eq!(body["dimensions"], 2);
}

#[tokio::test]
async fn openai_omits_dimensions_when_zero() {
    let srv = MockHttp::serve(vec![(200, r#"{"data": []}"#.to_string())]);
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::OpenAI, Some(&srv.base_url), Some("k"), 0)).unwrap();
    assert!(c
        .generate_embeddings(&texts(&["x"]))
        .await
        .unwrap()
        .is_empty());
    let body = srv.requests()[0].json();
    assert!(
        body.get("dimensions").is_none(),
        "dimensions should be skipped: {body}"
    );
}

#[tokio::test]
async fn openai_without_api_key_is_a_config_error() {
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::OpenAI, Some(&dead_base_url()), None, 4)).unwrap();
    let err = c.generate_embeddings(&texts(&["x"])).await.unwrap_err();
    assert!(
        matches!(err, EmbedError::ConfigError(ref m) if m.contains("OpenAI requires API key")),
        "{err:?}"
    );
}

#[tokio::test]
async fn openai_error_status_carries_the_body() {
    let srv = MockHttp::serve(vec![(401, r#"{"error":"bad key"}"#.to_string())]);
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::OpenAI, Some(&srv.base_url), Some("k"), 4)).unwrap();
    let err = c.generate_embeddings(&texts(&["x"])).await.unwrap_err();
    assert!(
        matches!(err, EmbedError::ApiError(ref m) if m.contains("OpenAI returned error") && m.contains("bad key")),
        "{err:?}"
    );
}

#[tokio::test]
async fn openai_malformed_body_is_a_serialization_error() {
    let srv = MockHttp::serve(vec![(
        200,
        r#"{"data": [{"embedding": "no"}]}"#.to_string(),
    )]);
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::OpenAI, Some(&srv.base_url), Some("k"), 4)).unwrap();
    let err = c.generate_embeddings(&texts(&["x"])).await.unwrap_err();
    assert!(matches!(err, EmbedError::SerializationError(_)), "{err:?}");
}

#[tokio::test]
async fn openai_unreachable_endpoint_is_a_network_error() {
    let c = EmbeddingClient::new(&cfg(
        LLMProvider::OpenAI,
        Some(&dead_base_url()),
        Some("k"),
        4,
    ))
    .unwrap();
    let err = c.generate_embeddings(&texts(&["x"])).await.unwrap_err();
    assert!(matches!(err, EmbedError::NetworkError(_)), "{err:?}");
}

// ---------------------------------------------------------------- Ollama

#[tokio::test]
async fn ollama_sends_one_request_per_text_in_order() {
    let srv = MockHttp::serve(vec![
        (200, r#"{"embedding": [1.0]}"#.to_string()),
        (200, r#"{"embedding": [2.0, 3.0]}"#.to_string()),
    ]);
    let c = EmbeddingClient::new(&cfg(LLMProvider::Ollama, Some(&srv.base_url), None, 0)).unwrap();
    let out = c
        .generate_embeddings(&texts(&["first", "second"]))
        .await
        .unwrap();
    assert_eq!(out, vec![vec![1.0], vec![2.0, 3.0]]);

    let reqs = srv.requests();
    assert_eq!(reqs.len(), 2);
    for (r, want) in reqs.iter().zip(["first", "second"]) {
        assert_eq!(r.path, "/api/embeddings");
        let b = r.json();
        assert_eq!(b["model"], "emb-model");
        assert_eq!(b["prompt"], want);
    }
}

#[tokio::test]
async fn ollama_empty_batch_sends_nothing() {
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::Ollama, Some(&dead_base_url()), None, 0)).unwrap();
    assert!(c.generate_embeddings(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn ollama_error_status_stops_the_batch() {
    let srv = MockHttp::serve(vec![(500, "model not found".to_string())]);
    let c = EmbeddingClient::new(&cfg(LLMProvider::Ollama, Some(&srv.base_url), None, 0)).unwrap();
    let err = c
        .generate_embeddings(&texts(&["a", "b"]))
        .await
        .unwrap_err();
    assert!(
        matches!(err, EmbedError::ApiError(ref m) if m.contains("Ollama returned error") && m.contains("model not found")),
        "{err:?}"
    );
    assert_eq!(srv.requests().len(), 1);
}

#[tokio::test]
async fn ollama_malformed_body_is_a_serialization_error() {
    let srv = MockHttp::serve(vec![(200, "{}".to_string())]);
    let c = EmbeddingClient::new(&cfg(LLMProvider::Ollama, Some(&srv.base_url), None, 0)).unwrap();
    let err = c.generate_embeddings(&texts(&["a"])).await.unwrap_err();
    assert!(matches!(err, EmbedError::SerializationError(_)), "{err:?}");
}

#[tokio::test]
async fn ollama_unreachable_endpoint_is_a_network_error() {
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::Ollama, Some(&dead_base_url()), None, 0)).unwrap();
    let err = c.generate_embeddings(&texts(&["a"])).await.unwrap_err();
    assert!(matches!(err, EmbedError::NetworkError(_)), "{err:?}");
}

// ---------------------------------------------------------------- Gemini

#[tokio::test]
async fn gemini_sends_batch_embed_request_and_parses_values() {
    let srv = MockHttp::serve(vec![(
        200,
        r#"{"embeddings": [{"values": [0.25]}, {"values": [0.75, 1.5]}]}"#.to_string(),
    )]);
    let c = EmbeddingClient::new(&cfg(
        LLMProvider::Gemini,
        Some(&srv.base_url),
        Some("gk"),
        0,
    ))
    .unwrap();
    let out = c.generate_embeddings(&texts(&["p", "q"])).await.unwrap();
    assert_eq!(out, vec![vec![0.25], vec![0.75, 1.5]]);

    let r = &srv.requests()[0];
    assert_eq!(r.path, "/models/emb-model:batchEmbedContents?key=gk");
    let b = r.json();
    assert_eq!(b["requests"].as_array().unwrap().len(), 2);
    assert_eq!(b["requests"][0]["model"], "models/emb-model");
    assert_eq!(b["requests"][0]["content"]["parts"][0]["text"], "p");
    assert_eq!(b["requests"][1]["content"]["parts"][0]["text"], "q");
}

#[tokio::test]
async fn gemini_without_api_key_is_a_config_error() {
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::Gemini, Some(&dead_base_url()), None, 0)).unwrap();
    let err = c.generate_embeddings(&texts(&["x"])).await.unwrap_err();
    assert!(
        matches!(err, EmbedError::ConfigError(ref m) if m.contains("Gemini requires API key")),
        "{err:?}"
    );
}

#[tokio::test]
async fn gemini_error_status_carries_the_body() {
    let srv = MockHttp::serve(vec![(429, "slow down".to_string())]);
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::Gemini, Some(&srv.base_url), Some("k"), 0)).unwrap();
    let err = c.generate_embeddings(&texts(&["x"])).await.unwrap_err();
    assert!(
        matches!(err, EmbedError::ApiError(ref m) if m.contains("Gemini returned error") && m.contains("slow down")),
        "{err:?}"
    );
}

#[tokio::test]
async fn gemini_malformed_body_is_a_serialization_error() {
    let srv = MockHttp::serve(vec![(200, r#"{"embedding": []}"#.to_string())]);
    let c =
        EmbeddingClient::new(&cfg(LLMProvider::Gemini, Some(&srv.base_url), Some("k"), 0)).unwrap();
    let err = c.generate_embeddings(&texts(&["x"])).await.unwrap_err();
    assert!(matches!(err, EmbedError::SerializationError(_)), "{err:?}");
}

#[tokio::test]
async fn gemini_unreachable_endpoint_is_a_network_error() {
    let c = EmbeddingClient::new(&cfg(
        LLMProvider::Gemini,
        Some(&dead_base_url()),
        Some("k"),
        0,
    ))
    .unwrap();
    let err = c.generate_embeddings(&texts(&["x"])).await.unwrap_err();
    assert!(matches!(err, EmbedError::NetworkError(_)), "{err:?}");
}

// ---------------------------------------------------------------- pipeline

#[tokio::test]
async fn pipeline_chunks_text_and_pairs_each_chunk_with_its_embedding() {
    let srv = MockHttp::serve(vec![(
        200,
        r#"{"data": [{"embedding": [1.0]}, {"embedding": [2.0]}, {"embedding": [3.0]}]}"#
            .to_string(),
    )]);
    let mut config = cfg(LLMProvider::OpenAI, Some(&srv.base_url), Some("k"), 0);
    config.chunk_size = 4;
    config.chunk_overlap = 1;
    let p = EmbedPipeline::new(config).unwrap();
    assert_eq!(p.model_id(), "emb-model");
    let chunks = p.process_text("abcdefghij").await.unwrap();
    let got: Vec<(&str, f32, &str)> = chunks
        .iter()
        .map(|c| {
            (
                c.text.as_str(),
                c.embedding[0],
                c.metadata["chunk_index"].as_str(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![("abcd", 1.0, "0"), ("defg", 2.0, "1"), ("ghij", 3.0, "2")]
    );
    assert_eq!(
        srv.requests()[0].json()["input"],
        serde_json::json!(["abcd", "defg", "ghij"])
    );
}

#[tokio::test]
async fn pipeline_propagates_embedding_errors() {
    let srv = MockHttp::serve(vec![(500, "down".to_string())]);
    let p =
        EmbedPipeline::new(cfg(LLMProvider::OpenAI, Some(&srv.base_url), Some("k"), 0)).unwrap();
    let err = p.process_text("hello").await.unwrap_err();
    assert!(matches!(err, EmbedError::ApiError(_)), "{err:?}");
}

#[test]
fn pipeline_new_propagates_client_config_errors() {
    let err = EmbedPipeline::new(cfg(LLMProvider::AzureOpenAI, None, Some("k"), 0))
        .err()
        .expect("azure without base url");
    assert!(matches!(err, EmbedError::ConfigError(_)));
}

#[tokio::test]
async fn pipeline_splits_non_ascii_text_without_panicking() {
    let mut config = cfg(LLMProvider::Mock, None, None, 0);
    config.chunk_size = 3;
    config.chunk_overlap = 0;
    let p = EmbedPipeline::new(config).unwrap();
    // "é" is two bytes: a 3-byte chunk boundary falls inside the second one.
    let chunks = p.process_text("éééé").await.unwrap();
    let joined: String = chunks.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(joined, "éééé");

    // A character wider than the chunk is a chunk of its own (#1573).
    let mut config = cfg(LLMProvider::Mock, None, None, 0);
    config.chunk_size = 2;
    config.chunk_overlap = 1;
    let p = EmbedPipeline::new(config).unwrap();
    let chunks = p.process_text("a😀b").await.unwrap();
    let texts: Vec<&str> = chunks.iter().map(|c| c.text.as_str()).collect();
    assert_eq!(texts, ["a", "😀", "b"]);
}
