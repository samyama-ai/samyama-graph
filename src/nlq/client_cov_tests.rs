//! Request-building, response-parsing and error-path tests for the HTTP
//! providers of [`NLQClient`], against a canned server on 127.0.0.1.

use super::*;
use crate::nlq::test_http::{dead_base_url, MockHttp};
use crate::nlq::NLQPipeline;

fn cfg(provider: LLMProvider, base: &str, key: Option<&str>, system: Option<&str>) -> NLQConfig {
    NLQConfig {
        enabled: true,
        provider,
        model: "test-model".to_string(),
        api_key: key.map(str::to_string),
        api_base_url: Some(base.to_string()),
        system_prompt: system.map(str::to_string),
    }
}

fn openai_ok(content: &str) -> String {
    serde_json::json!({ "choices": [ { "message": { "content": content } } ] }).to_string()
}

// ---------------------------------------------------------------- OpenAI

#[tokio::test]
async fn openai_builds_chat_request_and_returns_first_choice() {
    let srv = MockHttp::serve(vec![(200, openai_ok("MATCH (n) RETURN n"))]);
    let client = NLQClient::new(&cfg(
        LLMProvider::OpenAI,
        &srv.base_url,
        Some("sk-abc"),
        None,
    ))
    .unwrap();

    let out = client.generate_cypher("who knows alice?").await.unwrap();
    assert_eq!(out, "MATCH (n) RETURN n");

    let reqs = srv.requests();
    assert_eq!(reqs.len(), 1);
    let r = &reqs[0];
    assert_eq!(r.method, "POST");
    assert_eq!(r.path, "/chat/completions");
    assert_eq!(r.header("authorization"), Some("Bearer sk-abc"));
    let body = r.json();
    assert_eq!(body["model"], "test-model");
    assert_eq!(body["temperature"], 0.0);
    assert_eq!(body["messages"][0]["role"], "system");
    assert_eq!(body["messages"][0]["content"], "You are a Cypher expert.");
    assert_eq!(body["messages"][1]["role"], "user");
    assert_eq!(body["messages"][1]["content"], "who knows alice?");
}

#[tokio::test]
async fn openai_sends_configured_system_prompt() {
    let srv = MockHttp::serve(vec![(200, openai_ok("x"))]);
    let client = NLQClient::new(&cfg(
        LLMProvider::OpenAI,
        &srv.base_url,
        Some("k"),
        Some("You are a domain expert."),
    ))
    .unwrap();
    client.generate_cypher("q").await.unwrap();
    let body = srv.requests()[0].json();
    assert_eq!(body["messages"][0]["content"], "You are a domain expert.");
}

#[tokio::test]
async fn openai_with_no_choices_returns_empty_string() {
    let srv = MockHttp::serve(vec![(200, r#"{"choices": []}"#.to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();
    assert_eq!(client.generate_cypher("q").await.unwrap(), "");
    assert_eq!(srv.requests().len(), 1);
}

#[tokio::test]
async fn openai_without_api_key_is_a_config_error_and_sends_nothing() {
    // Nothing is listening: had the client tried to send, this would be a
    // network error rather than a configuration one.
    let client = NLQClient::new(&cfg(LLMProvider::OpenAI, &dead_base_url(), None, None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(
        matches!(err, NLQError::ConfigError(ref m) if m.contains("OpenAI requires API key")),
        "{err:?}"
    );
}

#[tokio::test]
async fn openai_non_success_status_is_an_api_error_naming_the_status() {
    let srv = MockHttp::serve(vec![(500, r#"{"error":"boom"}"#.to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(
        matches!(err, NLQError::ApiError(ref m) if m.contains("OpenAI error") && m.contains("500")),
        "{err:?}"
    );
}

#[tokio::test]
async fn openai_malformed_body_is_a_serialization_error() {
    let srv = MockHttp::serve(vec![(200, r#"{"unexpected": true}"#.to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(matches!(err, NLQError::SerializationError(_)), "{err:?}");
}

#[tokio::test]
async fn openai_unreachable_endpoint_is_a_network_error() {
    let client =
        NLQClient::new(&cfg(LLMProvider::OpenAI, &dead_base_url(), Some("k"), None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(matches!(err, NLQError::NetworkError(_)), "{err:?}");
}

// ---------------------------------------------------------------- Ollama

#[tokio::test]
async fn ollama_builds_generate_request_and_returns_response_field() {
    let srv = MockHttp::serve(vec![(200, r#"{"response": "RETURN 1"}"#.to_string())]);
    let client =
        NLQClient::new(&cfg(LLMProvider::Ollama, &srv.base_url, None, Some("sys"))).unwrap();
    assert_eq!(client.generate_cypher("one?").await.unwrap(), "RETURN 1");

    let r = &srv.requests()[0];
    assert_eq!(r.path, "/api/generate");
    assert_eq!(r.header("authorization"), None);
    let body = r.json();
    assert_eq!(body["model"], "test-model");
    assert_eq!(body["prompt"], "one?");
    assert_eq!(body["system"], "sys");
    assert_eq!(body["stream"], false);
}

#[tokio::test]
async fn ollama_defaults_system_prompt() {
    let srv = MockHttp::serve(vec![(200, r#"{"response": ""}"#.to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::Ollama, &srv.base_url, None, None)).unwrap();
    assert_eq!(client.generate_cypher("q").await.unwrap(), "");
    assert_eq!(
        srv.requests()[0].json()["system"],
        "You are a Cypher expert."
    );
}

#[tokio::test]
async fn ollama_non_success_status_is_an_api_error() {
    let srv = MockHttp::serve(vec![(404, "{}".to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::Ollama, &srv.base_url, None, None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(
        matches!(err, NLQError::ApiError(ref m) if m.contains("Ollama error") && m.contains("404")),
        "{err:?}"
    );
}

#[tokio::test]
async fn ollama_malformed_body_is_a_serialization_error() {
    let srv = MockHttp::serve(vec![(200, "not json".to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::Ollama, &srv.base_url, None, None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(matches!(err, NLQError::SerializationError(_)), "{err:?}");
}

#[tokio::test]
async fn ollama_unreachable_endpoint_is_a_network_error() {
    let client = NLQClient::new(&cfg(LLMProvider::Ollama, &dead_base_url(), None, None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(matches!(err, NLQError::NetworkError(_)), "{err:?}");
}

// ---------------------------------------------------------------- Gemini

#[tokio::test]
async fn gemini_builds_generate_content_request_and_returns_first_part() {
    let body = serde_json::json!({
        "candidates": [ { "content": { "role": "model", "parts": [ { "text": "MATCH (a) RETURN a" }, { "text": "ignored" } ] } } ]
    })
    .to_string();
    let srv = MockHttp::serve(vec![(200, body)]);
    let client = NLQClient::new(&cfg(
        LLMProvider::Gemini,
        &srv.base_url,
        Some("gkey"),
        Some("SYS"),
    ))
    .unwrap();
    assert_eq!(
        client.generate_cypher("find a").await.unwrap(),
        "MATCH (a) RETURN a"
    );

    let r = &srv.requests()[0];
    assert_eq!(r.path, "/models/test-model:generateContent?key=gkey");
    let req = r.json();
    assert_eq!(req["contents"][0]["role"], "user");
    assert_eq!(
        req["contents"][0]["parts"][0]["text"],
        "SYS\n\nQuestion: find a"
    );
    assert_eq!(req["generationConfig"]["temperature"], 0.0);
}

#[tokio::test]
async fn gemini_without_candidates_returns_empty_string() {
    let srv = MockHttp::serve(vec![(200, "{}".to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::Gemini, &srv.base_url, Some("k"), None)).unwrap();
    assert_eq!(client.generate_cypher("q").await.unwrap(), "");
    // Default system prompt is prepended.
    let req = srv.requests()[0].json();
    assert_eq!(
        req["contents"][0]["parts"][0]["text"],
        "You are a Cypher expert.\n\nQuestion: q"
    );
}

#[tokio::test]
async fn gemini_with_empty_candidates_or_parts_returns_empty_string() {
    let srv = MockHttp::serve(vec![
        (200, r#"{"candidates": []}"#.to_string()),
        (
            200,
            r#"{"candidates": [{"content": {"role": null, "parts": []}}]}"#.to_string(),
        ),
    ]);
    let client = NLQClient::new(&cfg(LLMProvider::Gemini, &srv.base_url, Some("k"), None)).unwrap();
    assert_eq!(client.generate_cypher("q").await.unwrap(), "");
    assert_eq!(client.generate_cypher("q").await.unwrap(), "");
    assert_eq!(srv.requests().len(), 2);
}

#[tokio::test]
async fn gemini_without_api_key_is_a_config_error() {
    let client = NLQClient::new(&cfg(LLMProvider::Gemini, &dead_base_url(), None, None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(
        matches!(err, NLQError::ConfigError(ref m) if m.contains("Gemini requires API key")),
        "{err:?}"
    );
}

#[tokio::test]
async fn gemini_error_status_carries_the_response_body() {
    let srv = MockHttp::serve(vec![(400, r#"{"error":"quota exhausted"}"#.to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::Gemini, &srv.base_url, Some("k"), None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(
        matches!(err, NLQError::ApiError(ref m) if m.contains("Gemini error") && m.contains("quota exhausted")),
        "{err:?}"
    );
}

#[tokio::test]
async fn gemini_malformed_body_is_a_serialization_error() {
    let srv = MockHttp::serve(vec![(200, r#"{"candidates": "nope"}"#.to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::Gemini, &srv.base_url, Some("k"), None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(matches!(err, NLQError::SerializationError(_)), "{err:?}");
}

#[tokio::test]
async fn gemini_unreachable_endpoint_is_a_network_error() {
    let client =
        NLQClient::new(&cfg(LLMProvider::Gemini, &dead_base_url(), Some("k"), None)).unwrap();
    let err = client.generate_cypher("q").await.unwrap_err();
    assert!(matches!(err, NLQError::NetworkError(_)), "{err:?}");
}

// ---------------------------------------------------------------- pipeline

#[tokio::test]
async fn pipeline_extracts_fenced_cypher_from_the_model_answer() {
    let srv = MockHttp::serve(vec![(
        200,
        openai_ok("Here you go:\n```cypher\nMATCH (p:Person) RETURN p.name\n```"),
    )]);
    let pipeline =
        NLQPipeline::new(cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();
    let out = pipeline
        .text_to_cypher("names?", "(:Person {name})")
        .await
        .unwrap();
    assert_eq!(out, "MATCH (p:Person) RETURN p.name");

    let prompt = srv.requests()[0].json()["messages"][1]["content"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        prompt.contains("(:Person {name})"),
        "schema missing from prompt: {prompt}"
    );
    assert!(
        prompt.contains("Question: \"names?\""),
        "question missing from prompt: {prompt}"
    );
}

#[tokio::test]
async fn pipeline_rejects_a_write_query_from_the_model() {
    let srv = MockHttp::serve(vec![(200, openai_ok("MATCH (n) DETACH DELETE n"))]);
    let pipeline =
        NLQPipeline::new(cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();
    let err = pipeline.text_to_cypher("wipe it", "").await.unwrap_err();
    assert!(
        matches!(err, NLQError::ValidationError(ref m) if m.contains("write operations")),
        "{err:?}"
    );
}

#[tokio::test]
async fn pipeline_propagates_client_errors() {
    let srv = MockHttp::serve(vec![(503, "{}".to_string())]);
    let pipeline =
        NLQPipeline::new(cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();
    let err = pipeline.text_to_cypher("q", "").await.unwrap_err();
    assert!(matches!(err, NLQError::ApiError(_)), "{err:?}");
}
