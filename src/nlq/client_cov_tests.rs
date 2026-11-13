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

// ------------------------------------------------- token usage (COST-07)

/// What a provider said its call used, as a stub would send it.
const PROMPT_TOKENS: u64 = 412;
const COMPLETION_TOKENS: u64 = 17;
const TOTAL_TOKENS: u64 = 429;

fn reported() -> TokenUsage {
    TokenUsage {
        prompt_tokens: Some(PROMPT_TOKENS),
        completion_tokens: Some(COMPLETION_TOKENS),
        total_tokens: Some(TOTAL_TOKENS),
    }
}

#[tokio::test]
async fn openai_usage_is_kept_not_dropped() {
    // The stub CH-AI-COST used: the engine parsed `choices` only and threw
    // `usage` away (#1790).
    let body = serde_json::json!({
        "choices": [ { "message": { "content": "MATCH (n) RETURN n" } } ],
        "usage": {
            "prompt_tokens": PROMPT_TOKENS,
            "completion_tokens": COMPLETION_TOKENS,
            "total_tokens": TOTAL_TOKENS
        }
    })
    .to_string();
    let srv = MockHttp::serve(vec![(200, body)]);
    let client = NLQClient::new(&cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();
    let c = client.complete("q").await.unwrap();
    assert_eq!(c.text, "MATCH (n) RETURN n");
    assert_eq!(c.usage, reported());
    assert!(c.latency > Duration::ZERO);
}

#[tokio::test]
async fn openai_without_usage_reports_none_rather_than_zero() {
    let srv = MockHttp::serve(vec![(200, openai_ok("x"))]);
    let client = NLQClient::new(&cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();
    let c = client.complete("q").await.unwrap();
    assert_eq!(c.usage, TokenUsage::default());
    assert!(!c.usage.is_reported());
}

#[tokio::test]
async fn ollama_eval_counts_are_its_usage_and_their_sum_its_total() {
    let body = format!(
        r#"{{"response": "RETURN 1", "prompt_eval_count": {PROMPT_TOKENS}, "eval_count": {COMPLETION_TOKENS}}}"#
    );
    let srv = MockHttp::serve(vec![(200, body)]);
    let client = NLQClient::new(&cfg(LLMProvider::Ollama, &srv.base_url, None, None)).unwrap();
    assert_eq!(client.complete("q").await.unwrap().usage, reported());
}

#[tokio::test]
async fn ollama_with_a_cached_prompt_has_no_total_to_invent() {
    // Ollama leaves `prompt_eval_count` out when the prompt was cached; the
    // total is then unknown, not the completion count alone.
    let body = format!(r#"{{"response": "RETURN 1", "eval_count": {COMPLETION_TOKENS}}}"#);
    let srv = MockHttp::serve(vec![(200, body)]);
    let client = NLQClient::new(&cfg(LLMProvider::Ollama, &srv.base_url, None, None)).unwrap();
    let usage = client.complete("q").await.unwrap().usage;
    assert_eq!(usage.completion_tokens, Some(COMPLETION_TOKENS));
    assert_eq!(usage.prompt_tokens, None);
    assert_eq!(usage.total_tokens, None);
}

#[tokio::test]
async fn gemini_usage_metadata_is_its_usage() {
    let body = serde_json::json!({
        "candidates": [ { "content": { "role": "model", "parts": [ { "text": "RETURN 1" } ] } } ],
        "usageMetadata": {
            "promptTokenCount": PROMPT_TOKENS,
            "candidatesTokenCount": COMPLETION_TOKENS,
            "totalTokenCount": TOTAL_TOKENS
        }
    })
    .to_string();
    let srv = MockHttp::serve(vec![(200, body)]);
    let client = NLQClient::new(&cfg(LLMProvider::Gemini, &srv.base_url, Some("g"), None)).unwrap();
    assert_eq!(client.complete("q").await.unwrap().usage, reported());
}

#[tokio::test]
async fn the_mock_calls_no_model_and_reports_no_usage() {
    let client = NLQClient::new(&cfg(LLMProvider::Mock, "", None, None)).unwrap();
    assert!(!client.complete("q").await.unwrap().usage.is_reported());
}

#[tokio::test]
async fn every_call_lands_in_the_metrics_failed_ones_too() {
    use crate::nlq::metrics::snapshot;
    let ok = MockHttp::serve(vec![(
        200,
        serde_json::json!({
            "choices": [ { "message": { "content": "x" } } ],
            "usage": { "prompt_tokens": PROMPT_TOKENS, "completion_tokens": COMPLETION_TOKENS }
        })
        .to_string(),
    )]);
    let before = snapshot();
    NLQClient::new(&cfg(LLMProvider::OpenAI, &ok.base_url, Some("k"), None))
        .unwrap()
        .complete("q")
        .await
        .unwrap();
    let after = snapshot();
    // Other tests in this binary make calls too, so these are lower bounds.
    assert!(after.calls > before.calls);
    assert!(after.prompt_tokens >= before.prompt_tokens + PROMPT_TOKENS);
    assert!(after.completion_tokens >= before.completion_tokens + COMPLETION_TOKENS);

    let failing = MockHttp::serve(vec![(500, String::new())]);
    let before = snapshot();
    NLQClient::new(&cfg(
        LLMProvider::OpenAI,
        &failing.base_url,
        Some("k"),
        None,
    ))
    .unwrap()
    .complete("q")
    .await
    .unwrap_err();
    assert!(snapshot().failed_calls > before.failed_calls);
}

#[test]
fn a_total_is_the_providers_or_the_sum_of_both_counts() {
    assert_eq!(
        TokenUsage::from_counts(Some(1), Some(2), Some(9)).total_tokens,
        Some(9)
    );
    assert_eq!(
        TokenUsage::from_counts(Some(1), Some(2), None).total_tokens,
        Some(3)
    );
    assert_eq!(
        TokenUsage::from_counts(None, Some(2), None).total_tokens,
        None
    );
    assert!(TokenUsage::from_counts(None, None, Some(4)).is_reported());
    assert!(!TokenUsage::from_counts(None, None, None).is_reported());
}

#[test]
fn usage_serialises_with_null_for_what_was_not_reported() {
    let json = serde_json::to_value(TokenUsage::from_counts(None, Some(2), None)).unwrap();
    assert_eq!(
        json,
        serde_json::json!({ "prompt_tokens": null, "completion_tokens": 2, "total_tokens": null })
    );
}

// ------------------------------------------- schema-constrained decoding
//
// `EVAL-09` (#1743) asks for output constrained by a schema rather than
// requested in prose. These tests assert the *payload* each provider is
// sent, because that is the whole of what this layer does: whether the
// model then obeys is the provider's business, and validating the answer is
// #2130's `SignatureRunner`. Each provider is checked both ways -- with a
// schema and without -- since a request that quietly carries a schema field
// when none was asked for would change every existing call.

fn schema() -> ResponseSchema {
    ResponseSchema {
        name: "cypher_answer".to_string(),
        schema: serde_json::json!({
            "type": "object",
            "properties": { "cypher": { "type": "string" } },
            "required": ["cypher"],
            "additionalProperties": false,
        }),
    }
}

#[tokio::test]
async fn openai_sends_the_schema_as_a_strict_json_schema() {
    let srv = MockHttp::serve(vec![(200, openai_ok("RETURN 1"))]);
    let client =
        NLQClient::new(&cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();

    let out = client
        .complete_structured("q", Some(&schema()))
        .await
        .unwrap();
    assert!(out.schema_enforced, "the schema went out, so it is enforced");

    let body = srv.requests()[0].json();
    assert_eq!(body["response_format"]["type"], "json_schema");
    assert_eq!(body["response_format"]["json_schema"]["name"], "cypher_answer");
    // `strict` is what constrains the decode; without it the schema is a
    // suggestion and `EVAL-09` is not satisfied.
    assert_eq!(body["response_format"]["json_schema"]["strict"], true);
    assert_eq!(
        body["response_format"]["json_schema"]["schema"]["required"][0],
        "cypher"
    );
}

#[tokio::test]
async fn openai_omits_response_format_when_no_schema_is_asked_for() {
    let srv = MockHttp::serve(vec![(200, openai_ok("RETURN 1"))]);
    let client =
        NLQClient::new(&cfg(LLMProvider::OpenAI, &srv.base_url, Some("k"), None)).unwrap();

    let out = client.complete("q").await.unwrap();
    assert!(!out.schema_enforced);
    assert!(
        srv.requests()[0].json().get("response_format").is_none(),
        "an unconstrained request must be what it was before this change"
    );
}

#[tokio::test]
async fn ollama_sends_the_schema_as_format() {
    let srv = MockHttp::serve(vec![(200, r#"{"response": "RETURN 1"}"#.to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::Ollama, &srv.base_url, None, None)).unwrap();

    let out = client
        .complete_structured("q", Some(&schema()))
        .await
        .unwrap();
    assert!(out.schema_enforced);

    let body = srv.requests()[0].json();
    // Ollama takes the schema itself, with no wrapper and no name.
    assert_eq!(body["format"]["type"], "object");
    assert_eq!(body["format"]["required"][0], "cypher");
}

#[tokio::test]
async fn ollama_omits_format_when_no_schema_is_asked_for() {
    let srv = MockHttp::serve(vec![(200, r#"{"response": ""}"#.to_string())]);
    let client = NLQClient::new(&cfg(LLMProvider::Ollama, &srv.base_url, None, None)).unwrap();

    assert!(!client.complete("q").await.unwrap().schema_enforced);
    assert!(srv.requests()[0].json().get("format").is_none());
}

#[tokio::test]
async fn gemini_sends_both_the_mime_type_and_the_schema() {
    let srv = MockHttp::serve(vec![(
        200,
        r#"{"candidates":[{"content":{"parts":[{"text":"RETURN 1"}]}}]}"#.to_string(),
    )]);
    let client =
        NLQClient::new(&cfg(LLMProvider::Gemini, &srv.base_url, Some("k"), None)).unwrap();

    let out = client
        .complete_structured("q", Some(&schema()))
        .await
        .unwrap();
    assert!(out.schema_enforced);

    let gc = &srv.requests()[0].json()["generationConfig"];
    // Gemini constrains the decode only when BOTH are set: the schema alone
    // is ignored without the MIME type, which is the easy half to get wrong.
    assert_eq!(gc["responseMimeType"], "application/json");
    assert_eq!(gc["responseSchema"]["type"], "object");
    assert_eq!(gc["temperature"], 0.0);
}

#[tokio::test]
async fn gemini_omits_both_when_no_schema_is_asked_for() {
    let srv = MockHttp::serve(vec![(
        200,
        r#"{"candidates":[{"content":{"parts":[{"text":"x"}]}}]}"#.to_string(),
    )]);
    let client =
        NLQClient::new(&cfg(LLMProvider::Gemini, &srv.base_url, Some("k"), None)).unwrap();

    assert!(!client.complete("q").await.unwrap().schema_enforced);
    let gc = &srv.requests()[0].json()["generationConfig"];
    assert!(gc.get("responseMimeType").is_none());
    assert!(gc.get("responseSchema").is_none());
}

#[tokio::test]
async fn a_provider_that_cannot_take_a_schema_answers_anyway_and_says_so() {
    // The mock calls no model and has no request body to carry a schema.
    // Asking for one must not fail the call -- a caller that validates its
    // own output still works -- but it must not claim the guarantee either.
    let client = NLQClient::new(&cfg(LLMProvider::Mock, "http://unused", None, None)).unwrap();

    let out = client
        .complete_structured("q", Some(&schema()))
        .await
        .unwrap();
    assert_eq!(out.text, "MATCH (n) RETURN n LIMIT 10");
    assert!(
        !out.schema_enforced,
        "asking for a schema a provider cannot take must report unenforced, \
         not silently pass for enforced"
    );
}

#[test]
fn only_the_http_providers_can_be_asked_for_a_schema() {
    assert!(supports_response_schema(&LLMProvider::OpenAI));
    assert!(supports_response_schema(&LLMProvider::Ollama));
    assert!(supports_response_schema(&LLMProvider::Gemini));
    assert!(!supports_response_schema(&LLMProvider::ClaudeCode));
    assert!(!supports_response_schema(&LLMProvider::Mock));
}
