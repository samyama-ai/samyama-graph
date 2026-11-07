//! `EmbeddedClient::nlq` and `nlq_with`: natural language to Cypher with the
//! graph in-process, no server (#2092).
//!
//! The provider is either `mock`, which answers a fixed query, or a stand-in
//! for OpenAI listening on 127.0.0.1, so a test can see what the model is
//! sent: the question, this graph's schema, the model and the key. No test
//! here reaches a real provider.
//!
//! Its own test binary, because one test sets `NLQ_PROVIDER`.

use std::sync::{Arc, Mutex};

use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
use samyama_sdk::{EmbeddedClient, NlqSettings, SamyamaClient, SamyamaError};
use serde_json::{json, Value};

/// Each request the stand-in was sent: its `Authorization` header and body.
type Seen = Arc<Mutex<Vec<(Option<String>, Value)>>>;

/// What the stand-in provider was sent, and the text it answers with.
#[derive(Clone)]
struct FakeOpenAi {
    answer: &'static str,
    seen: Seen,
}

async fn chat_completions(
    State(fake): State<FakeOpenAi>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    fake.seen.lock().unwrap().push((auth, body));
    Json(json!({
        "choices": [{ "message": { "content": fake.answer } }],
        "usage": { "prompt_tokens": 120, "completion_tokens": 9, "total_tokens": 129 }
    }))
}

/// Start the stand-in; returns its base URL and what it records.
async fn fake_openai(answer: &'static str) -> (String, Seen) {
    let fake = FakeOpenAi { answer, seen: Arc::default() };
    let seen = fake.seen.clone();
    let app = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    (format!("http://{addr}/v1"), seen)
}

async fn people() -> EmbeddedClient {
    let c = EmbeddedClient::new();
    for q in [
        "CREATE (:Person {name: 'Alice', age: 30})",
        "CREATE (:Person {name: 'Bob', age: 25})",
        "CREATE (:City {name: 'Pune'})",
        "MATCH (a:Person {name: 'Alice'}), (c:City) CREATE (a)-[:LIVES_IN]->(c)",
    ] {
        c.query("default", q).await.expect(q);
    }
    c
}

fn openai(base: &str, key: &str, model: &str) -> NlqSettings {
    NlqSettings {
        provider: Some("openai".into()),
        model: Some(model.into()),
        api_key: Some(key.into()),
        api_base_url: Some(base.into()),
    }
}

fn mock() -> NlqSettings {
    NlqSettings { provider: Some("mock".into()), ..Default::default() }
}

#[tokio::test]
async fn mock_translation_comes_back_and_runs() {
    let client = people().await;
    let cypher = client.nlq_with("Show me some nodes", &mock()).await.expect("nlq");
    assert_eq!(cypher, "MATCH (n) RETURN n LIMIT 10");
    let result = client.query_readonly("default", &cypher).await.expect("run");
    assert_eq!(result.records.len(), 3);
}

#[tokio::test]
async fn the_model_is_sent_the_question_the_schema_the_model_and_the_key() {
    let (base, seen) =
        fake_openai("```cypher\nMATCH (p:Person) RETURN p.name ORDER BY p.name\n```").await;
    let client = people().await;

    let cypher = client
        .nlq_with("Who are the people?", &openai(&base, "sk-test-2092", "gpt-test"))
        .await
        .expect("nlq");
    // The fence is stripped, as it is on the server.
    assert_eq!(cypher, "MATCH (p:Person) RETURN p.name ORDER BY p.name");

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "one model call");
    let (auth, body) = &seen[0];
    assert_eq!(auth.as_deref(), Some("Bearer sk-test-2092"));
    assert_eq!(body["model"], "gpt-test");
    let prompt = body["messages"][1]["content"].as_str().expect("user message");
    assert!(prompt.contains("Who are the people?"), "{prompt}");
    // This graph's schema, not an empty one: its labels and relationship.
    for word in ["Person", "City", "LIVES_IN"] {
        assert!(prompt.contains(word), "schema summary lacks {word}: {prompt}");
    }

    let rows = client.query_readonly("default", &cypher).await.expect("run").records;
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn a_query_that_writes_is_refused_and_the_graph_is_untouched() {
    let (base, _seen) = fake_openai("MATCH (n) DETACH DELETE n").await;
    let client = people().await;
    let e = client
        .nlq_with("Delete everything", &openai(&base, "sk-test", "gpt-test"))
        .await
        .unwrap_err();
    assert!(matches!(e, SamyamaError::QueryError(_)), "{e:?}");
    assert!(e.to_string().contains("write operations"), "{e}");
    assert_eq!(client.query_readonly("default", "MATCH (n) RETURN n").await.unwrap().records.len(), 3);
}

#[tokio::test]
async fn bad_settings_are_refused_before_any_model_call() {
    let (base, seen) = fake_openai("MATCH (n) RETURN n").await;
    let client = people().await;
    let no_key = NlqSettings {
        provider: Some("openai".into()),
        api_base_url: Some(base),
        // An empty key is no key: the server reads it the same way.
        api_key: Some("  ".into()),
        ..Default::default()
    };
    let e = client.nlq_with("q", &no_key).await.unwrap_err();
    assert!(e.to_string().contains("OPENAI_API_KEY"), "{e}");

    let unknown = NlqSettings { provider: Some("gpt".into()), ..Default::default() };
    let e = client.nlq_with("q", &unknown).await.unwrap_err();
    assert!(e.to_string().contains("unknown NLQ_PROVIDER \"gpt\""), "{e}");

    assert!(seen.lock().unwrap().is_empty(), "nothing was sent");
}

#[tokio::test]
async fn nlq_reads_the_provider_from_the_environment() {
    // The only test in this binary that touches the environment.
    std::env::set_var("NLQ_PROVIDER", "mock");
    let client = people().await;
    assert_eq!(client.nlq("anything").await.expect("nlq"), "MATCH (n) RETURN n LIMIT 10");
}
