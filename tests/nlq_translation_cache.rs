//! `/api/nlq` answers a repeated question from the translation cache (AI-13,
//! #1743): the same Cypher, with no second model call.
//!
//! Its own test binary, and a single test, because the handler reads the NLQ
//! provider from the process environment. The model is a stand-in
//! OpenAI-compatible endpoint that counts the calls it receives and answers
//! with a different statement each time, so a repeat that reached it would
//! come back different.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{routing::post, Json, Router};
use samyama::graph::GraphStore;
use samyama::http::HttpServer;
use serde_json::{json, Value};
use tokio::sync::RwLock;

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// An OpenAI-compatible `/chat/completions` that answers `LIMIT <n>` on its
/// n-th call.
async fn fake_model() -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let app = Router::new().route(
        "/chat/completions",
        post(move || {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                Json(json!({
                    "choices": [{"message": {"content": format!("MATCH (p:Person) RETURN p.name LIMIT {n}")}}],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110}
                }))
            }
        }),
    );
    (serve(app).await, calls)
}

async fn post_json(url: &str, body: Value) -> Value {
    let resp = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let v: Value = resp.json().await.unwrap();
    assert!(status.is_success(), "{status}: {v}");
    v
}

#[tokio::test]
async fn a_repeated_question_is_answered_from_the_cache_until_the_schema_changes() {
    let (model_url, calls) = fake_model().await;
    std::env::set_var("NLQ_PROVIDER", "openai");
    std::env::set_var("NLQ_MODEL", "stand-in");
    std::env::set_var("NLQ_API_BASE_URL", &model_url);
    std::env::set_var("OPENAI_API_KEY", "test-key");

    let store = Arc::new(RwLock::new(GraphStore::new()));
    let server = serve(HttpServer::new(store, 0).router()).await;
    let nlq = format!("{server}/api/nlq");
    let query = format!("{server}/api/query");
    post_json(&query, json!({"query": "CREATE (:Person {name: 'Ada'})"})).await;

    let ask = |q: &str| post_json(&nlq, json!({"question": q}));

    // First time: the model is called and its tokens are reported.
    let first = ask("Who is in the graph?").await;
    assert_eq!(first["cypher"], "MATCH (p:Person) RETURN p.name LIMIT 1");
    assert_eq!(first["cached"], false);
    assert_eq!(first["usage"]["total_tokens"], 110);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // The same question, spaced differently: the same statement, no call.
    let again = ask("  Who is in   the graph? ").await;
    assert_eq!(again["cypher"], first["cypher"]);
    assert_eq!(again["cached"], true);
    assert_eq!(again["usage"]["total_tokens"], Value::Null, "{again}");
    assert_eq!(again["llm_latency_ms"], 0.0);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a repeat reached the model"
    );

    // More data of the same shape keeps the translation.
    post_json(&query, json!({"query": "CREATE (:Person {name: 'Alan'})"})).await;
    assert_eq!(ask("Who is in the graph?").await["cached"], true);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // A different question is a different entry.
    let other = ask("Who is NOT in the graph?").await;
    assert_eq!(other["cached"], false);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // A new label changes the schema the prompt describes: ask the model again.
    post_json(&query, json!({"query": "CREATE (:City {name: 'Paris'})"})).await;
    let after = ask("Who is in the graph?").await;
    assert_eq!(after["cached"], false);
    assert_eq!(after["cypher"], "MATCH (p:Person) RETURN p.name LIMIT 3");
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    // A different model is a different translator.
    std::env::set_var("NLQ_MODEL", "another");
    assert_eq!(ask("Who is in the graph?").await["cached"], false);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}
