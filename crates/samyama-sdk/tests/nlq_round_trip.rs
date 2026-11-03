//! `RemoteClient::nlq` against the engine's own HTTP router, with the `Mock`
//! LLM provider, so the SDK and the handler cannot drift apart unnoticed (#438).
//!
//! Its own test binary, and a single test, because the handler reads
//! `NLQ_PROVIDER` from the process environment.

use std::sync::Arc;

use samyama::graph::GraphStore;
use samyama::http::HttpServer;
use samyama_sdk::{RemoteClient, SamyamaClient};
use tokio::sync::RwLock;

#[tokio::test]
async fn nlq_round_trips_through_the_real_handler_and_the_cypher_runs() {
    std::env::set_var("NLQ_PROVIDER", "mock");

    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store, 0).router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let client = RemoteClient::new(&format!("http://{addr}"));
    let cypher = client.nlq("Show me some nodes").await.expect("nlq");
    // The Mock provider's fixed answer.
    assert_eq!(cypher, "MATCH (n) RETURN n LIMIT 10");

    // What the caller does next: the generated query is runnable as-is.
    let result = client.query_readonly("default", &cypher).await.expect("query");
    assert_eq!(result.columns, vec!["n".to_string()]);
}
