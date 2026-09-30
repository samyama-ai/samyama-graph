//! `find_node_by_unique` in both clients: a node looked up by the external
//! key it was imported with (#542).
//!
//! The remote cases run against the engine's own HTTP router, so the query the
//! client builds is the one the server parses and binds.

use std::sync::Arc;

use samyama::graph::GraphStore;
use samyama::http::HttpServer;
use samyama_sdk::{EmbeddedClient, NodeId, RemoteClient, SamyamaClient};
use tokio::sync::RwLock;

const SETUP: [&str; 4] = [
    "CREATE CONSTRAINT FOR (a:Acct) REQUIRE a.krid IS UNIQUE",
    "CREATE (:Acct {krid: 1, name: 'one'})",
    "CREATE (:Acct {krid: 2, name: 'two'})",
    r#"CREATE (:Acct {krid: "it's", name: 'quoted'})"#,
];

/// The id `MATCH` gives the node with this name, for comparison.
async fn id_of(client: &impl SamyamaClient, name: &str) -> NodeId {
    let r = client
        .query_readonly(
            "default",
            &format!("MATCH (a:Acct {{name: '{name}'}}) RETURN id(a)"),
        )
        .await
        .expect("match");
    NodeId::new(r.records[0][0].as_u64().expect("integer id"))
}

#[tokio::test]
async fn the_embedded_client_finds_a_node_by_its_key() {
    let client = EmbeddedClient::new();
    for q in SETUP {
        client.query("default", q).await.expect(q);
    }

    let found = client
        .find_node_by_unique("default", "Acct", "krid", 2i64)
        .await
        .unwrap();
    assert_eq!(found, Some(id_of(&client, "two").await));
    let found = client
        .find_node_by_unique("default", "Acct", "krid", "it's")
        .await
        .unwrap();
    assert_eq!(found, Some(id_of(&client, "quoted").await));
    assert_eq!(
        client
            .find_node_by_unique("default", "Acct", "krid", 3i64)
            .await
            .unwrap(),
        None
    );

    // No constraint on `name`: refused, not scanned.
    let err = client
        .find_node_by_unique("default", "Acct", "name", "one")
        .await
        .expect_err("a lookup without a unique constraint was answered");
    assert!(
        err.to_string()
            .contains("No unique constraint on :Acct(name)"),
        "{err}"
    );
}

#[tokio::test]
async fn the_remote_client_finds_a_node_by_its_key() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store, 0).router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let client = RemoteClient::new(&format!("http://{addr}"));
    for q in SETUP {
        client.query("default", q).await.expect(q);
    }

    let found = client
        .find_node_by_unique("default", "Acct", "krid", 1i64)
        .await
        .unwrap();
    assert_eq!(found, Some(id_of(&client, "one").await));
    // Bound, not spliced: the quote in the key cannot end the literal.
    let found = client
        .find_node_by_unique("default", "Acct", "krid", "it's")
        .await
        .unwrap();
    assert_eq!(found, Some(id_of(&client, "quoted").await));
    assert_eq!(
        client
            .find_node_by_unique("default", "Acct", "krid", 3i64)
            .await
            .unwrap(),
        None
    );

    // Two nodes share `name`, which no constraint covers: an error, not a pick.
    client
        .query("default", "CREATE (:Acct {krid: 4, name: 'one'})")
        .await
        .unwrap();
    assert!(client
        .find_node_by_unique("default", "Acct", "name", "one")
        .await
        .is_err());
}
