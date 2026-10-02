//! The remote client's read-only call refuses writes (#1628), and its stream
//! reads the server's NDJSON rows with backpressure (#1632).
//!
//! Run against the engine's own HTTP router, so the request the client builds
//! is the one the server parses.

use std::sync::Arc;
use std::time::Duration;

use samyama::graph::GraphStore;
use samyama::http::HttpServer;
use samyama_sdk::{RemoteClient, SamyamaClient};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

async fn server() -> (RemoteClient, String) {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store, 0).router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let base = format!("http://{addr}");
    (RemoteClient::new(&base), base)
}

async fn count(client: &RemoteClient, q: &str) -> u64 {
    let r = client.query_readonly("default", q).await.expect(q);
    r.records[0][0].as_u64().expect("a count")
}

/// The eight destructive statements of the CH-AI-SEC measurement, in its
/// shapes: plain, behind a comment, behind a WITH, and each kind of write.
const DESTRUCTIVE: [&str; 8] = [
    "MATCH (n) DETACH DELETE n",
    "// tidy up\nMATCH (n) DETACH DELETE n",
    "WITH 1 AS x MATCH (n) DETACH DELETE n",
    "MATCH (n:T) SET n.v = 99",
    "CREATE (:T {v: 7})",
    "MERGE (:T {v: 8})",
    "MATCH (n:T) REMOVE n.v",
    "MATCH (n:T) REMOVE n:T",
];

#[tokio::test]
async fn query_readonly_refuses_every_write_and_the_graph_does_not_move() {
    let (client, base) = server().await;
    client
        .query("default", "CREATE (:T {v: 1}), (:T {v: 2})")
        .await
        .unwrap();

    for q in DESTRUCTIVE {
        let err = client
            .query_readonly("default", q)
            .await
            .expect_err(&format!("a write through the read-only call ran: {q}"));
        assert!(err.to_string().contains("read-only"), "{q}: {err}");
    }
    assert_eq!(count(&client, "MATCH (n:T) RETURN count(n)").await, 2);
    assert_eq!(
        count(&client, "MATCH (n:T) WHERE n.v IN [1, 2] RETURN count(n)").await,
        2
    );

    // The server's answer: 403 with a code, before anything runs.
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/query"))
        .json(&serde_json::json!({"query": "MATCH (n) DETACH DELETE n", "read_only": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["code"],
        "Samyama.ClientError.Statement.WriteInReadTransaction"
    );

    // Without the flag the route still writes, as it always has.
    client.query("default", "CREATE (:T {v: 3})").await.unwrap();
    assert_eq!(count(&client, "MATCH (n:T) RETURN count(n)").await, 3);
}

#[tokio::test]
async fn a_stream_yields_every_row_then_ends() {
    let (client, _) = server().await;
    let mut stream = client
        .query_stream(
            "default",
            "UNWIND range(1, 2000) AS i RETURN i, i * 2 AS twice",
        )
        .await
        .unwrap();
    assert_eq!(stream.columns(), ["i", "twice"]);
    let mut n = 0u64;
    while let Some(row) = stream.next_row().await.unwrap() {
        n += 1;
        assert_eq!(row[0].as_u64(), Some(n));
        assert_eq!(row[1].as_u64(), Some(2 * n));
    }
    assert_eq!(n, 2000);
    // After the trailer, still the end.
    assert!(stream.next_row().await.unwrap().is_none());
}

#[tokio::test]
async fn a_stream_refuses_a_write_before_it_runs() {
    let (client, _) = server().await;
    client.query("default", "CREATE (:T)").await.unwrap();
    assert!(client
        .query_stream("default", "MATCH (n) DETACH DELETE n")
        .await
        .is_err());
    assert_eq!(count(&client, "MATCH (n:T) RETURN count(n)").await, 1);
}

/// A consumer that stops early releases the server.
///
/// The stream holds the store's read lock while it produces rows, so a write
/// can only proceed once the server has stopped producing. Here the query
/// would produce a billion rows: if dropping the stream did not stop the
/// server, the write below would wait for all of them. The bound is a guard
/// against a hang, not a measurement.
///
/// Three nested thousand-element lists rather than one `range(1, 1e9)`: an
/// UNWIND builds its list before the first row, and a billion-element list
/// would never get as far as streaming.
#[tokio::test]
async fn dropping_a_stream_stops_the_server_producing_rows() {
    let (client, _) = server().await;
    let mut stream = client
        .query_stream(
            "default",
            "UNWIND range(1, 1000) AS a UNWIND range(1, 1000) AS b UNWIND range(1, 1000) AS c \
             RETURN c",
        )
        .await
        .unwrap();
    for expected in 1..=5u64 {
        assert_eq!(
            stream.next_row().await.unwrap().unwrap()[0].as_u64(),
            Some(expected)
        );
    }
    drop(stream);
    tokio::time::timeout(
        Duration::from_secs(120),
        client.query("default", "CREATE (:After)"),
    )
    .await
    .expect("the write waited on a stream nobody was reading")
    .unwrap();
}

/// A server that sends `body` as an NDJSON response and closes.
async fn canned(body: &'static str) -> RemoteClient {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await;
            let head = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\n\r\n";
            let _ = sock.write_all(head.as_bytes()).await;
            let _ = sock.write_all(body.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
    });
    RemoteClient::new(&format!("http://{addr}"))
}

#[tokio::test]
async fn an_error_trailer_and_a_missing_trailer_are_errors() {
    let client =
        canned("{\"columns\":[\"i\"]}\n{\"row\":[1]}\n{\"error\":\"boom\",\"rows\":1}\n").await;
    let mut s = client.query_stream("default", "RETURN 1").await.unwrap();
    assert_eq!(s.next_row().await.unwrap().unwrap()[0].as_u64(), Some(1));
    let err = s
        .next_row()
        .await
        .expect_err("an error trailer is an error");
    assert!(err.to_string().contains("boom"), "{err}");

    let client = canned("{\"columns\":[\"i\"]}\n{\"row\":[1]}\n").await;
    let mut s = client.query_stream("default", "RETURN 1").await.unwrap();
    assert!(s.next_row().await.unwrap().is_some());
    let err = s
        .next_row()
        .await
        .expect_err("a body with no trailer is incomplete");
    assert!(err.to_string().contains("without a trailer"), "{err}");
}
