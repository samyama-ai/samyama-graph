//! `RemoteClient::nlq` speaks the wire shape of `POST /api/nlq` (#438).
//!
//! These use a canned HTTP responder so each case controls the exact status
//! and body, and so the request the client sends can be read back. The round
//! trip against the engine's real handler is in `nlq_round_trip.rs`, which
//! runs in its own process because it sets `NLQ_PROVIDER`.

use samyama_sdk::{RemoteClient, SamyamaError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Answer one request with `status` and a JSON `body`; hand back the raw
/// request so the test can check what was sent.
async fn respond_once(status: &'static str, body: &'static str) -> (String, oneshot::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        // Read headers, then as much body as Content-Length says.
        loop {
            let n = sock.read(&mut chunk).await.expect("read");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&buf);
            if let Some(end) = text.find("\r\n\r\n") {
                let len = text[..end]
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                if buf.len() >= end + 4 + len {
                    break;
                }
            }
        }
        let reply = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(reply.as_bytes()).await.expect("write");
        let _ = tx.send(String::from_utf8_lossy(&buf).into_owned());
    });
    (format!("http://{addr}"), rx)
}

#[tokio::test]
async fn posts_the_question_to_the_nlq_route_and_returns_the_cypher() {
    let (url, request) = respond_once("200 OK", r#"{"cypher":"MATCH (p:Person) RETURN p.name"}"#).await;
    let client = RemoteClient::new(&url);

    let cypher = client.nlq("Who is in the graph?").await.expect("nlq");
    assert_eq!(cypher, "MATCH (p:Person) RETURN p.name");

    let raw = request.await.expect("request captured");
    assert!(raw.starts_with("POST /api/nlq "), "wrong route: {raw}");
    let body = &raw[raw.find("\r\n\r\n").expect("header end") + 4..];
    let json: serde_json::Value = serde_json::from_str(body).expect("JSON body");
    assert_eq!(json, serde_json::json!({ "question": "Who is in the graph?" }));
}

#[tokio::test]
async fn a_server_refusal_carries_the_servers_message() {
    // What the handler returns when `NLQ_PROVIDER` is unset, or when the
    // generated query fails the read-only check.
    let (url, _) = respond_once(
        "400 Bad Request",
        r#"{"error":"NLQ_PROVIDER is not set."}"#,
    )
    .await;
    let client = RemoteClient::new(&url);

    match client.nlq("anything").await {
        Err(SamyamaError::QueryError(msg)) => assert_eq!(msg, "NLQ_PROVIDER is not set."),
        other => panic!("expected QueryError, got {other:?}"),
    }
}

#[tokio::test]
async fn a_success_without_cypher_is_a_protocol_error_not_an_empty_query() {
    // An empty string would be passed on to `query` by a caller and fail there
    // with a message about Cypher, not about the server that sent it.
    let (url, _) = respond_once("200 OK", r#"{"answer":"42"}"#).await;
    let client = RemoteClient::new(&url);

    match client.nlq("anything").await {
        Err(SamyamaError::ProtocolError(_)) => {}
        other => panic!("expected ProtocolError, got {other:?}"),
    }
}
