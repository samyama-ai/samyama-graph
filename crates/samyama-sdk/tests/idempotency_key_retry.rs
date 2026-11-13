//! A write the client retries is applied once (#2176).
//!
//! The server runs the first request and its reply is held past the client's
//! timeout, which is the case where a retry used to make a second node: the
//! write had landed and the client could not tell. Every write now carries an
//! `Idempotency-Key`, made once per call, so the retry is answered with the
//! first reply. The count is read from the store, because a reply that looks
//! right proves nothing about how many nodes there are.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::Request;
use axum::middleware::{self, Next};
use samyama::graph::GraphStore;
use samyama::http::HttpServer;
use samyama_sdk::{ConnectionConfig, RemoteClient, SamyamaClient};
use tokio::sync::RwLock;

/// What the server saw: the key on each `/api/query` request, in order.
type Seen = Arc<Mutex<Vec<Option<String>>>>;

/// The engine's own router, with the reply to the first `/api/query` request
/// held for `hold` after the statement ran.
async fn serve(hold: Duration) -> (String, Arc<RwLock<GraphStore>>, Seen) {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let seen: Seen = Arc::default();
    let first = Arc::new(AtomicUsize::new(0));
    let (log, counter) = (Arc::clone(&seen), Arc::clone(&first));
    let app = HttpServer::new(Arc::clone(&store), 0)
        .router()
        .layer(middleware::from_fn(move |req: Request, next: Next| {
            let (log, counter) = (Arc::clone(&log), Arc::clone(&counter));
            async move {
                let is_query = req.uri().path() == "/api/query";
                if is_query {
                    let key = req
                        .headers()
                        .get("idempotency-key")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    log.lock().unwrap().push(key);
                }
                let res = next.run(req).await;
                if is_query && counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    tokio::time::sleep(hold).await;
                }
                res
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://{addr}"), store, seen)
}

fn client(url: &str, retries: u32) -> RemoteClient {
    RemoteClient::with_config(
        url,
        ConnectionConfig {
            timeout: Some(Duration::from_millis(400)),
            max_retries: retries,
            retry_base_delay: Duration::from_millis(10),
            ..Default::default()
        },
    )
}

fn labelled(store: &GraphStore, label: &str) -> usize {
    store
        .all_nodes()
        .iter()
        .filter(|n| n.labels.iter().any(|l| l.as_str() == label))
        .count()
}

/// The defect, end to end: the first attempt's write landed, its reply timed
/// out, and the retry must not make a second node.
#[tokio::test]
async fn a_write_retried_after_a_timeout_is_applied_once() {
    let (url, store, seen) = serve(Duration::from_secs(2)).await;
    let result = client(&url, 2).query("default", "CREATE (:Order {id: 1})").await;

    let keys = seen.lock().unwrap().clone();
    assert!(keys.len() >= 2, "the client did not retry: {keys:?}");
    assert_eq!(labelled(&*store.read().await, "Order"), 1, "the retry ran the write again");
    assert!(result.is_ok(), "the retry was not answered: {result:?}");
    let first = keys[0].clone().expect("a write was sent without a key");
    assert!(keys.iter().all(|k| k.as_deref() == Some(first.as_str())), "a retry changed the key: {keys:?}");
}

#[tokio::test]
async fn each_call_has_its_own_key_and_a_read_has_none() {
    let (url, store, seen) = serve(Duration::ZERO).await;
    let c = client(&url, 0);
    c.query("default", "CREATE (:Twice)").await.unwrap();
    c.query("default", "CREATE (:Twice)").await.unwrap();
    c.query_readonly("default", "MATCH (n) RETURN count(n)").await.unwrap();

    let keys = seen.lock().unwrap().clone();
    assert_eq!(keys.len(), 3);
    assert!(keys[0].is_some() && keys[1].is_some());
    assert_ne!(keys[0], keys[1], "two calls shared a key");
    assert_eq!(keys[2], None, "a read-only request carried a key");
    // Two calls are two writes; the key is not a dedup of identical statements.
    assert_eq!(labelled(&*store.read().await, "Twice"), 2);
}

/// A caller that got an error it cannot read sends the same call again with
/// its own key, and the write is not repeated.
#[tokio::test]
async fn a_caller_chosen_key_makes_a_second_call_safe() {
    let (url, store, seen) = serve(Duration::ZERO).await;
    let c = client(&url, 0);
    let first = c
        .query_with_idempotency_key("default", "CREATE (n:Paid) RETURN 1 AS ok", "pay-42")
        .await
        .unwrap();
    let again = c
        .query_with_idempotency_key("default", "CREATE (n:Paid) RETURN 1 AS ok", "pay-42")
        .await
        .unwrap();
    assert_eq!(again.records, first.records);
    assert_eq!(labelled(&*store.read().await, "Paid"), 1);
    assert!(seen.lock().unwrap().iter().all(|k| k.as_deref() == Some("pay-42")));

    let other = c
        .query_with_idempotency_key("default", "CREATE (:Other)", "pay-42")
        .await;
    assert!(other.is_err(), "a key reused for another statement was run");
    assert_eq!(labelled(&*store.read().await, "Other"), 0);
}
