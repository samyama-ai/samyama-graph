//! A write sent with an `Idempotency-Key` runs once, however often it is sent
//! (#2176).
//!
//! A client whose reply is lost cannot tell whether its write committed. On
//! 1.15.0 the header was ignored: the same `CREATE` sent twice with the same
//! key made two nodes. These tests drive the shipped router and count nodes,
//! because a reply that *looks* replayed proves nothing about the store.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::persistence::PersistenceManager;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

fn app_over(store: Arc<RwLock<GraphStore>>) -> axum::Router {
    HttpServer::new(store, 0).router()
}

fn app() -> axum::Router {
    app_over(Arc::new(RwLock::new(GraphStore::new())))
}

fn query(cypher: &str, key: Option<&str>) -> Request<Body> {
    query_body(serde_json::json!({ "query": cypher }), key)
}

fn query_body(body: serde_json::Value, key: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json");
    if let Some(k) = key {
        b = b.header("idempotency-key", k);
    }
    b.body(Body::from(body.to_string())).expect("request")
}

struct Reply {
    status: StatusCode,
    replayed: bool,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("JSON body")
    }
}

async fn send(app: &axum::Router, req: Request<Body>) -> Reply {
    let res = app.clone().oneshot(req).await.expect("response");
    let status = res.status();
    let replayed = res.headers().get("idempotent-replayed").is_some();
    let body = res
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes()
        .to_vec();
    Reply {
        status,
        replayed,
        body,
    }
}

async fn count(app: &axum::Router, label: &str) -> i64 {
    let r = send(
        app,
        query(&format!("MATCH (n:{label}) RETURN count(n)"), None),
    )
    .await;
    assert_eq!(
        r.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    r.json()["records"][0][0].as_i64().expect("a count")
}

#[tokio::test]
async fn the_same_key_twice_creates_one_node_and_replays_the_first_reply() {
    let app = app();
    let first = send(
        &app,
        query("CREATE (n:Dup {id: 1}) RETURN n.id", Some("k1")),
    )
    .await;
    let second = send(
        &app,
        query("CREATE (n:Dup {id: 1}) RETURN n.id", Some("k1")),
    )
    .await;

    assert_eq!(
        first.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&first.body)
    );
    assert!(
        !first.replayed,
        "the first reply was produced, not replayed"
    );
    assert_eq!(second.status, StatusCode::OK);
    assert!(second.replayed, "a replay must say so");
    assert_eq!(
        second.body, first.body,
        "the replay must be the first reply, byte for byte"
    );
    assert_eq!(count(&app, "Dup").await, 1);
}

#[tokio::test]
async fn without_a_key_a_repeated_write_still_runs_twice() {
    // The control: the header is what makes the difference, not something
    // else that started deduplicating writes.
    let app = app();
    send(&app, query("CREATE (:Plain)", None)).await;
    send(&app, query("CREATE (:Plain)", None)).await;
    assert_eq!(count(&app, "Plain").await, 2);
}

#[tokio::test]
async fn different_keys_are_different_writes() {
    let app = app();
    send(&app, query("CREATE (:Two)", Some("a"))).await;
    send(&app, query("CREATE (:Two)", Some("b"))).await;
    assert_eq!(count(&app, "Two").await, 2);
}

#[tokio::test]
async fn a_key_reused_for_a_different_request_is_refused_and_not_run() {
    let app = app();
    send(&app, query("CREATE (:Reuse {v: 1})", Some("k"))).await;
    let other = send(&app, query("CREATE (:Reuse {v: 2})", Some("k"))).await;
    assert_eq!(other.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        other.json()["code"],
        "Samyama.ClientError.Request.IdempotencyKeyReused"
    );
    assert_eq!(count(&app, "Reuse").await, 1);

    // Parameters are part of the request too.
    let p = |v: i64| {
        query_body(
            serde_json::json!({ "query": "CREATE (:P {v: $v})", "params": { "v": v } }),
            Some("kp"),
        )
    };
    assert_eq!(send(&app, p(1)).await.status, StatusCode::OK);
    assert!(send(&app, p(1)).await.replayed);
    assert_eq!(
        send(&app, p(2)).await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(count(&app, "P").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_with_one_key_run_once() {
    // On 1.15.0, 20 concurrent identical CREATEs made 20 nodes.
    let app = app();
    let handles: Vec<_> = (0..20)
        .map(|_| {
            let app = app.clone();
            tokio::spawn(async move { send(&app, query("CREATE (:Race)", Some("race"))).await })
        })
        .collect();
    let mut produced = 0;
    for h in handles {
        let r = h.await.expect("task");
        assert_eq!(r.status, StatusCode::OK);
        if !r.replayed {
            produced += 1;
        }
    }
    assert_eq!(produced, 1, "exactly one request may run the write");
    assert_eq!(count(&app, "Race").await, 1);
}

#[tokio::test]
async fn a_failed_write_is_not_remembered_and_its_retry_runs() {
    let app = app();
    send(
        &app,
        query("CREATE CONSTRAINT FOR (n:U) REQUIRE n.k IS UNIQUE", None),
    )
    .await;
    send(&app, query("CREATE (:U {k: 1, first: true})", None)).await;

    let failed = send(&app, query("CREATE (:U {k: 1})", Some("retry-me"))).await;
    assert_eq!(
        failed.status,
        StatusCode::BAD_REQUEST,
        "{}",
        String::from_utf8_lossy(&failed.body)
    );

    // The cause is removed; the retry must run, not replay the failure.
    send(&app, query("MATCH (n:U {first: true}) DELETE n", None)).await;
    let retried = send(&app, query("CREATE (:U {k: 1})", Some("retry-me"))).await;
    assert_eq!(
        retried.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&retried.body)
    );
    assert!(!retried.replayed);
    assert_eq!(count(&app, "U").await, 1);
}

#[tokio::test]
async fn a_key_on_a_read_is_accepted_and_the_read_runs() {
    let app = app();
    send(&app, query("CREATE (:R)", None)).await;
    let r = send(&app, query("MATCH (n:R) RETURN count(n)", Some("read"))).await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(!r.replayed);
}

#[tokio::test]
async fn a_malformed_key_is_refused_not_ignored() {
    let app = app();
    for bad in ["has space", &"x".repeat(256)] {
        let r = send(&app, query("CREATE (:Bad)", Some(bad))).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "key {bad:?}");
    }
    assert_eq!(count(&app, "Bad").await, 0);
}

#[tokio::test]
async fn a_key_on_a_statement_inside_a_transaction_is_refused_not_ignored() {
    let app = app();
    let in_tx = send(
        &app,
        query_body(
            serde_json::json!({ "query": "CREATE (:T)", "tx": "any" }),
            Some("k"),
        ),
    )
    .await;
    assert_eq!(in_tx.status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&in_tx.body).contains("COMMIT"));
    assert_eq!(count(&app, "T").await, 0);
}

fn post(uri: &str, key: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(k) = key {
        b = b.header("idempotency-key", k);
    }
    b.body(Body::from("{}")).expect("request")
}

/// BEGIN, one CREATE of `label` inside it; the transaction id.
async fn open_tx(app: &axum::Router, label: &str) -> String {
    let begun = send(app, post("/api/tx/begin", None)).await;
    assert_eq!(begun.status, StatusCode::OK);
    let tx = begun.json()["tx"].as_str().expect("tx id").to_string();
    let created = send(
        app,
        query_body(
            serde_json::json!({ "query": format!("CREATE (:{label})"), "tx": tx }),
            None,
        ),
    )
    .await;
    assert_eq!(created.status, StatusCode::OK);
    tx
}

async fn commit(app: &axum::Router, tx: &str, key: Option<&str>) -> Reply {
    send(app, post(&format!("/api/tx/{tx}/commit"), key)).await
}

/// The COMMIT reply was lost and the client sends it again. It used to get
/// 404, which reads as "not committed" and leads the client to run it again.
#[tokio::test]
async fn a_commit_asked_again_answers_what_the_first_did() {
    let app = app();
    let tx = open_tx(&app, "Again").await;
    let first = commit(&app, &tx, None).await;
    assert_eq!(first.status, StatusCode::OK);
    assert!(!first.replayed);

    let second = commit(&app, &tx, None).await;
    assert_eq!(second.status, StatusCode::OK);
    assert!(second.replayed);
    assert_eq!(second.body, first.body);
    assert_eq!(count(&app, "Again").await, 1);

    let rollback = send(&app, post(&format!("/api/tx/{tx}/rollback"), None)).await;
    assert_eq!(
        rollback.status,
        StatusCode::CONFLICT,
        "a committed transaction cannot be rolled back"
    );
    assert_eq!(count(&app, "Again").await, 1);
}

#[tokio::test]
async fn a_commit_of_a_transaction_never_begun_is_still_not_found() {
    let app = app();
    assert_eq!(
        commit(&app, "never", None).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        commit(&app, "never", Some("k")).await.status,
        StatusCode::NOT_FOUND
    );
}

/// The client could not tell whether its transaction committed and ran it
/// again from BEGIN with the same key. The second run is rolled back and the
/// first commit's reply returned: one node, not two.
#[tokio::test]
async fn a_transaction_run_again_under_a_committed_key_is_rolled_back() {
    let app = app();
    let first_tx = open_tx(&app, "Order").await;
    let first = commit(&app, &first_tx, Some("order-1")).await;
    assert_eq!(first.status, StatusCode::OK);
    assert!(!first.replayed);

    let second_tx = open_tx(&app, "Order").await;
    let second = commit(&app, &second_tx, Some("order-1")).await;
    assert_eq!(second.status, StatusCode::OK);
    assert!(second.replayed);
    assert_eq!(second.body, first.body, "the first commit's reply");
    assert_eq!(
        count(&app, "Order").await,
        1,
        "the re-run was not rolled back"
    );

    // The re-run released the lock: the store still takes writes.
    assert_eq!(
        send(&app, query("CREATE (:After)", None)).await.status,
        StatusCode::OK
    );
    // A new key is a new transaction.
    let third_tx = open_tx(&app, "Order").await;
    assert!(!commit(&app, &third_tx, Some("order-2")).await.replayed);
    assert_eq!(count(&app, "Order").await, 2);
}

/// A key already used for an autocommit write is a different request. The
/// COMMIT is refused and the transaction left open, so it can be committed
/// without the key.
#[tokio::test]
async fn a_commit_under_a_key_used_for_something_else_is_refused_and_left_open() {
    let app = app();
    send(&app, query("CREATE (:Other)", Some("shared"))).await;
    let tx = open_tx(&app, "Mine").await;
    let refused = commit(&app, &tx, Some("shared")).await;
    assert_eq!(refused.status, StatusCode::UNPROCESSABLE_ENTITY);

    let committed = commit(&app, &tx, None).await;
    assert_eq!(
        committed.status,
        StatusCode::OK,
        "the transaction was not left open"
    );
    assert!(!committed.replayed);
    assert_eq!(count(&app, "Mine").await, 1);
}

#[tokio::test]
async fn a_malformed_key_on_commit_is_refused_and_the_transaction_left_open() {
    let app = app();
    let tx = open_tx(&app, "Kept").await;
    assert_eq!(
        commit(&app, &tx, Some("has space")).await.status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(commit(&app, &tx, None).await.status, StatusCode::OK);
    assert_eq!(count(&app, "Kept").await, 1);
}

/// The record of a commit is written with its rows, so it survives a restart
/// that happens before the client's retry.
#[tokio::test]
async fn a_committed_transaction_is_remembered_across_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (tx, first) = {
        let pm = Arc::new(PersistenceManager::new(dir.path()).expect("persistence"));
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let app = HttpServer::new(store, 0).with_persistence(pm).router();
        let tx = open_tx(&app, "Paid").await;
        (tx.clone(), commit(&app, &tx, Some("pay-1")).await)
    };
    assert_eq!(first.status, StatusCode::OK);

    let pm = Arc::new(PersistenceManager::new(dir.path()).expect("reopen"));
    let mut store = GraphStore::new();
    pm.recover_into("default", &mut store).expect("recover");
    let app = HttpServer::new(Arc::new(RwLock::new(store)), 0)
        .with_persistence(pm)
        .router();

    let asked_again = commit(&app, &tx, None).await;
    assert_eq!(asked_again.status, StatusCode::OK);
    assert_eq!(asked_again.body, first.body);

    let rerun = open_tx(&app, "Paid").await;
    let replayed = commit(&app, &rerun, Some("pay-1")).await;
    assert!(replayed.replayed);
    assert_eq!(replayed.body, first.body);
    assert_eq!(count(&app, "Paid").await, 1);
}

#[tokio::test]
async fn a_client_can_ask_whether_a_keyed_write_committed() {
    let app = app();
    let get = |key: &str| {
        Request::builder()
            .uri(format!("/api/requests/{key}"))
            .body(Body::empty())
            .unwrap()
    };
    let unknown = send(&app, get("lookup")).await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    assert_eq!(unknown.json()["committed"], false);

    send(&app, query("CREATE (:Look)", Some("lookup"))).await;
    let known = send(&app, get("lookup")).await;
    assert_eq!(known.status, StatusCode::OK);
    assert_eq!(known.json()["committed"], true);
    assert!(known.json()["version"].as_u64().is_some());
}

#[tokio::test]
async fn a_key_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let req = || query("CREATE (n:Durable {id: 7}) RETURN n.id", Some("durable"));

    let first = {
        let pm = Arc::new(PersistenceManager::new(dir.path()).expect("persistence"));
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let app = HttpServer::new(store, 0).with_persistence(pm).router();
        send(&app, req()).await
    };
    assert_eq!(first.status, StatusCode::OK);

    // A restart: a new process recovers from RocksDB alone.
    let pm = Arc::new(PersistenceManager::new(dir.path()).expect("reopen"));
    let mut store = GraphStore::new();
    pm.recover_into("default", &mut store).expect("recover");
    let app = HttpServer::new(Arc::new(RwLock::new(store)), 0)
        .with_persistence(pm)
        .router();

    let retry = send(&app, req()).await;
    assert!(
        retry.replayed,
        "a retry after a restart must not run the write again"
    );
    assert_eq!(retry.body, first.body);
    assert_eq!(count(&app, "Durable").await, 1);
}
