//! A caller can supply parameter values over HTTP and over RESP (#1463).
//!
//! The engine has understood `$name` since the grammar was written, and the
//! AST has carried `params` for as long. Neither remote surface could fill it:
//! `POST /api/query` accepted a `params` object, dropped it, and then failed
//! the query with `Unresolved parameter`, and `GRAPH.QUERY` took exactly two
//! arguments. So every caller with a runtime value had to build the query text
//! around it, which is the thing parameters exist to prevent.
//!
//! The load-bearing assertion here is not "a parameter works". It is that a
//! **bound value is never re-parsed as Cypher**: a value of `1 RETURN 1` or
//! `' OR 1=1 --` has to come back as a string, one row, no extra clause. A
//! surface that interpolated the value into the query text would pass every
//! other test in this file.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::protocol::command::CommandHandler;
use samyama::protocol::resp::RespValue;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

// ---------------------------------------------------------------- HTTP ----

struct Http {
    app: axum::Router,
}

fn http() -> Http {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    Http {
        app: HttpServer::new(store, 0).router(),
    }
}

impl Http {
    async fn post(&self, body: Value) -> (StatusCode, Value) {
        let req = Request::builder()
            .method("POST")
            .uri("/api/query")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request");
        let resp = self.app.clone().oneshot(req).await.expect("response");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body");
        let value: Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("body is not JSON ({e}): {:?}", String::from_utf8_lossy(&bytes)));
        (status, value)
    }

    /// The single cell of a one-row, one-column result.
    async fn scalar(&self, query: &str, params: Value) -> Value {
        let (status, body) = self.post(json!({ "query": query, "params": params })).await;
        assert_eq!(status, StatusCode::OK, "query {query:?} failed: {body}");
        let records = body["records"].as_array().expect("records");
        assert_eq!(records.len(), 1, "expected exactly one row: {body}");
        records[0].as_array().expect("row")[0].clone()
    }
}

#[tokio::test]
async fn http_binds_a_parameter() {
    let h = http();
    assert_eq!(h.scalar("RETURN $x AS x", json!({"x": 1})).await, json!(1));
}

#[tokio::test]
async fn http_rejects_a_referenced_parameter_that_was_not_supplied() {
    let h = http();
    let (status, body) = h.post(json!({ "query": "RETURN $x AS x" })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let err = body["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("Unresolved parameter") && err.contains("x"),
        "a missing value must still name the parameter: {body}"
    );
}

#[tokio::test]
async fn http_rejects_a_supplied_parameter_the_query_never_uses() {
    // A typo in a key is otherwise invisible twice over: the binding is
    // dropped, and the failure that follows points at the query rather than at
    // the payload that is actually wrong.
    let h = http();
    let (status, body) = h
        .post(json!({ "query": "RETURN $name AS n", "params": {"nmae": "alice"} }))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let err = body["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("nmae"),
        "the error must name the unused key: {body}"
    );
}

// ------------------------------------------------------------ injection ----

#[tokio::test]
async fn a_bound_value_is_never_parsed_as_cypher_over_http() {
    let h = http();

    // If the value were interpolated, `RETURN 1 RETURN 1` would either fail to
    // parse or produce a second projection. Bound, it is one string.
    let v = h
        .scalar("RETURN $x AS x", json!({"x": "1 RETURN 1"}))
        .await;
    assert_eq!(v, json!("1 RETURN 1"), "the value was not returned verbatim");

    let v = h
        .scalar("RETURN $x AS x", json!({"x": "' OR 1=1 --"}))
        .await;
    assert_eq!(v, json!("' OR 1=1 --"));

    // The same value in the position injection actually targets: a predicate.
    // Interpolated, `WHERE n.name = '' OR 1=1 --'` matches every node; bound,
    // it matches the one node whose name is that literal string, and not the
    // other.
    let (status, body) = h
        .post(json!({ "query": "CREATE (:P {name: 'alice'}), (:P {name: \"' OR 1=1 --\"})" }))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = h
        .post(json!({
            "query": "MATCH (n:P) WHERE n.name = $name RETURN n.name AS name",
            "params": {"name": "' OR 1=1 --"},
        }))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let records = body["records"].as_array().expect("records");
    assert_eq!(
        records.len(),
        1,
        "the predicate matched more than the bound value -- the value reached the parser: {body}"
    );
    assert_eq!(records[0].as_array().unwrap()[0], json!("' OR 1=1 --"));
}

#[tokio::test]
async fn a_bound_value_is_never_parsed_as_cypher_over_resp() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let h = CommandHandler::new(None);
    let reply = graph_query(&h, &store, "RETURN $x AS x", &[("x", "1 RETURN 1")]).await;
    assert_eq!(
        rows(&reply),
        vec![vec!["1 RETURN 1".to_string()]],
        "RESP did not return the value verbatim: {reply:?}"
    );
}

// ----------------------------------------------------------- json types ----

#[tokio::test]
async fn http_maps_every_json_type() {
    let h = http();
    assert_eq!(h.scalar("RETURN $v AS v", json!({"v": 7})).await, json!(7));
    assert_eq!(
        h.scalar("RETURN $v AS v", json!({"v": 1.5})).await,
        json!(1.5)
    );
    assert_eq!(
        h.scalar("RETURN $v AS v", json!({"v": true})).await,
        json!(true)
    );
    assert_eq!(
        h.scalar("RETURN $v AS v", json!({"v": "s"})).await,
        json!("s")
    );
    assert_eq!(
        h.scalar("RETURN $v AS v", json!({"v": Value::Null})).await,
        Value::Null
    );
    // Lists and maps are checked through an expression rather than by
    // returning them whole: the HTTP renderer debug-formats container values,
    // so a whole-container assertion would test the renderer, not the binding.
    assert_eq!(
        h.scalar("RETURN $v[1] AS v", json!({"v": [10, 20, 30]})).await,
        json!(20)
    );
    assert_eq!(
        h.scalar("RETURN size($v) AS v", json!({"v": [10, 20, 30]})).await,
        json!(3)
    );
    assert_eq!(
        h.scalar(
            "RETURN $v.outer.inner AS v",
            json!({"v": {"outer": {"inner": 42}}})
        )
        .await,
        json!(42)
    );
}

#[tokio::test]
async fn http_refuses_a_number_with_no_exact_representation() {
    // 2^64 - 1 is not an i64, and turning it into an f64 would change it.
    // Refusing beats binding a different number than the caller sent.
    let h = http();
    let (status, body) = h
        .post(json!({ "query": "RETURN $v AS v", "params": {"v": 18446744073709551615u64} }))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("v"),
        "the error must name the parameter: {body}"
    );
}

// ------------------------------------------------------------ the cache ----

#[tokio::test]
async fn the_result_cache_keys_on_the_bound_values() {
    // Same query text, two parameter sets. A cache keyed on the text alone
    // answers the second from the first -- a wrong answer that looks right.
    let h = http();
    let ask = |v: i64| {
        let app = h.app.clone();
        async move {
            let req = Request::builder()
                .method("POST")
                .uri("/api/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"query": "RETURN $v AS v", "params": {"v": v}, "cache": true})
                        .to_string(),
                ))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            body["records"][0][0].clone()
        }
    };
    assert_eq!(ask(1).await, json!(1));
    assert_eq!(ask(2).await, json!(2), "the cache served the first answer");
    assert_eq!(ask(1).await, json!(1));
}

// ---------------------------------------------------------------- RESP ----

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(s.as_bytes().to_vec()))
}

async fn graph_query(
    handler: &CommandHandler,
    store: &Arc<RwLock<GraphStore>>,
    query: &str,
    params: &[(&str, &str)],
) -> RespValue {
    let mut args = vec![
        bulk("GRAPH.QUERY"),
        bulk("default"),
        bulk(query),
    ];
    for (k, v) in params {
        args.push(bulk(k));
        args.push(bulk(v));
    }
    handler
        .handle_command(&RespValue::Array(args), store)
        .await
}

/// Data rows of a `GRAPH.QUERY` reply, each cell rendered as text.
fn rows(reply: &RespValue) -> Vec<Vec<String>> {
    let RespValue::Array(rows) = reply else {
        panic!("not an array reply: {reply:?}");
    };
    rows.iter()
        .skip(1) // header
        .map(|row| match row {
            RespValue::Array(cells) => cells.iter().map(cell).collect(),
            other => panic!("row is not an array: {other:?}"),
        })
        .collect()
}

fn cell(v: &RespValue) -> String {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        RespValue::Integer(i) => i.to_string(),
        RespValue::Null => "null".to_string(),
        other => format!("{other:?}"),
    }
}

#[tokio::test]
async fn resp_binds_trailing_key_value_pairs() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let h = CommandHandler::new(None);
    let reply = graph_query(&h, &store, "RETURN $x AS x", &[("x", "1")]).await;
    assert_eq!(rows(&reply), vec![vec!["1".to_string()]], "{reply:?}");
}

#[tokio::test]
async fn resp_types_a_value_by_json_and_falls_back_to_text() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let h = CommandHandler::new(None);
    // `true` is JSON, so it binds as a boolean and the predicate holds.
    let reply = graph_query(&h, &store, "RETURN $b AS b", &[("b", "true")]).await;
    assert_eq!(rows(&reply), vec![vec!["true".to_string()]], "{reply:?}");
    // A bare word is not JSON, so it binds as the string it is.
    let reply = graph_query(&h, &store, "RETURN $s AS s", &[("s", "alice")]).await;
    assert_eq!(rows(&reply), vec![vec!["alice".to_string()]], "{reply:?}");
    // A quoted JSON string is the escape hatch for a numeric-looking string.
    let reply = graph_query(&h, &store, "RETURN $s + '!' AS s", &[("s", "\"12\"")]).await;
    assert_eq!(rows(&reply), vec![vec!["12!".to_string()]], "{reply:?}");
}

#[tokio::test]
async fn resp_rejects_an_odd_trailing_argument() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let h = CommandHandler::new(None);
    let args = vec![
        bulk("GRAPH.QUERY"),
        bulk("default"),
        bulk("RETURN $x AS x"),
        bulk("x"),
    ];
    let reply = h.handle_command(&RespValue::Array(args), &store).await;
    match reply {
        RespValue::Error(e) => assert!(
            e.contains("pair"),
            "the error should say the arguments are key/value pairs: {e}"
        ),
        other => panic!("a dangling key was accepted: {other:?}"),
    }
}

#[tokio::test]
async fn resp_rejects_a_supplied_parameter_the_query_never_uses() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let h = CommandHandler::new(None);
    let reply = graph_query(&h, &store, "RETURN $name AS n", &[("nmae", "alice")]).await;
    match reply {
        RespValue::Error(e) => assert!(e.contains("nmae"), "{e}"),
        other => panic!("an unused parameter was accepted: {other:?}"),
    }
}

#[tokio::test]
async fn resp_binds_a_parameter_on_a_write() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let h = CommandHandler::new(None);
    let reply = graph_query(
        &h,
        &store,
        "CREATE (n:P {name: $name}) RETURN n.name AS name",
        &[("name", "alice")],
    )
    .await;
    assert_eq!(rows(&reply), vec![vec!["alice".to_string()]], "{reply:?}");
}
