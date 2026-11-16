//! The slow-query log read and emptied over RESP, as `GET`/`DELETE
//! /api/slowlog` serve it over HTTP (#2079).
//!
//! Its own binary, and its tests one at a time: the slow-query ring is
//! process-wide, so a query in another test would land in the log this one
//! is counting.

use std::sync::{Arc, Mutex};

use samyama::graph::GraphStore;
use samyama::protocol::CommandHandler;
use samyama::RespValue;
use tokio::sync::RwLock;

static RING: Mutex<()> = Mutex::new(());

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(s.as_bytes().to_vec()))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn int(v: &RespValue) -> i64 {
    match v {
        RespValue::Integer(n) => *n,
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn field<'a>(entry: &'a RespValue, name: &str) -> &'a RespValue {
    let RespValue::Array(kv) = entry else { panic!("entry is not an array: {entry:?}") };
    let at = kv.iter().position(|v| *v == bulk(name)).unwrap_or_else(|| panic!("no {name} in {kv:?}"));
    &kv[at + 1]
}

fn is_error_containing(v: &RespValue, text: &str) -> bool {
    matches!(v, RespValue::Error(e) if e.contains(text))
}

#[tokio::test]
async fn the_slow_query_log_is_read_and_emptied_over_resp() {
    let _ring = RING.lock().unwrap_or_else(|e| e.into_inner());
    // Read by `QueryEngine::new`, so set before the handler is built.
    std::env::set_var("SLOW_QUERY_MS", "1");
    let handler = CommandHandler::new(None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let run = |c: RespValue| {
        let (handler, store) = (&handler, Arc::clone(&store));
        async move { handler.handle_command(&c, &store, None).await }
    };

    assert!(!matches!(run(cmd(&["GRAPH.SLOWLOG", "RESET"])).await, RespValue::Error(_)));
    let slow = "UNWIND range(1, 300000) AS x RETURN sum(x) AS s";
    for _ in 0..2 {
        assert!(!matches!(run(cmd(&["GRAPH.QUERY", "default", slow])).await, RespValue::Error(_)));
    }
    assert_eq!(int(&run(cmd(&["GRAPH.SLOWLOG", "LEN"])).await), 2);

    let RespValue::Array(one) = run(cmd(&["GRAPH.SLOWLOG", "get", "1"])).await else { panic!() };
    assert_eq!(one.len(), 1, "count limits the reply");
    let RespValue::Array(all) = run(cmd(&["GRAPH.SLOWLOG", "GET"])).await else { panic!() };
    assert_eq!(all.len(), 2);
    assert_eq!(field(&all[0], "query"), &bulk(slow));
    assert_eq!(int(field(&all[0], "rows")), 1);
    assert_eq!(int(field(&all[0], "failed")), 0);
    assert_eq!(int(field(&all[0], "threshold_ms")), 1);
    assert!(int(field(&all[0], "id")) > int(field(&all[1], "id")), "newest first");
    assert_eq!(&one[0], &all[0], "GET 1 is the newest");

    assert_eq!(int(&run(cmd(&["GRAPH.SLOWLOG", "RESET"])).await), 2, "RESET reports what it cleared");
    assert_eq!(int(&run(cmd(&["GRAPH.SLOWLOG", "LEN"])).await), 0);

    for bad in [&["GRAPH.SLOWLOG"][..], &["GRAPH.SLOWLOG", "PEEK"], &["GRAPH.SLOWLOG", "LEN", "x"]] {
        assert!(is_error_containing(&run(cmd(bad)).await, "usage"), "{bad:?}");
    }
    assert!(is_error_containing(&run(cmd(&["GRAPH.SLOWLOG", "GET", "-1"])).await, "count"));
}

/// Entries hold query text from every tenant, literals included: Admin only
/// when credentials are configured, as `GET /api/slowlog` is.
#[tokio::test]
async fn reading_the_slow_query_log_over_resp_needs_admin() {
    let _ring = RING.lock().unwrap_or_else(|e| e.into_inner());
    let handler = CommandHandler::new(None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let hash = "0".repeat(64);
    let reader = samyama::auth::Credential::parse(&format!("r:{hash}:roles=read")).unwrap().unwrap();
    let admin = samyama::auth::Credential::parse(&format!("a:{hash}:roles=admin")).unwrap().unwrap();
    for sub in [&["GRAPH.SLOWLOG", "GET"][..], &["GRAPH.SLOWLOG", "LEN"], &["GRAPH.SLOWLOG", "RESET"]] {
        let refused = handler.handle_command(&cmd(sub), &store, Some(&reader)).await;
        assert!(is_error_containing(&refused, "Admin"), "{sub:?}: {refused:?}");
        let allowed = handler.handle_command(&cmd(sub), &store, Some(&admin)).await;
        assert!(!matches!(allowed, RespValue::Error(_)), "{sub:?}: {allowed:?}");
    }
}
