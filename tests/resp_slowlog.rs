//! The slow-query log read and emptied over RESP, as `GET`/`DELETE
//! /api/slowlog` serve it over HTTP (#2079).
//!
//! Its own binary, and its tests one at a time: the slow-query ring is
//! process-wide, so a query in another test would land in the log this one
//! is counting.

use std::sync::Arc;

use samyama::graph::GraphStore;
use samyama::protocol::CommandHandler;
use samyama::RespValue;
use tokio::sync::RwLock;

static RING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(s.as_bytes().to_vec()))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().copied().map(bulk).collect())
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
    let _ring = RING.lock().await;
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

    for bad in [&["GRAPH.SLOWLOG"][..], &["GRAPH.SLOWLOG", "LEN", "x"], &["GRAPH.SLOWLOG", "GET", "1", "x"]] {
        assert!(is_error_containing(&run(cmd(bad)).await, "usage"), "{bad:?}");
    }
    // Anything else in the first place is FalkorDB's graph name.
    assert!(is_error_containing(&run(cmd(&["GRAPH.SLOWLOG", "PEEK"])).await, "single graph"));
    assert!(is_error_containing(&run(cmd(&["GRAPH.SLOWLOG", "GET", "-1"])).await, "count"));
}

/// Entries hold query text from every tenant, literals included: Admin only
/// when credentials are configured, as `GET /api/slowlog` is.
#[tokio::test]
async fn reading_the_slow_query_log_over_resp_needs_admin() {
    let _ring = RING.lock().await;
    let handler = CommandHandler::new(None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let hash = "0".repeat(64);
    let reader = samyama::auth::Credential::parse(&format!("r:{hash}:roles=read")).unwrap().unwrap();
    let admin = samyama::auth::Credential::parse(&format!("a:{hash}:roles=admin")).unwrap().unwrap();
    for sub in [
        &["GRAPH.SLOWLOG", "GET"][..],
        &["GRAPH.SLOWLOG", "LEN"],
        &["GRAPH.SLOWLOG", "RESET"],
        &["GRAPH.SLOWLOG", "default"],
        &["GRAPH.SLOWLOG", "default", "RESET"],
    ] {
        let refused = handler.handle_command(&cmd(sub), &store, Some(&reader)).await;
        assert!(is_error_containing(&refused, "Admin"), "{sub:?}: {refused:?}");
        let allowed = handler.handle_command(&cmd(sub), &store, Some(&admin)).await;
        assert!(!matches!(allowed, RespValue::Error(_)), "{sub:?}: {allowed:?}");
    }
}

/// FalkorDB's form, so a client moving over keeps working: `GRAPH.SLOWLOG
/// <graph>` lists [timestamp, command, query, duration], `... RESET` clears.
#[tokio::test]
async fn falkordbs_form_of_the_command_is_answered_in_falkordbs_shape() {
    let _ring = RING.lock().await;
    std::env::set_var("SLOW_QUERY_MS", "1");
    let handler = CommandHandler::new(None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let run = |c: RespValue| {
        let (handler, store) = (&handler, Arc::clone(&store));
        async move { handler.handle_command(&c, &store, None).await }
    };

    assert!(!matches!(run(cmd(&["GRAPH.SLOWLOG", "RESET"])).await, RespValue::Error(_)));
    let slow = "UNWIND range(1, 300000) AS x RETURN sum(x) AS s";
    assert!(!matches!(run(cmd(&["GRAPH.QUERY", "default", slow])).await, RespValue::Error(_)));

    let RespValue::Array(entries) = run(cmd(&["GRAPH.SLOWLOG", "default"])).await else { panic!() };
    assert_eq!(entries.len(), 1);
    let RespValue::Array(e) = &entries[0] else { panic!("{entries:?}") };
    assert_eq!(e.len(), 4, "{e:?}");
    let s = |v: &RespValue| match v {
        RespValue::BulkString(Some(b)) => String::from_utf8(b.clone()).unwrap(),
        other => panic!("expected a bulk string, got {other:?}"),
    };
    let now_s = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let ts: u64 = s(&e[0]).parse().unwrap();
    assert!(ts <= now_s && now_s - ts < 600, "timestamp is in seconds: {ts} vs {now_s}");
    assert_eq!(s(&e[1]), "GRAPH.QUERY");
    assert_eq!(s(&e[2]), slow);
    assert!(s(&e[3]).parse::<f64>().unwrap() >= 1.0, "duration in ms, past the threshold");

    assert_eq!(run(cmd(&["GRAPH.SLOWLOG", "default", "reset"])).await, RespValue::SimpleString("OK".into()));
    assert_eq!(int(&run(cmd(&["GRAPH.SLOWLOG", "LEN"])).await), 0);

    let other = run(cmd(&["GRAPH.SLOWLOG", "social"])).await;
    assert!(is_error_containing(&other, "single graph"), "{other:?}");
    let bad = run(cmd(&["GRAPH.SLOWLOG", "default", "PEEK"])).await;
    assert!(is_error_containing(&bad, "usage"), "{bad:?}");
}
