//! The RESP port writes to the audit log and answers `INFO` with real numbers
//! (#2081).
//!
//! Over a real socket: the entry is written where the reply is known, and what
//! has to hold is what a client sees and what lands in the file.

use samyama::http::server::{read_credentials, AuditLog};
use samyama::{GraphStore, RespServer, ServerConfig};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

async fn serve(audit: Option<&Path>) -> u16 {
    let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
    let config = ServerConfig { port, ..ServerConfig::default() };
    let mut server = RespServer::new(config, Arc::new(RwLock::new(GraphStore::new())));
    if let Some(path) = audit {
        server = server.with_audit_log(Arc::new(AuditLog::open(path).unwrap()));
    }
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return port;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the RESP server did not start");
}

/// Send one command and return the raw reply. Replies here all fit one read.
async fn send(s: &mut TcpStream, args: &[&str]) -> String {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{a}\r\n", a.len()));
    }
    s.write_all(out.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 65536];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
        .await
        .expect("no reply in 5s")
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn entries(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn field(info: &str, name: &str) -> Option<u64> {
    info.lines().find_map(|l| l.strip_prefix(&format!("{name}:"))?.trim().parse().ok())
}

#[tokio::test]
async fn writes_are_recorded_and_reads_are_not() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let port = serve(Some(&path)).await;
    let mut c = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    let create = "CREATE (:Secret {name: 'alice'})";
    send(&mut c, &["GRAPH.QUERY", "default", create]).await;
    send(&mut c, &["GRAPH.QUERY", "default", "MATCH (n) RETURN n"]).await;
    send(&mut c, &["GRAPH.RO_QUERY", "default", "MATCH (n) RETURN count(n)"]).await;
    send(&mut c, &["PING"]).await;
    let refused = send(&mut c, &["GRAPH.RO_QUERY", "default", "CREATE (:X)"]).await;
    assert!(refused.starts_with('-'), "{refused}");
    send(&mut c, &["GRAPH.BEGIN"]).await;
    send(&mut c, &["GRAPH.QUERY", "default", "CREATE (:InTxn)"]).await;
    send(&mut c, &["GRAPH.COMMIT"]).await;
    send(&mut c, &["GRAPH.BEGIN"]).await;
    send(&mut c, &["GRAPH.ROLLBACK"]).await;
    send(&mut c, &["GRAPH.QUERY", "default", "THIS IS NOT CYPHER"]).await;
    send(&mut c, &["GRAPH.DELETE", "default"]).await;

    let log = entries(&path);
    let seen: Vec<(&str, bool)> = log
        .iter()
        .map(|e| (e["command"].as_str().unwrap(), e["ok"].as_bool().unwrap()))
        .collect();
    assert_eq!(
        seen,
        [
            ("GRAPH.QUERY", true),
            ("GRAPH.RO_QUERY", false),
            ("GRAPH.QUERY", true),
            ("GRAPH.COMMIT", true),
            ("GRAPH.ROLLBACK", true),
            ("GRAPH.QUERY", false),
            ("GRAPH.DELETE", true),
        ]
    );

    let first = &log[0];
    assert_eq!(first["protocol"], "resp");
    assert_eq!(first["subject"], "unauthenticated");
    assert_eq!(first["tenant"], "default");
    assert_eq!(first["is_write"], true);
    let hash = first["query_hash"].as_str().unwrap();
    assert_eq!(hash.len(), 64, "{hash}");
    assert_ne!(hash, log[2]["query_hash"].as_str().unwrap(), "two statements, two hashes");
    assert!(log[3]["query_hash"].is_null(), "COMMIT has no statement");

    let text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.contains("alice") && !text.contains("Secret"), "the query text leaked: {text}");
}

#[tokio::test]
async fn an_entry_names_the_credential_and_a_refused_write_is_recorded() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.jsonl");
    let digest: String =
        Sha256::digest(b"s3cret-token").iter().map(|b| format!("{b:02x}")).collect();
    let creds = dir.path().join("creds");
    std::fs::write(&creds, format!("ops:{digest}:roles=admin\n")).unwrap();

    let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
    let config = ServerConfig { port, ..ServerConfig::default() };
    let server = RespServer::new(config, Arc::new(RwLock::new(GraphStore::new())))
        .with_credentials(Arc::new(read_credentials(&creds).unwrap()))
        .with_audit_log(Arc::new(AuditLog::open(&path).unwrap()));
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    let mut c = loop {
        match TcpStream::connect(("127.0.0.1", port)).await {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };

    let refused = send(&mut c, &["GRAPH.QUERY", "default", "CREATE (:X)"]).await;
    assert!(refused.starts_with("-NOAUTH"), "{refused}");
    send(&mut c, &["AUTH", "ops", "s3cret-token"]).await;
    send(&mut c, &["GRAPH.QUERY", "default", "CREATE (:X)"]).await;

    let log = entries(&path);
    assert_eq!(log.len(), 2, "{log:?}");
    assert_eq!((&log[0]["subject"], &log[0]["ok"]), (&"unauthenticated".into(), &false.into()));
    assert_eq!((&log[1]["subject"], &log[1]["ok"]), (&"ops".into(), &true.into()));
    assert!(!std::fs::read_to_string(&path).unwrap().contains("s3cret"));
}

#[tokio::test]
async fn info_reports_the_server_as_it_is() {
    let port = serve(None).await;
    let mut a = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut b = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    send(&mut a, &["GRAPH.QUERY", "default", "CREATE (:P {i: 1})-[:R]->(:P {i: 2})"]).await;
    send(&mut b, &["PING"]).await;

    let info = send(&mut a, &["INFO"]).await;
    assert_eq!(field(&info, "nodes_total"), Some(2), "{info}");
    assert_eq!(field(&info, "edges_total"), Some(1), "{info}");
    assert!(field(&info, "used_memory").unwrap() > 0, "{info}");
    assert!(field(&info, "connected_clients").unwrap() >= 2, "{info}");
    assert!(field(&info, "total_queries_processed").unwrap() >= 1, "{info}");
    for name in ["graphs_total", "total_queries_failed", "slow_queries_total", "ast_cache_hits", "result_cache_hits"] {
        assert!(field(&info, name).is_some(), "{name} missing: {info}");
    }

    // Inside a transaction the connection holds the writer's lock: its own INFO
    // must read through that lock, not wait on it, and sees its own write.
    send(&mut a, &["GRAPH.BEGIN"]).await;
    send(&mut a, &["GRAPH.QUERY", "default", "CREATE (:P {i: 3})"]).await;
    let own = send(&mut a, &["INFO"]).await;
    assert_eq!(field(&own, "nodes_total"), Some(3), "{own}");

    // Another connection cannot read the store, and says so rather than hanging
    // behind the transaction or reporting zero.
    let other = send(&mut b, &["INFO"]).await;
    assert_eq!(field(&other, "store_busy"), Some(1), "{other}");
    assert_eq!(field(&other, "used_memory"), None, "{other}");
    assert_eq!(field(&other, "nodes_total"), None, "{other}");

    send(&mut a, &["GRAPH.ROLLBACK"]).await;
    let after = send(&mut b, &["INFO"]).await;
    assert_eq!(field(&after, "nodes_total"), Some(2), "{after}");
    assert_eq!(field(&after, "store_busy"), None, "{after}");
}
