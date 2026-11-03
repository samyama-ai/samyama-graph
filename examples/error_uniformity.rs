//! Does the same fault produce the same error on every surface? (API-03)
//!
//! API-03 asks for an *identical error code, message, and repair suggestion for
//! the same condition on every surface*. CH-ERR measures LANG-12 — codes and
//! spans — against the embedded API only, and says so: the HTTP and RESP
//! surfaces were never compared against it, so API-03 has stood `unmeasured`.
//!
//! This runs the same fault corpus three ways in one process: the embedded
//! `QueryExecutor`, `GRAPH.QUERY` over RESP, and `POST /api/query` over HTTP.
//! Same store, same queries, same build — so a difference is the surface and
//! nothing else.
//!
//! **Normalisation is the trap.** Each surface has a wrapper: RESP prefixes
//! `ERR ` and escapes CR and LF because its line types cannot carry them
//! (#1322); HTTP wraps the text in a JSON `error` field. Strip too much and
//! every surface agrees by construction, which is how this measurement would
//! report a perfect score while a caller still could not write one error
//! handler. So only the transport envelope is removed — the RESP `ERR ` token
//! and its two escapes, and the JSON field — and what is compared after that is
//! the text the engine produced.
//!
//! Three properties, kept apart because they fail apart:
//!
//! * **same code** — the extracted code token matches across surfaces. A class
//!   where *no* surface produces a code is not agreement; it is counted as
//!   `no_code_anywhere` and excluded from the numerator and the denominator
//!   both, and reported, because "they all said nothing" is not uniformity.
//! * **same message** — the engine's text matches after the envelope is
//!   removed.
//! * **all surfaces errored** — a surface that accepted the bad query has not
//!   agreed about the error; it failed to have one. This is checked first,
//!   because a class where two surfaces error and one succeeds would otherwise
//!   score as two-thirds agreement on the strength of a silent wrong answer.
//!
//! ```text
//! cargo run --release --example error_uniformity -- --json uniformity.json
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::http::HttpServer;
use samyama::protocol::server::{RespServer, ServerConfig};
use samyama::query::QueryEngine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;

/// The CH-ERR corpus, unchanged. Written by fault class rather than by what the
/// engine produces, so a class we handle badly on one surface is visible.
const CASES: &[(&str, &str)] = &[
    ("syntax", "MATCH (n RETURN n"),
    ("syntax", "RETRUN 1"),
    ("unknown-function", "RETURN lenght('abc')"),
    ("unknown-procedure", "CALL nosuch.procedure()"),
    ("unknown-algorithm", "CALL algo.noSuchAlgorithmExists()"),
    ("unbound-variable", "RETURN x"),
    ("unbound-variable", "MATCH (n) RETURN m.name"),
    ("type-error", "RETURN 1 + {a: 1}"),
    ("type-error", "MATCH (n) RETURN n + 1"),
    ("bad-argument", "RETURN range(1, 10, 0)"),
    ("bad-argument", "RETURN substring('abc')"),
    ("collection-in-pattern", "WITH [1] AS xs MATCH (xs)-->() RETURN 1"),
    ("aggregate-misuse", "MATCH (n) WHERE count(*) > 1 RETURN n"),
];

/// The same detector CH-ERR uses, so the two measurements are talking about the
/// same thing when they both say "code".
fn extract_code(msg: &str) -> Option<String> {
    msg.split_whitespace()
        .map(|t| t.trim_matches(|c: char| !c.is_alphanumeric() && c != '.' && c != '-'))
        .find(|t| {
            (t.contains('.')
                && t.split('.').count() >= 3
                && t.chars().next().is_some_and(|c| c.is_ascii_uppercase()))
                || (t.contains('-')
                    && t.split('-')
                        .next()
                        .is_some_and(|p| p.len() >= 2 && p.chars().all(|c| c.is_ascii_uppercase())))
        })
        .map(str::to_string)
}

/// Remove the transport envelope and nothing else.
///
/// Deliberately short: every rule added here buys agreement that a caller does
/// not get. `ERR ` is RESP's error marker, not part of the message; the rest is
/// what the engine said.
fn strip_envelope(surface: &str, msg: &str) -> String {
    let m = msg.trim();
    match surface {
        // `ERR ` is RESP's error marker. The `\n` and `\r` escapes are the
        // encoder's own (#1322): RESP line types may not carry CR or LF, so
        // `escape_line` puts them in as two characters. Reversing exactly that
        // recovers the bytes the engine produced and adds no agreement -- it is
        // the transport being undone, not the messages being made to match. Any
        // rule beyond these two would be.
        "resp" => m
            .strip_prefix("ERR ")
            .unwrap_or(m)
            .replace("\\r", "\r")
            .replace("\\n", "\n")
            .trim()
            .to_string(),
        _ => m.trim().to_string(),
    }
}

/// The embedded surface, through `QueryEngine` -- which is what
/// `samyama_sdk::EmbeddedClient` holds and uses.
///
/// This called `parse_query` then `QueryExecutor::execute` directly, an
/// internal pair no embedded user reaches. It made no difference until errors
/// started carrying a span, which `QueryEngine` attaches because it is the
/// layer that has the query text; then this arm was the only one without one
/// and the three surfaces "disagreed" on a difference that exists nowhere but
/// in this probe (LANG-12).
fn embedded(store: &GraphStore, q: &str) -> String {
    match QueryEngine::new().execute(q, store) {
        Err(e) => format!("{e}"),
        Ok(_) => String::new(),
    }
}

/// One RESP round trip. A fresh connection per query: connection reuse would
/// make an error that poisons the session look like an error on the next query.
async fn resp(port: u16, q: &str) -> Result<String, String> {
    let mut s = TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let cmd = format!(
        "*3\r\n$11\r\nGRAPH.QUERY\r\n$7\r\ndefault\r\n${}\r\n{}\r\n",
        q.len(),
        q
    );
    s.write_all(cmd.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    let mut buf = vec![0u8; 65536];
    let n = s.read(&mut buf).await.map_err(|e| format!("read: {e}"))?;
    let raw = String::from_utf8_lossy(&buf[..n]).to_string();
    // `-` is RESP's error type byte. Anything else is a result: the query did
    // not fail on this surface, which is the finding, not a parse problem here.
    Ok(if let Some(rest) = raw.strip_prefix('-') {
        rest.lines().next().unwrap_or("").to_string()
    } else {
        String::new()
    })
}

async fn http(port: u16, q: &str) -> Result<String, String> {
    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/api/query"))
        .json(&serde_json::json!({"query": q, "graph": "default"}))
        .send()
        .await
        .map_err(|e| format!("send: {e}"))?;
    let body: serde_json::Value = resp.json().await.map_err(|e| format!("json: {e}"))?;
    Ok(body
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string())
}

/// One node with one real property, so `n.name + [1,2]` is String + List rather
/// than null + List -- `null + anything` is null in Cypher, and a fixture that
/// leaves the property unset makes the type-error case untriggerable.
fn fixture() -> GraphStore {
    let mut s = GraphStore::new();
    let n = s.create_node_with_labels([Label::new("N")]);
    s.set_node_property("default", n, "name", PropertyValue::String("a".into()))
        .unwrap();
    s
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1))
        .cloned();

    // `GraphStore` is not `Clone`, so the two stores are built by the same
    // function rather than copied. Same labels, same property, same order --
    // and the corpus does not depend on ids, so a divergence in the results is
    // the surface and not the fixture.
    let base = fixture();
    let store = Arc::new(RwLock::new(fixture()));

    // Ephemeral ports: a fixed port would collide with a developer's own server
    // and the failure would look like a uniformity defect.
    let resp_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let resp_port = resp_listener.local_addr().unwrap().port();
    drop(resp_listener);
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();

    let srv = RespServer::new(
        ServerConfig {
            address: "127.0.0.1".into(),
            port: resp_port,
            max_connections: 64,
            data_path: None,
        },
        store.clone(),
    );
    tokio::spawn(async move {
        let _ = srv.start().await;
    });
    let router = HttpServer::new(store.clone(), http_port).router();
    tokio::spawn(async move {
        let _ = axum::serve(http_listener, router).await;
    });
    // Wait for the RESP port to accept rather than sleeping a guess: a fixed
    // sleep either wastes time or measures a connection refusal as a defect.
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", resp_port)).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    let mut rows = Vec::new();
    for (class, q) in CASES {
        let mut msgs: BTreeMap<&str, String> = BTreeMap::new();
        msgs.insert("embedded", strip_envelope("embedded", &embedded(&base, q)));
        match resp(resp_port, q).await {
            Ok(m) => {
                msgs.insert("resp", strip_envelope("resp", &m));
            }
            Err(e) => {
                eprintln!("{class}: RESP transport failed: {e}");
                continue;
            }
        }
        match http(http_port, q).await {
            Ok(m) => {
                msgs.insert("http", strip_envelope("http", &m));
            }
            Err(e) => {
                eprintln!("{class}: HTTP transport failed: {e}");
                continue;
            }
        }

        let silent: Vec<&str> = msgs
            .iter()
            .filter(|(_, m)| m.is_empty())
            .map(|(s, _)| *s)
            .collect();
        let codes: BTreeMap<&str, Option<String>> = msgs
            .iter()
            .map(|(s, m)| (*s, extract_code(m)))
            .collect();
        let distinct_codes: std::collections::BTreeSet<_> = codes.values().cloned().collect();
        let distinct_msgs: std::collections::BTreeSet<_> = msgs.values().cloned().collect();
        let no_code_anywhere = codes.values().all(|c| c.is_none());

        rows.push(serde_json::json!({
            "class": class,
            "query": q,
            "silent_surfaces": silent,
            "all_errored": silent.is_empty(),
            "no_code_anywhere": no_code_anywhere,
            "same_code": silent.is_empty() && !no_code_anywhere && distinct_codes.len() == 1,
            "same_message": silent.is_empty() && distinct_msgs.len() == 1,
            "codes": codes,
            "messages": msgs.iter().map(|(s, m)| {
                (*s, m.chars().take(200).collect::<String>())
            }).collect::<BTreeMap<_, _>>(),
        }));
    }

    let probed = rows.len();
    let all_errored = rows.iter().filter(|r| r["all_errored"] == true).count();
    let codeless = rows.iter().filter(|r| r["no_code_anywhere"] == true).count();
    // The denominator for code agreement is the classes where a code exists to
    // agree about. Counting the codeless ones as agreeing would let the score
    // rise by *removing* codes.
    let code_comparable = rows
        .iter()
        .filter(|r| r["all_errored"] == true && r["no_code_anywhere"] == false)
        .count();
    let same_code = rows.iter().filter(|r| r["same_code"] == true).count();
    let same_message = rows.iter().filter(|r| r["same_message"] == true).count();

    let doc = serde_json::json!({
        "surfaces": ["embedded", "resp", "http"],
        "probed": probed,
        "all_errored": all_errored,
        "codeless_classes": codeless,
        "code_comparable": code_comparable,
        "same_code": same_code,
        "same_message": same_message,
        "cases": rows,
    });
    eprintln!(
        "{probed} classes: {all_errored} errored on all 3 surfaces, \
         same code {same_code}/{code_comparable} comparable ({codeless} codeless), \
         same message {same_message}/{all_errored}"
    );
    if let Some(path) = json_out {
        std::fs::write(&path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
        eprintln!("wrote {path}");
    } else {
        println!("{}", serde_json::to_string_pretty(&doc).unwrap());
    }
}
