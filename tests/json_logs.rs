//! `SAMYAMA_LOG_FORMAT=json` writes one JSON object per line (#2076).
//!
//! The server's JSON subscriber is installed over a buffer and a slow query is
//! run through it, so what is checked is the configuration the binary uses and
//! the event an operator would actually be looking for.

use std::io::Write;
use std::sync::{Arc, Mutex};

use samyama::graph::GraphStore;
use samyama::logging::{json_subscriber, LogFormat};
use samyama::query::{query_hash, QueryEngine};

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
    type Writer = Buffer;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn a_slow_query_is_logged_as_one_json_object_with_its_fields() {
    let out = Buffer::default();
    // Slow at 1 ms: the query below takes longer than that in any build.
    let engine = QueryEngine::new().with_slow_query_ms(1);
    let store = GraphStore::new();
    let query = "UNWIND range(1, 300000) AS x   RETURN sum(x) AS s";

    tracing::subscriber::with_default(json_subscriber(out.clone()), || {
        let span = tracing::info_span!("request", request_id = "r-1", tenant = "default");
        let _g = span.enter();
        engine.execute(query, &store).unwrap();
    });

    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}")))
        .collect();
    let slow = lines
        .iter()
        .find(|l| l["fields"]["message"] == "slow query")
        .unwrap_or_else(|| panic!("no slow-query event in {text}"));

    assert_eq!(slow["level"], "WARN");
    assert!(slow["timestamp"].is_string() && slow["target"].is_string(), "{slow}");
    let f = &slow["fields"];
    assert_eq!(f["failed"], false);
    assert_eq!(f["is_write"], false);
    assert_eq!(f["rows"], 1);
    assert_eq!(f["threshold_ms"], 1);
    assert!(f["elapsed_ms"].as_u64().unwrap() >= 1, "{f}");
    assert_eq!(f["query_hash"], query_hash(query));
    // The span the event ran inside, which is what correlates it.
    assert_eq!(slow["span"]["request_id"], "r-1", "{slow}");
    assert_eq!(slow["spans"][0]["tenant"], "default", "{slow}");
}

#[test]
fn the_query_hash_ignores_spacing_and_tells_statements_apart() {
    let a = query_hash("MATCH (n)  RETURN n");
    assert_eq!(a, query_hash("MATCH (n)\n RETURN n"));
    assert_ne!(a, query_hash("MATCH (m) RETURN m"));
    assert_eq!(a.len(), 16);
    assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn the_format_comes_from_the_flag_then_the_environment_then_text() {
    let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert_eq!(LogFormat::resolve(&args(&["samyama"]), None), Ok(LogFormat::Text));
    assert_eq!(LogFormat::resolve(&args(&["samyama"]), Some("JSON")), Ok(LogFormat::Json));
    assert_eq!(LogFormat::resolve(&args(&["samyama"]), Some("")), Ok(LogFormat::Text));
    assert_eq!(
        LogFormat::resolve(&args(&["samyama", "--log-format", "text"]), Some("json")),
        Ok(LogFormat::Text),
        "the flag wins over the environment"
    );
    assert_eq!(
        LogFormat::resolve(&args(&["samyama", "--log-format=json"]), None),
        Ok(LogFormat::Json)
    );
    // A typo is refused, not read as text.
    assert!(LogFormat::resolve(&args(&["samyama"]), Some("jsn")).unwrap_err().contains("jsn"));
    assert!(LogFormat::resolve(&args(&["samyama", "--log-format"]), None).is_err());
}
