//! A query that runs long enough says so in the log (REL-10).
//!
//! REL-10 asks whether somebody can diagnose a slow query without us. The
//! engine logged nothing about duration at all, so the first thing an operator
//! would ask -- *which* query is slow -- had no answer anywhere in the process.
//!
//! The threshold is set on the engine rather than through `SLOW_QUERY_MS`:
//! these tests run in parallel in one process, and a shared environment
//! variable would have them overwrite each other's setting. The variable still
//! supplies the default; this is the seam that makes it testable.

use std::sync::{Arc, Mutex};

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use tracing::subscriber;
use tracing_subscriber::layer::SubscriberExt;

/// Collects formatted log lines so a test can read them back.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run `queries` against an engine with this threshold, and return the log.
fn logs_for(threshold_ms: u64, queries: &[&str]) -> String {
    let sink = Captured::default();
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(sink.clone())
        .with_ansi(false);
    let collector = tracing_subscriber::registry().with(layer);

    subscriber::with_default(collector, || {
        let engine = QueryEngine::new().with_slow_query_ms(threshold_ms);
        let mut store = GraphStore::new();
        for q in queries {
            let _ = engine.execute_mut(q, &mut store, "default");
        }
    });

    let bytes = sink.0.lock().unwrap().clone();
    String::from_utf8_lossy(&bytes).to_string()
}

#[test]
fn a_query_past_the_threshold_is_logged_with_its_text() {
    // The text, not just a duration. "A query took 4.2 s" tells an operator
    // that something is wrong and nothing about what; the text is the only
    // part they can act on without us.
    // A query that genuinely takes a millisecond or two, rather than a
    // threshold low enough that anything counts: the log has to fire on real
    // elapsed time, and a 0 ms query proves nothing about that.
    let out = logs_for(
        1,
        &["UNWIND range(1, 20000) AS i CREATE (:Marker {tag: 'findme'})"],
    );
    assert!(out.contains("slow query"), "nothing logged:\n{out}");
    assert!(out.contains("findme"), "the query text is not in the line:\n{out}");
    assert!(out.contains("elapsed_ms"), "no duration:\n{out}");
    assert!(out.contains("threshold_ms"), "no threshold, so the line cannot be read in context:\n{out}");
}

#[test]
fn a_query_under_the_threshold_is_not_logged() {
    // The half that makes the other half mean something. A log that fires on
    // every query is not a slow-query log.
    let out = logs_for(60_000, &["CREATE (:Marker {tag: 'quiet'})"]);
    assert!(!out.contains("slow query"), "logged a fast query:\n{out}");
}

#[test]
fn zero_switches_the_log_off() {
    let out = logs_for(
        0,
        &["UNWIND range(1, 20000) AS i CREATE (:Marker {tag: 'silent'})"],
    );
    assert!(!out.contains("slow query"), "0 should disable the log:\n{out}");
}
