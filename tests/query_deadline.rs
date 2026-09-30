//! A query that runs past its deadline is stopped by the engine (#1311).
//!
//! REL-15 listed "query deadline exceeded" as a gap: `with_deadline` and
//! `check_deadline` existed, one executor-level test handed the executor a
//! deadline already in the past, and nothing drove a query through
//! `QueryEngine` -- the surface the RESP and HTTP handlers call -- past the
//! deadline the engine sets for itself from `SAMYAMA_QUERY_TIMEOUT`.
//!
//! The variable is read once, when the engine is built, and is whole seconds,
//! so the shortest deadline it can express is one second. This file holds one
//! test so that setting the variable cannot reach another test in the same
//! process.
//!
//! Nothing here asserts how long anything took. The query is sized to be far
//! more work than a second on any host in either build profile, and the test
//! asserts what comes back: a timeout error, not a result. A watchdog turns a
//! deadline that never fires into a failure instead of a hung test run.
//!
//! Not covered: `ResourceQuotas::max_query_time_ms`. Nothing reads it to stop a
//! query, so a tenant's own limit has no behaviour to test yet.

use std::sync::mpsc;
use std::time::Duration;

/// Only for a deadline that never fires. Not a timing assertion: a run that
/// ends before it passes or fails on what the engine returned.
const WATCHDOG: Duration = Duration::from_secs(600);

#[test]
fn a_query_past_the_engine_deadline_is_stopped_with_a_timeout_error() {
    // The only test in this binary, so no other thread reads the environment
    // while it is written.
    std::env::set_var("SAMYAMA_QUERY_TIMEOUT", "1");
    // The row budget would refuse the cross product first, which is a
    // different failure (row 22). Off, so the deadline is what stops it.
    std::env::set_var("SAMYAMA_ROW_BUDGET", "0");
    let engine = samyama::QueryEngine::new();
    let store = samyama::GraphStore::new();

    // A control on the same engine: an ordinary query is untouched by the
    // deadline, so the refusal below is about time, not about this engine.
    let small = engine
        .execute("UNWIND range(1, 10) AS x RETURN count(*) AS c", &store)
        .expect("a small query finishes inside the deadline");
    assert_eq!(small.records.len(), 1);

    // 40,000 x 40,000 = 1.6 billion rows into one count.
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let outcome = engine
            .execute(
                "UNWIND range(1, 40000) AS a UNWIND range(1, 40000) AS b RETURN count(*) AS c",
                &store,
            )
            .map(|batch| format!("{:?}", batch.records))
            .map_err(|e| e.to_string());
        // The deadline is per query, not a latch on the engine.
        let after = engine
            .execute("RETURN 1 AS one", &store)
            .map(|b| b.records.len())
            .map_err(|e| e.to_string());
        let _ = tx.send((outcome, after));
    });
    let (outcome, after) = rx
        .recv_timeout(WATCHDOG)
        .expect("the query was not stopped: the deadline never fired");

    match outcome {
        Err(msg) => assert!(
            msg.contains("timed out"),
            "stopped, but not by the deadline: {msg}"
        ),
        Ok(rows) => panic!("the query ran to completion past its deadline: {rows}"),
    }
    assert_eq!(after, Ok(1), "the engine refused the next query");
}
