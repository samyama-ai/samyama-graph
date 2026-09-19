//! No function call aborts the process, whatever it is called with.
//!
//! `RETURN toUpper()` panicked on `args[0]` — "index out of bounds: the len is
//! 0 but the index is 0" — which unwound the tokio worker and took the server
//! down. With no persistence configured that is the whole in-memory graph, from
//! fourteen characters, over an HTTP endpoint that reads no credential (#1328).
//!
//! **This is a class, not a case.** A sweep of every name in `KNOWN_FUNCTIONS`
//! against 0–3 arguments found **69** (name, argument-count) pairs that
//! panicked — 67 of them at zero arguments, plus `atan2` and `hasLabels` at
//! one. Fixing `toUpper` would have left sixty-eight.
//!
//! So the test is the sweep. A function added later that indexes its arguments
//! without a row in `MIN_ARITY` fails here rather than in production, which is
//! the only way this stays fixed.

use std::panic::{catch_unwind, AssertUnwindSafe};

use samyama::graph::GraphStore;
use samyama::query::executor::operator::KNOWN_FUNCTIONS;
use samyama::query::QueryEngine;

/// Run one call and say whether it aborted, rather than whether it succeeded.
///
/// An error is a perfectly good outcome here — most of these calls are
/// nonsense and *should* be refused. The only unacceptable answer is a panic.
fn panicked(query: &str) -> bool {
    catch_unwind(AssertUnwindSafe(|| {
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();
        let _ = engine.execute_mut(query, &mut store, "default");
    }))
    .is_err()
}

#[test]
fn no_known_function_panics_on_any_argument_count() {
    // The hook is silenced so a run that finds nothing is not buried in
    // backtraces, and restored afterwards so a genuine panic elsewhere in the
    // suite still prints.
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let mut offenders = Vec::new();
    for name in KNOWN_FUNCTIONS {
        // `KNOWN_FUNCTIONS` is lower case and nobody writes `toupper(`. The
        // first version of this sweep used the list's spelling and passed
        // while `toUpper()` still aborted the process, because the arity
        // lookup was case-sensitive. Both spellings, so that hole cannot
        // reopen.
        let upper: String = match name.split_once(|c: char| c == '.') {
            // `point.distance` -> `point.Distance` keeps the namespace intact.
            Some((ns, rest)) => format!("{ns}.{}{}", rest[..1].to_uppercase(), &rest[1..]),
            None => format!("{}{}", name[..1].to_uppercase(), &name[1..]),
        };
        for spelling in [name.to_string(), upper] {
            for argc in 0..=3usize {
                let args = vec!["1"; argc].join(", ");
                let query = format!("RETURN {spelling}({args})");
                if panicked(&query) {
                    offenders.push(format!("{spelling}/{argc}"));
                }
            }
        }
    }

    std::panic::set_hook(previous);

    assert!(
        offenders.is_empty(),
        "{} call(s) aborted the process instead of returning an error: {offenders:?}",
        offenders.len()
    );
}

#[test]
fn too_few_arguments_is_refused_with_a_message_that_says_how_many() {
    // "wrong number of arguments" sends the reader to the documentation. The
    // count sends them to the fix.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let err = engine
        .execute_mut("RETURN toUpper()", &mut store, "default")
        .expect_err("toUpper() takes one argument");
    let text = err.to_string();
    assert!(text.contains("at least 1 argument"), "{text}");
    assert!(text.contains("got 0"), "{text}");
}

#[test]
fn a_two_argument_function_says_two() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let err = engine
        .execute_mut("RETURN atan2(1)", &mut store, "default")
        .expect_err("atan2 takes two");
    assert!(err.to_string().contains("at least 2 arguments"), "{err}");
}

#[test]
fn the_right_number_of_arguments_still_works() {
    // The half that keeps the guard honest: a check that refuses everything
    // passes the sweep above and breaks the product.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in [
        "RETURN toUpper('ab') AS x",
        "RETURN atan2(1.0, 1.0) AS x",
        "RETURN abs(-1) AS x",
        "RETURN size([1, 2]) AS x",
    ] {
        engine
            .execute_mut(q, &mut store, "default")
            .unwrap_or_else(|e| panic!("{q} was refused: {e}"));
    }
}
