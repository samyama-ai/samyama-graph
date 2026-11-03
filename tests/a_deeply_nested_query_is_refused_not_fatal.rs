//! A query cannot nest deeply enough to end the process (#1475).
//!
//! `RETURN ((((…1…))))` at around 218 parentheses overflowed the stack of the
//! tokio worker handling it and **aborted the process**, taking every other
//! connection and every other tenant with it. With no authentication on the
//! HTTP surface (#1328) the precondition was reachability and nothing else.
//!
//! A Rust stack overflow **aborts rather than unwinds**, so no `catch_unwind`
//! at the request boundary can contain it. It has to be refused before the
//! recursion starts.
//!
//! # Why the limit counts every bracket, and why 100
//!
//! Parentheses crash first. `[` and `{` recurse through the same descent and
//! crash further out — measured at **5,000** brackets on an unpatched release
//! binary, where 2,000 still answered `200`. So a guard on `(` alone would have
//! left the same defect one order of magnitude away.
//!
//! The limit is set by three measurements, and the tightest wins:
//!
//! - deepest nesting in **4,265 query literals** in this repository: **7**
//! - release build overflows at: **~218**
//! - **debug build overflows at: 72** (71 still parses)
//!
//! The debug figure is the one that binds, because CI runs the workspace tests
//! in debug. This file was first written with a limit of 100 and **the test
//! binary itself overflowed in debug** — the limit has to clear the tightest
//! build, not the one the server happens to ship.
//!
//! `SAMYAMA_MAX_NESTING_DEPTH` raises it for a workload that genuinely needs
//! more. It cannot remove it.

use samyama::query::parser::{nesting_limit, parse_query};

fn nested(open: char, close: char, depth: usize) -> String {
    format!(
        "RETURN {}1{}",
        open.to_string().repeat(depth),
        close.to_string().repeat(depth)
    )
}

#[test]
fn a_query_at_the_limit_still_parses() {
    // `nesting_limit()`, not the constant: `SAMYAMA_MAX_NESTING_DEPTH` may be
    // set in the environment, and a test that hardcodes the default asserts
    // something about the build rather than about the running configuration.
    let limit = nesting_limit();
    let q = nested('(', ')', limit);
    assert!(
        parse_query(&q).is_ok(),
        "depth {limit} is the limit in force and must be accepted, not refused"
    );
}

#[test]
fn one_past_the_limit_is_refused_with_a_message_that_says_why() {
    let limit = nesting_limit();
    let q = nested('(', ')', limit + 1);
    let err = parse_query(&q).expect_err("depth beyond the limit must be refused").to_string();
    assert!(err.contains(&(limit + 1).to_string()), "the error must state the depth found: {err}");
    assert!(err.contains(&limit.to_string()), "the error must state the limit: {err}");
}

/// The shape that actually aborted the process.
#[test]
fn the_depth_that_used_to_abort_the_process_is_refused() {
    let limit = nesting_limit();
    for depth in [218usize, 250, 1000].into_iter().filter(|d| *d > limit) {
        assert!(
            parse_query(&nested('(', ')', depth)).is_err(),
            "depth {depth} used to overflow the stack and must now be refused"
        );
    }
}

/// `[` and `{` recurse through the same descent and crashed at 5,000.
#[test]
fn brackets_and_braces_are_bounded_too() {
    let deep = 5_000.max(nesting_limit() + 1);
    for (open, close) in [('[', ']'), ('{', '}')] {
        assert!(
            parse_query(&nested(open, close, deep)).is_err(),
            "{open}{close} nesting at {deep} overflowed an unpatched binary and must be refused"
        );
    }
}

/// Brackets inside a string literal are data, not nesting. Counting them would
/// refuse a perfectly ordinary query.
#[test]
fn brackets_inside_a_string_do_not_count() {
    let q = format!("RETURN '{}' AS s", "(".repeat(300));
    assert!(
        parse_query(&q).is_ok(),
        "300 parens inside a string literal are data and must not trip the limit"
    );
}

/// The deepest thing this repository actually writes is 7.
#[test]
fn a_realistically_nested_query_is_unaffected() {
    let q = "CREATE (:City {name: 'x', loc: point({latitude: 1.0, longitude: 2.0})})";
    // Parsing is the contract here; whether CREATE accepts a point value is a
    // separate question this test deliberately does not assert.
    assert!(parse_query(q).is_ok(), "a depth-7 query must parse");
}
