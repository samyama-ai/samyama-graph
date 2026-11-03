//! Which statements are writes (#1111).
//!
//! Each server used to decide this by matching strings against the query text, with
//! a different list of keywords per transport. The RESP list had no `REMOVE`; the
//! HTTP list only looked past the first keyword when the statement began with
//! `MATCH`, so `UNWIND [1] AS x CREATE (:X)` — the canonical parameterised bulk
//! insert — was routed to the read-only executor and refused with a 400.
//!
//! `Query::is_write()` replaces both. It is a second implementation of a judgement
//! the planner also makes, so the first test here is that the two agree; without it
//! this file would be documenting a third list rather than removing two.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

/// Statements whose classification is not in doubt, including every shape the two
/// string matchers disagreed about.
const WRITES: &[&str] = &[
    "CREATE (:X {v: 1})",
    "MATCH (n:X) SET n.v = 2",
    "MATCH (n:X) DELETE n",
    "MATCH (n:X) DETACH DELETE n",
    "MATCH (n:X) REMOVE n.v",
    "MERGE (:X {v: 1})",
    // Rejected with a 400 over HTTP before this change: not `MATCH`-prefixed.
    "UNWIND [1] AS x CREATE (:X {v: x})",
    "WITH 1 AS x CREATE (:X {v: x})",
    "FOREACH (x IN [1] | CREATE (:X {v: x}))",
    "MATCH (n:X) WITH n LIMIT 1 SET n.v = 3",
];

const READS: &[&str] = &[
    "MATCH (n:X) RETURN n",
    "RETURN 1",
    "MATCH (n:X) WHERE n.v > 1 RETURN count(n)",
    "UNWIND [1, 2] AS x RETURN x",
    "WITH 1 AS x RETURN x",
    // The text says DELETE; the statement does not.
    "MATCH (n:X) WHERE n.note = \"please DELETE this\" RETURN n",
];

fn engine() -> QueryEngine {
    QueryEngine::new()
}

#[test]
fn writes_are_writes_and_reads_are_reads() {
    let e = engine();
    for q in WRITES {
        assert!(e.statement_is_write(q).unwrap(), "classified as a read: {q}");
    }
    for q in READS {
        assert!(!e.statement_is_write(q).unwrap(), "classified as a write: {q}");
    }
}

/// The read-only executor refuses a write with a typed error, so a statement this
/// module calls a read must actually run there. That is the property the string
/// matchers broke: `UNWIND [1] AS x CREATE (:X)` was called a read and came back
/// `WriteInReadTransaction`, which is the planner disagreeing after the fact.
#[test]
fn nothing_classified_as_a_read_is_refused_by_the_read_executor() {
    let e = engine();
    let store = GraphStore::new();
    for q in READS {
        if let Err(err) = e.execute(q, &store) {
            assert!(
                !err.to_string().contains("WriteInReadTransaction"),
                "classified as a read and refused as a write: {q} ({err})"
            );
        }
    }
}

/// And the converse: every statement called a write is one the read executor
/// refuses. A write misfiled as a read is the expensive direction — before #1107 it
/// would also have skipped the mutation journal.
#[test]
fn everything_classified_as_a_write_is_refused_by_the_read_executor() {
    let e = engine();
    let store = GraphStore::new();
    for q in WRITES {
        let err = e
            .execute(q, &store)
            .err()
            .unwrap_or_else(|| panic!("the read executor accepted a write: {q}"));
        assert!(
            err.to_string().contains("WriteInReadTransaction"),
            "{q} failed for another reason, so this asserts nothing: {err}"
        );
    }
}

/// A statement that does not parse is not a write, and is not an error *here*: the
/// caller gets the parse error from execution, which is where it got it before.
#[test]
fn an_unparseable_statement_is_not_classified() {
    assert!(engine().statement_is_write("MATCH ((( RETURN").is_err());
}
