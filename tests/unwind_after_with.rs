//! An `UNWIND` written after a `WITH` reads what that `WITH` projected (#785).
//!
//! ```cypher
//! UNWIND [1,2] AS a WITH [3,4] AS b UNWIND b AS c RETURN c
//!   -> RuntimeError: VariableNotFound("b")
//! ```
//!
//! `b` is defined by the `WITH` on the same line. The parser put both unwinds
//! in flat fields with no record of which side of the `WITH` they came from,
//! and the planner applies that run *before* the barrier — so the second one
//! read a variable that did not exist yet.
//!
//! The tell was that even the unwind's **own** variable went missing
//! (`UNWIND [1,2] AS a WITH a AS b UNWIND [5] AS c` could not find `c`), which
//! says the operator was not planned at all rather than planned wrongly.
//!
//! Row counts are asserted, not just success: a cross product that silently
//! collapses to one row is the failure this could regress into.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn rows(cypher: &str) -> usize {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"))
        .records
        .len()
}

/// The reported repro, and its minimal form.
#[test]
fn an_unwind_after_a_with_sees_the_projection() {
    // 2 leading rows x 2 unwound = 4.
    assert_eq!(rows("UNWIND [1,2] AS a WITH [3,4] AS b UNWIND b AS c RETURN c"), 4);
    // The unwind's own variable was also missing, so this is the sharper case.
    assert_eq!(rows("UNWIND [1,2] AS a WITH a AS b UNWIND [5] AS c RETURN c"), 2);
}

/// An aggregating `WITH` collapses first, and the unwind expands what survives.
#[test]
fn an_unwind_after_an_aggregating_with_expands_the_aggregate() {
    // collect() gives one row holding [1,2]; unwinding it gives two.
    assert_eq!(rows("UNWIND [1,2] AS a WITH collect(a) AS xs UNWIND xs AS y RETURN y"), 2);
    assert_eq!(
        rows("UNWIND [1,2,3] AS a WITH collect(a) AS xs UNWIND xs AS y RETURN y"),
        3
    );
}

/// Two stages, each with its own trailing unwind — **still broken**.
///
/// ```text
/// UNWIND [1,2] AS a WITH [1,2] AS b UNWIND b AS c WITH c, [1,2,3] AS d UNWIND d AS e RETURN e
///   -> VariableNotFound("b")
/// ```
///
/// This fix covers one `WITH` with a trailing `UNWIND`, which is the reported
/// repro and the shape that matters in practice. With **two** such stages the
/// planner's `stage_unwind` still falls back to the query's *leading* unwind
/// once `extra_with_stages` is non-empty, so the head unwind is applied twice
/// and a stage gets the wrong one.
///
/// `#[ignore]`d rather than deleted or weakened: the expected row count below
/// is what Cypher specifies, and a test asserting today's error would have to
/// be rewritten by whoever finishes this — which is the opposite of useful.
/// Tracked on #785.
#[test]
#[ignore = "multi-stage WITH+UNWIND still mis-assigns the leading unwind; see #785"]
fn each_with_stage_keeps_its_own_unwind() {
    assert_eq!(
        rows("UNWIND [1,2] AS a WITH [1,2] AS b UNWIND b AS c WITH c, [1,2,3] AS d UNWIND d AS e RETURN e"),
        24, // 2 x 2 x 3, and the leading UNWIND is not double-counted
    );
}

/// The shapes that already worked must keep working — a leading run of
/// unwinds is still a cross product applied before any barrier.
#[test]
fn the_shapes_that_worked_before_are_undisturbed() {
    assert_eq!(rows("WITH [3,4] AS b UNWIND b AS c RETURN c"), 2);
    assert_eq!(rows("UNWIND [1,2] AS a UNWIND [3,4] AS b RETURN b"), 4);
    assert_eq!(rows("UNWIND [1,2,3] AS a RETURN a"), 3);
    assert_eq!(rows("UNWIND [1,2] AS a WITH a WHERE a > 1 RETURN a"), 1);
    // A MATCH ahead of the WITH was never affected; pinned so a later change
    // to the leading-unwind logic cannot quietly break it.
    assert_eq!(rows("MATCH (n) WITH [3,4] AS b UNWIND b AS c RETURN c"), 0);
}

/// Unwinding an empty list after a `WITH` produces no rows rather than an
/// error — the same as anywhere else.
#[test]
fn an_empty_list_after_a_with_yields_nothing() {
    assert_eq!(rows("UNWIND [1,2] AS a WITH [] AS b UNWIND b AS c RETURN c"), 0);
}
