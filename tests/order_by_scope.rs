//! `ORDER BY` may not name a variable that is out of scope (#777).
//!
//! ```text
//! MATCH (a:A), (b:B), (c:C)
//! WITH a, b
//! WITH a ORDER BY c        <-- c was dropped two clauses ago
//! RETURN a
//! ```
//!
//! 40 TCK scenarios assert a `SyntaxError` here and we answered them, sorting
//! by a column that does not exist.
//!
//! `validate.rs` opens by saying scope analysis is deliberately absent, because
//! getting it slightly wrong rejects **valid** queries — a far worse failure
//! than accepting an invalid one. So most of this file is the accept side. Two
//! choices keep the rule safe:
//!
//! * the allowed set is the projection **plus the scope that preceded it**, so
//!   `MATCH (n) RETURN n.name ORDER BY n.age` stays legal;
//! * a shape the walk cannot account for leaves the scope empty and checks
//!   nothing.
//!
//! Both directions were wrong in the first implementation, and each mistake
//! measured as progress on its own — see the notes on the individual tests.

use samyama::query::parser::parse_query;

fn rejected(q: &str) -> bool {
    parse_query(q).is_err()
}
fn accepted(q: &str) -> bool {
    parse_query(q).is_ok()
}

/// The TCK's shape: a variable dropped by an earlier projection.
#[test]
fn sorting_by_a_dropped_variable_is_refused() {
    assert!(rejected(
        "MATCH (a:A), (b:B), (c:C) WITH a, b WITH a ORDER BY c RETURN a"
    ));
    // No pattern at all — the projection is the only scope there is.
    assert!(rejected(
        "WITH 1 AS a, 'b' AS b, 3 AS c, true AS d WITH a, b WITH a ORDER BY c RETURN a"
    ));
    // In any position among several sort keys.
    assert!(rejected(
        "WITH 1 AS a, 3 AS c WITH a WITH a ORDER BY a, c RETURN a"
    ));
}

/// A variable that was never bound anywhere.
#[test]
fn sorting_by_a_never_defined_variable_is_refused() {
    assert!(rejected("MATCH (a:A) WITH a ORDER BY zzz RETURN a"));
}

/// **A property of an in-scope variable is fine even when the projected column
/// is something else.**
///
/// `RETURN n.name ORDER BY n.age` is legal Cypher: the projected column is
/// `n.name`, but `n` is in scope. A rule that allowed only projected *columns*
/// would reject this, and that is the single most likely way to get this wrong.
#[test]
fn sorting_by_a_property_of_an_in_scope_variable_is_allowed() {
    assert!(accepted("MATCH (n) RETURN n.name ORDER BY n.age"));
    assert!(accepted("MATCH (n) WITH n RETURN n.name ORDER BY n.age"));
    assert!(accepted("MATCH (n) RETURN n ORDER BY n.name DESC"));
}

/// Ordinary shapes that must keep working.
#[test]
fn the_accept_side_is_undisturbed() {
    for q in [
        "MATCH (n) RETURN n ORDER BY n.name",
        "MATCH (n) WITH n AS m RETURN m ORDER BY m.name",
        "MATCH (a)-[r]->(b) WITH a, r, b ORDER BY r.weight RETURN a, b",
        "UNWIND [3,1,2] AS x RETURN x ORDER BY x",
        "UNWIND [3,1,2] AS x WITH x AS y RETURN y ORDER BY y",
        "MATCH (n) RETURN count(n) AS c ORDER BY c",
        "MATCH (n) RETURN n.name AS nm ORDER BY nm",
        // Renaming through two projections — a shape that broke the first
        // implementation because it walked the WITH stages in the wrong order.
        "UNWIND [1,2] AS x WITH x AS y WITH y AS x RETURN x ORDER BY x",
        "MATCH (a) RETURN a AS a, a AS b ORDER BY a",
    ] {
        assert!(accepted(q), "must still be accepted: {q}");
    }
}

/// A literal sort key names no variable and cannot be undefined.
#[test]
fn a_constant_sort_key_is_not_a_variable() {
    assert!(accepted("RETURN 1 AS r ORDER BY 1"));
    assert!(accepted("MATCH (n) RETURN n ORDER BY 1"));
}

/// Sorting by an expression over in-scope variables.
#[test]
fn expressions_over_in_scope_variables_are_allowed() {
    assert!(accepted("WITH 1 AS a WITH a ORDER BY a * a DESC RETURN a"));
    assert!(accepted("WITH 1 AS a WITH a ORDER BY -1 * a ASC, a DESC RETURN a"));
}
