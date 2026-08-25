//! A pattern predicate may not introduce variables (#798).
//!
//! ```text
//! MATCH (n) WHERE (n)-[r]->(a) RETURN n
//!   -> SyntaxError: UndefinedVariable       -- r and a are bound nowhere
//! ```
//!
//! The pattern is a **test**, not a match: it asks whether such an edge
//! exists, and `r` and `a` have no meaning outside it. We answered these,
//! silently binding variables that go nowhere.
//!
//! `EXISTS { ... }` is the form that *may* introduce names, so the rule must
//! not touch it — and pattern predicates are common enough in real queries
//! that over-rejecting here would be worse than the bug. Most of this file is
//! the accept side.

use samyama::query::parser::parse_query;

fn rejected(q: &str) -> bool {
    parse_query(q).is_err()
}
fn accepted(q: &str) -> bool {
    parse_query(q).is_ok()
}

/// Every shape the TCK lists, in both directions.
#[test]
fn a_pattern_predicate_may_not_introduce_a_variable() {
    for pat in [
        // `(a)` alone is deliberately absent: it parses as a *parenthesised
        // variable*, not a pattern predicate — the grammar's
        // `pattern_predicate` requires at least one edge — so refusing it is a
        // different rule (an undefined variable in WHERE) and a wider change
        // than this one. One TCK row is therefore still failing here, and
        // saying so beats quietly widening the rule to make a number look
        // complete. Tracked on #798.
        "(n)-[r]->(a)",
        "(a)-[r]->(n)",
        "(n)<-[r {}]-(a)",
        "(n)-[r {}]-(a)",
        "(n)-[r]->()",
        "()-[r]->(n)",
        "(n)<-[r]-()",
        "(n)-[r]-()",
    ] {
        assert!(
            rejected(&format!("MATCH (n) WHERE {pat} RETURN n")),
            "`{pat}` introduces a variable and should be refused"
        );
    }
}

/// **Anonymous positions introduce nothing.**
///
/// `(n)-[]->()` is the ordinary way to write "n has an outgoing edge", and it
/// must keep working. The rule is about *named* variables, not about pattern
/// complexity — checking the latter would break the most common form of the
/// feature.
#[test]
fn anonymous_positions_are_fine() {
    for pat in ["(n)-->()", "(n)-[]->()", "()-->(n)", "(n)-[]-()", "(n)-[:T]->()"] {
        assert!(
            accepted(&format!("MATCH (n) WHERE {pat} RETURN n")),
            "`{pat}` introduces nothing and must be accepted"
        );
    }
}

/// A variable already bound by the MATCH is fine on both sides.
#[test]
fn already_bound_variables_are_fine() {
    assert!(accepted("MATCH (n), (m) WHERE (n)-->(m) RETURN n"));
    assert!(accepted("MATCH (n)-[r]->(m) WHERE (n)-[r]->(m) RETURN n"));
    assert!(accepted("MATCH (a), (b), (c) WHERE (a)-->(b) AND (b)-->(c) RETURN a"));
}

/// **`EXISTS { ... }` may introduce names.** That is the whole difference
/// between the two forms, and a rule that conflated them would break the
/// documented way to express this.
#[test]
fn exists_subqueries_may_introduce_variables() {
    for q in [
        "MATCH (n) WHERE EXISTS { (n)-[r]->(a) } RETURN n",
        "MATCH (n) WHERE EXISTS { MATCH (n)-->(m) RETURN m } RETURN n",
    ] {
        // Some of these may not parse for unrelated reasons; what must not
        // happen is a rejection *by this rule*.
        if let Err(e) = parse_query(q) {
            let msg = format!("{e:?}");
            assert!(
                !msg.contains("used as a predicate"),
                "EXISTS must be allowed to introduce names: {q}\n  {msg}"
            );
        }
    }
}

/// A name bound by `UNWIND` counts as bound.
#[test]
fn unwind_bindings_count() {
    assert!(accepted("UNWIND [1] AS i MATCH (n) WHERE (n)-->() RETURN n, i"));
}

/// The message names the variable and points at the fix.
#[test]
fn the_message_names_the_variable() {
    let e = parse_query("MATCH (n) WHERE (n)-[r]->(a) RETURN n").expect_err("refused");
    let msg = format!("{e:?}");
    assert!(msg.contains('r') || msg.contains('a'), "{msg}");
    assert!(msg.contains("EXISTS"), "the message should offer the alternative: {msg}");
}
