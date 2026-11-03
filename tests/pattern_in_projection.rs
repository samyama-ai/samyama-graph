//! A bare pattern cannot be projected (#880).
//!
//! ```cypher
//! MATCH (n) RETURN (n)-[]->()
//! MATCH (n) WITH (n)-[]->() AS x RETURN x
//! ```
//!
//! A pattern there is a **predicate written where a value belongs**. The engine
//! evaluated it as one and projected the boolean — an answer to a question
//! nobody asked, in a column the user thought would hold something else.
//!
//! The scoping is the interesting part, and most of this file guards it.
//! `EXISTS { … }` desugars to the same AST node and is legal anywhere; a
//! pattern comprehension is a different node; and a bare pattern inside a list
//! comprehension's own `WHERE` is a predicate in a predicate position. A rule
//! that walked the whole expression tree would reject all three — and #798 is
//! the precedent for what that costs.

use samyama::query::parser::parse_query;

/// The two forms the TCK names.
#[test]
fn a_bare_pattern_cannot_be_projected() {
    assert!(parse_query("MATCH (n) RETURN (n)-[]->()").is_err());
    assert!(parse_query("MATCH (n) WITH (n)-[]->() AS x RETURN x").is_err());
    assert!(parse_query("MATCH (n) RETURN (n)-[:R]->(m) AS x").is_err());
    // Through a later WITH stage too.
    assert!(parse_query("MATCH (n) WITH n WITH (n)-[]->() AS x RETURN x").is_err());
}

/// **Everything else that looks similar must still parse.**
#[test]
fn the_legitimate_uses_are_untouched() {
    for cypher in [
        // A pattern predicate in a WHERE is the position it belongs in.
        "MATCH (n) WHERE (n)-[]->() RETURN n",
        "MATCH (n) WHERE NOT (n)-[]->() RETURN n",
        // EXISTS is a value expression and may be projected.
        "MATCH (n) RETURN EXISTS { (n)-[]->() } AS e",
        "MATCH (n) WITH EXISTS { (n)-[]->() } AS e RETURN e",
        // A pattern comprehension is a list, and may be projected.
        "MATCH (n) RETURN [(n)-->(m) | m] AS c",
        // Ordinary projections.
        "MATCH (n) RETURN n",
        "MATCH (n) RETURN n.name AS name",
        "MATCH (n) WITH n AS m RETURN m",
    ] {
        assert!(parse_query(cypher).is_ok(), "refused `{cypher}`");
    }
}

/// The message says what to use instead, since the fix for the query is not
/// obvious from "unexpected syntax".
#[test]
fn the_error_suggests_the_alternative() {
    let e = format!("{:?}", parse_query("MATCH (n) RETURN (n)-[]->()").unwrap_err());
    assert!(e.contains("EXISTS"), "error does not point at EXISTS: {e}");
}
