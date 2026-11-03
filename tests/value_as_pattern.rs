//! A variable bound to a value cannot then be a pattern (#795).
//!
//! ```text
//! WITH 123 AS n MATCH (n) RETURN n     -> SyntaxError: VariableTypeConflict
//! ```
//!
//! The check existed but was narrow in two independent ways, and each hid the
//! other:
//!
//! * it recognised only **lists and maps** — the shape #654 needed — so every
//!   scalar walked past;
//! * it visited only **node and edge** variables in a pattern, never the path
//!   variable, so `WITH 123 AS p MATCH p = ()-[]-()` was never examined.
//!
//! Widening one alone gets 8 of the 16 scenarios and looks like progress.
//!
//! Both checks also existed in **two copies**, for the two AST shapes. They now
//! share one predicate, so the next widening does not have to find both.

use samyama::query::parser::parse_query;

fn rejected(q: &str) -> bool {
    parse_query(q).is_err()
}
fn accepted(q: &str) -> bool {
    parse_query(q).is_ok()
}

/// Every scalar the TCK lists, as a node variable.
#[test]
fn a_scalar_cannot_be_a_node_pattern() {
    for lit in ["true", "123", "123.4", "'foo'"] {
        assert!(
            rejected(&format!("WITH {lit} AS n MATCH (n) RETURN n")),
            "{lit} as a node should be refused"
        );
    }
}

/// ...as a relationship variable.
#[test]
fn a_scalar_cannot_be_a_relationship_pattern() {
    for lit in ["true", "123", "123.4", "'foo'"] {
        assert!(
            rejected(&format!("WITH {lit} AS r MATCH ()-[r]-() RETURN r")),
            "{lit} as a relationship should be refused"
        );
    }
}

/// ...and as a **path** variable, which the walk skipped entirely.
#[test]
fn a_scalar_cannot_be_a_path_pattern() {
    for lit in ["true", "123", "123.4", "'foo'"] {
        assert!(
            rejected(&format!("WITH {lit} AS p MATCH p = ()-[]-() RETURN p")),
            "{lit} as a path should be refused"
        );
    }
}

/// Lists and maps are still refused — the case the check was originally
/// written for (#654) must not be lost while widening it.
#[test]
fn the_original_collection_case_still_holds() {
    assert!(rejected("WITH [1] AS users MATCH (users)-->(m) RETURN m"));
    assert!(rejected("WITH {a: 1} AS m MATCH (m) RETURN m"));
}

/// **A name re-bound to something that could be an entity is fine.**
#[test]
fn a_rebound_name_is_free_again() {
    assert!(accepted("MATCH (x) WITH 1 AS n WITH x AS n MATCH (n)-->(m) RETURN m"));
}

/// Ordinary queries are undisturbed. This is where a widened type check does
/// damage, so it carries the most cases.
#[test]
fn ordinary_patterns_are_undisturbed() {
    for q in [
        "MATCH (n) RETURN n",
        "MATCH (a)-[r]->(b) RETURN a, r, b",
        "MATCH p = (a)-[*1..3]->(b) RETURN p",
        "MATCH (n) WITH n MATCH (n)-->(m) RETURN m",
        "MATCH (n) WITH n AS x MATCH (x)-->(m) RETURN m",
        "MATCH p = (a)-->(b) WITH p MATCH (c) RETURN p, c",
        // A property is not knowable from the text and must pass through.
        "MATCH (n) WITH n.friend AS f MATCH (f)-->(m) RETURN m",
        "UNWIND [1, 2] AS i MATCH (n) RETURN n, i",
    ] {
        assert!(accepted(q), "must still be accepted: {q}");
    }
}
