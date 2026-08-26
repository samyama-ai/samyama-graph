//! A function applied to the wrong kind of entity (#901).
//!
//! ```cypher
//! MATCH (r) RETURN type(r)     -- returned a null column
//! MATCH (n) RETURN length(n)   -- returned a null column
//! ```
//!
//! `type()` asks a relationship for its type, and a node does not have one.
//! `length()` asks a path how long it is. The TCK wants both refused **at
//! compile time**, which is possible only because the pattern says what kind
//! each variable is — the same `EntityKind` map that already backs the
//! one-variable-one-kind rule.
//!
//! Only a variable whose kind a pattern fixes is checked. An expression, a
//! parameter, or a name a `WITH` recomputed has no kind here and is left alone:
//! this module treats rejecting a valid query as the worse failure.

use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

#[test]
fn a_function_that_wants_a_relationship_refuses_a_node() {
    assert!(refused("MATCH (r) RETURN type(r)"));
    assert!(refused("MATCH (n) RETURN startNode(n)"));
    assert!(refused("MATCH (n) RETURN endNode(n)"));
}

#[test]
fn a_function_that_wants_a_path_refuses_a_node_or_a_relationship() {
    assert!(refused("MATCH (n) RETURN length(n)"));
    assert!(refused("MATCH ()-[r]-() RETURN length(r)"));
    assert!(refused("MATCH (n) RETURN nodes(n)"));
    assert!(refused("MATCH ()-[r]-() RETURN relationships(r)"));
}

#[test]
fn a_function_that_wants_a_node_refuses_a_relationship() {
    assert!(refused("MATCH ()-[r]-() RETURN labels(r)"));
}

/// The right kind, and every kind the check cannot know.
#[test]
fn the_correct_forms_and_the_unknowable_ones_are_untouched() {
    for cypher in [
        "MATCH ()-[r]-() RETURN type(r)",
        "MATCH (n) RETURN labels(n)",
        "MATCH p = (a)-->(b) RETURN length(p)",
        "MATCH p = (a)-->(b) RETURN nodes(p), relationships(p)",
        "MATCH ()-[r]-() RETURN startNode(r), endNode(r)",
        // Nested one level down — still checked, still correct.
        "MATCH p = (a)-->(b) RETURN size(nodes(p))",
        // No kind is known for these, so nothing is claimed about them.
        "RETURN length($p)",
        "WITH 'abc' AS s RETURN length(s)",
        "MATCH (n) WITH n.path AS p RETURN length(p)",
        "MATCH (n) RETURN length(head([1, 2]))",
    ] {
        assert!(!refused(cypher), "wrongly refused `{cypher}`");
    }
}
