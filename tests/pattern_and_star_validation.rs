//! Two compile-time rules the engine was missing (#958).
//!
//! ```cypher
//! MATCH (a)-[r]->()-[r]->(a) RETURN r   -- RelationshipUniquenessViolation
//! MATCH () RETURN *                     -- NoVariablesInScope
//! ```
//!
//! Both used to **succeed with zero rows**. The first can never match whatever
//! the graph holds — Cypher traverses relationships edge-distinctly, so one
//! pattern cannot use the same relationship twice — and an empty result is
//! indistinguishable from a graph that simply lacks the shape. The second asks
//! for all of nothing.
//!
//! The `RETURN *` rule needed a flag set during star expansion, because
//! validation runs *after* it: by then the `*` has become zero columns, which
//! looks exactly like a projection that was empty on purpose.

use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

fn accepted(cypher: &str) -> bool {
    parse_query(cypher).is_ok()
}

#[test]
fn a_relationship_variable_twice_in_one_pattern_is_refused() {
    assert!(refused("MATCH (a)-[r]->()-[r]->(a) RETURN r"));
    assert!(refused("MATCH (a)-[r]->(b)-[r]->(c) RETURN a"));
}

#[test]
fn the_message_names_the_relationship() {
    let err = format!("{:?}", parse_query("MATCH (a)-[r]->()-[r]->(a) RETURN r").unwrap_err());
    assert!(err.contains('r'), "{err}");
}

#[test]
fn distinct_relationship_variables_are_fine() {
    assert!(accepted("MATCH (a)-[r1]->()-[r2]->(a) RETURN r1, r2"));
    assert!(accepted("MATCH (a)-[r]->(b) RETURN r"));
}

#[test]
fn the_same_name_in_two_patterns_is_a_different_rule() {
    // `MATCH (a)-[r]->(b) MATCH (c)-[r]->(d)` re-uses a *bound* relationship,
    // which is legal and means the same edge. The rule is per path, not per
    // query, and conflating them would reject valid Cypher.
    assert!(accepted("MATCH (a)-[r]->(b) MATCH (c)-[r]->(d) RETURN r"));
    assert!(accepted("MATCH (a)-[r]->(b), (c)-[r2]->(d) RETURN r, r2"));
}

#[test]
fn return_star_with_nothing_in_scope_is_refused() {
    assert!(refused("MATCH () RETURN *"));
}

#[test]
fn return_star_with_something_in_scope_is_fine() {
    assert!(accepted("MATCH (n) RETURN *"));
    assert!(accepted("MATCH ()-[r]->() RETURN *"));
    assert!(accepted("MATCH p = ()-->() RETURN *"));
    assert!(accepted("UNWIND [1, 2] AS x RETURN *"));
    assert!(accepted("CREATE (n) RETURN *"));
}

#[test]
fn a_with_star_that_projects_nothing_is_legal() {
    // Only RETURN is the error. Flagging WITH too broke two TCK scenarios that
    // must pass, which is how this rule found its own edge.
    assert!(accepted("MATCH () CREATE () WITH * CREATE ()"));
    assert!(accepted("MATCH () CREATE () WITH * MATCH () CREATE ()"));
}

#[test]
fn an_explicit_projection_beside_a_star_keeps_it_alive() {
    // `RETURN *, 1` projects something even when the star finds nothing, so it
    // is not the error the rule is about.
    assert!(accepted("MATCH (n) RETURN *, 1 AS one"));
}
