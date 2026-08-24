//! One variable may not name a node here and a relationship there (#765).
//!
//! openCypher binds a variable to an *entity*, and an entity has a kind: a
//! node, a relationship, or a path. `MATCH (r), ()-[r]-()` asks for `r` to be
//! two of those at once, which has no answer, and the TCK asserts a failure
//! for every arrangement of it — same pattern, same clause, preceding clause,
//! all three kinds against each other.
//!
//! **227 scenarios**, all of this one rule, and we returned rows for every one
//! of them. Rows, not an error: the second binding simply overwrote the first,
//! so the query "succeeded" with an answer to a question that was never
//! well-formed.
//!
//! The rule is scoped the same way #684 scoped relationship isomorphism, and
//! for the same reason: getting the scope wrong in the *other* direction
//! rejects valid queries, which `validate.rs` opens by calling the worse
//! failure. So the negative cases below matter more than the positive ones.

use samyama::query::parser::parse_query;

fn rejected(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

fn accepted(cypher: &str) -> bool {
    parse_query(cypher).is_ok()
}

/// A node variable reused as a relationship in the same pattern.
#[test]
fn a_node_variable_may_not_be_a_relationship_in_the_same_pattern() {
    assert!(rejected("MATCH (r), ()-[r]-() RETURN r"));
    assert!(rejected("MATCH ()-[r]-(), (r) RETURN r"));
}

/// ...and across clauses in the same scope. The TCK spells this "in a
/// preceding MATCH" and it is the larger half of the 227.
#[test]
fn the_conflict_is_caught_across_match_clauses() {
    assert!(rejected("MATCH (r) MATCH ()-[r]-() RETURN r"));
    assert!(rejected("MATCH ()-[r]-() MATCH (r) RETURN r"));
}

/// A path variable is a third kind, not a synonym for either.
#[test]
fn a_path_variable_conflicts_with_a_node_and_with_a_relationship() {
    assert!(rejected("MATCH p = ()-[]-(), (p) RETURN p"));
    assert!(rejected("MATCH p = ()-[]-(), ()-[p]-() RETURN p"));
}

/// The same kind twice is not a conflict — that is ordinary Cypher and is how
/// a pattern refers back to something it already bound.
#[test]
fn reusing_a_variable_as_the_same_kind_is_fine() {
    assert!(accepted("MATCH (a)-[:R]->(b) MATCH (a)-[:S]->(c) RETURN a, b, c"));
    assert!(accepted("MATCH (a)-[r]->(b) RETURN a, r, b"));
    assert!(accepted("MATCH (a), (b) RETURN a, b"));
}

/// A `WITH` opens a new scope, and a name it does not carry forward is free
/// again.
///
/// This is the guard against over-rejecting. `r` is a relationship before the
/// WITH and is not projected through it, so the CREATE binds a *fresh* node
/// that happens to share the name.
///
/// It is also the bug this file was written for. The first version of the
/// check walked `create_clause` and `merge_clause` **before** applying the
/// WITH reset — reading a write clause against a scope it never sees — and so
/// rejected this valid query. Found by reading the code, not by a failing
/// test, which is exactly why it needs one.
#[test]
fn a_pattern_after_with_may_reuse_a_dropped_name() {
    assert!(accepted("MATCH (a)-[r]->(b) WITH a MATCH (r) RETURN a, r"));
}

/// The `CREATE`/`MERGE` forms of the same thing are rejected, but **not by
/// this rule** -- `CreateOnBoundVariable` and `MergeOnBoundVariable` predate
/// it and are not WITH-aware:
///
/// ```text
/// MATCH (a)-[r]->(b) WITH a CREATE (r:X)
///   -> Variable `r` already declared; CREATE cannot add labels or properties to it
/// ```
///
/// `r` is *not* carried through that WITH, so it is out of scope and the
/// CREATE should bind a fresh node. Filed separately (#764) rather than fixed
/// here, because it is a different rule with its own scope question. Asserting
/// the current behaviour keeps this file honest about what it does and does
/// not cover -- and turns red the moment #764 is fixed, which is the point.
#[test]
fn create_and_merge_after_with_are_over_rejected_by_an_older_rule() {
    assert!(rejected("MATCH (a)-[r]->(b) WITH a CREATE (r:X) RETURN a"));
    assert!(rejected("MATCH (a)-[r]->(b) WITH a MERGE (r:X) RETURN a"));
    // Not the kind rule: a fresh name is accepted in the same position.
    assert!(accepted("MATCH (a)-[r]->(b) WITH a CREATE (z:X) RETURN a"));
}

/// A name carried *through* a WITH keeps its kind, so the conflict still
/// applies on the far side. Without this the rule would be trivially evaded by
/// inserting a WITH.
#[test]
fn a_name_carried_through_with_keeps_its_kind() {
    assert!(rejected("MATCH (a)-[r]->(b) WITH r MATCH (r) RETURN r"));
    assert!(rejected("MATCH (a)-[r]->(b) WITH r AS q MATCH (q) RETURN q"));
    // The other direction: a *node* carried forward may not become a
    // relationship. I first wrote this as an `accepted` case in the
    // dropped-name test above and it failed -- correctly. `a` is projected by
    // the WITH, so it is still a node on the far side, and the rule is right
    // to refuse. The test was wrong, not the code.
    assert!(rejected("MATCH (a)-[r]->(b) WITH a MATCH ()-[a]-() RETURN a"));
}

/// Anonymous positions bind nothing and cannot conflict.
#[test]
fn anonymous_positions_are_not_variables() {
    assert!(accepted("MATCH (), ()-[]-() RETURN 1"));
    assert!(accepted("MATCH ()-[]->()-[]->() RETURN 1"));
}
