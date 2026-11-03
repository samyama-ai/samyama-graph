//! `EXISTS { }` in all three spellings openCypher allows (#858).
//!
//! ```cypher
//! EXISTS { (n)-->() }                     -- a bare pattern, no MATCH keyword
//! EXISTS { (n)-->(m) WHERE n.p = m.p }    -- and an optional WHERE
//! EXISTS { MATCH (n)-->() RETURN true }   -- a full subquery
//! ```
//!
//! `MATCH` was mandatory and `RETURN` was not allowed, so eight of the nine
//! `ExistentialSubquery` scenarios failed at `parse:` — before the evaluator
//! saw them.
//!
//! The projection is parsed and **discarded**: `EXISTS { … }` is true iff the
//! subquery produces at least one row, so what those rows contain cannot change
//! the answer. `returns_the_same_answer_whatever_the_projection` pins that,
//! because it is the part a reader is most likely to doubt.
//!
//! Every assertion here runs against a **populated** store. Parsing is not the
//! claim; answering correctly is, and an empty graph makes every `EXISTS` false
//! whether or not the pattern means anything.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut store = GraphStore::new();
    for setup in [
        "CREATE (:A {prop: 1})-[:R]->(:B {prop: 1})",
        "CREATE (:C {prop: 2})",
    ] {
        let q = parse_query(setup).expect("setup parses");
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .expect("setup runs");
    }
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"))
        .records
        .len()
}

/// Three nodes exist; only the `:A` has an outgoing relationship.
#[test]
fn a_bare_pattern_needs_no_match_keyword() {
    let s = store();
    assert_eq!(rows(&s, "MATCH (n) WHERE exists { (n)-->() } RETURN n"), 1);
    assert_eq!(rows(&s, "MATCH (n) WHERE exists { MATCH (n)-->() } RETURN n"), 1);
    // A relationship type nothing has.
    assert_eq!(rows(&s, "MATCH (n) WHERE exists { (n)-[:NA]->() } RETURN n"), 0);
}

/// The optional `WHERE` filters the subquery, not the outer match.
#[test]
fn a_bare_pattern_may_carry_a_where() {
    let s = store();
    assert_eq!(
        rows(&s, "MATCH (n) WHERE exists { (n)-->(m) WHERE n.prop = m.prop } RETURN n"),
        1
    );
    assert_eq!(
        rows(&s, "MATCH (n) WHERE exists { (n)-->(m) WHERE n.prop <> m.prop } RETURN n"),
        0
    );
    assert_eq!(
        rows(&s, "MATCH (n) WHERE exists { (n)-[r]->() WHERE type(r) = 'NA' } RETURN n"),
        0
    );
}

/// **The projection cannot change the answer**, because `EXISTS` asks only
/// whether a row exists. Returning `false` from the subquery is still true.
#[test]
fn returns_the_same_answer_whatever_the_projection() {
    let s = store();
    for projection in ["true", "false", "n", "1 + 1", "*"] {
        assert_eq!(
            rows(&s, &format!("MATCH (n) WHERE exists {{ MATCH (n)-->() RETURN {projection} }} RETURN n")),
            1,
            "RETURN {projection}"
        );
    }
}

/// Nested subqueries fall out of the same change: the inner `exists` sits
/// inside a `where_clause` and goes through the ordinary expression grammar.
#[test]
fn subqueries_nest() {
    let s = store();
    assert_eq!(
        rows(&s, "MATCH (n) WHERE exists { MATCH (m) WHERE exists { (n)-[]->(m) } RETURN m } RETURN n"),
        1
    );
}

/// A **bare pattern predicate** in a `WHERE` still may not introduce variables,
/// which is the distinction #798 exists for. `EXISTS { <pattern> }` may.
#[test]
fn a_bare_pattern_predicate_is_still_restricted() {
    let s = store();
    // `m` is introduced inside the EXISTS, which is allowed.
    assert!(parse_query("MATCH (n) WHERE exists { (n)-->(m) } RETURN n").is_ok());
    let _ = &s;
    // The same name introduced by a bare predicate is not.
    assert!(
        parse_query("MATCH (n) WHERE (n)-->(m) RETURN n").is_err(),
        "a bare pattern predicate must not introduce `m`"
    );
}
