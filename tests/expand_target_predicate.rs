//! A selective equality on the far side of an expansion is applied *during*
//! the walk, not to the rows it produces.
//!
//! LDBC IC11 is the case this exists for. Its plan was
//!
//! ```text
//! Filter (org.name = "..." AND wa.workFrom < 2012)
//! +- Expand ((friend)-[:WORK_AT]->(org))
//! ```
//!
//! so every friend-of-friend's employer became a record and was then thrown
//! away. `ExpandOperator` had a target *label* filter and no target *property*
//! filter, so there was nowhere for the predicate to go (#656).
//!
//! The pushdown is **additive**: the planner's own filter stays where it is.
//! That is the whole safety argument — this can reduce what is materialised
//! and cannot change what is returned. A wrong pushdown is a wrong answer; a
//! missing one is a slow query, and only one of those is worth risking.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    for cypher in [
        "CREATE (:Person {id: 1}), (:Person {id: 2}), (:Person {id: 3})",
        "CREATE (:Org {name: 'Wanted'}), (:Org {name: 'Other'}), (:Org {name: 'Third'})",
        "MATCH (p:Person {id: 1}), (o:Org {name: 'Wanted'}) CREATE (p)-[:WORKS_AT {y: 2001}]->(o)",
        "MATCH (p:Person {id: 1}), (o:Org {name: 'Other'}) CREATE (p)-[:WORKS_AT {y: 2002}]->(o)",
        "MATCH (p:Person {id: 2}), (o:Org {name: 'Third'}) CREATE (p)-[:WORKS_AT {y: 2003}]->(o)",
        "MATCH (p:Person {id: 3}), (o:Org {name: 'Wanted'}) CREATE (p)-[:WORKS_AT {y: 2004}]->(o)",
    ] {
        let q = parse_query(cypher).expect("setup should parse");
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .expect("setup should run");
    }
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .len()
}

#[test]
fn a_where_equality_on_the_expansion_target_selects_the_same_rows() {
    // The answer, not the plan. If the pushdown ever stops being additive this
    // is what notices.
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (p:Person)-[:WORKS_AT]->(o:Org) WHERE o.name = 'Wanted' RETURN p"),
        2,
        "persons 1 and 3 work at Wanted"
    );
    assert_eq!(
        rows(&store, "MATCH (p:Person)-[:WORKS_AT]->(o:Org) WHERE o.name = 'Other' RETURN p"),
        1
    );
    assert_eq!(
        rows(&store, "MATCH (p:Person)-[:WORKS_AT]->(o:Org) WHERE o.name = 'Absent' RETURN p"),
        0
    );
}

#[test]
fn the_inline_form_and_the_where_form_agree() {
    // `{name: 'Wanted'}` and `WHERE o.name = 'Wanted'` are the same question.
    // They are planned differently, which is exactly why they are worth
    // comparing.
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (p:Person)-[:WORKS_AT]->(o:Org {name: 'Wanted'}) RETURN p"),
        rows(&store, "MATCH (p:Person)-[:WORKS_AT]->(o:Org) WHERE o.name = 'Wanted' RETURN p")
    );
}

#[test]
fn a_predicate_on_the_edge_is_still_applied() {
    // The pushdown reads predicates on the *target*; an edge predicate in the
    // same WHERE must not be lost while doing so.
    let store = graph();
    assert_eq!(
        rows(
            &store,
            "MATCH (p:Person)-[w:WORKS_AT]->(o:Org) WHERE o.name = 'Wanted' AND w.y < 2003 RETURN p"
        ),
        1,
        "only person 1 was at Wanted before 2003"
    );
}

#[test]
fn a_non_equality_predicate_on_the_target_is_unaffected() {
    // Only `= <literal>` is pushed. Anything else has to keep working through
    // the ordinary filter.
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (p:Person)-[:WORKS_AT]->(o:Org) WHERE o.name <> 'Wanted' RETURN p"),
        2
    );
}
