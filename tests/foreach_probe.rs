//! FOREACH body kinds, one test per gap #465 listed as unimplemented.
//!
//! The issue named four: MERGE, DELETE, nested FOREACH, and relationship patterns in
//! the body. All four work now — they were built across the parser, the AST's ordered
//! body list and the planner — and nothing was asserting it, so the issue could not tell
//! that it had been fixed. These are the assertions that close it, and that would notice
//! if one of the five body kinds were dropped again the way SET and CREATE once dropped
//! DELETE and REMOVE.
use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn count(store: &GraphStore, engine: &QueryEngine, q: &str) -> i64 {
    let r = engine.execute(q, store).unwrap();
    let v = r.records[0].values().next().unwrap().as_property().unwrap().clone();
    match v {
        samyama::graph::PropertyValue::Integer(i) => i,
        other => panic!("{other:?}"),
    }
}

#[test]
fn foreach_body_kinds_all_execute() {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    e.execute_mut("CREATE (:Person {id: 1})", &mut s, "default").unwrap();

    // relationship pattern in a CREATE body
    e.execute_mut("MATCH (p:Person {id: 1}) FOREACH (t IN ['a','b','c'] | \
                   CREATE (p)-[:TAGGED]->(:Tag {name: t}))", &mut s, "default").unwrap();
    assert_eq!(count(&s, &e, "MATCH (:Person)-[:TAGGED]->(t:Tag) RETURN count(t)"), 3,
               "relationship pattern in a FOREACH body");

    // MERGE body: the three tags exist, so this must add none
    e.execute_mut("FOREACH (t IN ['a','b','c'] | MERGE (:Tag {name: t}))", &mut s, "default").unwrap();
    assert_eq!(count(&s, &e, "MATCH (t:Tag) RETURN count(t)"), 3, "MERGE in a FOREACH body");

    // nested FOREACH
    e.execute_mut("FOREACH (x IN [1,2] | FOREACH (y IN [10,20] | CREATE (:N {v: x * y})))",
                  &mut s, "default").unwrap();
    assert_eq!(count(&s, &e, "MATCH (n:N) RETURN count(n)"), 4, "nested FOREACH");

    // SET + REMOVE bodies
    e.execute_mut("MATCH (t:Tag) WITH collect(t) AS ts FOREACH (t IN ts | SET t.seen = true)",
                  &mut s, "default").unwrap();
    assert_eq!(count(&s, &e, "MATCH (t:Tag) WHERE t.seen = true RETURN count(t)"), 3, "SET body");

    // DELETE body
    e.execute_mut("MATCH (n:N) WITH collect(n) AS ns FOREACH (n IN ns | DELETE n)",
                  &mut s, "default").unwrap();
    assert_eq!(count(&s, &e, "MATCH (n:N) RETURN count(n)"), 0, "DELETE body");
}

#[test]
fn a_return_inside_foreach_is_a_parse_error() {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    assert!(e.execute_mut("FOREACH (x IN [1] | RETURN x)", &mut s, "default").is_err(),
            "RETURN in a FOREACH body must be refused, not ignored");
}
