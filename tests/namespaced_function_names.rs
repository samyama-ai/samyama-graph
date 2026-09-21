//! A function name may carry more than one namespace segment.
//!
//! `function_name` allowed **one** — `date.truncate`, `duration.between` — so
//! `apoc.text.join(...)`, two segments and the commonest shape in a Neo4j
//! corpus, was a parse error. The message read:
//!
//! ```text
//! expected EOI, union_clause, order_by_clause, skip_clause, limit_clause…
//! ```
//!
//! A user migrating from Neo4j learns nothing from that. With the name
//! accepted, the same query reaches the evaluator and comes back as *Unknown
//! function: apoc.text.join*, which names the thing they have to replace.
//!
//! **Accepting a name is not implementing it**, and the refusal is the point:
//! it is a better refusal. Found by pointing `examples/compatibility_report`
//! at a corpus of Neo4j idioms (INT-11), where this was one of six causes and
//! the only one whose message identified nothing.
//!
//! # The risk this change carries
//!
//! `function_name` is greedy over dots, and `n.doc.a.b` is property access.
//! They do not collide because `function_call` requires a `(` immediately
//! after the name, so a dotted path with no call backtracks to
//! `property_access` — but that is an argument, and the tests below are the
//! evidence. A regression here would break every nested map read in the
//! engine, which is why they come first.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn store() -> GraphStore {
    let mut store = GraphStore::new();
    QueryEngine::new()
        .execute_mut(
            "CREATE (d:D {doc: {a: {b: {c: 7}}}, n: 1, name: 'x'})",
            &mut store,
            "default",
        )
        .expect("create");
    store
}

fn cell(store: &GraphStore, query: &str) -> String {
    let b = QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    format!("{:?}", b.records[0].bindings()[0].1)
}

#[test]
fn a_nested_property_path_is_still_property_access() {
    // First, because a regression here breaks every nested map read. Four
    // segments, which is more than the old `function_name` could have eaten
    // and exactly what the new one could.
    let s = store();
    assert!(cell(&s, "MATCH (d:D) RETURN d.doc.a.b.c AS v").contains('7'));
    assert!(cell(&s, "MATCH (d:D) RETURN d.n AS v").contains('1'));
    assert!(cell(&s, "MATCH (d:D) RETURN d.doc.a AS v").contains('b'));
}

#[test]
fn a_nested_path_works_in_a_predicate_too() {
    // `WHERE` parses expressions by a different route than `RETURN`, so it
    // gets its own case rather than being assumed to follow.
    let s = store();
    assert!(cell(&s, "MATCH (d:D) WHERE d.doc.a.b.c = 7 RETURN d.n AS v").contains('1'));
}

#[test]
fn a_two_segment_function_name_parses() {
    assert!(
        samyama::query::parse_query("RETURN apoc.text.join(['a'], ',') AS j").is_ok(),
        "a two-segment function name must reach the evaluator"
    );
    assert!(
        samyama::query::parse_query("RETURN a.b.c.d(1) AS x").is_ok(),
        "depth is not the thing being limited"
    );
}

#[test]
fn an_unimplemented_namespaced_function_names_itself_in_the_error() {
    // The whole point. A parse error that lists the grammar's expectations
    // tells a migrating user nothing; this tells them which function to
    // replace.
    let s = store();
    let err = QueryEngine::new()
        .execute("RETURN apoc.text.join(['a'], ',') AS j", &s)
        .expect_err("apoc is not implemented");
    let text = err.to_string();
    assert!(
        text.contains("apoc.text.join"),
        "the error must name the function: {text}"
    );
    assert!(
        text.to_lowercase().contains("unknown function"),
        "and say that it is the function that is unknown: {text}"
    );
}

#[test]
fn the_one_segment_names_that_already_worked_still_do() {
    // `duration.between` was implemented and unreachable from Cypher until
    // one namespace segment was allowed (#769). Widening the rule must not
    // take it away again.
    let s = store();
    let v = cell(
        &s,
        "RETURN duration.between(datetime('2024-01-01T00:00:00Z'), \
         datetime('2024-01-02T00:00:00Z')) AS d",
    );
    assert!(!v.is_empty(), "duration.between returned nothing");
}

#[test]
fn a_plain_function_is_unaffected() {
    let s = store();
    assert!(cell(&s, "RETURN toUpper('ab') AS x").contains("AB"));
    assert!(cell(&s, "MATCH (d:D) RETURN count(d) AS c").contains('1'));
}
