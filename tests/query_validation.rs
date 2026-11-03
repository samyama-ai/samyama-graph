//! Queries that must be rejected (openCypher TCK negative scenarios).
//!
//! 55 TCK scenarios assert only that a query is refused. An engine that runs
//! them anyway answers a question that was never well-formed, and two of those
//! answers are actively harmful:
//!
//! * `RETURN a AS x, b AS x` — one column has to win, and the caller silently
//!   loses the other;
//! * `MATCH (a) CREATE (a:Foo)` — reads as "add a label", which is what `SET`
//!   is for. Cypher makes it an error precisely because the intent is
//!   ambiguous.
//!
//! The tests come in pairs on purpose. A validation rule is only worth having
//! if it rejects the invalid form *and* leaves the valid one alone, and the
//! expensive failure here is the second half: rejecting a legal query would
//! break every loader and benchmark in this repo. That is also why scope
//! analysis ("is this variable defined?") is deliberately not implemented —
//! several TCK scenarios want it, and getting it slightly wrong costs more
//! than the scenarios are worth.

use samyama::query::parser::parse_query;

#[track_caller]
fn rejected(cypher: &str) {
    assert!(
        parse_query(cypher).is_err(),
        "should have been rejected, but parsed: {cypher}"
    );
}

#[track_caller]
fn accepted(cypher: &str) {
    assert!(
        parse_query(cypher).is_ok(),
        "should have been accepted, but was rejected: {cypher}"
    );
}

#[test]
fn two_result_columns_may_not_share_a_name() {
    rejected("MATCH (a), (b) RETURN a AS x, b AS x");
    rejected("MATCH (a) RETURN a, a");
    accepted("MATCH (a), (b) RETURN a AS x, b AS y");
}

#[test]
fn two_with_items_may_not_share_a_name() {
    rejected("MATCH (a) WITH a AS x, a AS x RETURN x");
    accepted("MATCH (a) WITH a AS x, a AS y RETURN x, y");
}

#[test]
fn unaliased_expressions_do_not_collide_with_each_other() {
    // Only aliases and bare variables produce a column name that can clash;
    // two `count(*)` items are given generated names. Rejecting these would
    // be over-reach.
    accepted("MATCH (a) RETURN count(*), count(*)");
}

#[test]
fn union_branches_must_have_the_same_columns() {
    rejected("MATCH (a) RETURN a AS x UNION MATCH (b) RETURN b AS y");
    accepted("MATCH (a) RETURN a AS x UNION MATCH (b) RETURN b AS x");
}

#[test]
fn union_and_union_all_may_not_be_mixed() {
    rejected(
        "MATCH (a) RETURN a AS x UNION MATCH (b) RETURN b AS x \
         UNION ALL MATCH (c) RETURN c AS x",
    );
    accepted("MATCH (a) RETURN a AS x UNION ALL MATCH (b) RETURN b AS x");
}

#[test]
fn create_may_not_add_labels_to_a_variable_match_already_bound() {
    rejected("MATCH (a) CREATE (a:Foo)");
    rejected("MATCH (a) CREATE (a {x: 1})");
}

#[test]
fn a_bare_re_mention_that_attaches_a_relationship_stays_legal() {
    // This is how you write an edge between two matched nodes, and it is the
    // single most common write query in this codebase. If the rule above ever
    // starts rejecting it, every loader breaks.
    accepted("MATCH (a), (b) CREATE (a)-[:R]->(b)");
    accepted("MATCH (a:Person) CREATE (a)-[:OWNS]->(:Thing)");
}

#[test]
fn a_standalone_re_mention_of_a_matched_variable_is_refused() {
    // `MATCH (a) CREATE (a)` used to be listed alongside the cases above as
    // legal. It is not: Cypher raises `VariableAlreadyBound`, Neo4j rejects
    // it, and the TCK asserts the error (Create1 [13]). The two were
    // conflated because both are "bare", but a bare mention inside a
    // relationship pattern is doing work — attaching an edge — and a
    // standalone one re-creates a node that already exists (#663).
    rejected("MATCH (a) CREATE (a)");
}

#[test]
fn reusing_a_variable_inside_one_create_stays_legal() {
    // Bound by the CREATE itself rather than by a MATCH — legal, and the
    // subject of its own test file.
    accepted("CREATE (a), (b), (a)-[:R]->(b)");
    accepted("CREATE (a:A), (b:B), (a)-[:R]->(b)");
}

#[test]
fn ordinary_queries_are_unaffected() {
    // A spot check across the shapes this repo actually runs, because the
    // cost of a false rejection is much higher than the benefit of a true one.
    for q in [
        "MATCH (n) RETURN n",
        "MATCH (a)-[r]->(b) RETURN *",
        "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name ORDER BY name LIMIT 10",
        "UNWIND [1, 2] AS a UNWIND [3, 4] AS b RETURN a, b",
        "MATCH (a) WITH a.x AS v, count(*) AS c WHERE c > 1 RETURN v, c",
        "MERGE (a:L) ON CREATE SET a.n = 1 ON MATCH SET a.n = 2",
        "MATCH (a) RETURN a.name AS name UNION ALL MATCH (b) RETURN b.name AS name",
    ] {
        accepted(q);
    }
}

// ---------------------------------------------------------- label predicates

// `n:Label` used as a *value* rather than as a pattern — `WHERE n:Person`,
// `RETURN n:Person AS isPerson`. It parses as a postfix on a term, which puts
// it at the same binding strength as `IS NULL`.
//
// The first implementation read the labels from the wrong nesting level, found
// none, and fell through to the `IS NULL` branch — so `n:A` silently became
// `n IS NULL`. It parsed, it ran, and it returned a plausible boolean. That is
// why these tests assert the *value*, and assert both polarities.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};

fn labelled_graph() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (:A:B {n: 'ab'}), (:A {n: 'a'}), (:C {n: 'c'})").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("fixture should run");
    store
}

fn one_bool(store: &GraphStore, cypher: &str) -> Option<bool> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    match batch.records[0].get("r") {
        Some(Value::Property(PropertyValue::Boolean(b))) => Some(*b),
        Some(Value::Property(PropertyValue::Null)) => None,
        other => panic!("expected a boolean, got {other:?}"),
    }
}

fn names(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    let mut out: Vec<String> = batch
        .records
        .iter()
        .map(|r| match r.get("x") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => panic!("expected a name, got {other:?}"),
        })
        .collect();
    out.sort();
    out
}

#[test]
fn a_label_test_returns_true_only_when_the_label_is_present() {
    let store = labelled_graph();
    assert_eq!(one_bool(&store, "MATCH (n) WHERE n.n = 'a' RETURN n:A AS r"), Some(true));
    assert_eq!(one_bool(&store, "MATCH (n) WHERE n.n = 'a' RETURN n:C AS r"), Some(false));
}

#[test]
fn a_multi_label_test_requires_every_label() {
    let store = labelled_graph();
    assert_eq!(one_bool(&store, "MATCH (n) WHERE n.n = 'ab' RETURN n:A:B AS r"), Some(true));
    // `a` has A but not B, so the conjunction is false.
    assert_eq!(one_bool(&store, "MATCH (n) WHERE n.n = 'a' RETURN n:A:B AS r"), Some(false));
}

#[test]
fn a_label_test_filters_in_where() {
    let store = labelled_graph();
    assert_eq!(names(&store, "MATCH (n) WHERE n:A RETURN n.n AS x"), vec!["a", "ab"]);
    assert_eq!(names(&store, "MATCH (n) WHERE n:A:B RETURN n.n AS x"), vec!["ab"]);
    assert_eq!(names(&store, "MATCH (n) WHERE NOT n:C RETURN n.n AS x"), vec!["a", "ab"]);
}

#[test]
fn parenthesising_a_label_test_changes_nothing() {
    let store = labelled_graph();
    assert_eq!(one_bool(&store, "MATCH (n) WHERE n.n = 'a' RETURN (n:A) AS r"), Some(true));
}

#[test]
fn is_null_still_parses_as_is_null() {
    // The regression the nesting bug caused: a label test that failed to read
    // its labels became `IS NULL`. This pins the other direction — `IS NULL`
    // must not become a label test.
    let store = labelled_graph();
    assert_eq!(one_bool(&store, "MATCH (n) WHERE n.n = 'a' RETURN n.missing IS NULL AS r"), Some(true));
    assert_eq!(one_bool(&store, "MATCH (n) WHERE n.n = 'a' RETURN n.n IS NULL AS r"), Some(false));
    assert_eq!(one_bool(&store, "MATCH (n) WHERE n.n = 'a' RETURN n.n IS NOT NULL AS r"), Some(true));
}

#[test]
fn a_map_literal_is_not_read_as_a_label_test() {
    // `:` is also the map-literal separator, so the postfix must not steal it.
    let store = labelled_graph();
    let q = parse_query("RETURN {a: 1, b: 2} AS m").expect("map literal should still parse");
    let batch = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert!(matches!(
        batch.records[0].get("m"),
        Some(Value::Property(PropertyValue::Map(_)))
    ));
}

// ------------------------------------------------- CREATE relationship rules

// A relationship being *created* has to say exactly what it is. These three
// forms are ambiguous rather than merely unsupported, which is why Cypher
// rejects them and why accepting them means inventing an answer:
//
//   CREATE (a)-->(b)        — what kind of edge?
//   CREATE (a)-[:R]-(b)     — pointing which way?
//   CREATE (a)-[:R*2]->(b)  — and what is the node in the middle?
//
// The same three patterns are perfectly good in MATCH, where they mean "any
// type", "either direction" and "two hops". That asymmetry is the whole
// reason the rule lives in validation and not in the grammar, and every test
// below has a MATCH counterpart asserting the pattern still parses there.

#[test]
fn creating_a_relationship_requires_a_type() {
    rejected("CREATE (a)-->(b)");
    accepted("CREATE (a)-[:R]->(b)");
    accepted("MATCH (a)-->(b) RETURN a");
}

#[test]
fn creating_a_relationship_requires_a_direction() {
    rejected("CREATE (a)-[:R]-(b)");
    accepted("CREATE (a)-[:R]->(b)");
    accepted("CREATE (a)<-[:R]-(b)");
    accepted("MATCH (a)-[:R]-(b) RETURN a");
}

#[test]
fn a_variable_length_relationship_cannot_be_created() {
    rejected("CREATE (a)-[:R*2]->(b)");
    rejected("CREATE (a)-[:R*]->(b)");
    accepted("MATCH (a)-[:R*2]->(b) RETURN a");
}

#[test]
fn create_may_not_rebind_a_matched_relationship() {
    rejected("MATCH (a)-[r]->(b) CREATE (a)-[r:R]->(b)");
    // A fresh name is fine.
    accepted("MATCH (a)-[r]->(b) CREATE (a)-[r2:R]->(b)");
}

#[test]
fn the_ordinary_create_forms_are_untouched() {
    for q in [
        "CREATE (a)-[:R]->(b)",
        "CREATE (a:A)-[r:R {w: 1}]->(b:B)",
        "CREATE (a), (b), (a)-[:R]->(b)",
        "CREATE (h:Hub) CREATE (h)-[:E]->(:Leaf)",
        "MATCH (a:P), (b:P) CREATE (a)-[:KNOWS]->(b)",
    ] {
        accepted(q);
    }
}
