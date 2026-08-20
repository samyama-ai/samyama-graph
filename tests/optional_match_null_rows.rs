//! A leading `OPTIONAL MATCH` that matches nothing still produces one row, and
//! everything downstream has to cope with the null it binds.
//!
//! ```cypher
//! OPTIONAL MATCH (a:DoesNotExist) RETURN a     -- one row, a = null
//! ```
//!
//! We returned **no rows**. A non-leading OPTIONAL MATCH gets its null-filled
//! row from the left outer join; a leading one has no left side at all, so
//! there was nothing to null-fill (#671).
//!
//! Fixing that made a second defect reachable for the first time, which is the
//! more interesting half: a following non-optional `MATCH (a)-->(b)` on the
//! null row raised `"a is not a node"` and failed the whole query. Expanding
//! from null yields nothing — the row disappears quietly, which is exactly
//! what `OPTIONAL MATCH (a) WITH a MATCH (a)-->(b)` means when nothing
//! matched.
//!
//! Those two have to be fixed together. Either alone is worse than neither:
//! the first without the second turns a silently-empty result into a hard
//! error, and the second alone is unreachable.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (:A)-[:R]->(:B)").expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");
    store
}

fn run(store: &GraphStore, cypher: &str) -> Vec<Option<Value>> {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run, not error: {e}"))
        .records
        .iter()
        .map(|r| r.get("x").cloned())
        .collect()
}

#[test]
fn a_leading_optional_match_that_finds_nothing_returns_one_null_row() {
    let store = graph();
    let rows = run(&store, "OPTIONAL MATCH (a:DoesNotExist) RETURN a AS x");
    assert_eq!(rows.len(), 1, "the row exists even though nothing matched");
    assert!(matches!(rows[0], Some(Value::Null) | None), "and `a` is null: {rows:?}");
}

#[test]
fn a_leading_optional_match_that_finds_something_is_unchanged() {
    let store = graph();
    let rows = run(&store, "OPTIONAL MATCH (a:A) RETURN a AS x");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0], Some(Value::Node(..)) | Some(Value::NodeRef(_))));
}

#[test]
fn expanding_from_a_null_binding_yields_no_rows_rather_than_an_error() {
    // The TCK asserts an empty result here (Match3 [27]), not a failure. This
    // became reachable only once the leading OPTIONAL MATCH produced a null
    // row to expand from.
    let store = graph();
    assert!(run(&store, "OPTIONAL MATCH (a:Nope) WITH a MATCH (a)-->(b) RETURN b AS x").is_empty());
}

#[test]
fn expanding_from_a_real_node_after_an_optional_match_still_works() {
    // The guard: "expand from null yields nothing" must not become "expand
    // after an OPTIONAL MATCH yields nothing".
    let store = graph();
    assert_eq!(
        run(&store, "OPTIONAL MATCH (a:A) WITH a MATCH (a)-->(b) RETURN b AS x").len(),
        1
    );
}

#[test]
fn setting_a_property_on_a_null_binding_is_a_no_op() {
    // TCK Set1 [8]: one null row back, no error, and nothing written.
    let mut store = graph();
    let q = parse_query("OPTIONAL MATCH (a:DoesNotExist) SET a.num = 42 RETURN a AS x")
        .expect("query should parse");
    let out = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("SET on a null binding should not error");
    assert_eq!(out.records.len(), 1);
    assert!(matches!(out.records[0].get("x"), Some(Value::Null) | None));
}

#[test]
fn a_parenthesised_property_target_is_the_same_as_a_bare_one() {
    // TCK Set1 [3]/[4]. `(n).name` and `n.name` are the same thing; the
    // parenthesised form was a parse error, which reads as SET not supporting
    // an ordinary target rather than as four missing characters of grammar.
    let mut store = GraphStore::new();
    for cypher in ["CREATE (:A {name: 'orig'})"] {
        let q = parse_query(cypher).expect("setup should parse");
        MutQueryExecutor::new(&mut store, "default".to_string()).execute(&q).expect("setup");
    }
    let q = parse_query("MATCH (n:A) SET (n).name = 'neo4j' RETURN (n).name AS x")
        .expect("`(n).name` should parse");
    let out = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("should run");
    assert_eq!(
        out.records[0].get("x"),
        Some(&Value::Property(samyama::graph::PropertyValue::String("neo4j".into())))
    );
}

#[test]
fn parentheses_still_group_an_ordinary_expression() {
    // `(1 + 2) * 3` shares its prefix with `(n).name`; the grammar change must
    // not touch it.
    let store = GraphStore::new();
    let q = parse_query("RETURN (1 + 2) * 3 AS x").expect("should parse");
    let out = QueryExecutor::new(&store).execute(&q).expect("should run");
    assert_eq!(
        out.records[0].get("x"),
        Some(&Value::Property(samyama::graph::PropertyValue::Integer(9)))
    );
}
