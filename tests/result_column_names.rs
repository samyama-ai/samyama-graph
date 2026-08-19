//! An unaliased result column is named after the expression as written.
//!
//! The planner reconstructed the name from the AST, in ten separate places
//! that did not agree, and gave up on anything that was not a variable or a
//! property:
//!
//! ```text
//! RETURN 1 + 1        ->  col_0      RETURN count(*)  ->  count()
//! ```
//!
//! `col_0` is not a name a client can ask for — the column exists but cannot
//! be selected by key. `count()` is worse: a plausible name for a column that
//! does not have it, on the most common aggregate anyone writes. The `*` was
//! dropped because the reconstruction walks argument expressions and `*` is
//! not one; no amount of care there recovers text that was discarded at parse
//! time, which is why the name now comes from the source (#635).

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn columns(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .columns
}

fn two_nodes() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (:L {v: 1}), (:L {v: 2})").expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");
    store
}

#[test]
fn an_unaliased_column_is_named_after_the_expression() {
    let store = two_nodes();
    for (cypher, expected) in [
        ("RETURN 1 + 1", vec!["1 + 1"]),
        ("RETURN 'x'", vec!["'x'"]),
        ("UNWIND [1, 2] AS x RETURN x * 2", vec!["x * 2"]),
        ("MATCH (n:L) RETURN n.v", vec!["n.v"]),
        ("MATCH (n) RETURN n", vec!["n"]),
    ] {
        assert_eq!(columns(&store, cypher), expected, "for `{cypher}`");
    }
}

#[test]
fn count_star_keeps_its_star() {
    // The reconstruction produced `count()` because `*` is not an argument
    // expression. A caller asking for the `count(*)` key got nothing.
    let store = two_nodes();
    assert_eq!(columns(&store, "MATCH () RETURN count(*)"), vec!["count(*)"]);
    assert_eq!(columns(&store, "MATCH (n) RETURN count(n)"), vec!["count(n)"]);
    assert_eq!(
        columns(&store, "MATCH (n) RETURN n.v, count(*)"),
        vec!["n.v", "count(*)"]
    );
}

#[test]
fn an_alias_still_wins() {
    let store = two_nodes();
    assert_eq!(columns(&store, "MATCH (n) RETURN n.v AS val"), vec!["val"]);
    assert_eq!(columns(&store, "MATCH () RETURN count(*) AS total"), vec!["total"]);
}

#[test]
fn the_named_column_is_the_one_the_row_carries() {
    // Naming the column in the header but binding the row under a different
    // key would look identical in a column listing and return null to every
    // caller, so the lookup is asserted rather than the header alone.
    let store = two_nodes();
    let q = parse_query("MATCH () RETURN count(*)").expect("query should parse");
    let out = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(out.columns, vec!["count(*)"]);
    assert!(
        out.records[0].get("count(*)").is_some(),
        "the row must bind the column it advertises, got {:?}",
        out.records[0]
    );
}
