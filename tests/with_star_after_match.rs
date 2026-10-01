//! `WITH *` / `RETURN *` see what a MATCH after an earlier WITH bound (#1591).
//!
//! ```cypher
//! MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH * RETURN a.p, b.p
//! -- VariableNotBound: Variable not found: b (in scope: a)
//! ```
//!
//! A query with several WITHs keeps the *earlier* ones in `extra_with_stages`
//! and the **last** in `with_clause`. The star expansion walked `with_clause`
//! first, so the last `WITH *` was expanded against the scope of the first
//! WITH and dropped everything bound between them. Every query here is
//! checked against the same query with the variables listed explicitly: that
//! equality is the assertion.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:N {p: 1})-[:R {w: 5}]->(b:N {p: 2})").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .unwrap();
    store
}

/// Column names and the rows, each row rendered and the set sorted so that
/// row order does not matter.
fn run(store: &GraphStore, cypher: &str) -> (Vec<String>, Vec<String>) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` parses: {e:?}"));
    let batch = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` runs: {e:?}"));
    let mut rows: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            batch
                .columns
                .iter()
                .map(|c| format!("{c}={:?}", r.get(c)))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect();
    rows.sort();
    (batch.columns, rows)
}

/// `star` and `explicit` return the same columns and rows, and at least one
/// row, so that agreeing on nothing does not pass.
fn same(star: &str, explicit: &str) {
    let store = store();
    let got = run(&store, star);
    let want = run(&store, explicit);
    assert!(!want.1.is_empty(), "`{explicit}` returned no rows");
    assert_eq!(got, want, "`{star}` vs `{explicit}`");
}

#[test]
fn with_star_after_match_after_with() {
    same(
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH * RETURN a.p, b.p",
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH a, b RETURN a.p, b.p",
    );
}

#[test]
fn with_star_where_after_match_after_with() {
    same(
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH * WHERE b.p > a.p RETURN a.p, b.p",
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH a, b WHERE b.p > a.p RETURN a.p, b.p",
    );
}

#[test]
fn with_star_after_optional_match_after_with() {
    same(
        "MATCH (a:N) WITH a OPTIONAL MATCH (a)-[:R]->(b:N) WITH * RETURN a.p, b.p",
        "MATCH (a:N) WITH a OPTIONAL MATCH (a)-[:R]->(b:N) WITH a, b RETURN a.p, b.p",
    );
}

#[test]
fn with_star_after_unwind_after_with() {
    same(
        "MATCH (a:N) WITH a UNWIND [10, 20] AS x WITH * RETURN a.p, x",
        "MATCH (a:N) WITH a UNWIND [10, 20] AS x WITH a, x RETURN a.p, x",
    );
}

#[test]
fn three_withs_with_matches_between() {
    same(
        "MATCH (a:N {p: 1}) WITH a MATCH (a)-[:R]->(b:N) WITH * \
         MATCH (c:N) WHERE c.p = b.p WITH * RETURN a.p, b.p, c.p",
        "MATCH (a:N {p: 1}) WITH a MATCH (a)-[:R]->(b:N) WITH a, b \
         MATCH (c:N) WHERE c.p = b.p WITH a, b, c RETURN a.p, b.p, c.p",
    );
}

#[test]
fn return_star_after_match_after_with() {
    let store = store();
    let (columns, _) = run(&store, "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) RETURN *");
    let mut sorted = columns.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["a".to_string(), "b".to_string()]);
    same(
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) RETURN *",
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) RETURN a, b",
    );
}

#[test]
fn return_star_after_with_star_after_match_after_with() {
    same(
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH * RETURN *",
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH a, b RETURN a, b",
    );
}

#[test]
fn star_with_an_extra_item() {
    same(
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH *, a.p + 1 AS q RETURN a.p, b.p, q",
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH a, b, a.p + 1 AS q RETURN a.p, b.p, q",
    );
}

#[test]
fn relationship_and_path_variables() {
    same(
        "MATCH (a:N) WITH a MATCH p = (a)-[r:R]->(b:N) WITH * RETURN r.w, length(p), b.p",
        "MATCH (a:N) WITH a MATCH p = (a)-[r:R]->(b:N) WITH a, p, r, b RETURN r.w, length(p), b.p",
    );
    same(
        "MATCH (a:N) WITH a MATCH p = (a)-[r:R]->(b:N) RETURN *",
        "MATCH (a:N) WITH a MATCH p = (a)-[r:R]->(b:N) RETURN a, p, r, b",
    );
}

/// Anonymous pattern elements are not variables, so a star does not expose
/// them: the columns are exactly the named ones.
#[test]
fn anonymous_elements_are_not_exposed() {
    let store = store();
    let (columns, rows) = run(
        &store,
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(:N) WITH * RETURN *",
    );
    assert_eq!(columns, vec!["a".to_string()]);
    assert_eq!(rows.len(), 1);
    let (columns, _) = run(&store, "MATCH (a:N) WITH a MATCH (a)-[]->(b) RETURN *");
    let mut sorted = columns.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["a".to_string(), "b".to_string()]);
}

/// A WITH after the MATCH still narrows: the fix is about where scope is
/// taken, not about keeping everything ever bound.
#[test]
fn a_narrowing_with_still_narrows() {
    let store = store();
    let (columns, _) = run(
        &store,
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH b WITH * RETURN *",
    );
    assert_eq!(columns, vec!["b".to_string()]);
}

/// Each UNION branch expands its own stars against its own scope. The
/// branches' stars were not expanded at all, so the first branch was checked
/// against a literal `*` column.
#[test]
fn union_branches_expand_their_own_stars() {
    same(
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH * RETURN a.p AS x \
         UNION MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH * RETURN b.p AS x",
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH a, b RETURN a.p AS x \
         UNION MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH a, b RETURN b.p AS x",
    );
    same(
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) RETURN * \
         UNION ALL MATCH (a:N {p: 1}) WITH a MATCH (a)-[:R]->(b:N) RETURN *",
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) RETURN a, b \
         UNION ALL MATCH (a:N {p: 1}) WITH a MATCH (a)-[:R]->(b:N) RETURN a, b",
    );
}

/// What a subquery binds stays in it, except the columns a `CALL {}` returns.
#[test]
fn subqueries_do_not_leak_scope() {
    same(
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WHERE EXISTS { MATCH (b)<-[:R]-(z) } WITH * RETURN *",
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WHERE EXISTS { MATCH (b)<-[:R]-(z) } WITH a, b RETURN a, b",
    );
    same(
        "MATCH (a:N) CALL { WITH a MATCH (a)-[:R]->(b:N) RETURN b } RETURN *",
        "MATCH (a:N) CALL { WITH a MATCH (a)-[:R]->(b:N) RETURN b } RETURN a, b",
    );
}
