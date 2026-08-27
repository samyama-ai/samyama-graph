//! `WITH *` carries scope forward in a clause pipeline too (#892).
//!
//! ```cypher
//! CREATE (a) WITH * CREATE (b) CREATE (a)<-[:T]-(b)   -- created three nodes
//! ```
//!
//! `Query` has two shapes — the by-kind fields and the clause pipeline — and
//! the star-expansion pass only ever walked the first. In the pipeline the
//! `*` survived parsing as a variable literally named `*`, so the `WITH`
//! projected nothing, every binding was dropped, and the later `CREATE (a)`
//! read `a` as a new node.
//!
//! Nothing errored. The query created a node too many and reported success,
//! which is why the TCK scored it green until the harness started checking
//! side effects (#888).

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` parses: {e:?}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` runs: {e:?}"));
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("count query parses");
    QueryExecutor::new(store)
        .execute(&q)
        .expect("count query runs")
        .records
        .len()
}

/// The star and the explicit list must agree — that equality is the assertion,
/// not the number 2 on its own.
#[test]
fn a_star_projects_what_naming_the_variable_would() {
    for cypher in [
        "CREATE (a) WITH a CREATE (b) CREATE (a)<-[:T]-(b)",
        "CREATE (a) WITH * CREATE (b) CREATE (a)<-[:T]-(b)",
        "CREATE (a) WITH a WITH * CREATE (b) CREATE (a)<-[:T]-(b)",
        "CREATE (a) WITH * WITH * CREATE (b) CREATE (a)<-[:T]-(b)",
    ] {
        let mut store = GraphStore::new();
        run(&mut store, cypher);
        assert_eq!(count(&store, "MATCH (n) RETURN n"), 2, "nodes after `{cypher}`");
        assert_eq!(count(&store, "MATCH ()-[r]->() RETURN r"), 1, "rels after `{cypher}`");
    }
}

/// A `WITH` that narrows still narrows: the star is scope, not "everything
/// ever bound".
#[test]
fn a_later_with_narrows_the_scope_a_star_sees() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:X {n: 1}), (:Y {n: 2})");
    let q = parse_query("MATCH (x:X), (y:Y) WITH x WITH * RETURN *").expect("parses");
    let batch = QueryExecutor::new(&store).execute(&q).expect("runs");
    assert_eq!(batch.columns, vec!["x".to_string()], "only x survives the narrowing WITH");
}

/// UNWIND and MATCH bind into the pipeline's scope as well.
#[test]
fn unwind_and_match_enter_the_scope_a_star_sees() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:X)");
    let q = parse_query("MATCH (x:X) UNWIND [1] AS i WITH * RETURN *").expect("parses");
    let batch = QueryExecutor::new(&store).execute(&q).expect("runs");
    let mut columns = batch.columns.clone();
    columns.sort();
    assert_eq!(columns, vec!["i".to_string(), "x".to_string()]);
}
