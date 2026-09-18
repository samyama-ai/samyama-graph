//! `YIELD col AS alias` binds the alias (#1318).
//!
//! It parsed, ran, and bound nothing: every reference to the alias failed
//! `VariableNotBound`, while a query that aliased and then never used the alias
//! succeeded. So the form looked supported right up to the point of using it,
//! and ALGO-04's baseline recorded "aliased yields work" on the strength of it.
//!
//! The aliases are applied once, where both execution paths converge, rather
//! than in each of the sixty-odd `execute_*` functions — each binds its own
//! column names and every one of them would have to remember.

use samyama::graph::{EdgeType, GraphStore};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let a = s.create_node("N");
    let b = s.create_node("N");
    let c = s.create_node("N");
    s.create_edge(a, b, EdgeType::new("R")).unwrap();
    s.create_edge(b, c, EdgeType::new("R")).unwrap();
    s
}

fn rows(s: &GraphStore, q: &str) -> Result<usize, String> {
    let p = parse_query(q).map_err(|e| format!("{e:?}"))?;
    QueryExecutor::new(s).execute(&p).map(|r| r.records.len()).map_err(|e| e.to_string())
}

#[test]
fn an_aliased_yield_column_can_be_referenced() {
    let s = store();
    assert_eq!(rows(&s, "CALL algo.pageRank() YIELD node AS a, score AS b RETURN a, b"), Ok(3));
    assert_eq!(rows(&s, "CALL algo.pageRank() YIELD node AS a, score RETURN a, score"), Ok(3));
    assert_eq!(rows(&s, "CALL algo.pageRank() YIELD node, score AS b RETURN node, b"), Ok(3));
}

#[test]
fn the_alias_works_for_other_algorithms_too() {
    // The rename is applied where both execution paths converge, so an
    // algorithm added later inherits it rather than having to remember.
    let s = store();
    assert_eq!(rows(&s, "CALL algo.wcc() YIELD node AS n, componentId AS c RETURN n, c"), Ok(3));
}

#[test]
fn an_alias_survives_order_by_and_limit() {
    let s = store();
    assert_eq!(
        rows(&s, "CALL algo.pageRank() YIELD node AS a, score AS b RETURN b ORDER BY b DESC LIMIT 2"),
        Ok(2)
    );
}

#[test]
fn the_original_column_name_still_works_unaliased() {
    // The failure this guards is a rename that *moves* the binding instead of
    // adding one, which would break every query that does not alias.
    let s = store();
    assert_eq!(rows(&s, "CALL algo.pageRank() YIELD node, score RETURN node, score"), Ok(3));
    assert_eq!(rows(&s, "CALL algo.wcc() YIELD node, componentId RETURN node"), Ok(3));
}

#[test]
fn an_alias_of_a_column_the_algorithm_does_not_produce_is_not_bound() {
    // Not a silent success: referencing it fails, which is what tells the
    // caller the column name was wrong.
    let s = store();
    assert!(rows(&s, "CALL algo.pageRank() YIELD nosuchcolumn AS x RETURN x").is_err());
}
