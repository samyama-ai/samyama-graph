//! A row-invariant `hierarchy_lca` is evaluated once, and a varying one is not (#1399).
//!
//! H7 of the HIER corpus is
//!
//! ```text
//! MATCH (a:Term {code:"T012"}), (b:Term {code:"T034"}), (c:Term)
//! WHERE id(c) IN hierarchy_lca(a, b) RETURN max(c.level)
//! ```
//!
//! `a` and `b` are each pinned to one node; `c` ranges over 9,331 terms. The
//! call does not depend on `c`, and it was evaluated once per row — each time
//! walking the hierarchy registry under two `RwLock`s, cloning an `Arc`, and
//! building two vectors for an answer that had not changed.
//!
//! `FilterOperator` now keeps the last `(function, argument node ids) ->
//! answer` for the functions whose result depends only on their arguments.
//!
//! # What these cases are for
//!
//! Not the speed. The corpus measures that, three runs either side:
//!
//! ```text
//! H7 indexed ms   without  4.258  4.167  4.300
//!                 with     3.063  3.059  3.093
//! ```
//!
//! A cache's risk is a **stale hit**: returning the previous row's answer when
//! this row's arguments differ. That is what is pinned here, because it is the
//! failure that would return a wrong answer rather than a slow one. It would
//! also be invisible in H7 itself, where every row shares the same arguments.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn rows(store: &GraphStore, cypher: &str, col: &str) -> Vec<i64> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .iter()
        .map(|r| match r.get(col) {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("expected an integer in `{col}`, got {other:?}"),
        })
        .collect()
}

/// Two branches under one root, so a pair's LCA depends on which pair it is:
///
/// ```text
///        root
///        /  \
///      left right
///      /  \    \
///    l1   l2    r1
/// ```
fn forked() -> GraphStore {
    let mut store = GraphStore::new();
    for (code, level) in [
        ("root", 0),
        ("left", 1),
        ("right", 1),
        ("l1", 2),
        ("l2", 2),
        ("r1", 2),
    ] {
        run(
            &mut store,
            &format!("CREATE (:Term {{code: \"{code}\", level: {level}}})"),
        );
    }
    for (child, parent) in [
        ("left", "root"),
        ("right", "root"),
        ("l1", "left"),
        ("l2", "left"),
        ("r1", "right"),
    ] {
        run(
            &mut store,
            &format!(
                "MATCH (x:Term {{code:\"{child}\"}}), (y:Term {{code:\"{parent}\"}}) \
                 CREATE (x)-[:BROADER]->(y)"
            ),
        );
    }
    run(
        &mut store,
        "CREATE HIERARCHY INDEX t ON ()-[:BROADER]->() MEASURE units AGGREGATE sum, count",
    );
    store
}

#[test]
fn the_invariant_call_gives_the_same_answer_it_always_did() {
    // H7's shape: both arguments pinned, the scanned variable irrelevant to
    // the call. `l1` and `l2` are siblings under `left`.
    let store = forked();
    let got = rows(
        &store,
        "MATCH (a:Term {code:\"l1\"}), (b:Term {code:\"l2\"}), (c:Term) \
         WHERE id(c) IN hierarchy_lca(a, b) RETURN max(c.level) AS lvl",
        "lvl",
    );
    assert_eq!(got, vec![1], "the LCA of l1 and l2 is `left`, at level 1");

    // Across the fork, the answer is the root.
    let got = rows(
        &store,
        "MATCH (a:Term {code:\"l1\"}), (b:Term {code:\"r1\"}), (c:Term) \
         WHERE id(c) IN hierarchy_lca(a, b) RETURN max(c.level) AS lvl",
        "lvl",
    );
    assert_eq!(got, vec![0], "across the fork the LCA is `root`, at level 0");
}

#[test]
fn a_varying_argument_is_not_served_the_previous_rows_answer() {
    // The case a cache breaks. Here the *first* argument comes from the scan,
    // so every row is a different call, and a memo that ignored the arguments
    // -- or keyed on only some of them -- would answer every row with whatever
    // the first row produced.
    //
    // Per-row truth, LCA with `l1`:
    //   root -> root(0)   left -> left(1)   right -> root(0)
    //   l1   -> l1(2)     l2   -> left(1)   r1    -> root(0)
    // so the levels, sorted, are 0,0,0,1,1,2.
    let store = forked();
    let mut got = rows(
        &store,
        "MATCH (d:Term), (b:Term {code:\"l1\"}), (c:Term) \
         WHERE id(c) IN hierarchy_lca(d, b) RETURN c.level AS lvl",
        "lvl",
    );
    got.sort();
    assert_eq!(
        got,
        vec![0, 0, 0, 1, 1, 2],
        "each row's LCA must be computed for that row's `d`"
    );
}

#[test]
fn alternating_arguments_do_not_stick() {
    // A single-entry cache is right for the invariant case and has to *miss*
    // when the arguments alternate. Asking for two different pairs in one
    // query is the smallest version of that: if the entry stuck, both halves
    // would report the same level.
    let store = forked();
    let mut got = rows(
        &store,
        "MATCH (a:Term {code:\"l1\"}), (b:Term), (c:Term) \
         WHERE b.code IN [\"l2\", \"r1\"] AND id(c) IN hierarchy_lca(a, b) \
         RETURN c.level AS lvl",
        "lvl",
    );
    got.sort();
    assert_eq!(
        got,
        vec![0, 1],
        "l1/l2 gives `left` (1) and l1/r1 gives `root` (0); one value for both \
         would mean the cached entry was reused across different arguments"
    );
}

#[test]
fn the_index_still_agrees_with_the_traversal() {
    // The metamorphic control: the same question asked without the function at
    // all. A cache that changed the answer would show here even if every case
    // above happened to agree with itself.
    let store = forked();
    let by_function = rows(
        &store,
        "MATCH (a:Term {code:\"l1\"}), (b:Term {code:\"l2\"}), (c:Term) \
         WHERE id(c) IN hierarchy_lca(a, b) RETURN max(c.level) AS lvl",
        "lvl",
    );
    let by_traversal = rows(
        &store,
        "MATCH (a:Term {code:\"l1\"})-[:BROADER*0..]->(c:Term)<-[:BROADER*0..]-(b:Term {code:\"l2\"}) \
         RETURN max(c.level) AS lvl",
        "lvl",
    );
    assert_eq!(by_function, by_traversal, "the index and the traversal disagree");
}
