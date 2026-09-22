//! `CALL … YIELD … WITH …` (#1375, LANG-11, ALGO-04).
//!
//! `CALL algo.pageRank() YIELD node, score WITH score RETURN count(*)` was
//! refused with "`CALL` is not yet supported in this clause position". Two
//! places said so, and both were telling the truth:
//!
//! - `parse_clause_pipeline` had no `Rule::call_clause` arm, so a CALL in the
//!   ordered-clause pipeline fell to the catch-all, even though the grammar's
//!   `pipeline_clause` lists `call_clause`.
//! - the pipeline planner refused `Clause::Call` with a comment saying CALL
//!   was still to come.
//!
//! # Why the parse error was load-bearing
//!
//! The shape-specific path keeps the query's clauses in by-kind fields with no
//! order between them, and plans the WITH barrier three hundred lines before it
//! plans the CALL. For a query that *begins* with CALL, `operator` is still
//! `None` when the barrier block runs, so `if let Some(op) = operator` skips it
//! and the WITH is **dropped**. Teaching only the grammar to accept this query
//! would have turned a parse error into a wrong answer.
//!
//! So this goes through the clause pipeline, which is the thing that does
//! represent order. The cases below therefore check *results*, not that the
//! query parses: a dropped WITH still parses and still runs.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let a = s.create_node("P");
    let b = s.create_node("P");
    let c = s.create_node("P");
    for (n, name) in [(a, "Ada"), (b, "Grace"), (c, "Alan")] {
        let _ = s.set_node_property("default", n, "name", PropertyValue::String(name.into()));
    }
    // Ada <- Grace, Ada <- Alan, so Ada outranks both.
    let _ = s.create_edge(b, a, "KNOWS");
    let _ = s.create_edge(c, a, "KNOWS");
    s
}

fn run(s: &GraphStore, cypher: &str) -> Vec<Vec<(String, Value)>> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    let cols = batch.columns.clone();
    batch
        .records
        .iter()
        .map(|r| {
            cols.iter()
                .map(|c| (c.clone(), r.get(c).cloned().unwrap_or(Value::Null)))
                .collect()
        })
        .collect()
}

fn one_int(s: &GraphStore, cypher: &str, col: &str) -> i64 {
    let rows = run(s, cypher);
    assert_eq!(rows.len(), 1, "{cypher}\n  wanted one row, got {rows:?}");
    match rows[0].iter().find(|(c, _)| c == col).map(|(_, v)| v) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("{cypher}\n  wanted an integer in `{col}`, got {other:?}"),
    }
}

#[test]
fn call_yield_with_projects_and_counts() {
    let s = store();
    // The plain case from the issue.
    assert_eq!(
        one_int(&s, "CALL algo.pageRank() YIELD node, score WITH score RETURN count(*) AS c", "c"),
        3,
        "one row per node survives the WITH"
    );
}

#[test]
fn the_where_after_with_is_not_dropped() {
    let s = store();
    // The case that made the parse error load-bearing. If the WITH barrier is
    // skipped, this returns 3 -- every node -- instead of the filtered count,
    // and it returns it without erroring.
    let all = one_int(
        &s,
        "CALL algo.pageRank() YIELD node, score WITH node, score WHERE score > 0.0 RETURN count(*) AS c",
        "c",
    );
    assert_eq!(all, 3, "every score is above zero");

    let none = one_int(
        &s,
        "CALL algo.pageRank() YIELD node, score WITH node, score WHERE score > 1.0 RETURN count(*) AS c",
        "c",
    );
    assert_eq!(
        none, 0,
        "no score exceeds 1.0, so a WITH that is actually applied filters everything out"
    );
}

#[test]
fn with_narrows_the_columns_it_carries() {
    let s = store();
    // A WITH that is dropped leaves `node` bound, and this query would then
    // succeed instead of failing. The error is the evidence that the barrier
    // ran.
    let q = parse_query("CALL algo.pageRank() YIELD node, score WITH score RETURN node").unwrap();
    let err = QueryExecutor::new(&s).execute(&q);
    assert!(
        err.is_err(),
        "`node` was not carried through the WITH, so returning it must fail: {err:?}"
    );
}

#[test]
fn with_can_alias_and_order_what_yield_produced() {
    let s = store();
    let rows = run(
        &s,
        "CALL algo.pageRank() YIELD node, score WITH score AS s ORDER BY s DESC LIMIT 1 RETURN s",
    );
    assert_eq!(rows.len(), 1);
    match rows[0].iter().find(|(c, _)| c == "s").map(|(_, v)| v) {
        Some(Value::Property(PropertyValue::Float(f))) => {
            assert!(*f > 0.0, "the top score is a real number, got {f}")
        }
        other => panic!("wanted a float, got {other:?}"),
    }
}

#[test]
fn an_aggregate_after_with_sees_every_yielded_row() {
    let s = store();
    let rows = run(
        &s,
        "CALL algo.pageRank() YIELD node, score WITH sum(score) AS total RETURN total",
    );
    assert_eq!(rows.len(), 1, "an aggregate with no grouping key gives one row");
    match rows[0].iter().find(|(c, _)| c == "total").map(|(_, v)| v) {
        Some(Value::Property(PropertyValue::Float(f))) => {
            assert!((*f - 1.0).abs() < 0.05, "pageRank scores sum to about 1, got {f}")
        }
        other => panic!("wanted a float, got {other:?}"),
    }
}

#[test]
fn call_then_call_is_the_other_algo_04_form() {
    // ALGO-04 counted 7 of 9 composition forms. `with-after-yield` is the rest
    // of this file; `call-then-call` is the second gap, and it is the same
    // ordering problem -- a query with two CALLs cannot be held in a single
    // `call_clause` field at all.
    let s = store();

    // Nothing shared: the second YIELD renames `node`, so this is a product.
    assert_eq!(
        one_int(
            &s,
            "CALL algo.pageRank() YIELD node, score \
             CALL algo.degree() YIELD node AS n2, degree \
             RETURN count(*) AS c",
            "c",
        ),
        9,
        "three nodes against three nodes is nine rows"
    );

    // Sharing `node` joins on it instead, which is the row count that says the
    // join ran rather than a product being taken and filtered later.
    assert_eq!(
        one_int(
            &s,
            "CALL algo.pageRank() YIELD node, score \
             WITH node, score \
             CALL algo.degree() YIELD node, degree \
             RETURN count(*) AS c",
            "c",
        ),
        3,
        "the shared `node` is a join key, so three rows, not nine"
    );
}

#[test]
fn a_second_with_still_barriers_after_the_first() {
    // Two WITH stages after a CALL, the first filtering and the second
    // aggregating. A pipeline that applied only the last WITH would return 3.
    let s = store();
    assert_eq!(
        one_int(
            &s,
            "CALL algo.pageRank() YIELD node, score \
             WITH score WHERE score > 1.0 \
             WITH count(*) AS c RETURN c",
            "c",
        ),
        0,
        "the first WITH's filter removes every row before the second counts them"
    );
}

#[test]
fn a_match_before_the_call_still_works() {
    let s = store();
    // The control. Threading CALL through the pipeline must not change the
    // shape that already worked.
    assert_eq!(
        one_int(&s, "MATCH (p:P) RETURN count(*) AS c", "c"),
        3,
        "a plain MATCH is unaffected"
    );
    assert_eq!(
        one_int(&s, "CALL algo.pageRank() YIELD node, score RETURN count(*) AS c", "c"),
        3,
        "CALL with no WITH is unaffected"
    );
}
