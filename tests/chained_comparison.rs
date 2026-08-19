//! `1 < n.num < 3` means `1 < n.num AND n.num < 3`.
//!
//! Left-associative parsing makes it `(1 < n.num) < 3`, which compares a
//! boolean to 3 — "Cannot compare these types" on ordinary Cypher.
//!
//! The rewrite keys on the **token sequence**, not on the parsed tree, and
//! that is the whole safety argument. Parentheses are inline in `primary`, so
//! `(a < b) = true` and `a < b < c` are indistinguishable once parsed — but at
//! the token level the first has one top-level comparison operator and the
//! second has two. Expanding only when *every* top-level operator is a
//! comparison therefore cannot touch a parenthesised comparison being compared
//! to something, and cannot touch anything joined by AND/OR or arithmetic.
//!
//! That conservatism has a cost, pinned by the last test here: a chain mixed
//! into a larger expression is still refused. It fails as an error rather than
//! a wrong answer, which is the right way round to be incomplete.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn three_nodes() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("UNWIND [1, 2, 3] AS i CREATE ({num: i, name: toString(i)})")
        .expect("setup should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup should run");
    store
}

fn nums(store: &GraphStore, cypher: &str) -> Vec<i64> {
    let q = parse_query(cypher).expect("query should parse");
    let mut out: Vec<i64> = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .iter()
        .filter_map(|r| match r.get("v") {
            Some(Value::Property(p)) => p.as_integer(),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out
}

#[test]
fn a_numeric_range_selects_the_middle() {
    let store = three_nodes();
    assert_eq!(nums(&store, "MATCH (n) WHERE 1 < n.num < 3 RETURN n.num AS v"), vec![2]);
    assert_eq!(nums(&store, "MATCH (n) WHERE 1 <= n.num < 3 RETURN n.num AS v"), vec![1, 2]);
    assert_eq!(nums(&store, "MATCH (n) WHERE 1 < n.num <= 3 RETURN n.num AS v"), vec![2, 3]);
    assert_eq!(nums(&store, "MATCH (n) WHERE 1 <= n.num <= 3 RETURN n.num AS v"), vec![1, 2, 3]);
}

#[test]
fn an_empty_range_selects_nothing_rather_than_erroring() {
    let store = three_nodes();
    assert_eq!(nums(&store, "MATCH (n) WHERE 10 < n.num <= 3 RETURN n.num AS v"), Vec::<i64>::new());
}

#[test]
fn a_chain_of_strings_works_the_same_way() {
    let store = three_nodes();
    let q = parse_query("MATCH (n) WHERE '1' < n.name < '3' RETURN n.num AS v")
        .expect("query should parse");
    let got = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(got.records.len(), 1);
}

#[test]
fn a_parenthesised_comparison_is_not_a_chain() {
    // The case the token-level rule exists to protect. Rewriting this as
    // `1 < 2 AND 2 = true` would be a wrong answer, and the parsed tree cannot
    // tell it apart from a chain.
    let store = GraphStore::new();
    let q = parse_query("RETURN (1 < 2) = true AS v").expect("query should parse");
    let got = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(
        got.records[0].get("v"),
        Some(&Value::Property(samyama::graph::PropertyValue::Boolean(true)))
    );
}

#[test]
fn ordinary_expressions_are_untouched() {
    let store = three_nodes();
    assert_eq!(nums(&store, "MATCH (n) WHERE 1 < n.num RETURN n.num AS v"), vec![2, 3]);
    assert_eq!(
        nums(&store, "MATCH (n) WHERE n.num > 1 AND n.num < 3 RETURN n.num AS v"),
        vec![2]
    );
    let q = parse_query("RETURN 1 + 2 * 3 AS v").expect("query should parse");
    let got = QueryExecutor::new(&GraphStore::new()).execute(&q).expect("should run");
    assert_eq!(
        got.records[0].get("v"),
        Some(&Value::Property(samyama::graph::PropertyValue::Integer(7))),
        "precedence is still the Pratt parser's job"
    );
}

#[test]
fn a_chain_inside_a_larger_expression_is_still_refused() {
    // Deliberately not handled: the top-level operators here are `<`, `<`,
    // `AND`, `=`, so the rule declines and the expression takes the ordinary
    // path, where the chain is a type error. Recognising a chain *inside* a
    // wider expression means reimplementing operator precedence outside the
    // Pratt parser, which is how two parsers start disagreeing.
    //
    // This asserts the shape of the gap, not that the gap is desirable. An
    // error is the right way to be incomplete; the failure to avoid is
    // answering it wrongly.
    let store = three_nodes();
    let q = parse_query("MATCH (n) WHERE 1 < n.num < 3 AND n.num = 2 RETURN n.num AS v")
        .expect("it parses");
    assert!(
        QueryExecutor::new(&store).execute(&q).is_err(),
        "a mixed chain must fail loudly rather than return the wrong rows"
    );
}
