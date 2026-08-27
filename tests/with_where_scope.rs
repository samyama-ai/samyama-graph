//! A `WITH`'s `WHERE` can see the variables the projection drops (#840).
//!
//! ```cypher
//! UNWIND [0,1] AS i UNWIND [0,1] AS j
//! WITH i AS a, j AS b
//! WHERE i <> j
//! RETURN a, b
//! ```
//!
//! Two rows expected, **zero returned, no error**. The predicate ran inside the
//! barrier, after the projection had dropped `i` and `j`, so it evaluated to
//! null for every row — and a filter that matches nothing is a legitimate
//! outcome, so nothing distinguished this from a correct empty result.
//!
//! The split is by conjunct, and both halves are pinned below: a predicate
//! naming a projected alias must still run *after* the barrier, or an
//! aggregate would be filtered before it is computed.
//!
//! It lives in `build_with_barrier` because **both** planner paths call it. My
//! first attempt patched the by-kind stage loop and moved nothing, because the
//! query that motivated it parses into the clause pipeline — the same trap
//! `ast_shape_parity.rs` exists for (#797).

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"))
        .records
        .len()
}

fn one_edge() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE ()-[:T]->()").expect("parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("creates");
    store
}

/// A predicate naming only dropped variables runs before the barrier.
#[test]
fn a_where_may_name_variables_the_with_drops() {
    let store = GraphStore::new();
    assert_eq!(rows(&store, "UNWIND [1,2,3] AS i WITH i*10 AS x WHERE i > 1 RETURN x"), 2);
    assert_eq!(
        rows(&store, "UNWIND [0,1] AS i UNWIND [0,1] AS j WITH i AS a, j AS b WHERE i <> j RETURN a, b"),
        2
    );
}

/// A predicate naming a projected alias still runs after it — including when
/// the alias is an aggregate, which cannot be filtered before it exists.
#[test]
fn a_where_naming_an_alias_still_runs_after_the_barrier() {
    let store = GraphStore::new();
    assert_eq!(rows(&store, "UNWIND [1,2,3] AS i WITH i*10 AS x WHERE x > 10 RETURN x"), 2);
    assert_eq!(rows(&store, "UNWIND [1,2,3] AS i WITH count(*) AS c WHERE c > 1 RETURN c"), 1);
    assert_eq!(
        rows(&store, "UNWIND [1,1,2,2,3] AS i WITH i, count(*) AS c WHERE c > 1 RETURN i"),
        2
    );
}

/// A predicate mixing both scopes is split, not sent wholly to one side.
#[test]
fn a_mixed_predicate_is_split_by_conjunct() {
    let store = GraphStore::new();
    assert_eq!(
        rows(&store, "UNWIND [1,2,3] AS i WITH i*10 AS x WHERE i > 1 AND x < 30 RETURN x"),
        1
    );
}

/// Ordering an entity against anything is null, not a `TypeError` — otherwise
/// one incomparable pair takes down a query that compares many.
#[test]
fn ordering_an_entity_yields_null() {
    let store = one_edge();
    for expr in ["n < 1", "n < ''", "r < 1", "p < 1", "n < n", "1 > n", "p >= r"] {
        let cypher = format!("MATCH p = (n)-[r]->() RETURN {expr} AS x");
        let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
        let batch = QueryExecutor::new(&store)
            .execute(&q)
            .unwrap_or_else(|e| panic!("{cypher} raised {e:?} instead of yielding null"));
        assert!(
            matches!(
                batch.records.first().and_then(|r| r.get("x")),
                Some(samyama::query::executor::Value::Property(
                    samyama::graph::PropertyValue::Null
                )) | Some(samyama::query::executor::Value::Null)
            ),
            "{cypher} did not yield null"
        );
    }
}

/// Identity comparison on entities is unaffected, and arithmetic on one is
/// still an error — `null` there would hide a mistake rather than express one.
#[test]
fn equality_and_arithmetic_are_unchanged() {
    let store = one_edge();
    let q = parse_query("MATCH (n) WITH n LIMIT 1 MATCH (m) WITH n, m LIMIT 1 RETURN n = m AS x")
        .expect("parses");
    assert!(QueryExecutor::new(&store).execute(&q).is_ok());

    let bad = parse_query("MATCH (n) RETURN n + 1 AS x").expect("parses");
    assert!(
        QueryExecutor::new(&store).execute(&bad).is_err(),
        "arithmetic on an entity should still raise"
    );
}

/// The `Comparison2` scenario end to end: of every pair drawn from one value
/// per type, only the two numbers order.
#[test]
fn only_numbers_order_across_types() {
    let store = one_edge();
    let cypher = "
        MATCH p = (n)-[r]->()
        WITH [n, r, p, '', 1, 3.14, true, null, [], {}] AS types
        UNWIND range(0, size(types) - 1) AS i
        UNWIND range(0, size(types) - 1) AS j
        WITH types[i] AS lhs, types[j] AS rhs
        WHERE i <> j
        WITH lhs, rhs, lhs < rhs AS result
        WHERE result
        RETURN lhs, rhs";
    assert_eq!(rows(&store, cypher), 1);
}
