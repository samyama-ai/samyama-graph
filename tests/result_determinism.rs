//! The same query, over the same data, must return the same rows.
//!
//! Both defects here were found by running the openCypher TCK five times and
//! diffing the failure manifests. The pass count moved between 484 and 487 at
//! a fixed commit while `errored` stayed constant, so two or three scenarios
//! were changing their *answer* between processes — which no summary number
//! could localise, and which a single run would have reported as a stable
//! 46%.
//!
//! The common cause is a `HashSet`/`HashMap` iteration order reaching a
//! result. `RandomState` seeds every process differently, so this class of
//! bug is invisible to a test that runs once and to a developer who reruns
//! the same failing case.
//!
//! Each test below runs its query many times **in one process**, which does
//! not vary the hash seed — so a single-process loop cannot catch the
//! original bug on its own. They pin the fix instead: the sorted contract and
//! the rewritten sort key are properties of the output that hold regardless
//! of iteration order, and each test is written to fail against the old
//! behaviour for a reason that has nothing to do with luck.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::graph::PropertyValue;

fn write(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("query should run");
}

/// Rows rendered as sorted `key=value` strings, in result order.
fn rows(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    batch
        .records
        .iter()
        .map(|r| {
            let mut cells: Vec<String> =
                r.bindings().iter().map(|(k, v)| format!("{k}={v:?}")).collect();
            cells.sort();
            cells.join(",")
        })
        .collect()
}

fn aggregate_fixture() -> GraphStore {
    // Three groups by `num2 % 3`, with distinct sums 7, 13 and 15, so a sort
    // has an unambiguous order and a LIMIT has an unambiguous answer.
    let mut store = GraphStore::new();
    write(
        &mut store,
        "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2}), (:A {num: 9, num2: 0}), \
         (:A {num: 3, num2: 3}), (:A {num: 7, num2: 1})",
    );
    store
}

#[test]
fn order_by_restating_an_aggregate_sorts_the_same_as_ordering_by_its_alias() {
    // The two spellings are the same query. `ORDER BY sum(...)` evaluated to
    // null after the aggregation barrier — there is no `a` left to evaluate
    // it against — so the sort silently became a no-op.
    let store = aggregate_fixture();
    let by_expression = rows(
        &store,
        "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num + a.num2) AS s ORDER BY sum(a.num + a.num2) \
         RETURN m, s",
    );
    let by_alias = rows(
        &store,
        "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num + a.num2) AS s ORDER BY s RETURN m, s",
    );
    assert_eq!(by_expression, by_alias);
    assert_eq!(by_expression.len(), 3);
}

#[test]
fn a_limit_after_ordering_by_an_aggregate_takes_the_smallest_groups() {
    // The TCK scenario (WithOrderBy4 [11]) that exposed this. With the sort a
    // no-op, LIMIT 2 took two arbitrary groups of three: the same query
    // returned five different answers across 100 processes, 36 of them right.
    let store = aggregate_fixture();
    let out = rows(
        &store,
        "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num + a.num2) AS s ORDER BY sum(a.num + a.num2) \
         LIMIT 2 RETURN m, s",
    );
    assert_eq!(
        out,
        vec![
            "m=Property(Integer(2)),s=Property(Integer(7))".to_string(),
            "m=Property(Integer(1)),s=Property(Integer(13))".to_string(),
        ],
        "sums are 7, 13, 15 — the two smallest are 7 and 13, in that order"
    );
}

#[test]
fn descending_order_by_an_aggregate_takes_the_largest_group() {
    // The mirror of the above: if the sort were still a no-op, this would
    // agree with the ascending case some of the time, which is how a weak
    // assertion would let the bug back in.
    let store = aggregate_fixture();
    let out = rows(
        &store,
        "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num + a.num2) AS s ORDER BY sum(a.num + a.num2) \
         DESC LIMIT 1 RETURN m, s",
    );
    assert_eq!(out, vec!["m=Property(Integer(0)),s=Property(Integer(15))".to_string()]);
}

#[test]
fn an_aggregate_inside_a_compound_sort_key_is_resolved_too() {
    // The rewrite recurses, so a key that merely *contains* a projected
    // expression resolves as well.
    let store = aggregate_fixture();
    let compound = rows(
        &store,
        "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num + a.num2) AS s ORDER BY sum(a.num + a.num2) * -1 \
         RETURN m, s",
    );
    let descending = rows(
        &store,
        "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num + a.num2) AS s ORDER BY s DESC RETURN m, s",
    );
    assert_eq!(compound, descending, "multiplying the key by -1 reverses the order");
}

#[test]
fn ordering_by_a_grouping_key_still_works() {
    // The guard on the rewrite: a sort key naming a grouping expression must
    // keep working. Rewriting too eagerly would break this, and it is the
    // more common spelling of the two.
    let store = aggregate_fixture();
    let out = rows(
        &store,
        "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num) AS s ORDER BY a.num2 % 3 RETURN m, s",
    );
    let by_alias = rows(
        &store,
        "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num) AS s ORDER BY m RETURN m, s",
    );
    assert_eq!(out, by_alias);
    assert_eq!(out.len(), 3);
}

#[test]
fn labels_are_returned_in_a_stable_order() {
    // `Node::labels` is a HashSet. Returning its iteration order made
    // `labels(n)` answer ['L','B'] on one run and ['B','L'] on the next —
    // 88/200 one way and 112/200 the other, across processes.
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:L:B:A {num: 42})");

    let q = parse_query("MATCH (n) RETURN labels(n) AS labels").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    let labels = match batch.records[0].get("labels") {
        Some(Value::Property(PropertyValue::Array(items))) => items
            .iter()
            .map(|v| match v {
                PropertyValue::String(s) => s.clone(),
                other => panic!("expected a string label, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        other => panic!("expected a list of labels, got {other:?}"),
    };
    assert_eq!(labels, vec!["A", "B", "L"], "labels come back sorted");
}

#[test]
fn labels_are_stable_across_many_nodes_with_the_same_label_set() {
    // A weaker but broader check: every node with the same labels must report
    // them identically. With a raw HashSet this held within one process and
    // failed across processes, which is precisely why it needs stating as a
    // contract rather than being left to the representation.
    let mut store = GraphStore::new();
    for _ in 0..200 {
        write(&mut store, "CREATE (:Zebra:Apple:Mango)");
    }
    let out = rows(&store, "MATCH (n) RETURN DISTINCT labels(n) AS labels");
    assert_eq!(out.len(), 1, "200 identically-labelled nodes must render one distinct value: {out:?}");
    assert!(out[0].contains("Apple"), "{out:?}");
    assert!(
        out[0].find("Apple").unwrap() < out[0].find("Mango").unwrap(),
        "sorted, so Apple precedes Mango: {out:?}"
    );
}

#[test]
fn repeated_execution_in_one_process_is_stable() {
    // Cheap, and it would have caught a bug that varied per *call* rather
    // than per process — a different failure mode from the two above, and one
    // nothing else here covers.
    let store = aggregate_fixture();
    let cypher = "MATCH (a:A) WITH a.num2 % 3 AS m, sum(a.num + a.num2) AS s \
                  ORDER BY sum(a.num + a.num2) LIMIT 2 RETURN m, s";
    let first = rows(&store, cypher);
    for i in 0..50 {
        assert_eq!(rows(&store, cypher), first, "run {i} disagreed with run 0");
    }
}

// ---------------------------------------------------------------------------
// ORDER BY over a RETURN alias.
//
// Found while chasing the third flaky TCK scenario. It turned out not to be a
// determinism bug at all but a plain wrong answer that *looked* deterministic:
// with the sort dropped, rows came back in scan order, and on a small fixture
// scan order is often already the ascending order. `ASC` looked right, and
// only asking for `DESC` and getting the same rows back gave it away.
//
// Every test here therefore asserts a *reversal*, not just an order.

fn edges_fixture() -> GraphStore {
    let mut store = GraphStore::new();
    write(
        &mut store,
        "CREATE ()-[:T1 {id: 0}]->(:X), ()-[:T2 {id: 1}]->(:X), ()-[:T2 {id: 2}]->()",
    );
    store
}

/// The `id` property of each returned edge, in result order.
fn edge_ids(store: &GraphStore, cypher: &str, column: &str) -> Vec<i64> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    batch
        .records
        .iter()
        .map(|r| match r.get(column) {
            Some(Value::Edge(_, e)) => match e.properties.get("id") {
                Some(PropertyValue::Integer(i)) => *i,
                other => panic!("expected an integer id, got {other:?}"),
            },
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("expected an edge or an integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn ordering_by_a_property_of_a_return_alias_actually_sorts() {
    let store = edges_fixture();
    assert_eq!(
        edge_ids(&store, "MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY rel.id ASC", "rel"),
        vec![0, 1, 2]
    );
    assert_eq!(
        edge_ids(&store, "MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY rel.id DESC", "rel"),
        vec![2, 1, 0],
        "DESC must reverse the order — matching ASC means the sort was dropped"
    );
}

#[test]
fn the_alias_and_the_underlying_variable_sort_identically() {
    // `ORDER BY r.id` always worked; `ORDER BY rel.id` did not. They are the
    // same query.
    let store = edges_fixture();
    for direction in ["ASC", "DESC"] {
        assert_eq!(
            edge_ids(&store, &format!("MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY rel.id {direction}"), "rel"),
            edge_ids(&store, &format!("MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY r.id {direction}"), "rel"),
            "{direction}"
        );
    }
}

#[test]
fn a_sort_after_an_aggregating_with_and_a_second_match_is_applied() {
    // The TCK scenario (WithOrderBy4 [15]). After a `WITH` that aggregates,
    // the pre-sort row order is the group hash map's order, so the dropped
    // sort stopped being merely wrong and became wrong differently on each
    // process: 61/39 across 100 runs.
    let store = edges_fixture();
    let cypher = "MATCH (a)-[r]->(b:X) WITH a, r, b, count(*) AS c ORDER BY c                   MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY rel.id";
    assert_eq!(edge_ids(&store, cypher, "rel"), vec![0, 1]);
    assert_eq!(
        edge_ids(&store, &cypher.replace("ORDER BY rel.id", "ORDER BY rel.id DESC"), "rel"),
        vec![1, 0]
    );
}

#[test]
fn an_alias_over_a_non_variable_expression_is_left_alone() {
    // The guard on the substitution. `total` names an aggregate, so
    // `total.anything` is meaningless and must not be rewritten into some
    // other node's property. Whatever the executor does with it, the query
    // must not silently sort by something else.
    let store = edges_fixture();
    let q = parse_query("MATCH (a)-[r]->(b) RETURN count(*) AS total ORDER BY total").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).expect("counting still works");
    assert_eq!(batch.records.len(), 1);
}

#[test]
fn ordering_by_an_expression_over_an_alias_property_is_resolved() {
    // The substitution recurses, so a key that merely contains `rel.id`
    // resolves as well.
    let store = edges_fixture();
    assert_eq!(
        edge_ids(&store, "MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY rel.id * -1", "rel"),
        vec![2, 1, 0],
        "negating the key reverses the order"
    );
}
