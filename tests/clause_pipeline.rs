//! Clauses in the order Cypher allows, not the order the grammar assumed (#617).
//!
//! Every statement rule in the grammar encodes one permitted clause order, with
//! writes at the end. Cypher does not work that way — a write may sit before a
//! `WITH`, and two writes may be separated by a projection — so
//! `MATCH (n) SET n.x = 1 WITH n RETURN n.x` was a syntax error.
//!
//! Underneath the syntax was a worse problem, and it is what most of these
//! tests are about. Making the grammar accept the query was not enough: the
//! default `next_mut` on a pass-through operator delegates to `next`, which
//! reads its input **read-only**, so a materialising operator severed
//! mutability for everything below it. The first working version of this
//! parsed the query, planned it correctly, ran it, returned rows — and did not
//! write. The store was unchanged and nothing said so.
//!
//! So the tests here assert the **graph after**, not just the rows returned.
//! A write that reports success and does nothing is the failure mode this
//! whole change had to avoid, and it is invisible to any test that only reads
//! the result set.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:P {n: 'a', x: 0, y: 9}), (:P {n: 'b', x: 0, y: 9})");
    store
}

fn run(store: &mut GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("query should run")
        .records
        .len()
}

/// A single integer read back from the graph.
fn scalar(store: &GraphStore, cypher: &str) -> Option<i64> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    match batch.records.first().and_then(|r| r.get("v")) {
        Some(Value::Property(PropertyValue::Integer(n))) => Some(*n),
        _ => None,
    }
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store).execute(&q).expect("query should run").records.len()
}

#[test]
fn a_set_before_a_with_actually_writes() {
    // The whole point. This parsed, planned, ran and returned the *old* value
    // while leaving the store untouched.
    let mut store = fixture();
    run(&mut store, "MATCH (p:P) SET p.x = 1 WITH p RETURN p.x AS v");
    assert_eq!(scalar(&store, "MATCH (p:P {n: 'a'}) RETURN p.x AS v"), Some(1));
    assert_eq!(scalar(&store, "MATCH (p:P {n: 'b'}) RETURN p.x AS v"), Some(1));
}

#[test]
fn the_projection_after_the_write_sees_the_new_value() {
    // Separate from the above: the store can be correct while the rows the
    // caller gets are stale, and both were wrong here.
    let mut store = fixture();
    let q = parse_query("MATCH (p:P {n: 'a'}) SET p.x = 7 WITH p RETURN p.x AS v").unwrap();
    let batch = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("query should run");
    assert_eq!(
        batch.records[0].get("v"),
        Some(&Value::Property(PropertyValue::Integer(7)))
    );
}

#[test]
fn a_remove_before_a_with_actually_removes() {
    let mut store = fixture();
    run(&mut store, "MATCH (p:P) REMOVE p.y WITH p RETURN p.y AS v");
    assert_eq!(scalar(&store, "MATCH (p:P {n: 'a'}) RETURN p.y AS v"), None);
}

#[test]
fn a_delete_between_two_withs_actually_deletes() {
    let mut store = fixture();
    run(&mut store, "MATCH (p:P) WITH p DELETE p WITH 1 AS d RETURN d AS v");
    assert_eq!(count(&store, "MATCH (p:P) RETURN p"), 0);
}

#[test]
fn a_query_may_open_with_a_with() {
    // No reading clause at all — the pipeline starts from a single empty row.
    let store = GraphStore::new();
    let q = parse_query("WITH 1 AS a UNWIND [10, 20] AS b WITH a, b RETURN a + b AS v").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).expect("query should run");
    let mut got: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("v") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{other:?}"),
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![11, 21]);
}

#[test]
fn a_where_after_a_with_filters() {
    let store = GraphStore::new();
    let q = parse_query("WITH [1, 2, 3] AS xs UNWIND xs AS x WITH x WHERE x > 1 RETURN x AS v").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(batch.records.len(), 2);
}

#[test]
fn a_create_before_a_with_actually_creates() {
    let mut store = GraphStore::new();
    let rows = run(&mut store, "CREATE (a) WITH a CREATE (b) CREATE (a)<-[:T]-(b)");
    assert_eq!(rows, 0, "a data write with no RETURN returns no rows");
    assert_eq!(count(&store, "MATCH (n) RETURN n"), 2);
    assert_eq!(count(&store, "MATCH ()-[:T]->() RETURN 1 AS z"), 1);
}

#[test]
fn a_create_after_an_unwind_runs_once_per_row() {
    let mut store = GraphStore::new();
    run(&mut store, "UNWIND [1, 2, 3] AS x CREATE (n:N {num: x}) WITH n RETURN n.num AS v");
    assert_eq!(count(&store, "MATCH (n:N) RETURN n"), 3);
    assert_eq!(scalar(&store, "MATCH (n:N) WHERE n.num = 2 RETURN n.num AS v"), Some(2));
}

#[test]
fn a_create_references_variables_already_in_scope() {
    // The rule that stops the second clause making a second `a`. Getting the
    // order wrong here — adding the pattern's own variables to scope before
    // deciding what to create — makes the clause create nothing at all.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (a:A) WITH a CREATE (a)-[:R]->(:B)");
    assert_eq!(count(&store, "MATCH (n:A) RETURN n"), 1, "exactly one A");
    assert_eq!(count(&store, "MATCH (:A)-[:R]->(:B) RETURN 1 AS z"), 1);
}

#[test]
fn an_unsupported_clause_position_is_refused_not_mis_planned() {
    // FOREACH is not threaded through the pipeline yet. The parser accepts the
    // order, so the planner must say no rather than fall back to the by-kind
    // fields — which are empty for these queries, and would be read as "no
    // FOREACH at all", i.e. as a query that simply does less than it says.
    //
    // Keep this test pointed at whatever is still unsupported. It started on
    // MERGE and moved here when MERGE landed; the assertion is about the
    // refusal existing at the boundary, not about which clause is outside it.
    let err = parse_query("CREATE (a:A) WITH a FOREACH (i IN [1, 2] | SET a.n = i)")
        .expect_err("must refuse rather than silently drop the clause");
    let msg = err.to_string();
    assert!(msg.contains("FOREACH"), "the message should name the clause: {msg}");
}

#[test]
fn ordinary_queries_do_not_go_near_the_pipeline() {
    // The fallback only runs when every shape-specific rule has rejected the
    // input. If a common query started taking it, the blast radius of this
    // change would be the whole engine rather than a handful of clause orders.
    for cypher in [
        "MATCH (n) RETURN n",
        "MATCH (a)-[r]->(b) WHERE a.x = 1 RETURN a, r, b",
        "CREATE (n:P {x: 1}) RETURN n",
        "MATCH (n:P) SET n.x = 2",
        "UNWIND [1, 2] AS x RETURN x",
        "MERGE (n:P {x: 1})",
        "MATCH (n) DETACH DELETE n",
    ] {
        let q = parse_query(cypher).expect("should parse");
        assert!(
            !q.needs_clause_pipeline,
            "{cypher} was routed through the clause pipeline"
        );
    }
}

#[test]
fn the_clause_list_records_written_order() {
    let q = parse_query("MATCH (p:P) SET p.x = 1 WITH p RETURN p.x AS v").unwrap();
    let kinds: Vec<&str> = q.clauses.iter().map(|c| c.kind()).collect();
    assert_eq!(kinds, vec!["MATCH", "SET", "WITH", "RETURN"]);
}

#[test]
fn order_by_a_column_the_return_does_not_carry_still_sorts() {
    // `WITH p, count(q) AS rng RETURN p ORDER BY rng` sorts on a column the
    // projection drops. Placing the sort *above* the projection leaves the key
    // unbound, the sort silently becomes a no-op, and the rows come back in
    // whatever order the barrier produced — which is hash order, so the answer
    // differs between processes.
    //
    // `CH-DETERM` caught exactly this after the pipeline first landed: one
    // scenario flipping across five runs. The assertion is on the order, and
    // the fixture is built so the sorted order differs from the natural one.
    let store = GraphStore::new();
    let cypher = "WITH [0, 1] AS prows, [[2], [3, 4]] AS qrows \
                  UNWIND prows AS p UNWIND qrows[p] AS q \
                  WITH p, count(q) AS rng RETURN p AS v ORDER BY rng DESC";
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(&store).execute(&q).expect("query should run");
    let got: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("v") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{other:?}"),
        })
        .collect();
    // rng is 1 for p=0 and 2 for p=1, so DESC puts p=1 first — the reverse of
    // the order the rows are produced in. Ascending would pass by accident.
    assert_eq!(got, vec![1, 0]);
}

#[test]
fn repeated_runs_of_a_pipeline_query_agree() {
    // A cheap guard on the class CH-DETERM found. It cannot vary the hash seed
    // within one process, so it would not have caught the original defect —
    // but it costs nothing and catches a per-call variation.
    let store = GraphStore::new();
    let cypher = "WITH [3, 1, 2] AS xs UNWIND xs AS x WITH x RETURN x AS v ORDER BY x";
    let q = parse_query(cypher).expect("query should parse");
    let first: Vec<String> = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap()
        .records
        .iter()
        .map(|r| format!("{:?}", r.get("v")))
        .collect();
    for _ in 0..20 {
        let again: Vec<String> = QueryExecutor::new(&store)
            .execute(&q)
            .unwrap()
            .records
            .iter()
            .map(|r| format!("{:?}", r.get("v")))
            .collect();
        assert_eq!(again, first);
    }
    assert_eq!(first.len(), 3);
}

#[test]
fn both_planning_paths_agree_on_an_expression_valued_merge_property() {
    // The two queries express the same thing: MERGE a node whose key comes
    // from the row rather than from a literal. The first goes through the
    // established planner, the second through the clause pipeline.
    //
    // This test began life asserting they both *refused* it. They did, and the
    // point was that only one of them refusing would be the dangerous
    // outcome — the pipeline read none of the by-kind clause fields the guard
    // inspected, so it planned a MERGE on the label alone and
    // `UNWIND ['a','b','a'] AS x MERGE (n:N {v: x})` created a single node and
    // reported success. #642 made the query answerable, so the assertion flips
    // to what it was always really about: the two paths must do the same
    // thing, and that thing must now be correct.
    let mut legacy_store = GraphStore::new();
    run(&mut legacy_store, "CREATE (:Src {k: 'a'}), (:Src {k: 'b'}), (:Src {k: 'a'})");
    run(&mut legacy_store, "MATCH (a:Src) MERGE (n:N {v: a.k})");

    let mut pipeline_store = GraphStore::new();
    run(
        &mut pipeline_store,
        "UNWIND ['a', 'b', 'a'] AS x MERGE (n:N {v: x}) WITH n RETURN n.v AS v",
    );

    for (label, store) in [("legacy", &legacy_store), ("pipeline", &pipeline_store)] {
        assert_eq!(
            count(store, "MATCH (n:N) RETURN n"),
            2,
            "{label}: one node per distinct key, not one per row and not one overall"
        );
        assert_eq!(count(store, "MATCH (n:N {v: 'a'}) RETURN n"), 1, "{label}");
        assert_eq!(count(store, "MATCH (n:N {v: 'b'}) RETURN n"), 1, "{label}");
    }
}

#[test]
fn merge_below_a_barrier_runs_once_per_input_row() {
    // MERGE in the pipeline takes an input operator, so it runs per row rather
    // than once. Two rows, distinct literals, must leave two nodes; a third row
    // repeating the first must not add a third.
    let mut store = GraphStore::new();
    let q = parse_query("UNWIND [1, 2, 1] AS x MERGE (n:N) WITH n RETURN 1 AS r")
        .expect("query should parse");
    let out = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("merge below a barrier should execute");
    assert_eq!(out.records.len(), 3, "one row in, one row out");
    assert_eq!(
        count(&store, "MATCH (n:N) RETURN n"),
        1,
        "MERGE is match-or-create: the second and third rows find the first node"
    );
}

/// Rows of `(x, y)` from a two-column query, sorted so the assertion does not
/// depend on scan order.
fn pairs(store: &mut GraphStore, cypher: &str) -> Vec<(String, String)> {
    let q = parse_query(cypher).expect("query should parse");
    let out = MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should execute: {e}"));
    let mut rows: Vec<(String, String)> = out
        .records
        .iter()
        .map(|r| (format!("{:?}", r.get("x")), format!("{:?}", r.get("y"))))
        .collect();
    rows.sort();
    rows
}

fn two_a_two_b() -> GraphStore {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A {k: 1}), (:A {k: 2}), (:B {k: 1}), (:B {k: 9})");
    run(&mut store, "MATCH (a:A {k: 1}), (b:B {k: 1}) CREATE (a)-[:R]->(b)");
    store
}

#[test]
fn a_match_after_a_with_sharing_no_variable_is_a_cartesian_product() {
    let mut store = two_a_two_b();
    let rows = pairs(&mut store, "MATCH (a:A) WITH a MATCH (b:B) RETURN a.k AS x, b.k AS y");
    assert_eq!(rows.len(), 4, "two As against two Bs, uncorrelated: {rows:?}");
}

#[test]
fn a_match_after_a_with_sharing_a_variable_stays_correlated() {
    // The failure this guards against is the previous test's answer showing up
    // here: joining on nothing turns a correlated MATCH into a cross product,
    // which is a wrong answer rather than an error. Only one A has an :R edge.
    let mut store = two_a_two_b();
    let rows = pairs(
        &mut store,
        "MATCH (a:A) WITH a MATCH (a)-[:R]->(b:B) RETURN a.k AS x, b.k AS y",
    );
    assert_eq!(rows.len(), 1, "only a.k = 1 has an outgoing :R: {rows:?}");
    assert!(rows[0].0.contains('1') && rows[0].1.contains('1'));
}

#[test]
fn an_optional_match_after_a_with_keeps_the_unmatched_row() {
    let mut store = two_a_two_b();
    let rows = pairs(
        &mut store,
        "MATCH (a:A) WITH a OPTIONAL MATCH (a)-[:R]->(b:B) RETURN a.k AS x, b.k AS y",
    );
    assert_eq!(rows.len(), 2, "both As survive: {rows:?}");
    assert_eq!(
        rows.iter().filter(|(_, y)| y.contains("Null")).count(),
        1,
        "the A with no edge keeps a null b: {rows:?}"
    );
}

#[test]
fn a_match_can_read_what_an_earlier_clause_created() {
    // The join operators materialise both sides, and materialising the left one
    // read-only makes any write below it refuse outright — this query returned
    // "requires mutable store access" rather than rows. The write has to run
    // before the join reads.
    let mut store = two_a_two_b();
    let rows = pairs(
        &mut store,
        "CREATE (n:C {k: 7}) WITH n MATCH (a:A) RETURN a.k AS x, n.k AS y",
    );
    assert_eq!(rows.len(), 2, "one row per A: {rows:?}");
    assert!(rows.iter().all(|(_, y)| y.contains('7')), "{rows:?}");
    assert_eq!(count(&store, "MATCH (n:C) RETURN n"), 1, "exactly one C, created once");
}

#[test]
fn a_match_after_an_unwind_filters_per_row() {
    let mut store = two_a_two_b();
    let rows = pairs(
        &mut store,
        "UNWIND [1, 2] AS k MATCH (a:A) WHERE a.k = k RETURN k AS x, a.k AS y",
    );
    assert_eq!(rows.len(), 2, "each k finds its own A, not both: {rows:?}");
    assert!(rows.iter().all(|(x, y)| x == y), "{rows:?}");
}
