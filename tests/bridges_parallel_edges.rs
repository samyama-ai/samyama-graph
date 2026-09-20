//! A pair of parallel edges is not a bridge (#1308).
//!
//! `algo.bridges` reported `a = b` — two parallel `LINK` edges — as a bridge.
//! Removing either one leaves the graph connected, so it is not one. The search
//! was skipping *every* edge back to the DFS parent on node identity, rather
//! than the single edge it arrived on.
//!
//! **The obvious fix breaks the commoner case**, which is why this sat open.
//! The symmetrised neighbour list puts the parent in twice for two different
//! reasons: two parallel `a -> b` edges (two undirected edges) and a reciprocal
//! `a -> b`, `b -> a` pair (**one** undirected edge, and genuinely a bridge).
//! Skipping the parent exactly once fixes the first and breaks the second.
//!
//! So the convention comes first, and it is now written down in
//! `docs/ALGORITHM-CONVENTIONS.md`: **a reciprocal pair is one undirected
//! edge**, so the multiplicity between two nodes is `max(forward, reverse)`.
//! That is what the traversal already did for reciprocal pairs, so nothing
//! else moves — the tests below pin both halves, because a change that fixed
//! the parallel case by breaking `x <-> y` would pass a test for either one
//! alone.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn graph(statements: &[&str]) -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in statements {
        engine
            .execute_mut(q, &mut store, "default")
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    store
}

fn bridge_count(store: &mut GraphStore) -> usize {
    QueryEngine::new()
        .execute_mut(
            "CALL algo.bridges() YIELD source, target RETURN source, target",
            store,
            "default",
        )
        .expect("algo.bridges")
        .records
        .len()
}

fn articulation_count(store: &mut GraphStore) -> usize {
    QueryEngine::new()
        .execute_mut(
            "CALL algo.articulationPoints() YIELD node RETURN node",
            store,
            "default",
        )
        .expect("algo.articulationPoints")
        .records
        .len()
}

/// `a = b` (two parallel edges) and `b -> c`.
fn parallel_plus_tail() -> GraphStore {
    graph(&[
        "CREATE (:N {n: 'a'}), (:N {n: 'b'}), (:N {n: 'c'})",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (b:N {n: 'b'}), (c:N {n: 'c'}) CREATE (b)-[:LINK]->(c)",
    ])
}

#[test]
fn parallel_edges_are_not_a_bridge() {
    // The defect, measured: this reported two bridges, `(a,b)` and `(b,c)`.
    // Only `(b,c)` is one — cutting either `a` to `b` edge leaves the graph
    // connected.
    let mut store = parallel_plus_tail();
    assert_eq!(
        bridge_count(&mut store),
        1,
        "a = b is not a bridge; only b -> c is"
    );
}

#[test]
fn the_genuine_bridge_beside_them_is_still_found() {
    // The half that stops the fix being "report nothing". A traversal that
    // skipped the parent every time and also lost `(b,c)` would pass the test
    // above on its own.
    //
    // The ids are fetched separately rather than joined in one statement:
    // `CALL ... YIELD ... MATCH` is not supported yet (samyama-graph#617).
    let mut store = parallel_plus_tail();
    let engine = QueryEngine::new();
    let id_of = |store: &mut GraphStore, name: &str| -> i64 {
        let b = engine
            .execute_mut(
                &format!("MATCH (n:N {{n: '{name}'}}) RETURN id(n) AS id"),
                store,
                "default",
            )
            .expect("id");
        match &b.records[0].bindings()[0].1 {
            samyama::query::executor::record::Value::Property(
                samyama::graph::PropertyValue::Integer(i),
            ) => *i,
            other => panic!("id came back as {other:?}"),
        }
    };
    let b_id = id_of(&mut store, "b");
    let c_id = id_of(&mut store, "c");

    let batch = engine
        .execute_mut(
            "CALL algo.bridges() YIELD source, target RETURN source, target",
            &mut store,
            "default",
        )
        .expect("bridges");
    let pairs: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            r.bindings()
                .iter()
                .map(|(_, v)| format!("{v:?}"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    // `algo.bridges` yields nodes, not ids, so the match is on `NodeId(n)`.
    let wanted = format!("NodeId({b_id})");
    let wanted_c = format!("NodeId({c_id})");
    assert!(
        pairs
            .iter()
            .any(|p| p.contains(&wanted) && p.contains(&wanted_c)),
        "b -> c is a bridge and was not reported: {pairs:?}"
    );
}

#[test]
fn a_reciprocal_pair_is_still_a_bridge() {
    // The case the textbook fix would have broken, and the commoner shape in
    // our data. `x -> y` and `y -> x` is one undirected edge; cutting it
    // disconnects the graph.
    let mut store = graph(&[
        "CREATE (:N {n: 'x'}), (:N {n: 'y'})",
        "MATCH (x:N {n: 'x'}), (y:N {n: 'y'}) CREATE (x)-[:LINK]->(y)",
        "MATCH (x:N {n: 'x'}), (y:N {n: 'y'}) CREATE (y)-[:LINK]->(x)",
    ]);
    assert_eq!(bridge_count(&mut store), 1);
}

#[test]
fn three_parallel_edges_are_not_a_bridge_either() {
    // `max(forward, reverse)` has to carry the count, not just "more than one".
    let mut store = graph(&[
        "CREATE (:N {n: 'a'}), (:N {n: 'b'})",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
    ]);
    assert_eq!(bridge_count(&mut store), 0);
}

#[test]
fn a_reciprocal_pair_plus_a_third_edge_is_not_a_bridge() {
    // forward 2, reverse 1: one reciprocal pair collapses and the leftover
    // forward edge stands on its own, so two undirected edges and no bridge.
    // This is the case that separates `max(f, r)` from "reciprocal means one".
    let mut store = graph(&[
        "CREATE (:N {n: 'a'}), (:N {n: 'b'})",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (b)-[:LINK]->(a)",
    ]);
    assert_eq!(bridge_count(&mut store), 0);
}

#[test]
fn a_path_still_has_two_bridges() {
    let mut store = graph(&[
        "CREATE (:N {n: 'a'}), (:N {n: 'b'}), (:N {n: 'c'})",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (b:N {n: 'b'}), (c:N {n: 'c'}) CREATE (b)-[:LINK]->(c)",
    ]);
    assert_eq!(bridge_count(&mut store), 2);
}

#[test]
fn a_triangle_has_none() {
    let mut store = graph(&[
        "CREATE (:N {n: 'p'}), (:N {n: 'q'}), (:N {n: 'r'})",
        "MATCH (p:N {n: 'p'}), (q:N {n: 'q'}) CREATE (p)-[:LINK]->(q)",
        "MATCH (q:N {n: 'q'}), (r:N {n: 'r'}) CREATE (q)-[:LINK]->(r)",
        "MATCH (r:N {n: 'r'}), (p:N {n: 'p'}) CREATE (r)-[:LINK]->(p)",
    ]);
    assert_eq!(bridge_count(&mut store), 0);
}

#[test]
fn articulation_points_share_the_traversal_and_the_fix() {
    // `b` is a cut vertex — removing it strands `c` — and `a` is not, whatever
    // the multiplicity of the edges to it. The defect was in the shared
    // traversal, so this moves with `bridges` and is pinned with it.
    let mut store = parallel_plus_tail();
    assert_eq!(articulation_count(&mut store), 1, "only b is a cut vertex");
}

#[test]
fn two_parallel_edges_leave_no_cut_vertex() {
    let mut store = graph(&[
        "CREATE (:N {n: 'a'}), (:N {n: 'b'})",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (a:N {n: 'a'}), (b:N {n: 'b'}) CREATE (a)-[:LINK]->(b)",
    ]);
    assert_eq!(articulation_count(&mut store), 0);
}
