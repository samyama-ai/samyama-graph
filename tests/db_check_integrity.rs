//! `CALL db.checkIntegrity()` — every edge references two existing nodes (#1143).
//!
//! Filed after an instance was found holding 813 edges whose endpoints resolved to
//! null, plus edges referencing node ids from fixtures loaded much earlier, while
//! its node side was clean. Every query against it was silently wrong, and it
//! surfaced only as a row count nobody could explain.
//!
//! **The cause was never reproduced**, and this does not claim to fix it. Four
//! attempts including a 60-round soak came back clean, so "DETACH DELETE deletes
//! nodes but not edges" is wrong. What is here is a detector: it turns a silent
//! wrong answer into a loud one, and makes the original report closeable either way
//! — run the soak with the check on.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

const T: &str = "default";

fn violations(store: &GraphStore) -> Vec<String> {
    QueryEngine::new()
        .execute("CALL db.checkIntegrity()", store)
        .expect("db.checkIntegrity")
        .records
        .iter()
        .map(|r| format!("{:?}", r.get("detail")))
        .collect()
}

/// A sound store reports nothing.
///
/// The important half of the pair: a check that fires on a healthy graph is one
/// people turn off.
#[test]
fn a_sound_store_reports_no_violations() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "CREATE (a:N {eid: \"a\"})-[:E]->(b:N {eid: \"b\"})-[:E]->(c:N {eid: \"c\"})",
            &mut store,
            T,
        )
        .unwrap();
    assert!(violations(&store).is_empty());
    assert!(store.check_integrity().is_empty());
}

/// No public API can create the reported state, which is itself evidence.
///
/// `insert_recovered_edge` — the lowest-level way in, used by crash recovery —
/// validates that both endpoints exist and refuses otherwise. `DETACH DELETE`
/// removes the edges. So a dangling edge cannot be produced by any sequence of
/// legal operations, which is consistent with the report's own guess that a fault
/// interrupted a write, and is why four reproduction attempts from the query
/// language came back clean.
///
/// The detector is unit-tested against a hand-built violation in
/// `graph::store::tests`, where the fields are reachable; exposing a
/// dangling-edge constructor here would be adding the hazard to the public API in
/// order to test for it.
#[test]
fn a_dangling_edge_cannot_be_created_through_a_public_api() {
    use samyama::graph::{Edge, EdgeId, NodeId};

    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine.execute_mut("CREATE (a:N {eid: \"a\"})", &mut store, T).unwrap();
    let a = store.get_nodes_by_label(&"N".into())[0].id;

    let missing = NodeId::new(9_999);
    assert!(store.get_node(missing).is_none());
    let edge = Edge::new(EdgeId::new(500), a, missing, samyama::graph::EdgeType::new("E"));
    assert!(
        store.insert_recovered_edge(edge).is_err(),
        "recovery must refuse an edge whose endpoint does not exist; accepting it \
         is how the state in #1143 would be reachable"
    );
    assert!(violations(&store).is_empty());
}

/// The soak the report asks for, with the check on.
///
/// Six fixtures with different node ids, cycled, reset with `DETACH DELETE` between
/// rounds — the harness shape from the report. Clean here, as it was there, which is
/// the evidence that `DETACH DELETE` and reload is not the cause.
///
/// Kept short enough for CI. The original 60-round soak was also clean; running 60
/// here would add minutes and no information.
#[test]
fn detach_delete_and_reload_stays_sound_across_rounds() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    let fixtures = [
        ("n", "m"),
        ("a3", "a4"),
        ("a5", "a6"),
        ("x", "y"),
        ("p", "q"),
        ("u", "v"),
    ];

    for round in 0..12 {
        let (a, b) = fixtures[round % fixtures.len()];
        engine
            .execute_mut(
                &format!(
                    "CREATE (x:N {{eid: \"{a}\"}}), (y:N {{eid: \"{b}\"}})"
                ),
                &mut store,
                T,
            )
            .unwrap();
        engine
            .execute_mut(
                &format!(
                    "MATCH (x:N {{eid: \"{a}\"}}), (y:N {{eid: \"{b}\"}}) CREATE (x)-[:E]->(y)"
                ),
                &mut store,
                T,
            )
            .unwrap();

        let found = violations(&store);
        assert!(found.is_empty(), "round {round} after load: {found:?}");

        engine.execute_mut("MATCH (n) DETACH DELETE n", &mut store, T).unwrap();

        let found = violations(&store);
        assert!(found.is_empty(), "round {round} after DETACH DELETE: {found:?}");
        assert_eq!(
            store.edge_count(),
            0,
            "round {round}: DETACH DELETE left {} edges behind — the shape the \
             original report describes",
            store.edge_count()
        );
    }
}
