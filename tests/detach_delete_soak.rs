//! `DETACH DELETE` then reload, repeatedly, leaves no dangling edge (#1143).
//!
//! #1143 reports a long-lived instance found holding **813 edges whose endpoints
//! resolve to null**, plus edges referencing node ids from fixtures loaded much
//! earlier, while the node side was clean. Every query against it was silently
//! wrong — that is how it surfaced, the engine answering 1 row where it should
//! have answered 3.
//!
//! The issue is filed with a negative result: four reproduction attempts came
//! back clean, including a 60-round soak, so `DETACH DELETE` resets correctly in
//! isolation and "it deletes nodes but not edges" is **wrong**. The remaining
//! suspicion is a write interrupted by a fault, which a soak cannot trigger on
//! purpose.
//!
//! What the issue asks for to make it closeable either way is this: run the soak
//! **with the invariant asserted**. `check_integrity` and `CALL
//! db.checkIntegrity()` arrived in #1146, so that is now possible, and this is
//! that run — pinned so the answer does not have to be re-derived by hand next
//! time the symptom appears.
//!
//! It cannot prove the absence of the reported bug. It converts "we could not
//! reproduce it by hand once" into a check that runs on every commit, which is
//! the difference between a memory and a guarantee.
//!
//! `SAMYAMA_SOAK_ROUNDS` raises the round count for a longer run by hand; the
//! default is sized for CI.

use samyama::graph::GraphStore;
use samyama::query::{MutQueryExecutor, QueryEngine, parse_query};

const T: &str = "default";

/// Six fixtures, deliberately varied the way #1143's harness varied them:
/// different node ids between rounds, so an edge surviving a reset points at an
/// id the next fixture does not define — which is exactly the reported symptom.
const FIXTURES: &[&[(&str, &str)]] = &[
    &[("n", "m")],
    &[("a3", "a4"), ("a4", "a3")],
    &[("a5", "a6"), ("a6", "a5"), ("a5", "a5")],
    &[("p", "q"), ("q", "r"), ("r", "p")],
    &[("x", "y")],
    &[("s1", "s2"), ("s2", "s3"), ("s3", "s4"), ("s4", "s1")],
];

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse {cypher}: {e}"));
    let mut ex = MutQueryExecutor::new(store, T.to_string());
    ex.execute(&q).unwrap_or_else(|e| panic!("execute {cypher}: {e}"));
}

fn load(store: &mut GraphStore, edges: &[(&str, &str)]) {
    let mut ids: Vec<&str> = Vec::new();
    for (a, b) in edges {
        for n in [a, b] {
            if !ids.contains(n) {
                ids.push(n);
            }
        }
    }
    for id in &ids {
        run(store, &format!("CREATE (:N {{eid: \"{id}\"}})"));
    }
    for (a, b) in edges {
        run(
            store,
            &format!(
                "MATCH (a:N), (b:N) WHERE a.eid = \"{a}\" AND b.eid = \"{b}\" CREATE (a)-[:E]->(b)"
            ),
        );
    }
}

#[test]
fn cycling_fixtures_through_detach_delete_never_leaves_a_dangling_edge() {
    let rounds: usize = std::env::var("SAMYAMA_SOAK_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);

    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    for round in 0..rounds {
        let fixture = FIXTURES[round % FIXTURES.len()];
        load(&mut store, fixture);

        // The invariant, after the write batch rather than only at the end: a
        // store that goes bad in round 7 and is reset in round 8 would look
        // clean to an end-of-run check.
        let violations = store.check_integrity();
        assert!(
            violations.is_empty(),
            "round {round} left {} dangling edge(s) after loading fixture {}: {violations:?}",
            violations.len(),
            round % FIXTURES.len()
        );

        // Every edge the fixture asked for is present and traversable. Without
        // this the test passes on a store that silently dropped the whole
        // fixture, which is the failure #1143 actually reports -- a wrong row
        // count, not an error.
        let got = engine
            .execute("MATCH (x:N)-[:E]->(y:N) RETURN x.eid, y.eid", &store)
            .expect("traverse")
            .records
            .len();
        assert_eq!(
            got,
            fixture.len(),
            "round {round}: {} edges loaded, {got} traversable",
            fixture.len()
        );

        run(&mut store, "MATCH (n) DETACH DELETE n");

        // And the reset actually reset. An edge surviving here is #1143's shape:
        // the next fixture defines different ids, so its endpoints resolve to
        // null and every later answer is quietly wrong.
        let violations = store.check_integrity();
        assert!(
            violations.is_empty(),
            "round {round} left {} dangling edge(s) after DETACH DELETE: {violations:?}",
            violations.len()
        );
        let left = engine
            .execute("MATCH (x)-[r]->(y) RETURN x, y", &store)
            .expect("post-reset scan")
            .records
            .len();
        assert_eq!(left, 0, "round {round}: DETACH DELETE left {left} edge(s) behind");
    }
}
