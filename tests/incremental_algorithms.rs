//! `incremental: true` on `algo.pageRank`, `algo.wcc` and `algo.degree`, driven
//! through Cypher (ALGO-14, #1697).
//!
//! The promise under test: whatever was written between two calls -- created,
//! deleted, relabelled, rolled back -- an incremental call answers what a
//! plain call on the same graph answers. Degree exactly; WCC the same
//! partition; PageRank within the bound the algorithms crate states. Whether a
//! call updated or rebuilt is read from `GraphStore::incremental_states`, so
//! the tests also show the update path is the one taken when it should be.

use std::collections::HashMap;

use samyama::algo::incremental::{Algorithm, RebuildReason, Refresh};
use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

const TENANT: &str = "default";

/// The bound `samyama_graph_algorithms::incremental` documents for a graph
/// with no dangling nodes, at the default damping of 0.85.
fn pagerank_bound() -> f64 {
    2.0 * samyama::algo::RESIDUAL_TOLERANCE / (1.0 - 0.85)
}

fn write(store: &mut GraphStore, q: &str) {
    QueryEngine::new().execute_mut(q, store, TENANT).unwrap_or_else(|e| panic!("{q}: {e}"));
}

/// `id -> value` for a `RETURN id(node) AS id, <col> AS v` query.
fn by_id(store: &GraphStore, q: &str) -> HashMap<i64, f64> {
    let batch = QueryEngine::new().execute(q, store).unwrap_or_else(|e| panic!("{q}: {e}"));
    batch
        .records
        .iter()
        .map(|r| {
            let id = r.get("id").and_then(|v| v.as_property()).and_then(|p| p.as_integer()).unwrap();
            let v = r.get("v").and_then(|v| v.as_property()).unwrap();
            let v = v.as_float().or_else(|| v.as_integer().map(|i| i as f64)).unwrap();
            (id, v)
        })
        .collect()
}

fn call(algorithm: &str, column: &str, config: &str) -> String {
    format!("CALL {algorithm}({config}) YIELD node, {column} RETURN id(node) AS id, {column} AS v")
}

fn degree(store: &GraphStore, incremental: bool) -> HashMap<i64, f64> {
    by_id(store, &call("algo.degree", "score", &format!("{{incremental: {incremental}}}")))
}

fn components(store: &GraphStore, incremental: bool) -> HashMap<i64, f64> {
    by_id(store, &call("algo.wcc", "componentId", &format!("{{incremental: {incremental}}}")))
}

fn pagerank_incremental(store: &GraphStore) -> HashMap<i64, f64> {
    by_id(store, &call("algo.pageRank", "score", "{incremental: true}"))
}

/// The plain call, iterated to its fixed point so it is the thing the
/// incremental scores approximate rather than a 20-iteration approximation.
fn pagerank_converged(store: &GraphStore) -> HashMap<i64, f64> {
    by_id(store, &call("algo.pageRank", "score", "{iterations: 1000}"))
}

fn l1(a: &HashMap<i64, f64>, b: &HashMap<i64, f64>) -> f64 {
    assert_eq!(a.len(), b.len(), "different node sets");
    a.iter().map(|(k, v)| (v - b[k]).abs()).sum()
}

/// Two nodes share a component id in one answer exactly when they do in the
/// other. The ids themselves may differ after a merge -- the module says so.
fn same_partition(a: &HashMap<i64, f64>, b: &HashMap<i64, f64>) -> bool {
    let mut fwd = HashMap::new();
    let mut back = HashMap::new();
    a.len() == b.len()
        && a.iter().all(|(id, &ca)| {
            let Some(&cb) = b.get(id) else { return false };
            *fwd.entry(ca.to_bits()).or_insert(cb.to_bits()) == cb.to_bits()
                && *back.entry(cb.to_bits()).or_insert(ca.to_bits()) == ca.to_bits()
        })
}

fn last(store: &GraphStore, algorithm: Algorithm) -> Refresh {
    store
        .incremental_states()
        .status()
        .into_iter()
        .find(|s| s.algorithm == algorithm && s.label.is_none() && s.edge_type.is_none())
        .unwrap_or_else(|| panic!("no unfiltered {algorithm:?} state"))
        .last
}

/// A ring of `n` `:P` nodes, so no node is dangling.
fn ring(n: usize) -> GraphStore {
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..n).map(|_| s.create_node("P")).collect();
    for i in 0..n {
        s.create_edge(ids[i], ids[(i + 1) % n], "L").unwrap();
    }
    s
}

#[test]
fn created_relationships_are_applied_as_deltas_and_match_a_plain_call() {
    let mut s = ring(40);
    assert_eq!(degree(&s, true), degree(&s, false));
    pagerank_incremental(&s);
    components(&s, true);

    write(&mut s, "MATCH (a:P), (b:P) WHERE id(a) = 3 AND id(b) = 25 CREATE (a)-[:L]->(b)");
    write(&mut s, "MATCH (a:P) WHERE id(a) = 7 CREATE (a)-[:L]->(:P)-[:L]->(a)");

    assert_eq!(degree(&s, true), degree(&s, false));
    assert_eq!(last(&s, Algorithm::Degree), Refresh::Updated { nodes: 1, relationships: 3 });
    let err = l1(&pagerank_incremental(&s), &pagerank_converged(&s));
    assert!(err <= pagerank_bound(), "PageRank L1 {err} above {}", pagerank_bound());
    assert_eq!(last(&s, Algorithm::PageRank), Refresh::Updated { nodes: 1, relationships: 3 });
    assert!(same_partition(&components(&s, true), &components(&s, false)));
    assert_eq!(last(&s, Algorithm::Wcc), Refresh::Updated { nodes: 1, relationships: 3 });
}

#[test]
fn separate_components_merge_when_joined() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:P)-[:L]->(:P), (:P)-[:L]->(:P), (:P)");
    let before = components(&s, true);
    let distinct = |c: &HashMap<i64, f64>| c.values().map(|v| v.to_bits()).collect::<std::collections::HashSet<_>>().len();
    assert_eq!(distinct(&before), 3);
    write(&mut s, "MATCH (a:P), (b:P) WHERE id(a) = 1 AND id(b) = 3 CREATE (a)-[:L]->(b)");
    let after = components(&s, true);
    assert_eq!(distinct(&after), 2);
    assert!(same_partition(&after, &components(&s, false)));
    assert!(matches!(last(&s, Algorithm::Wcc), Refresh::Updated { relationships: 1, .. }));
}

#[test]
fn deletes_rebuild_and_still_match_a_plain_call() {
    let mut s = ring(30);
    degree(&s, true);
    components(&s, true);
    pagerank_incremental(&s);

    write(&mut s, "MATCH (a:P)-[r:L]->(b:P) WHERE id(a) = 4 DELETE r");
    assert_eq!(degree(&s, true), degree(&s, false));
    assert_eq!(last(&s, Algorithm::Degree), Refresh::Rebuilt { reason: RebuildReason::Rewritten });

    write(&mut s, "MATCH (a:P) WHERE id(a) = 12 DETACH DELETE a");
    assert_eq!(degree(&s, true), degree(&s, false));
    assert!(same_partition(&components(&s, true), &components(&s, false)));
    assert_eq!(last(&s, Algorithm::Wcc), Refresh::Rebuilt { reason: RebuildReason::Rewritten });
    // Two cuts in a ring leave dangling ends, which the unfiltered bound does
    // not cover; the batch run is the reference at its fixed point either way.
    let err = l1(&pagerank_incremental(&s), &pagerank_converged(&s));
    assert!(err < 1e-2, "PageRank L1 {err} after deletes");
}

#[test]
fn a_label_filtered_call_follows_labels_set_and_removed() {
    let mut s = ring(12);
    let q = |s: &GraphStore| by_id(s, &call("algo.degree", "score", "{label: 'Q', incremental: true}"));
    let plain = |s: &GraphStore| by_id(s, &call("algo.degree", "score", "{label: 'Q'}"));
    write(&mut s, "MATCH (n:P) WHERE id(n) < 4 SET n:Q");
    assert_eq!(q(&s), plain(&s));
    write(&mut s, "MATCH (n:P) WHERE id(n) = 4 SET n:Q");
    assert_eq!(q(&s), plain(&s));
    write(&mut s, "MATCH (n:Q) WHERE id(n) = 1 REMOVE n:Q");
    assert_eq!(q(&s), plain(&s));
    write(&mut s, "CREATE (:Q)");
    assert_eq!(q(&s), plain(&s));
    let labelled = QueryEngine::new().execute("MATCH (n:Q) RETURN n", &s).unwrap().records.len();
    assert_eq!(q(&s).len(), labelled);
}

#[test]
fn a_rolled_back_transaction_leaves_the_answer_a_plain_call_gives() {
    let mut s = ring(10);
    degree(&s, true);
    s.begin_session_transaction().unwrap();
    write(&mut s, "MATCH (a:P), (b:P) WHERE id(a) = 0 AND id(b) = 5 CREATE (a)-[:L]->(b)");
    write(&mut s, "MATCH (a:P)-[r:L]->(b:P) WHERE id(a) = 2 DELETE r");
    write(&mut s, "CREATE (:P)");
    assert_eq!(degree(&s, true), degree(&s, false), "inside the transaction");
    s.rollback_session_transaction().unwrap();
    assert_eq!(degree(&s, true), degree(&s, false), "after the rollback");
    assert_eq!(degree(&s, true).len(), 10);
}

/// Every write a Cypher statement can make, in a random order, with the
/// incremental answers checked against plain ones after each. A write path
/// that skipped the structure log would show up here as a mismatch -- unless
/// the count check caught it, in which case it shows up as a `CountMismatch`
/// rebuild, which this also refuses.
#[test]
fn a_random_mix_of_writes_never_leaves_an_incremental_answer_wrong() {
    let mut s = ring(25);
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = move |m: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % m
    };
    // The first incremental calls build; every call after is an update or a
    // rebuild, which is what the loop checks.
    degree(&s, true);
    components(&s, true);
    let (mut updates, mut rebuilds) = (0, 0);
    for step in 0..60 {
        let ids: Vec<i64> = degree(&s, false).keys().copied().collect();
        let pick = |r: u64| ids[r as usize % ids.len()];
        let q = match next(10) {
            0..=4 => format!(
                "MATCH (a:P), (b:P) WHERE id(a) = {} AND id(b) = {} CREATE (a)-[:L]->(b)",
                pick(next(1000)),
                pick(next(1000))
            ),
            5 | 6 => format!("MATCH (a:P) WHERE id(a) = {} CREATE (a)-[:L]->(:P)", pick(next(1000))),
            7 => "CREATE (:P)".to_string(),
            8 => format!("MATCH (a:P)-[r]->() WHERE id(a) = {} DELETE r", pick(next(1000))),
            _ if ids.len() > 5 => format!("MATCH (a:P) WHERE id(a) = {} DETACH DELETE a", pick(next(1000))),
            _ => "CREATE (:P)".to_string(),
        };
        write(&mut s, &q);
        assert_eq!(degree(&s, true), degree(&s, false), "step {step}: {q}");
        assert!(same_partition(&components(&s, true), &components(&s, false)), "step {step}: {q}");
        for algorithm in [Algorithm::Degree, Algorithm::Wcc] {
            match last(&s, algorithm) {
                Refresh::Updated { .. } => updates += 1,
                Refresh::Rebuilt { reason: RebuildReason::Rewritten } => rebuilds += 1,
                other => panic!("step {step}: {q}: {algorithm:?} {other:?}"),
            }
        }
    }
    // Both paths were exercised, so the comparison above covered both.
    assert!(updates > 0 && rebuilds > 0, "updates {updates}, rebuilds {rebuilds}");
}

#[test]
fn bad_arguments_are_refused_with_the_reason() {
    let s = ring(4);
    let refused = |q: &str| QueryEngine::new().execute(q, &s).err().map(|e| e.to_string()).unwrap_or_default();
    let cases = [
        ("CALL algo.pageRank({incremental: 'yes'}) YIELD node RETURN node", "must be true or false"),
        ("CALL algo.pageRank({incremental: true, iterations: 5}) YIELD node RETURN node", "`iterations` has no meaning"),
        ("CALL algo.pageRank({incremental: true, damping: 1.0}) YIELD node RETURN node", "damping must be above 0 and below 1"),
        ("CALL algo.wcc({incremental: true, graph: 'g'}) YIELD node RETURN node", "cannot be given together"),
        ("CALL algo.closeness({incremental: true}) YIELD node RETURN node", "unknown config key `incremental`"),
    ];
    for (q, expected) in cases {
        let e = refused(q);
        assert!(e.contains(expected), "{q}\n  expected `{expected}`, got: {e}");
    }
    assert!(s.incremental_states().status().is_empty(), "a refused call kept a state");
}

#[test]
fn graphs_keep_their_own_state() {
    let mut a = ring(5);
    let b = ring(8);
    degree(&a, true);
    degree(&b, true);
    write(&mut a, "CREATE (:P)");
    assert_eq!(degree(&a, true).len(), 6);
    assert_eq!(degree(&b, true).len(), 8);
    assert_eq!(last(&b, Algorithm::Degree), Refresh::Updated { nodes: 0, relationships: 0 });
}
