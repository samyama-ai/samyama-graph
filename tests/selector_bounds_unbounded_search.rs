//! A selector bounds an otherwise unbounded search (#1648).
//!
//! ISO/IEC 39075 Sec. 5 requires an unbounded quantifier to sit in the scope of a
//! restrictor **or a selector**, so that the answer is finite. The restrictor half has
//! always worked here: TRAIL is bounded by the edge count, ACYCLIC and SIMPLE by the
//! node count. The selector half was refused, because a selector was implemented as
//! "enumerate every candidate, then choose", and under WALK the candidate set is
//! infinite on any graph with a cycle.
//!
//! `ANY` and `ANY SHORTEST` do not need the candidates. One path per endpoint pair
//! suffices, and the first-reach breadth-first traversal this operator already has
//! produces exactly that, terminating through its visited set rather than through a
//! bound on length.
//!
//! It is also *correct* under every restrictor, which is the part worth stating: a
//! first-reach path never revisits a node, so it is simple, and a simple path
//! satisfies WALK, TRAIL, ACYCLIC and SIMPLE alike. Being shortest, it also satisfies
//! ANY SHORTEST.
//!
//! `ALL SHORTEST` is a different question -- it wants *every* minimum-length path, which
//! first-reach cannot give -- and stays refused under an unbounded WALK until that is
//! built. The refusal names the limit.

use samyama::graph::GraphStore;
use samyama::query::{Dialect, QueryEngine};

const T: &str = "default";

/// `a =e1,e2=> b -e3-> c -e4-> a`: parallel edges and a cycle, so an unbounded walk
/// has infinitely many paths and a bounded search has to prove it terminates.
fn cyclic() -> GraphStore {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    for n in ["a", "b", "c"] {
        engine
            .execute_mut(&format!("CREATE (:N {{eid: \"{n}\", name: \"{n}\"}})"), &mut store, T)
            .unwrap();
    }
    for (from, to) in [("a", "b"), ("a", "b"), ("b", "c"), ("c", "a")] {
        engine
            .execute_mut(
                &format!(
                    "MATCH (x:N {{eid: \"{from}\"}}), (y:N {{eid: \"{to}\"}}) CREATE (x)-[:E]->(y)"
                ),
                &mut store,
                T,
            )
            .unwrap();
    }
    store
}

fn run(store: &GraphStore, q: &str) -> Result<usize, String> {
    QueryEngine::new()
        .execute_with_dialect(q, store, Dialect::Gql)
        .map(|b| b.records.len())
        .map_err(|e| e.to_string())
}

/// What must become true. Three endpoint pairs are reachable from `a` -- a, b and c --
/// and a single-path selector takes one from each, so the answer is three rows however
/// infinite the candidate set is.
///
/// Pinned as a refusal because that is what the engine does today, and pinned at all
/// so that the day it answers, this test fails and has to be updated deliberately
/// rather than drifting. #1648.
#[test]
fn a_single_path_selector_does_not_yet_bound_an_unbounded_walk() {
    for q in [
        "MATCH ANY SHORTEST (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid",
        "MATCH ANY (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid",
    ] {
        let err = run(&cyclic(), q).expect_err(
            "when this starts answering, the expectation is Ok(3) and this test should \
             say so",
        );
        assert!(
            err.to_lowercase().contains("walk") || err.to_lowercase().contains("unbounded"),
            "the refusal must name the limit: {err}"
        );
    }
}

/// Why the obvious shortcut is wrong, pinned so nobody takes it again.
///
/// Routing a single-path selector to the first-reach traversal looks right: it wants
/// one path per endpoint pair and that is exactly what first-reach produces, with no
/// bound on length. It is wrong because first-reach marks its source visited at depth
/// zero and can never reach it again, so the `(v, v)` pair a cycle produces is simply
/// missing. The query answers, and is short a row -- the silent-wrong-answer class.
///
/// On this fixture `a` is reachable from `a` in three hops, so a correct answer has
/// that pair in it and a first-reach answer does not.
#[test]
fn the_start_node_is_a_reachable_endpoint_and_must_not_be_lost() {
    let s = cyclic();
    let q = "MATCH ANY SHORTEST (x:N)-[:E*1..4]->(y:N) WHERE x.name = 'a' RETURN y.eid";
    let b = QueryEngine::new().execute_with_dialect(q, &s, Dialect::Gql).unwrap();
    let ends: Vec<String> = b
        .records
        .iter()
        .filter_map(|r| match r.get("y.eid") {
            Some(samyama::query::executor::Value::Property(
                samyama::graph::PropertyValue::String(s),
            )) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(
        ends.iter().any(|e| e == "a"),
        "a is reachable from a in three hops; an answer without it has lost the \
         (v, v) pair: {ends:?}"
    );
}

#[test]
fn a_selector_that_needs_every_candidate_is_refused() {
    // ALL SHORTEST wants every minimum-length path, which first-reach cannot give --
    // the two parallel edges a->b are two shortest paths and it keeps one. Refusing is
    // the honest answer until the two-phase search exists; returning one of them and
    // calling it ALL SHORTEST would be the silent-wrong-answer class.
    let q = "MATCH ALL SHORTEST (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    let err = run(&cyclic(), q).expect_err("ALL SHORTEST over an unbounded walk must be refused");
    assert!(
        err.to_lowercase().contains("walk") || err.to_lowercase().contains("unbounded"),
        "the refusal must name the limit: {err}"
    );
}

#[test]
fn all_over_an_unbounded_walk_is_still_refused() {
    // The standard forbids this one outright: the answer itself is infinite.
    let q = "MATCH WALK (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    assert!(run(&cyclic(), q).is_err());
}

#[test]
fn a_bounded_selector_query_is_unchanged() {
    // The control. Nothing about bounded patterns moves.
    let s = cyclic();
    for q in [
        "MATCH ANY SHORTEST (x:N)-[:E*1..4]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid",
        "MATCH ANY (x:N)-[:E*1..4]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid",
    ] {
        assert_eq!(run(&s, q), Ok(3), "{q}");
    }
    assert_eq!(
        run(&s, "MATCH ALL SHORTEST (x:N)-[:E*1..4]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid"),
        Ok(6),
        "the two parallel edges a->b double the shortest path to every node \
         downstream of b, not only to b: two to b, two to c, two back to a"
    );
}

#[test]
fn a_restrictor_still_bounds_it_on_its_own() {
    // Unchanged behaviour: TRAIL is bounded by the edge count with no selector at all.
    let q = "MATCH TRAIL (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    assert_eq!(run(&cyclic(), q), Ok(9));
}
