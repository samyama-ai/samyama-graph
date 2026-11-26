//! A selector bounds an otherwise unbounded search (#1648).
//!
//! ISO/IEC 39075 Sec. 5 requires an unbounded quantifier to sit in the scope of a
//! restrictor **or a selector**, so that the answer is finite. The restrictor half has
//! always worked here: TRAIL is bounded by the edge count, ACYCLIC and SIMPLE by the
//! node count. The selector half was refused, because a selector was implemented as
//! "enumerate every candidate, then choose", and under WALK the candidate set is
//! infinite on any graph with a cycle.
//!
//! `ANY` and `ANY SHORTEST` do not need the candidates: one walk per endpoint pair is
//! the answer. They are now answered by a breadth-first search over states
//! `(node, min(depth, min_hops))`, which visits each state once and so ends whatever the
//! length bound. Keying on the pair rather than the node is what keeps the `(v, v)` pair
//! a cycle produces: a first-reach traversal marks its source visited at depth zero and
//! loses it, which is why the first attempt at this was reverted.
//!
//! `ALL SHORTEST` is a different question -- it wants *every* minimum-length walk, and
//! this search keeps one -- and stays refused under an unbounded WALK. The refusal names
//! the limit.

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

fn ends_and_lengths(store: &GraphStore, q: &str) -> Vec<(String, i64)> {
    let b = QueryEngine::new()
        .execute_with_dialect(q, store, Dialect::Gql)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let mut out: Vec<(String, i64)> = b
        .records
        .iter()
        .map(|r| {
            let end = match r.get("end") {
                Some(samyama::query::executor::Value::Property(
                    samyama::graph::PropertyValue::String(s),
                )) => s.clone(),
                other => panic!("{q}: end is {other:?}"),
            };
            let len = match r.get("len") {
                Some(samyama::query::executor::Value::Property(
                    samyama::graph::PropertyValue::Integer(n),
                )) => *n,
                other => panic!("{q}: len is {other:?}"),
            };
            (end, len)
        })
        .collect();
    out.sort();
    out
}

fn pairs(xs: &[(&str, i64)]) -> Vec<(String, i64)> {
    xs.iter().map(|&(e, n)| (e.to_string(), n)).collect()
}

/// The acceptance case. Three endpoint pairs are reachable from `a` -- a, b and c --
/// and a single-path selector takes one from each, so the answer is three rows however
/// infinite the candidate set is.
#[test]
fn a_single_path_selector_bounds_an_unbounded_walk() {
    for q in [
        "MATCH ANY SHORTEST (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid",
        "MATCH ANY (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid",
    ] {
        assert_eq!(run(&cyclic(), q), Ok(3), "{q}");
    }
}

/// The zero-length walk is the source's shortest walk to itself, and it is there once:
/// the cycle back to `a` is longer, so it is not a second answer for the same pair.
#[test]
fn from_zero_hops_the_source_is_its_own_shortest_end_once() {
    let q = "MATCH ANY SHORTEST WALK p = (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' \
             RETURN y.eid AS end, length(p) AS len";
    assert_eq!(
        ends_and_lengths(&cyclic(), q),
        pairs(&[("a", 0), ("b", 1), ("c", 2)])
    );
}

/// The trap the reverted attempt fell into, now on the unbounded search: from one hop
/// up, `a` is still an end, by the three-hop cycle.
#[test]
fn from_one_hop_the_cycle_back_to_the_start_is_kept() {
    let q = "MATCH ANY SHORTEST WALK p = (x:N)-[:E*1..]->(y:N) WHERE x.name = 'a' \
             RETURN y.eid AS end, length(p) AS len";
    assert_eq!(
        ends_and_lengths(&cyclic(), q),
        pairs(&[("a", 3), ("b", 1), ("c", 2)])
    );
}

/// A WALK may repeat nodes and edges, and must, to reach a lower bound longer than the
/// cycle: at least five edges from `a` reaches `c` at five, `a` at six, `b` at seven.
#[test]
fn a_lower_bound_past_the_cycle_is_reached_by_going_round() {
    let q = "MATCH ANY SHORTEST WALK p = (x:N)-[:E*5..]->(y:N) WHERE x.name = 'a' \
             RETURN y.eid AS end, length(p) AS len";
    assert_eq!(
        ends_and_lengths(&cyclic(), q),
        pairs(&[("a", 6), ("b", 7), ("c", 5)])
    );
}

/// The path binds the walk it found, node by node: `a -> b -> c -> a`.
#[test]
fn the_path_is_the_walk_that_was_found() {
    let q =
        "MATCH ANY SHORTEST WALK p = (x:N)-[:E*1..]->(y:N) WHERE x.name = 'a' AND y.name = 'a' \
             RETURN [n IN nodes(p) | n.eid] AS ids";
    let b = QueryEngine::new()
        .execute_with_dialect(q, &cyclic(), Dialect::Gql)
        .unwrap();
    assert_eq!(b.records.len(), 1);
    let ids = format!("{:?}", b.records[0].get("ids"));
    for n in ["\"a\"", "\"b\"", "\"c\""] {
        assert!(ids.contains(n), "{ids}");
    }
    assert_eq!(
        ids.matches("\"a\"").count(),
        2,
        "starts and ends at a: {ids}"
    );
}

/// The end-node pattern still filters, and a target bound twice still means "ends here".
#[test]
fn the_end_pattern_still_constrains_the_answer() {
    let s = cyclic();
    assert_eq!(
        run(
            &s,
            "MATCH ANY SHORTEST (x:N)-[:E*1..]->(y:Missing) WHERE x.name = 'a' RETURN y"
        ),
        Ok(0)
    );
    assert_eq!(
        run(&s, "MATCH ANY SHORTEST (x:N)-[:E*1..]->(x) RETURN x.eid"),
        Ok(3),
        "each node returns to itself round the cycle, once"
    );
}

/// Every source, every end: on a complete graph of 30 nodes each node reaches all 30
/// (itself by two hops), so 900 rows. The candidate walks are uncountable; the states
/// are 30 per source.
#[test]
fn it_terminates_on_a_dense_cyclic_graph() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut("UNWIND range(1, 30) AS i CREATE (:K {i: i})", &mut store, T)
        .unwrap();
    engine
        .execute_mut(
            "MATCH (a:K), (b:K) WHERE a.i <> b.i CREATE (a)-[:E]->(b)",
            &mut store,
            T,
        )
        .unwrap();
    assert_eq!(
        run(
            &store,
            "MATCH ANY SHORTEST (x:K)-[:E*1..]->(y:K) RETURN x.i, y.i"
        ),
        Ok(900)
    );
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
/// that pair in it and a first-reach answer does not. Bounded here, so it runs on the
/// enumeration; `from_one_hop_the_cycle_back_to_the_start_is_kept` is the same trap on
/// the unbounded search.
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
    // ALL SHORTEST wants every minimum-length walk, which the single-walk search cannot
    // give -- the two parallel edges a->b are two shortest walks and it keeps one.
    // Refusing is the honest answer until a search that keeps every one exists;
    // returning one of them and calling it ALL SHORTEST would be the
    // silent-wrong-answer class.
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

/// A selector partitions every input row, not only the first. Found while building the
/// unbounded search: the operator kept each row's end node and length in a second queue
/// that was not popped when the row was returned, so from the second source on the
/// selector chose among the previous source's ends and dropped rows, silently. This was
/// wrong on bounded queries too: unanchored, each of the 3 sources reaches all 3 nodes
/// within four hops, so 9 rows, and the stale queue answered 8 or 6.
#[test]
fn a_selector_partitions_every_input_row_not_only_the_first() {
    let s = cyclic();
    for q in [
        "MATCH ANY SHORTEST (x:N)-[:E*1..4]->(y:N) RETURN x.eid, y.eid",
        "MATCH ANY (x:N)-[:E*1..4]->(y:N) RETURN x.eid, y.eid",
        "MATCH ANY SHORTEST WALK (x:N)-[:E*1..4]->(y:N) RETURN x.eid, y.eid",
        "MATCH ANY SHORTEST WALK (x:N)-[:E*1..]->(y:N) RETURN x.eid, y.eid",
    ] {
        assert_eq!(run(&s, q), Ok(9), "{q}");
    }
}

#[test]
fn a_restrictor_still_bounds_it_on_its_own() {
    // Unchanged behaviour: TRAIL is bounded by the edge count with no selector at all.
    let q = "MATCH TRAIL (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    assert_eq!(run(&cyclic(), q), Ok(9));
}
