//! `A(0, n)` equals `A(0, 0)` union `A(1, n)`, as multisets (#1140).
//!
//! A variable-length pattern with lower bound 0 returned one row per distinct end
//! node instead of one row per path: `(a)-[:E*0..2]->(y)` over two parallel `a->b`
//! edges answered `a, b, c` where the correct multiset is `a, b, b, c, c`. The query
//! succeeded; only the answer was wrong.
//!
//! The identity asserted here holds under **every** ISO/IEC 39075 path mode — WALK,
//! TRAIL, ACYCLIC and SIMPLE — because a path has exactly one length and no
//! restrictor mentions the quantifier bounds. So this is a conformance property, not
//! a commitment to one semantics: whatever `*1..n` means, `*0..n` must be that plus
//! the zero-length match.
//!
//! Asserted as a relation between two of the engine's own answers rather than
//! against a table of expected values. A table encodes what I believed on the day I
//! wrote it; this stays true when the path semantics change, and it is the shape a
//! metamorphic suite uses.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

const T: &str = "default";

/// The reproducer's graph: `a =e1,e2=> b -e3-> c -e4-> a`.
///
/// The two parallel `a->b` edges are the point. With one edge per pair, first-reach
/// deduplication and correct multiset semantics agree on every query here, and the
/// bug is invisible.
const PARALLEL: &[(&str, &str)] = &[("a", "b"), ("a", "b"), ("b", "c"), ("c", "a")];

/// A diamond: two distinct 2-hop routes from `a` to `d`.
const DIAMOND: &[(&str, &str)] = &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")];

/// A triangle, the shape the multiplicity guard's own doc comment cites.
const TRIANGLE: &[(&str, &str)] = &[("a", "b"), ("b", "c"), ("c", "a")];

fn build(edges: &[(&str, &str)]) -> GraphStore {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    let mut names: Vec<&str> = edges.iter().flat_map(|&(a, b)| [a, b]).collect();
    names.sort_unstable();
    names.dedup();
    for n in names {
        engine
            .execute_mut(&format!("CREATE (:N {{eid: \"{n}\"}})"), &mut store, T)
            .unwrap();
    }
    for &(a, b) in edges {
        engine
            .execute_mut(
                &format!(
                    "MATCH (x:N {{eid: \"{a}\"}}), (y:N {{eid: \"{b}\"}}) CREATE (x)-[:E]->(y)"
                ),
                &mut store,
                T,
            )
            .unwrap();
    }
    store
}

/// End-node ids for a pattern, sorted — a multiset, since order is not the claim.
fn ends(store: &GraphStore, pattern: &str) -> Vec<String> {
    let engine = QueryEngine::new();
    let q = format!("MATCH (x:N {{eid: \"a\"}})-[:E{pattern}]->(y) RETURN y.eid");
    let batch = engine.execute(&q, store).unwrap_or_else(|e| panic!("{q}: {e}"));
    let mut out: Vec<String> = batch
        .records
        .iter()
        .map(|r| format!("{:?}", r.get("y.eid")))
        .collect();
    out.sort();
    out
}

#[test]
fn zero_lower_bound_is_the_zero_length_match_plus_the_one_bounded_form() {
    for (name, edges) in [
        ("parallel", PARALLEL),
        ("diamond", DIAMOND),
        ("triangle", TRIANGLE),
    ] {
        let store = build(edges);
        let zero = ends(&store, "*0..0");
        assert_eq!(zero.len(), 1, "{name}: *0..0 is the source and nothing else");

        for n in 1..=4 {
            let actual = ends(&store, &format!("*0..{n}"));
            let mut expected = zero.clone();
            expected.extend(ends(&store, &format!("*1..{n}")));
            expected.sort();
            assert_eq!(
                actual, expected,
                "{name}: *0..{n} must be *0..0 plus *1..{n} as a multiset. \
                 Got {} rows, expected {}. A lower bound of 0 taking a different \
                 traversal from a lower bound of 1 is how these diverge.",
                actual.len(),
                expected.len()
            );
        }
    }
}

/// The parallel-edge case with its numbers written down, so a reader can see what
/// the property above is worth without running it.
///
/// This is the reproducer from #1140. Kept alongside the property, not instead of
/// it: the property is what survives a change in path semantics, and this is what
/// makes the property legible.
#[test]
fn the_reported_case_answers_one_row_per_path() {
    let store = build(PARALLEL);
    for (pattern, rows) in [
        ("*0..0", 1), // a
        ("*0..1", 3), // a, b, b
        ("*0..2", 5), // a, b, b, c, c
        ("*1..1", 2), // b, b
        ("*1..2", 4), // b, b, c, c
        ("*2..2", 2), // c, c
    ] {
        assert_eq!(
            ends(&store, pattern).len(),
            rows,
            "{pattern} over two parallel a->b edges"
        );
    }
}

/// An empty interval matches nothing, and `*0..0` is not empty.
///
/// `expand_trails` emits on `depth >= min_hops` and used to rely on the descend
/// guard for the upper bound, which held only while `min_hops >= 1`. Routing
/// `*0..0` there emitted every depth-1 neighbour — three rows for a one-row answer.
#[test]
fn an_empty_interval_matches_nothing_and_zero_zero_matches_the_source() {
    let store = build(PARALLEL);
    assert_eq!(ends(&store, "*0..0").len(), 1);
    assert_eq!(ends(&store, "*1..0").len(), 0, "*1..0 is empty");
    assert_eq!(ends(&store, "*2..1").len(), 0, "*2..1 is empty");
}
