//! Metamorphic relations for quantified path patterns (#1142).
//!
//! Three relations that hold under **every** path mode in ISO/IEC 39075 — WALK,
//! TRAIL, ACYCLIC and SIMPLE alike — because a path has exactly one length and no
//! restrictor mentions the quantifier bounds:
//!
//! ```text
//! M1   A(lo, hi)  is a sub-multiset of  A(lo, hi+1)
//! M2   A(lo, hi)  is a sub-multiset of  A(lo-1, hi)
//! M3   A(lo, hi)  ==  multiset sum over k in [lo, hi] of  A(k, k)
//! ```
//!
//! An engine that breaks one contradicts itself, whatever it believes a path is.
//!
//! **No expected values, and no commitment to a semantics.** Every assertion here
//! compares two of the engine's own answers. That is the property that makes these
//! worth having: our other tests assert answers we chose, and #1140 survived them
//! because they encoded the same assumption the bug did. These cannot encode our
//! assumptions, and they stay true now that #1141 has added three more path modes.
//!
//! On this engine before #1140 was fixed, this file produced 24 violations, every
//! one with a `0..n` query on one side — a single root cause.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use std::collections::BTreeMap;

const T: &str = "default";

/// Six small graphs, chosen so that the relations have something to catch.
///
/// A tree makes every path mode agree and every relation hold trivially, so a suite
/// of trees would pass against an engine that ignored the quantifier. Each of these
/// contains at least one of: parallel edges, a cycle, or two routes of different
/// length between the same pair.
const GRAPHS: &[(&str, &[(&str, &str)])] = &[
    // Two parallel a->b edges: multiplicity is observable, which first-reach
    // deduplication destroys. This is #1140's graph.
    ("parallel", &[("a", "b"), ("a", "b"), ("b", "c"), ("c", "a")]),
    // A cycle: separates TRAIL from ACYCLIC, and makes A(k,k) non-empty for every k.
    ("triangle", &[("a", "b"), ("b", "c"), ("c", "a")]),
    // Two routes of equal length: ALL SHORTEST has more than one answer.
    ("diamond", &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")]),
    // Two routes of different length between the same pair.
    ("ladder", &[("a", "b"), ("b", "d"), ("a", "c"), ("c", "e"), ("e", "d")]),
    // A self-loop: length-1 path from a node to itself.
    ("selfloop", &[("a", "a"), ("a", "b"), ("b", "c")]),
    // A chain, as the control: relations must hold here too, and any violation
    // on a chain is a much simpler bug than one that needs a cycle.
    ("chain", &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")]),
];

fn build(edges: &[(&str, &str)]) -> GraphStore {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    let mut names: Vec<&str> = edges.iter().flat_map(|&(a, b)| [a, b]).collect();
    names.sort_unstable();
    names.dedup();
    for n in names {
        engine.execute_mut(&format!("CREATE (:N {{eid: \"{n}\"}})"), &mut store, T).unwrap();
    }
    for &(a, b) in edges {
        engine
            .execute_mut(
                &format!("MATCH (x:N {{eid: \"{a}\"}}), (y:N {{eid: \"{b}\"}}) CREATE (x)-[:E]->(y)"),
                &mut store,
                T,
            )
            .unwrap();
    }
    store
}

/// The answer to `(a)-[:E*lo..hi]->(y)` as a multiset of end nodes.
///
/// A multiset, not a set: collapsing to a set is exactly the defect these relations
/// exist to catch, so counting is the whole point.
fn answer(store: &GraphStore, lo: usize, hi: usize) -> BTreeMap<String, usize> {
    let q = format!("MATCH (x:N {{eid: \"a\"}})-[:E*{lo}..{hi}]->(y) RETURN y.eid");
    let batch = QueryEngine::new()
        .execute(&q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let mut out = BTreeMap::new();
    for rec in &batch.records {
        *out.entry(format!("{:?}", rec.get("y.eid"))).or_insert(0) += 1;
    }
    out
}

fn is_sub_multiset(a: &BTreeMap<String, usize>, b: &BTreeMap<String, usize>) -> bool {
    a.iter().all(|(k, &n)| b.get(k).copied().unwrap_or(0) >= n)
}

fn multiset_sum(parts: &[BTreeMap<String, usize>]) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for p in parts {
        for (k, &n) in p {
            *out.entry(k.clone()).or_insert(0) += n;
        }
    }
    out
}

/// The maximum upper bound tested. Four is enough to exceed every graph's diameter
/// here, so the relations are exercised past the point where new paths stop
/// appearing — which is where an off-by-one in the bound handling shows.
const MAX_HI: usize = 4;

#[test]
fn m1_widening_the_upper_bound_only_adds_paths() {
    let mut violations = Vec::new();
    for &(name, edges) in GRAPHS {
        let g = build(edges);
        for lo in 0..=MAX_HI {
            for hi in lo..MAX_HI {
                let narrow = answer(&g, lo, hi);
                let wide = answer(&g, lo, hi + 1);
                if !is_sub_multiset(&narrow, &wide) {
                    violations.push(format!(
                        "{name}: A({lo},{hi}) is not contained in A({lo},{}) — \
                         {narrow:?} vs {wide:?}",
                        hi + 1
                    ));
                }
            }
        }
    }
    assert!(violations.is_empty(), "M1 broken in {} cases:\n{}", violations.len(), violations.join("\n"));
}

#[test]
fn m2_lowering_the_lower_bound_only_adds_paths() {
    let mut violations = Vec::new();
    for &(name, edges) in GRAPHS {
        let g = build(edges);
        for lo in 1..=MAX_HI {
            for hi in lo..=MAX_HI {
                let narrow = answer(&g, lo, hi);
                let wide = answer(&g, lo - 1, hi);
                if !is_sub_multiset(&narrow, &wide) {
                    violations.push(format!(
                        "{name}: A({lo},{hi}) is not contained in A({},{hi}) — \
                         {narrow:?} vs {wide:?}",
                        lo - 1
                    ));
                }
            }
        }
    }
    assert!(violations.is_empty(), "M2 broken in {} cases:\n{}", violations.len(), violations.join("\n"));
}

/// The decisive one: an interval is the sum of its exact lengths.
///
/// M1 and M2 are containment and can be satisfied by an engine that under-reports
/// consistently. This one fixes the count exactly, and it is the relation that
/// #1140 broke: `A(0,2)` answered three rows where `A(0,0) + A(1,1) + A(2,2)` is
/// five.
#[test]
fn m3_an_interval_is_the_sum_of_its_exact_lengths() {
    let mut violations = Vec::new();
    for &(name, edges) in GRAPHS {
        let g = build(edges);
        for lo in 0..=MAX_HI {
            for hi in lo..=MAX_HI {
                let whole = answer(&g, lo, hi);
                let parts: Vec<_> = (lo..=hi).map(|k| answer(&g, k, k)).collect();
                let summed = multiset_sum(&parts);
                if whole != summed {
                    violations.push(format!(
                        "{name}: A({lo},{hi}) = {whole:?} but the sum of A(k,k) for \
                         k in [{lo},{hi}] is {summed:?}"
                    ));
                }
            }
        }
    }
    assert!(violations.is_empty(), "M3 broken in {} cases:\n{}", violations.len(), violations.join("\n"));
}

/// The relations hold under the GQL path modes too, which is what makes them
/// durable rather than a snapshot of today's semantics (#1141).
///
/// A restrictor changes *which* paths are candidates; it does not change that a
/// path has one length. So M3 must hold separately within each mode — and comparing
/// across modes would be wrong, which is why each mode is summed against itself.
#[test]
fn m3_holds_within_each_gql_path_mode() {
    let engine = QueryEngine::new();
    let mut violations = Vec::new();
    for &(name, edges) in GRAPHS {
        let g = build(edges);
        for mode in ["TRAIL ", "ACYCLIC ", "SIMPLE "] {
            let ans = |lo: usize, hi: usize| -> BTreeMap<String, usize> {
                let q = format!(
                    "MATCH {mode}(x:N {{eid: \"a\"}})-[:E*{lo}..{hi}]->(y) RETURN y.eid"
                );
                let batch = engine.execute(&q, &g).unwrap_or_else(|e| panic!("{q}: {e}"));
                let mut out = BTreeMap::new();
                for rec in &batch.records {
                    *out.entry(format!("{:?}", rec.get("y.eid"))).or_insert(0) += 1;
                }
                out
            };
            for lo in 0..=3 {
                for hi in lo..=3 {
                    let whole = ans(lo, hi);
                    let summed = multiset_sum(&(lo..=hi).map(|k| ans(k, k)).collect::<Vec<_>>());
                    if whole != summed {
                        violations.push(format!(
                            "{name} {}: A({lo},{hi}) = {whole:?} but sum of A(k,k) is {summed:?}",
                            mode.trim()
                        ));
                    }
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "M3 broken under a path mode in {} cases:\n{}",
        violations.len(),
        violations.join("\n")
    );
}
