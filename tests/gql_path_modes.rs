//! GQL path restrictors and selectors (ISO/IEC 39075:2024), #1141.
//!
//! Four restrictors say which paths are candidates, four selectors say which
//! candidates to return per endpoint pair. Before this the dialect had one of each:
//! `TRAIL` implicitly, and `ALL`.
//!
//! The tests below assert the *differences between modes* rather than absolute row
//! counts wherever the difference is the point. A count can be right for the wrong
//! reason; TRAIL and ACYCLIC disagreeing on a cycle cannot be.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

const T: &str = "default";

fn graph(edges: &[(&str, &str)]) -> GraphStore {
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

fn rows(store: &GraphStore, query: &str) -> usize {
    QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .records
        .len()
}

/// A directed triangle `a -> b -> c -> a`.
const TRIANGLE: &[(&str, &str)] = &[("a", "b"), ("b", "c"), ("c", "a")];

/// Two 2-hop routes and one 3-hop route from `a` to `d`.
const LADDER: &[(&str, &str)] =
    &[("a", "b"), ("b", "d"), ("a", "c"), ("c", "d"), ("a", "e"), ("e", "f"), ("f", "d")];

/// The four restrictors are four different questions, and a cycle separates them.
///
/// Without a cycle every restrictor agrees, which is why a test on a tree would
/// pass against an engine that ignored the keyword entirely.
#[test]
fn the_restrictors_disagree_on_a_cycle() {
    let g = graph(TRIANGLE);
    let q = |m: &str, hops: &str| {
        format!("MATCH {m}(x:N {{eid: \"a\"}})-[:E{hops}]->(y) RETURN y.eid")
    };

    // TRAIL may revisit a node, so three hops returns to `a`: b, c, a.
    assert_eq!(rows(&g, &q("TRAIL ", "*1..3")), 3);
    // ACYCLIC may not, so the return to `a` is excluded: b, c.
    assert_eq!(
        rows(&g, &q("ACYCLIC ", "*1..3")),
        2,
        "ACYCLIC must exclude the path that returns to its own start"
    );
    // SIMPLE permits first == last, so the cycle is legal again: b, c, a.
    assert_eq!(
        rows(&g, &q("SIMPLE ", "*1..3")),
        3,
        "SIMPLE permits the endpoints to coincide, so a cycle is a simple path"
    );
    // WALK may reuse an edge, so a fourth hop reaches `b` a second time.
    assert_eq!(
        rows(&g, &q("WALK ", "*1..4")),
        4,
        "WALK may retake an edge; TRAIL at *1..4 still answers 3"
    );
    assert_eq!(rows(&g, &q("TRAIL ", "*1..4")), 3);

    // The default is unchanged: an unannotated pattern is TRAIL, which is what
    // openCypher's relationship-uniqueness rule already required.
    assert_eq!(rows(&g, &q("", "*1..3")), rows(&g, &q("TRAIL ", "*1..3")));
    assert_eq!(rows(&g, &q("", "*1..4")), rows(&g, &q("TRAIL ", "*1..4")));
}

/// SIMPLE is not prefix-closed, and this is the case that shows it.
///
/// A path that has returned to its source is complete: extending it would put the
/// source in the interior, which SIMPLE forbids. So `a -> b -> c -> a` is legal and
/// `a -> b -> c -> a -> b` is not, even though the second has the first as a prefix.
#[test]
fn a_simple_path_that_closes_cannot_be_extended() {
    let g = graph(TRIANGLE);
    let q = |hops: &str| {
        format!("MATCH SIMPLE (x:N {{eid: \"a\"}})-[:E{hops}]->(y) RETURN y.eid")
    };
    // b, c, a at *1..3 — and nothing further at *1..4 or *1..5, because the only
    // way onward is through `a`, which is now interior.
    assert_eq!(rows(&g, &q("*1..3")), 3);
    assert_eq!(rows(&g, &q("*1..4")), 3, "extending a closed simple path is not allowed");
    assert_eq!(rows(&g, &q("*1..5")), 3);
}

/// The selectors partition on the endpoint pair and choose within it.
#[test]
fn the_selectors_choose_among_candidates_per_endpoint_pair() {
    let g = graph(LADDER);
    let q = |m: &str| {
        format!("MATCH {m}(x:N {{eid: \"a\"}})-[:E*1..3]->(y:N {{eid: \"d\"}}) RETURN y.eid")
    };
    // Three paths a->d: two of length 2, one of length 3.
    assert_eq!(rows(&g, &q("")), 3, "ALL is the default");
    assert_eq!(rows(&g, &q("ALL ")), 3);
    assert_eq!(rows(&g, &q("ALL SHORTEST ")), 2, "both length-2 paths, not the length-3");
    assert_eq!(rows(&g, &q("ANY ")), 1);
    assert_eq!(rows(&g, &q("ANY SHORTEST ")), 1);
}

/// A selector and a restrictor compose, and the restrictor applies first.
#[test]
fn a_selector_and_a_restrictor_compose() {
    let g = graph(TRIANGLE);
    let all = "MATCH ACYCLIC (x:N {eid: \"a\"})-[:E*1..3]->(y) RETURN y.eid";
    let any = "MATCH ANY ACYCLIC (x:N {eid: \"a\"})-[:E*1..3]->(y) RETURN y.eid";
    assert_eq!(rows(&g, all), 2, "ACYCLIC candidates: b, c");
    // One per endpoint pair, and the pairs are (a,b) and (a,c) — so ANY does not
    // reduce this to one row. A selector partitions; it does not take a global head.
    assert_eq!(rows(&g, any), 2, "ANY is one path per endpoint pair, not one overall");
}

/// An unbounded WALK is refused rather than served from a capped subset.
///
/// The standard forbids it under `ALL` because the answer is infinite on any graph
/// with a cycle. It is refused here under **every** selector, which is stricter than
/// the standard and deliberately so: `ANY SHORTEST WALK` is finite in principle, but
/// this implementation enumerates candidates and then applies the selector, so the
/// selector never gets a turn. Allowing it hung this test. Refusing until there is a
/// shortest-first traversal is the honest position, and the message says so.
#[test]
fn an_unbounded_walk_is_refused_and_the_alternatives_are_not() {
    let g = graph(TRIANGLE);
    let engine = QueryEngine::new();

    let err = engine
        .execute("MATCH WALK (a)-[:E*]->(b) RETURN b", &g)
        .expect_err("an unbounded WALK under ALL has no finite answer");
    let msg = err.to_string();
    assert!(msg.contains("InvalidPattern"), "{msg}");
    assert!(
        msg.contains("*1..4") && msg.contains("TRAIL"),
        "the error must name a way out, not only refuse: {msg}"
    );
    // And it must distinguish this engine's limit from the standard's, since the
    // standard would allow a shortest selector here.
    assert!(
        msg.contains("enumerated before the selector"),
        "the error must say which limit is being hit: {msg}"
    );

    // A shortest selector does not rescue it.
    assert!(QueryEngine::new()
        .execute("MATCH ANY SHORTEST WALK (a)-[:E*]->(b) RETURN b", &g)
        .is_err());

    // Each of the ways out actually works.
    for ok in [
        "MATCH WALK (a)-[:E*1..4]->(b) RETURN b",
        "MATCH TRAIL (a)-[:E*]->(b) RETURN b",
        "MATCH ACYCLIC (a)-[:E*]->(b) RETURN b",
    ] {
        engine.execute(ok, &g).unwrap_or_else(|e| panic!("{ok} must be allowed: {e}"));
    }
}

/// Every spelling parses, including the combination and the named-path form.
#[test]
fn every_mode_parses_including_combinations() {
    use samyama::query::parser::parse_query;
    for q in [
        "MATCH WALK (a)-[:E*1..3]->(b) RETURN b",
        "MATCH TRAIL (a)-[:E*1..3]->(b) RETURN b",
        "MATCH ACYCLIC (a)-[:E*1..3]->(b) RETURN b",
        "MATCH SIMPLE (a)-[:E*1..3]->(b) RETURN b",
        "MATCH ALL (a)-[:E*1..3]->(b) RETURN b",
        "MATCH ANY (a)-[:E*1..3]->(b) RETURN b",
        "MATCH ALL SHORTEST (a)-[:E*1..3]->(b) RETURN b",
        "MATCH ANY SHORTEST (a)-[:E*1..3]->(b) RETURN b",
        "MATCH ALL SHORTEST ACYCLIC (a)-[:E*1..3]->(b) RETURN b",
        "MATCH ANY SHORTEST SIMPLE (a)-[:E*1..3]->(b) RETURN b",
        "MATCH p = ACYCLIC (a)-[:E*1..3]->(b) RETURN p",
        // Case-insensitive, like every other keyword in the dialect.
        "MATCH any shortest acyclic (a)-[:E*1..3]->(b) RETURN b",
    ] {
        parse_query(q).unwrap_or_else(|e| panic!("{q}: {e}"));
    }
}

/// `ALL` and `ANY` must not be shadowed as selectors when they are function calls
/// or variable names — the PEG takes the first alternative and does not backtrack.
#[test]
fn all_and_any_still_work_as_functions_and_names() {
    let g = graph(TRIANGLE);
    let engine = QueryEngine::new();
    for q in [
        "MATCH (n:N) RETURN count(n)",
        "RETURN all(x IN [1, 2] WHERE x > 0) AS r",
        "RETURN any(x IN [1, 2] WHERE x > 1) AS r",
    ] {
        engine.execute(q, &g).unwrap_or_else(|e| panic!("{q}: {e}"));
    }
}
