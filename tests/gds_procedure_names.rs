//! GDS procedure names reach our algorithms, and only where the answer matches.
//!
//! INT-04. Dispatch stripped a `gds.` prefix, which resolves `gds.pageRank` --
//! a spelling nobody writes. A GDS procedure name carries a maturity namespace
//! and an execution mode: `gds.pageRank.stream`, `gds.alpha.adamicAdar.stream`.
//! Measured against the GDS procedures whose semantics we implement, none of
//! their real spellings resolved.
//!
//! The half worth guarding is the other one: a name whose semantics differ must
//! keep failing. An alias that returns a different answer under a name the user
//! already trusts is worse than a missing alias, and a coverage count never
//! shows it.

use samyama::graph::{GraphStore, Label};
use samyama::query::executor::operator::AlgorithmOperator;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let a = s.create_node_with_labels([Label::new("N")]);
    let b = s.create_node_with_labels([Label::new("N")]);
    s.create_edge(a, b, "R").unwrap();
    s
}

fn run(q: &str) -> Result<(), String> {
    let s = store();
    let plan = parse_query(q).map_err(|e| format!("{e}"))?;
    QueryExecutor::new(&s)
        .execute(&plan)
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

#[test]
fn the_streaming_mode_resolves() {
    for name in [
        "gds.pageRank.stream",
        "gds.wcc.stream",
        "gds.triangleCount.stream",
        "gds.localClusteringCoefficient.stream",
        "gds.labelPropagation.stream",
        "gds.spanningTree.stream",
        "gds.alpha.adamicAdar.stream",
        "gds.beta.closeness.stream",
    ] {
        assert!(
            AlgorithmOperator::is_algorithm(name),
            "{name} does not resolve"
        );
    }
}

#[test]
fn the_maturity_namespace_carries_no_semantics() {
    assert_eq!(
        AlgorithmOperator::canonical_name("gds.alpha.jaccard.stream"),
        AlgorithmOperator::canonical_name("gds.jaccard.stream")
    );
    assert_eq!(
        AlgorithmOperator::canonical_name("gds.beta.closeness.stream"),
        AlgorithmOperator::canonical_name("closeness")
    );
}

#[test]
fn dijkstra_maps_to_the_weighted_one() {
    // The obvious mapping is the wrong one: GDS's dijkstra is weighted and our
    // `shortestPath` is an unweighted BFS. Aliasing it there would answer a
    // different question and return plausible numbers while doing it.
    assert_eq!(
        AlgorithmOperator::canonical_name("gds.shortestPath.dijkstra.stream"),
        "weightedpath"
    );
    assert_ne!(
        AlgorithmOperator::canonical_name("gds.shortestPath.dijkstra.stream"),
        AlgorithmOperator::canonical_name("shortestPath")
    );
}

#[test]
fn a_divergent_name_does_not_resolve() {
    // `gds.alpha.triangles` lists one row per triangle; our `triangleCount`
    // returns a count per node. Same word, different result. This was aliased
    // for one revision and the probe caught it -- the coverage ratio could not
    // have, because a divergent name is outside that ratio.
    assert!(
        !AlgorithmOperator::is_algorithm("gds.alpha.triangles"),
        "triangles resolved; it returns a different shape from triangleCount"
    );
    assert!(!AlgorithmOperator::is_algorithm("gds.fastRP.stream"));
    assert!(!AlgorithmOperator::is_algorithm("gds.graph.project"));
}

#[test]
fn a_writing_mode_is_refused_with_the_reason() {
    // "Unknown procedure" sends the reader looking for an algorithm that is in
    // fact right there. The mode is what is missing, and the message says so.
    for (q, mode) in [
        ("CALL gds.pageRank.write() YIELD node RETURN count(*)", "write"),
        ("CALL gds.wcc.mutate() YIELD node RETURN count(*)", "mutate"),
        ("CALL gds.pageRank.stats() YIELD node RETURN count(*)", "stats"),
    ] {
        let err = run(q).expect_err("a writing mode must not run");
        assert!(
            err.contains(mode),
            "the message does not name the mode {mode}: {err}"
        );
        assert!(
            err.contains(".stream") || err.contains("algo."),
            "the message does not say what to call instead: {err}"
        );
        assert!(
            !err.contains("Unknown procedure:"),
            "still the generic error: {err}"
        );
    }
}

#[test]
fn the_streaming_mode_actually_runs() {
    // Resolving is not running: `is_algorithm` returning true and the call then
    // failing is the shape #1316 had. So execute one.
    run("CALL gds.pageRank.stream() YIELD node, score RETURN count(*)")
        .expect("gds.pageRank.stream resolved but did not run");
}

#[test]
fn our_own_spellings_are_unchanged() {
    // The aliasing must not move any existing name.
    for name in ["pageRank", "algo.pageRank", "algo.pagerank", "samyama.pageRank"] {
        assert_eq!(AlgorithmOperator::canonical_name(name), "pagerank", "{name}");
    }
}

#[test]
fn a_gds_name_and_an_aliased_yield_work_together() {
    // These two changes met at the same call site. #1318 added
    // `.with_aliases(...)` to the `AlgorithmOperator::new` here, and the GDS
    // routing changed the branch that reaches it; the merge conflicted, and
    // resolving it by taking either side alone would have silently reverted
    // the other. Each has its own passing test, which is exactly the state in
    // which two correct fixes compose wrong.
    //
    // So: a GDS spelling *and* an aliased yield, in one query.
    run("CALL gds.pageRank.stream() YIELD score AS rank RETURN rank ORDER BY rank DESC LIMIT 1")
        .expect("a gds name with an aliased yield");
}
