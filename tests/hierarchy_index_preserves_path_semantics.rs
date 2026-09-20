//! Turning the hierarchy index on must not change the answer (#1343).
//!
//! A variable-length pattern yields **one row per path**. In a multi-parent
//! hierarchy a node reachable two ways contributes twice, and that is what
//! openCypher specifies and what the engine does without the index.
//!
//! The OEH index answers a *subsumption* question — is `d` under `r`? — which
//! is set-shaped and has no notion of how many paths got there. Substituting it
//! for `-[:T*0..]->` returned the distinct-node sum instead:
//!
//! ```text
//! H2-13: indexed=225  baseline=235   "sum over the multi-parent subtree of K0_0"
//! ```
//!
//! That is the bad kind of bug. Not a slower query — a different number, on
//! exactly the graphs the index exists for, with nothing saying so.
//!
//! The rewrite is now declined when a node inside the pinned subtree is
//! reachable by more than one path, which is precisely when the two readings
//! differ. A single-parent subtree inside a multi-parent hierarchy still gets
//! the rewrite: declining on the whole poset would have switched the index off
//! for every near-tree, and near-trees are most of the real ontologies.
//!
//! `subsumes()` is unaffected. It asks the set question on purpose.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn store_from(statements: &[&str]) -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in statements {
        engine
            .execute_mut(q, &mut store, "default")
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    store
}

/// The diamond from the issue: `L` sits under both `A` and `B`, both under `R`.
///
/// Reflexive descendants of `R` as a *set*: R, A, B, L — 0+10+20+5 = 35.
/// As *paths*: R, A, B, L-via-A, L-via-B — 0+10+20+5+5 = 40.
const DIAMOND: &[&str] = &[
    "CREATE (:T {code:'R', units:0}), (:T {code:'A', units:10}),
            (:T {code:'B', units:20}), (:T {code:'L', units:5})",
    "MATCH (a:T {code:'A'}), (r:T {code:'R'}) CREATE (a)-[:MAPS_TO]->(r)",
    "MATCH (b:T {code:'B'}), (r:T {code:'R'}) CREATE (b)-[:MAPS_TO]->(r)",
    "MATCH (l:T {code:'L'}), (a:T {code:'A'}) CREATE (l)-[:MAPS_TO]->(a)",
    "MATCH (l:T {code:'L'}), (b:T {code:'B'}) CREATE (l)-[:MAPS_TO]->(b)",
];

const ROLLUP: &str =
    "MATCH (d)-[:MAPS_TO*0..]->(r:T {code:'R'}) RETURN sum(d.units) AS v";
const SCAN: &str = "MATCH (d)-[:MAPS_TO*0..]->(r:T {code:'R'}) RETURN d.code AS c";

fn scalar(store: &GraphStore, query: &str) -> String {
    let batch = QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    format!("{:?}", batch.records[0].bindings()[0].1)
}

fn row_count(store: &GraphStore, query: &str) -> usize {
    QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .records
        .len()
}

fn plan_description(query: &str, store: &GraphStore) -> String {
    let parsed = samyama::query::parse_query(query).unwrap();
    let planner = samyama::query::executor::planner::QueryPlanner::new();
    let plan = planner.plan(&parsed, store).unwrap();
    plan.root.describe().format(0)
}

fn with_index(statements: &[&str], ddl: &str) -> GraphStore {
    let mut store = store_from(statements);
    QueryEngine::new()
        .execute_mut(ddl, &mut store, "default")
        .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    store
}

#[test]
fn a_multi_parent_subtree_gives_the_same_sum_with_and_without_the_index() {
    // The defect, measured. Before the fix: 35 with the index, 40 without.
    let plain = store_from(DIAMOND);
    let indexed = with_index(
        DIAMOND,
        "CREATE HIERARCHY INDEX h ON ()-[:MAPS_TO]->() MEASURE units AGGREGATE sum",
    );
    assert_eq!(
        scalar(&plain, ROLLUP),
        scalar(&indexed, ROLLUP),
        "building an index changed the answer"
    );
}

#[test]
fn a_multi_parent_subtree_gives_the_same_row_count_with_and_without_the_index() {
    // The descendant-scan half. `L` is reachable two ways, so it is two rows.
    let plain = store_from(DIAMOND);
    let indexed = with_index(DIAMOND, "CREATE HIERARCHY INDEX h ON ()-[:MAPS_TO]->()");
    assert_eq!(row_count(&plain, SCAN), 5, "R, A, B, L-via-A, L-via-B");
    assert_eq!(
        row_count(&plain, SCAN),
        row_count(&indexed, SCAN),
        "building an index changed the number of rows"
    );
}

#[test]
fn the_rewrite_still_fires_on_a_single_parent_subtree_of_the_same_index() {
    // The half that stops the fix being "switch the index off". `A`'s subtree
    // is a tree — L reaches A one way — so the rewrite is sound there and must
    // still happen, in the very same multi-parent hierarchy.
    let indexed = with_index(
        DIAMOND,
        "CREATE HIERARCHY INDEX h ON ()-[:MAPS_TO]->() MEASURE units AGGREGATE sum",
    );
    let plan = plan_description(
        "MATCH (d)-[:MAPS_TO*0..]->(r:T {code:'A'}) RETURN sum(d.units) AS v",
        &indexed,
    );
    assert!(
        plan.contains("Hierarchy"),
        "a sound subtree lost its rewrite: {plan}"
    );
    let plain = store_from(DIAMOND);
    assert_eq!(
        scalar(&plain, "MATCH (d)-[:MAPS_TO*0..]->(r:T {code:'A'}) RETURN sum(d.units) AS v"),
        scalar(
            &indexed,
            "MATCH (d)-[:MAPS_TO*0..]->(r:T {code:'A'}) RETURN sum(d.units) AS v"
        ),
    );
}

#[test]
fn the_unsound_subtree_loses_the_rewrite_and_says_so_in_the_plan() {
    // The mechanism, not just the answer: a test that only compared sums would
    // still pass if the rewrite fired and happened to agree.
    let indexed = with_index(
        DIAMOND,
        "CREATE HIERARCHY INDEX h ON ()-[:MAPS_TO]->() MEASURE units AGGREGATE sum",
    );
    let plan = plan_description(ROLLUP, &indexed);
    assert!(
        !plan.contains("Hierarchy"),
        "the multi-parent subtree must fall back to the traversal: {plan}"
    );
}

#[test]
fn a_pure_tree_keeps_its_rewrite() {
    // A tree has one path per node, so the two readings coincide and nothing
    // about this fix may touch it. This is the case the index was measured on.
    let tree = &[
        "CREATE (:T {code:'R', units:0}), (:T {code:'A', units:10}), (:T {code:'L', units:5})",
        "MATCH (a:T {code:'A'}), (r:T {code:'R'}) CREATE (a)-[:MAPS_TO]->(r)",
        "MATCH (l:T {code:'L'}), (a:T {code:'A'}) CREATE (l)-[:MAPS_TO]->(a)",
    ];
    let indexed = with_index(
        tree,
        "CREATE HIERARCHY INDEX h ON ()-[:MAPS_TO]->() MEASURE units AGGREGATE sum",
    );
    let plan = plan_description(ROLLUP, &indexed);
    assert!(plan.contains("Hierarchy"), "a tree lost its rewrite: {plan}");
    assert_eq!(scalar(&store_from(tree), ROLLUP), scalar(&indexed, ROLLUP));
}

#[test]
fn subsumes_still_answers_the_set_question() {
    // `subsumes()` is the spelling for "is d under r", and it counts each node
    // once by design. The fix must not move it.
    let indexed = with_index(DIAMOND, "CREATE HIERARCHY INDEX h ON ()-[:MAPS_TO]->()");
    let n = scalar(
        &indexed,
        "MATCH (d:T), (r:T {code:'R'}) WHERE subsumes(d, r) RETURN count(d) AS n",
    );
    assert!(n.contains('4'), "R, A, B, L counted once each: {n}");
}
