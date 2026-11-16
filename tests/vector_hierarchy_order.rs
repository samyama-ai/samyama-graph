//! Vector search under a hierarchy root is ordered by selectivity (#445).
//!
//! `CALL db.index.vector.queryNodes(...) YIELD node MATCH (r:Term {code: ...})
//! WHERE subsumes(node, r)` used to take the global k nearest and then discard
//! the ones outside the subtree, so a small subtree returned fewer than k rows
//! and cost a full graph search. The planner now prices the two orders with
//! the subtree size the OEH index answers: a small subtree is enumerated and
//! scored exactly (hierarchy-first), a large one is searched with the subtree
//! test as the search's filter (vector-first).

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

/// Root `T` with a small branch `A` (A + 4 leaves) and a large branch `B`
/// (B + 194 leaves): 201 terms, each with a distinct 2-d embedding.
const SMALL_LEAVES: usize = 4;
const LARGE_LEAVES: usize = 194;

fn embedding(i: usize) -> [f64; 2] {
    [
        ((i * 37) % 101) as f64 / 101.0,
        ((i * 53) % 97) as f64 / 97.0,
    ]
}

/// Every term as (code, parent code, embedding), in creation order.
fn terms() -> Vec<(String, Option<&'static str>, [f64; 2])> {
    let mut out = vec![
        ("T".to_string(), None, embedding(0)),
        ("A".to_string(), Some("T"), embedding(1)),
        ("B".to_string(), Some("T"), embedding(2)),
    ];
    for j in 0..SMALL_LEAVES {
        out.push((format!("A{j}"), Some("A"), embedding(3 + j)));
    }
    for j in 0..LARGE_LEAVES {
        out.push((format!("B{j}"), Some("B"), embedding(3 + SMALL_LEAVES + j)));
    }
    out
}

/// The codes under `root`, reflexively.
fn subtree(root: &str) -> Vec<String> {
    terms()
        .into_iter()
        .filter(|(code, parent, _)| {
            root == "T" || code == root || parent.is_some_and(|p| p == root)
        })
        .map(|(code, _, _)| code)
        .collect()
}

/// `vector_options` is the OPTIONS map's tail after `dimensions` and
/// `similarity`, so a test can make the graph search cheap enough to win.
fn build(vector_options: &str) -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let mut run = |q: &str| {
        engine
            .execute_mut(q, &mut store, "default")
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    };
    for (code, parent, [x, y]) in terms() {
        match parent {
            None => run(&format!(
                "CREATE (:Term {{code: '{code}', emb: [{x}, {y}]}})"
            )),
            Some(p) => run(&format!(
                "MATCH (p:Term {{code: '{p}'}}) \
                 CREATE (:Term {{code: '{code}', emb: [{x}, {y}]}})-[:IS_A]->(p)"
            )),
        }
    }
    run(&format!(
        "CREATE VECTOR INDEX emb FOR (n:Term) ON (n.emb) \
         OPTIONS {{dimensions: 2, similarity: 'l2'{vector_options}}}"
    ));
    run("CREATE HIERARCHY INDEX h ON ()-[:IS_A]->()");
    store
}

const QUERY_POINT: [f64; 2] = [0.5, 0.5];

fn scoped_query(root: &str, k: usize) -> String {
    let [x, y] = QUERY_POINT;
    format!(
        "CALL db.index.vector.queryNodes('Term', 'emb', [{x}, {y}], {k}) YIELD node, distance \
         MATCH (r:Term {{code: '{root}'}}) WHERE subsumes(node, r) \
         RETURN node.code AS c, distance ORDER BY distance, c"
    )
}

/// The plan tree as EXPLAIN prints it, refusing one that stops at an operator
/// with no description, so that a "does not contain" holds for the whole tree
/// (#1826).
fn explain(store: &GraphStore, query: &str) -> String {
    let parsed = samyama::query::parse_query(query).unwrap();
    let plan = samyama::query::executor::planner::QueryPlanner::new()
        .plan(&parsed, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let text = plan.root.describe().format(0);
    assert!(
        samyama::query::executor::undescribed_plan_lines(&text).is_empty(),
        "{text}"
    );
    text
}

fn codes(store: &GraphStore, query: &str) -> Vec<String> {
    let batch = QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    batch
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(v) => v
                .as_property()
                .and_then(|p| p.as_string())
                .unwrap_or_default()
                .to_string(),
            None => panic!("no column c in {r:?}"),
        })
        .collect()
}

/// The k nearest codes under `root` by exact L2 distance, ties by code.
fn exact(root: &str, k: usize) -> Vec<String> {
    let under = subtree(root);
    let mut scored: Vec<(f64, String)> = terms()
        .into_iter()
        .filter(|(code, _, _)| under.contains(code))
        .map(|(code, _, [x, y])| {
            let d = ((x - QUERY_POINT[0]).powi(2) + (y - QUERY_POINT[1]).powi(2)).sqrt();
            (d, code)
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, c)| c).collect()
}

#[test]
fn a_small_subtree_is_enumerated_and_a_large_one_searched() {
    // m = 2 and ef = 8 make the graph search cheap, so the whole tree goes
    // vector-first while five nodes out of 201 still go hierarchy-first.
    let store = build(", m: 2, ef: 8");

    let small = explain(&store, &scoped_query("A", 2));
    assert!(small.contains("order=hierarchy-first"), "{small}");
    assert!(small.contains("subtree=5"), "{small}");
    assert!(
        small.contains("hierarchy: subsumes(node, r) via h"),
        "{small}"
    );

    let large = explain(&store, &scoped_query("T", 2));
    assert!(large.contains("order=vector-first"), "{large}");
    assert!(large.contains("subtree=201"), "{large}");
}

#[test]
fn hierarchy_first_returns_the_exact_k_nearest_in_the_subtree() {
    let store = build("");
    for (root, k) in [("A", 3), ("A", 10), ("B", 1), ("B", 7)] {
        let q = scoped_query(root, k);
        assert!(explain(&store, &q).contains("order=hierarchy-first"), "{q}");
        assert_eq!(codes(&store, &q), exact(root, k), "{q}");
    }
}

#[test]
fn vector_first_still_fills_k_from_inside_the_subtree() {
    // Approximate, so only what the search guarantees is asserted: k rows,
    // all of them under the root. Before #445 the subtree test ran after a
    // global top-k and a small branch came back short.
    let store = build(", m: 2, ef: 8");
    for root in ["T", "B"] {
        let q = scoped_query(root, 5);
        let got = codes(&store, &q);
        assert_eq!(got.len(), 5, "{q}: {got:?}");
        let under = subtree(root);
        assert!(got.iter().all(|c| under.contains(c)), "{q}: {got:?}");
    }
}

#[test]
fn a_small_subtree_gets_k_rows_where_the_global_top_k_would_miss_it() {
    let store = build("");
    // The global 3 nearest are not all in A's subtree, so filtering them would
    // come back short; the scoped search returns three of A's.
    let global: Vec<String> = exact("T", 3);
    let under_a = subtree("A");
    assert!(
        global.iter().any(|c| !under_a.contains(c)),
        "fixture: the global top-3 {global:?} should leave A's subtree"
    );
    assert_eq!(codes(&store, &scoped_query("A", 3)), exact("A", 3));
}

#[test]
fn an_ambiguous_anchor_is_not_scoped() {
    // Two nodes match the pin, so the MATCH contributes two rows and the
    // search cannot be scoped under either.
    let mut store = build("");
    QueryEngine::new()
        .execute_mut("CREATE (:Term {code: 'A'})", &mut store, "default")
        .unwrap();
    let plan = explain(&store, &scoped_query("A", 3));
    assert!(!plan.contains("hierarchy:"), "{plan}");
}
