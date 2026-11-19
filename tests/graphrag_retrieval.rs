//! `algo.graphRag` / `algo.retrieve` from Cypher: vector search, graph
//! expansion, hierarchy and time in one call, with three strategies (AI-07,
//! #1702).
//!
//! The graph is two topics. Each has two text chunks with an embedding and a
//! year, joined by `NEXT`, both mentioning an entity, which is related to a
//! second entity; one `RELATED` edge joins the topics' second entities:
//!
//! ```text
//!   topic X                            topic Y
//!   cx1 [1,0] 2020 -NEXT-> cx2 [.9,.1] 2024        cy1 [0,1] 2024 -NEXT-> cy2 [.1,.9] 2023
//!      \                  /                           \                  /
//!       MENTIONS -> ex1 <- MENTIONS                     MENTIONS -> ey1 <- MENTIONS
//!                    |                                               |
//!                 RELATED -> ex2 -------- RELATED --------> ey2 <- RELATED
//! ```

use std::collections::HashMap;

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn run(store: &mut GraphStore, q: &str) {
    QueryEngine::new().execute_mut(q, store, "default").unwrap_or_else(|e| panic!("{q}: {e}"));
}

/// The graph above, and each node's id by name.
fn topics() -> (GraphStore, HashMap<&'static str, i64>) {
    let mut s = GraphStore::new();
    run(
        &mut s,
        "CREATE (cx1:Chunk {name: 'cx1', emb: [1.0, 0.0], year: 2020}), \
                (cx2:Chunk {name: 'cx2', emb: [0.9, 0.1], year: 2024}), \
                (cy1:Chunk {name: 'cy1', emb: [0.0, 1.0], year: 2024}), \
                (cy2:Chunk {name: 'cy2', emb: [0.1, 0.9], year: 2023}), \
                (ex1:Entity {name: 'ex1'}), (ex2:Entity {name: 'ex2'}), \
                (ey1:Entity {name: 'ey1'}), (ey2:Entity {name: 'ey2'}), \
                (cx1)-[:NEXT]->(cx2), (cx1)-[:MENTIONS]->(ex1), (cx2)-[:MENTIONS]->(ex1), (ex1)-[:RELATED]->(ex2), \
                (cy1)-[:NEXT]->(cy2), (cy1)-[:MENTIONS]->(ey1), (cy2)-[:MENTIONS]->(ey1), (ey1)-[:RELATED]->(ey2), \
                (ex2)-[:RELATED]->(ey2)",
    );
    run(
        &mut s,
        "CREATE VECTOR INDEX chunks FOR (n:Chunk) ON (n.emb) OPTIONS {dimensions: 2, similarity: 'cosine'}",
    );
    let ids = names(&s);
    (s, ids)
}

fn names(s: &GraphStore) -> HashMap<&'static str, i64> {
    let rows = QueryEngine::new().execute("MATCH (n) RETURN n.name AS name, id(n) AS id", s).unwrap();
    let all = ["cx1", "cx2", "cy1", "cy2", "ex1", "ex2", "ey1", "ey2", "catX", "catY", "root"];
    rows.records
        .iter()
        .map(|r| {
            let name = r.get("name").and_then(|v| v.as_property()).and_then(|p| p.as_string()).unwrap().to_string();
            let id = r.get("id").and_then(|v| v.as_property()).and_then(|p| p.as_integer()).unwrap();
            (*all.iter().find(|n| **n == name).expect("a known name"), id)
        })
        .collect()
}

/// One retrieved row.
#[derive(Debug, Clone, PartialEq)]
struct Row {
    node: i64,
    score: f64,
    hops: Option<i64>,
    seed: Option<i64>,
    community: Option<i64>,
    path: Vec<i64>,
}

fn retrieve(s: &GraphStore, procedure: &str, config: &str) -> Result<Vec<Row>, String> {
    let q = format!(
        "CALL {procedure}({config}) YIELD node, score, hops, seed, community, path \
         RETURN id(node) AS node, score, hops, seed, community, path"
    );
    let rows = QueryEngine::new().execute(&q, s).map_err(|e| e.to_string())?;
    let int = |r: &samyama::query::executor::Record, k: &str| r.get(k).and_then(|v| v.as_property()).and_then(|p| p.as_integer());
    Ok(rows
        .records
        .iter()
        .map(|r| Row {
            node: int(r, "node").unwrap(),
            score: r.get("score").and_then(|v| v.as_property()).and_then(|p| p.as_float()).unwrap(),
            hops: int(r, "hops"),
            seed: int(r, "seed"),
            community: int(r, "community"),
            path: match r.get("path").and_then(|v| v.as_property()) {
                Some(samyama::graph::PropertyValue::Array(a)) => a.iter().map(|x| x.as_integer().unwrap()).collect(),
                other => panic!("path is not a list: {other:?}"),
            },
        })
        .collect())
}

fn graph_rag(s: &GraphStore, extra: &str) -> Vec<Row> {
    let config = format!("{{label: 'Chunk', property: 'emb', {extra}}}");
    retrieve(s, "algo.graphRag", &config).unwrap_or_else(|e| panic!("{config}: {e}"))
}

fn by_node(rows: &[Row]) -> HashMap<i64, &Row> {
    rows.iter().map(|r| (r.node, r)).collect()
}

/// Every consecutive pair on every path is joined by a relationship, either
/// way: the path is one the graph really has.
fn assert_paths_are_real(s: &GraphStore, rows: &[Row]) {
    for row in rows.iter().filter(|r| r.hops.is_some()) {
        assert_eq!(row.path.first().copied(), row.seed, "{row:?}: the path does not start at the seed");
        assert_eq!(row.path.last().copied(), Some(row.node), "{row:?}: the path does not end at the node");
        assert_eq!(row.path.len() as i64 - 1, row.hops.unwrap(), "{row:?}");
        for pair in row.path.windows(2) {
            let q = format!("MATCH (a)-[]-(b) WHERE id(a) = {} AND id(b) = {} RETURN count(*) AS n", pair[0], pair[1]);
            let n = QueryEngine::new().execute(&q, s).unwrap().records[0]
                .get("n")
                .and_then(|v| v.as_property())
                .and_then(|p| p.as_integer())
                .unwrap();
            assert!(n > 0, "{row:?}: no relationship between {} and {}", pair[0], pair[1]);
        }
    }
}

#[test]
fn local_returns_the_nearest_chunk_and_what_is_within_hops_of_it() {
    let (s, id) = topics();
    let rows = graph_rag(&s, "query: [1.0, 0.0], k: 1, hops: 2");
    let got = by_node(&rows);
    // cx1 is the question exactly; one hop: ex1 and cx2; two: ex2.
    let mut nodes: Vec<i64> = got.keys().copied().collect();
    nodes.sort_unstable();
    let mut want = vec![id["cx1"], id["cx2"], id["ex1"], id["ex2"]];
    want.sort_unstable();
    assert_eq!(nodes, want);
    let seed = got[&id["cx1"]];
    assert_eq!((seed.hops, seed.seed, seed.path.clone()), (Some(0), Some(id["cx1"]), vec![id["cx1"]]));
    assert!((seed.score - 1.0).abs() < 1e-9, "an identical vector scores 1: {}", seed.score);
    for r in &rows {
        let expected = seed.score * 0.5f64.powi(r.hops.unwrap() as i32);
        assert!((r.score - expected).abs() < 1e-12, "{r:?}: score is not similarity * decay^hops");
        assert_eq!(r.community, None, "local reads no communities");
    }
    assert_eq!(got[&id["ex2"]].path, vec![id["cx1"], id["ex1"], id["ex2"]]);
    assert!(rows.windows(2).all(|w| w[0].score >= w[1].score), "not highest score first");
    assert_paths_are_real(&s, &rows);
}

#[test]
fn local_with_two_seeds_keeps_each_nodes_best_reason() {
    let (s, id) = topics();
    let rows = graph_rag(&s, "query: [0.7, 0.7], k: 4, hops: 1, limit: 100");
    // Every node is returned once.
    let mut seen: Vec<i64> = rows.iter().map(|r| r.node).collect();
    seen.sort_unstable();
    let before = seen.len();
    seen.dedup();
    assert_eq!(before, seen.len(), "a node was returned twice");
    // ex1 is one hop from both X chunks; it keeps the better-scoring one.
    let ex1 = by_node(&rows)[&id["ex1"]];
    let seed_score = |n: i64| by_node(&rows)[&n].score;
    let best = seed_score(id["cx1"]).max(seed_score(id["cx2"]));
    assert!((ex1.score - best * 0.5).abs() < 1e-12, "{ex1:?}");
    assert_paths_are_real(&s, &rows);
}

#[test]
fn global_returns_the_community_the_question_is_about() {
    let (s, id) = topics();
    let rows = graph_rag(&s, "query: [1.0, 0.0], k: 2, strategy: 'global', communities: 1");
    let got = by_node(&rows);
    // Both seeds are topic X's chunks; its community is returned whole and
    // topic Y's chunks are not.
    assert!(got.contains_key(&id["cx1"]) && got.contains_key(&id["cx2"]));
    assert!(!got.contains_key(&id["cy1"]) && !got.contains_key(&id["cy2"]));
    let community = rows[0].community.expect("global reports the community");
    assert!(rows.iter().all(|r| r.community == Some(community) && r.hops.is_none() && r.seed.is_none()));
    assert!(rows.iter().all(|r| (r.score - rows[0].score).abs() < 1e-12), "members carry the community's score");
}

#[test]
fn drift_is_local_plus_the_rest_of_the_seeds_community() {
    let (s, id) = topics();
    // `hops: 0`, so the local answer is the seed alone and everything else
    // drift returns came from the seed's community. (With one hop the walk
    // already covers cx2 and ex1, and Louvain puts ex2 -- which sits between
    // the topics -- wherever it scores best, so there may be nothing left.)
    let local = graph_rag(&s, "query: [1.0, 0.0], k: 1, hops: 0");
    let drift = graph_rag(&s, "query: [1.0, 0.0], k: 1, hops: 0, strategy: 'drift'");
    let d = by_node(&drift);
    for r in &local {
        let same = d[&r.node];
        assert_eq!((same.score, same.hops, same.path.clone()), (r.score, r.hops, r.path.clone()), "drift changed a local row");
    }
    let extra: Vec<&Row> = drift.iter().filter(|r| r.hops.is_none()).collect();
    assert!(!extra.is_empty(), "drift added nothing");
    // cx2 shares cx1's chunk, its entity and its `NEXT` edge: whatever else
    // Louvain decides, they are one community.
    assert!(extra.iter().any(|r| r.node == id["cx2"]), "{extra:?}");
    let seed_community = d[&id["cx1"]].community;
    for r in extra {
        assert_eq!(r.community, seed_community, "{r:?} is outside the seed's community");
        assert!((r.score - 0.5).abs() < 1e-12, "{r:?}: one hop past `hops`");
        assert!(r.path.is_empty() && r.seed == Some(id["cx1"]));
    }
}

#[test]
fn a_time_window_keeps_seeds_and_the_walk_inside_it() {
    let (s, id) = topics();
    let rows = graph_rag(&s, "query: [1.0, 0.0], k: 1, hops: 3, timeProperty: 'year', since: 2023");
    let got = by_node(&rows);
    // cx1 (2020) is nearest but outside, so cx2 is the seed. The entities have
    // no year, so they are outside too, and the walk cannot reach topic Y
    // through them.
    assert_eq!(rows.iter().find(|r| r.hops == Some(0)).map(|r| r.node), Some(id["cx2"]));
    assert!(!got.contains_key(&id["cx1"]), "a node before the window was returned");
    assert!(!got.contains_key(&id["ex1"]), "a node with no year was returned");
    assert_eq!(rows.len(), 1, "the walk passed through a node outside the window: {rows:?}");
    let until = graph_rag(&s, "query: [0.0, 1.0], k: 4, hops: 0, timeProperty: 'year', until: 2023");
    let years: Vec<i64> = until.iter().map(|r| r.node).collect();
    assert_eq!(years.len(), 2, "only cx1 (2020) and cy2 (2023) end by 2023: {until:?}");
    assert!(years.contains(&id["cx1"]) && years.contains(&id["cy2"]));
}

#[test]
fn a_hierarchy_scope_takes_seeds_only_from_its_subtree() {
    let (mut s, _) = topics();
    run(
        &mut s,
        "CREATE (root:Category {name: 'root'}), (catX:Category {name: 'catX'}), (catY:Category {name: 'catY'}), \
                (catX)-[:IS_A]->(root), (catY)-[:IS_A]->(root)",
    );
    run(&mut s, "MATCH (c:Chunk), (k:Category {name: 'catX'}) WHERE c.name STARTS WITH 'cx' CREATE (c)-[:IS_A]->(k)");
    run(&mut s, "MATCH (c:Chunk), (k:Category {name: 'catY'}) WHERE c.name STARTS WITH 'cy' CREATE (c)-[:IS_A]->(k)");
    run(&mut s, "CREATE HIERARCHY INDEX topics ON ()-[:IS_A]->()");
    let id = names(&s);
    // Nearest to [1, 0] overall is cx1; under catY it is cy2.
    let rows = graph_rag(&s, &format!("query: [1.0, 0.0], k: 1, hops: 0, hierarchy: 'topics', under: {}", id["catY"]));
    assert_eq!(rows.iter().map(|r| r.node).collect::<Vec<_>>(), vec![id["cy2"]]);
    // The walk may leave the subtree: what a fact connects to need not be in
    // its branch.
    let rows = graph_rag(&s, &format!("query: [1.0, 0.0], k: 1, hops: 1, hierarchy: 'topics', under: {}", id["catY"]));
    assert!(rows.iter().any(|r| r.node == id["ey1"]));
    let err = retrieve(&s, "algo.graphRag", &format!("{{query: [1.0, 0.0], label: 'Chunk', property: 'emb', hierarchy: 'topics', under: {}}}", id["ex1"])).unwrap_err();
    assert!(err.contains("is not in hierarchy"), "{err}");
}

#[test]
fn an_edge_type_limits_the_walk() {
    let (s, id) = topics();
    let rows = graph_rag(&s, "query: [1.0, 0.0], k: 1, hops: 3, edgeType: 'NEXT'");
    let mut nodes: Vec<i64> = rows.iter().map(|r| r.node).collect();
    nodes.sort_unstable();
    let mut want = vec![id["cx1"], id["cx2"]];
    want.sort_unstable();
    assert_eq!(nodes, want, "the walk followed a relationship of another type");
}

#[test]
fn retrieve_is_the_same_procedure_and_the_answer_is_repeatable() {
    let (s, _) = topics();
    let config = "{query: [0.6, 0.4], label: 'Chunk', property: 'emb', k: 3, strategy: 'drift'}";
    let a = retrieve(&s, "algo.graphRag", config).unwrap();
    let b = retrieve(&s, "algo.retrieve", config).unwrap();
    let c = retrieve(&s, "algo.graphRag", config).unwrap();
    assert!(!a.is_empty());
    assert_eq!(a, b, "algo.retrieve answered differently");
    assert_eq!(a, c, "the same call answered differently");
    let limited = retrieve(&s, "algo.graphRag", "{query: [0.6, 0.4], label: 'Chunk', property: 'emb', k: 3, strategy: 'drift', limit: 2}").unwrap();
    assert_eq!(limited, a[..2].to_vec(), "limit is not the top of the full answer");
}

#[test]
fn bad_calls_are_refused_with_the_reason() {
    let (s, _) = topics();
    let base = "label: 'Chunk', property: 'emb'";
    let cases = [
        (format!("{{{base}}}"), "`query` is required"),
        ("{query: [1.0, 0.0]}".to_string(), "`label` and `property` are required"),
        (format!("{{query: 'text', {base}}}"), "`query` must be a non-empty list of numbers"),
        (format!("{{query: [], {base}}}"), "`query` must be a non-empty list of numbers"),
        (format!("{{query: [1.0, 'x'], {base}}}"), "`query` must be a list of numbers"),
        (format!("{{query: [1.0, 0.0], {base}, strategy: 'nearby'}}"), "unknown strategy `nearby`"),
        (format!("{{query: [1.0, 0.0], {base}, hops: 11}}"), "`hops` must be an integer from 0 to 10"),
        (format!("{{query: [1.0, 0.0], {base}, k: 0}}"), "`k` must be an integer from 1 to 1000"),
        (format!("{{query: [1.0, 0.0], {base}, decay: 0}}"), "`decay` must be above 0 and at most 1"),
        (format!("{{query: [1.0, 0.0], {base}, decay: 'half'}}"), "`decay` must be a finite number"),
        (format!("{{query: [1.0, 0.0], {base}, limit: 0}}"), "`limit` must be an integer"),
        (format!("{{query: [1.0, 0.0], {base}, timeProperty: 'year'}}"), "go together"),
        (format!("{{query: [1.0, 0.0], {base}, since: 2020}}"), "go together"),
        (format!("{{query: [1.0, 0.0], {base}, hierarchy: 'h'}}"), "`hierarchy` and `under` go together"),
        (format!("{{query: [1.0, 0.0], {base}, hierarchy: 'none', under: 1}}"), "no usable hierarchy index named `none`"),
        (format!("{{query: [1.0, 0.0], {base}, colour: 'red'}}"), "unknown config key `colour`"),
        ("{query: [1.0, 0.0], label: 'Entity', property: 'emb'}".to_string(), "no vector index on :Entity(emb)"),
        (format!("{{query: [1.0, 0.0, 0.0], {base}}}"), "dimension"),
    ];
    let mut wrong = Vec::new();
    for (config, expected) in &cases {
        match retrieve(&s, "algo.graphRag", config) {
            Ok(rows) => wrong.push(format!("{config}: answered {} rows", rows.len())),
            Err(e) if !e.to_lowercase().contains(&expected.to_lowercase()) => {
                wrong.push(format!("{config}: expected `{expected}`, got {e}"))
            }
            Err(_) => {}
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
