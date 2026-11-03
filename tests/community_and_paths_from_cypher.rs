//! Louvain, modularity, path enumeration, random walk, ArticleRank (ALGO-01).

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph(n: usize, edges: &[(usize, usize)]) -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let x = s.create_node_with_labels([Label::new("N")]);
        s.set_node_property("default", x, "name", PropertyValue::String(format!("n{i}"))).unwrap();
        ids.push(x);
    }
    for &(a, b) in edges { s.create_edge(ids[a], ids[b], "R").unwrap(); }
    (s, ids)
}

fn rows(store: &GraphStore, cypher: &str) -> Vec<Vec<String>> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store).execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let cols = r.columns.clone();
    r.records.iter().map(|rec| cols.iter().map(|c| format!("{:?}", rec.get(c))).collect()).collect()
}

fn fails(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(cypher).unwrap();
    format!("{:?}", QueryExecutor::new(store).execute(&q).unwrap_err())
}

/// Two triangles joined by one edge.
fn two_cliques() -> (GraphStore, Vec<NodeId>) {
    graph(6, &[(0,1),(1,2),(2,0),(3,4),(4,5),(5,3),(2,3)])
}

#[test]
fn louvain_finds_the_two_cliques_and_reports_its_modularity() {
    let (s, _) = two_cliques();
    let r = rows(&s, "CALL algo.louvain() YIELD node, communityId, modularity \
                      RETURN node.name AS n, communityId AS c, modularity AS q");
    assert_eq!(r.len(), 6);
    let comms: Vec<&str> = r.iter().map(|x| x[1].as_str()).collect();
    let distinct: std::collections::HashSet<_> = comms.iter().collect();
    assert_eq!(distinct.len(), 2, "two triangles are two communities: {r:?}");
    // The score travels with the partition: a community id alone says nothing
    // about whether the partition is any good.
    assert!(r[0][2].contains("0.357"), "modularity should be ~0.3571: {r:?}");
}

#[test]
fn modularity_scores_a_partition_the_caller_supplies() {
    let (s, ids) = two_cliques();
    let part: String = ids.iter().enumerate()
        .map(|(i, id)| format!("[{}, {}]", id.as_u64(), if i < 3 { 0 } else { 1 }))
        .collect::<Vec<_>>().join(", ");
    let r = rows(&s, &format!(
        "CALL algo.modularity([{part}]) YIELD modularity RETURN modularity"));
    assert!(r[0][0].contains("0.357"), "{r:?}");
}

#[test]
fn an_incomplete_partition_is_refused_rather_than_defaulted() {
    // Defaulting the absentees into one community would silently score a
    // different partition than the caller gave.
    let (s, ids) = two_cliques();
    let e = fails(&s, &format!(
        "CALL algo.modularity([[{}, 0]]) YIELD modularity RETURN modularity", ids[0].as_u64()));
    assert!(e.contains("no community"), "{e}");
    assert!(e.contains("complete"), "{e}");
}

#[test]
fn label_propagation_is_routed_to_cdlp_rather_than_duplicated() {
    let (s, _) = two_cliques();
    assert_eq!(
        rows(&s, "CALL algo.labelPropagation() YIELD node, communityId RETURN node.name AS n, communityId AS c"),
        rows(&s, "CALL algo.cdlp() YIELD node, communityId RETURN node.name AS n, communityId AS c"),
    );
}

#[test]
fn all_shortest_paths_returns_every_route_not_one() {
    // A diamond: two equally short ways from 0 to 3. `shortestPath` would
    // report one and a caller could not tell the pair apart from a pair with
    // a single route.
    let (s, ids) = graph(4, &[(0,1),(0,2),(1,3),(2,3)]);
    let r = rows(&s, &format!(
        "CALL algo.allShortestPaths({}, {}) YIELD path, rank, cost RETURN rank, cost",
        ids[0].as_u64(), ids[3].as_u64()));
    assert_eq!(r.len(), 2, "both routes: {r:?}");
    assert!(r[0][1].contains("Integer(2)"), "two hops each: {r:?}");
}

#[test]
fn yens_returns_the_second_best_route_too() {
    // Which matters when the best one is the thing that just failed.
    let (s, ids) = graph(5, &[(0,1),(1,4),(0,2),(2,4),(0,3),(3,4)]);
    let r = rows(&s, &format!(
        "CALL algo.yens({}, {}, 3) YIELD path, rank, cost RETURN rank, cost",
        ids[0].as_u64(), ids[4].as_u64()));
    assert_eq!(r.len(), 3, "three distinct routes exist: {r:?}");
    assert!(r[0][0].contains("Integer(1)") && r[2][0].contains("Integer(3)"), "{r:?}");
}

#[test]
fn a_star_without_a_heuristic_is_dijkstra_and_says_so() {
    let (s, ids) = graph(4, &[(0,1),(1,2),(2,3)]);
    let r = rows(&s, &format!(
        "CALL algo.aStar({}, {}) YIELD path, cost RETURN cost",
        ids[0].as_u64(), ids[3].as_u64()));
    assert_eq!(r.len(), 1);
    assert!(r[0][0].contains("3"), "three unit-weight hops: {r:?}");
}

#[test]
fn a_random_walk_repeats_for_the_same_seed_and_differs_for_another() {
    // A sampled result nobody can reproduce is not a result.
    let (s, ids) = graph(6, &[(0,1),(1,2),(2,0),(0,3),(3,4),(4,5),(5,0)]);
    let q = |seed: i64| format!(
        "CALL algo.randomWalk({}, {{steps: 12, seed: {seed}}}) YIELD node, step \
         RETURN node.name AS n, step", ids[0].as_u64());
    assert_eq!(rows(&s, &q(7)), rows(&s, &q(7)), "same seed must repeat");
    assert_ne!(rows(&s, &q(7)), rows(&s, &q(99)), "a different seed should differ");
}

#[test]
fn a_walk_that_cannot_continue_stops_rather_than_teleporting() {
    let (s, ids) = graph(3, &[(0,1),(1,2)]);
    let r = rows(&s, &format!(
        "CALL algo.randomWalk({}, {{steps: 50, seed: 1}}) YIELD node, step RETURN step",
        ids[0].as_u64()));
    assert_eq!(r.len(), 3, "0 -> 1 -> 2 and then nowhere: {r:?}");
}

#[test]
fn article_rank_discounts_a_prolific_linker() {
    // n0 links to everyone; n1 links only to n5. ArticleRank adds the average
    // out-degree to each denominator, so n5's vote from the focused linker
    // counts for more than a vote from the indiscriminate one.
    let (s, _) = graph(6, &[(0,1),(0,2),(0,3),(0,4),(0,5),(1,5)]);
    let r = rows(&s, "CALL algo.articleRank() YIELD node, score \
                      RETURN node.name AS n, score");
    assert_eq!(r.len(), 6);
    assert!(r[0][0].contains("n5"), "n5 has two in-links and should lead: {r:?}");
}
