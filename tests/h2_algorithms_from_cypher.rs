//! The 25 algorithms added for the H2 coverage target, through Cypher.
//!
//! The unit tests in `samyama-graph-algorithms` prove the algorithms. These
//! prove the *procedures*: that the dispatch reaches them, that the YIELD
//! names match what the operator binds, and that the refusals are refusals
//! rather than empty results.
//!
//! The YIELD names matter more than they look. `algo.temporalShortestPath`
//! yields `path, times, arrival` while its three siblings yield `node, time`,
//! and a wrong name there fails *only when the query succeeds* — no rows means
//! nothing reads the binding. Every procedure below is called with the names
//! it actually binds, and a test that returns rows is what proves it.

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
    for &(a, b) in edges {
        s.create_edge(ids[a], ids[b], "R").unwrap();
    }
    (s, ids)
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

fn scalar(store: &GraphStore, cypher: &str, col: &str) -> f64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store).execute(&q).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    match r.records[0].get(col) {
        Some(Value::Property(PropertyValue::Float(f))) => *f,
        Some(Value::Property(PropertyValue::Integer(i))) => *i as f64,
        other => panic!("{cypher}: {col} was {other:?}"),
    }
}

fn err(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    format!("{}", QueryExecutor::new(store).execute(&q).unwrap_err())
}

/// A path 0->1->2->3, acyclic and connected.
fn path4() -> (GraphStore, Vec<NodeId>) { graph(4, &[(0, 1), (1, 2), (2, 3)]) }

/// A directed triangle: cyclic, strongly connected.
fn triangle() -> GraphStore { graph(3, &[(0, 1), (1, 2), (2, 0)]).0 }

// ---- Ranking

#[test]
fn katz_returns_a_score_per_node() {
    let (s, _) = path4();
    assert_eq!(rows(&s, "CALL algo.katz({alpha: 0.05}) YIELD node, score RETURN node, score"), 4);
}

#[test]
fn katz_refuses_a_divergent_alpha_rather_than_ranking_on_it() {
    // alpha above 1/lambda_max makes the series diverge. The last iterate is
    // still a well-formed vector, so returning it would hand back a ranking
    // computed from a divergent sum -- plausible and meaningless.
    let s = triangle();
    let e = err(&s, "CALL algo.katz({alpha: 5.0}) YIELD node, score RETURN node");
    assert!(e.contains("did not converge"), "{e}");
    assert!(e.contains("ArgumentError"), "should carry the argument code: {e}");
}

#[test]
fn hits_yields_hub_and_authority_together() {
    // Both from one call: a hub is defined by the authorities it points at and
    // vice versa, so two procedures would run the same fixed point twice and
    // invite comparing hubs from one run with authorities from another.
    let (s, _) = path4();
    assert_eq!(rows(&s, "CALL algo.hits() YIELD node, hub, authority RETURN node, hub, authority"), 4);
}

#[test]
fn personalized_page_rank_keeps_mass_near_the_source() {
    // Two disjoint edges. Personalising on the first component must leave the
    // second at zero -- if it does not, the dangling mass is being spread
    // uniformly and the personalisation is decorative.
    let (s, ids) = graph(4, &[(0, 1), (2, 3)]);
    let q = format!(
        "CALL algo.personalizedPageRank([{}]) YIELD node, score \
         WHERE node.name = \"n3\" RETURN score", ids[0].as_u64());
    assert!(scalar(&s, &q, "score") < 0.01, "far component should get no mass");
}

#[test]
fn voterank_yields_election_order() {
    let (s, _) = graph(7, &[(1, 0), (2, 0), (3, 0), (5, 4), (6, 4)]);
    assert_eq!(rows(&s, "CALL algo.voteRank({k: 2}) YIELD node, rank RETURN node, rank"), 2);
}

// ---- Paths

#[test]
fn bellman_ford_reaches_every_node_on_a_path() {
    let (s, ids) = path4();
    let q = format!("CALL algo.bellmanFord({}) YIELD node, distance RETURN node", ids[0].as_u64());
    assert_eq!(rows(&s, &q), 4);
}

#[test]
fn all_pairs_reports_only_reachable_pairs() {
    // 0->1 and an isolated 2. Six ordered pairs exist; one is reachable.
    // Storing infinity for the rest is how an unreachable pair ends up inside
    // an average.
    let (s, _) = graph(3, &[(0, 1)]);
    assert_eq!(rows(&s, "CALL algo.allPairsShortestPath() YIELD source, target, hops RETURN hops"), 1);
}

#[test]
fn wiener_index_refuses_a_graph_in_pieces_and_points_at_the_alternative() {
    let (s, _) = graph(3, &[(0, 1)]);
    let e = err(&s, "CALL algo.wienerIndex() YIELD wienerIndex RETURN wienerIndex");
    assert!(e.contains("unreachable"), "{e}");
    // The message names what to use instead. An error that says only "no"
    // makes the caller guess.
    assert!(e.contains("globalEfficiency"), "{e}");
}

#[test]
fn dag_longest_path_refuses_a_cycle_and_names_the_tool_to_find_it() {
    let s = triangle();
    let e = err(&s, "CALL algo.dagLongestPath() YIELD node, position RETURN node");
    assert!(e.contains("cycle"), "{e}");
    assert!(e.contains("findCycle"), "{e}");
}

#[test]
fn transitive_closure_keeps_the_self_pair_of_a_cycle() {
    // Every node reaches every node including itself, so 9 pairs on a
    // triangle. The self-pair is the fact that the node is on a cycle.
    let s = triangle();
    assert_eq!(rows(&s, "CALL algo.transitiveClosure() YIELD source, target RETURN source"), 9);
    // A path has no self-pairs: 0->1,0->2,0->3,1->2,1->3,2->3 is 6.
    let (p, _) = path4();
    assert_eq!(rows(&p, "CALL algo.transitiveClosure() YIELD source, target RETURN source"), 6);
}

// ---- Structure

#[test]
fn bipartite_splits_an_even_cycle_and_refuses_an_odd_one() {
    let (square, _) = graph(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
    assert_eq!(rows(&square, "CALL algo.bipartite() YIELD node, side RETURN node, side"), 4);
    let tri = triangle();
    let e = err(&tri, "CALL algo.bipartite() YIELD node, side RETURN node");
    assert!(e.contains("odd cycle"), "{e}");
}

#[test]
fn matching_and_colouring_and_dominating_set_all_return_rows() {
    let (s, _) = graph(4, &[(0, 1), (1, 2), (2, 3)]);
    assert_eq!(rows(&s, "CALL algo.maximalMatching() YIELD source, target RETURN source"), 2);
    assert_eq!(rows(&s, "CALL algo.colouring() YIELD node, colour RETURN node"), 4);
    assert!(rows(&s, "CALL algo.dominatingSet() YIELD node RETURN node") >= 1);
}

#[test]
fn k_truss_drops_a_cycle_that_k_core_keeps() {
    // A 4-cycle is 2-core and triangle-free, so the 3-truss is empty. This is
    // the case that makes truss a different question rather than a rename.
    let (cycle, _) = graph(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
    assert_eq!(rows(&cycle, "CALL algo.kTruss({k: 3}) YIELD node RETURN node"), 0);
    let tri = triangle();
    assert_eq!(rows(&tri, "CALL algo.kTruss({k: 3}) YIELD node RETURN node"), 3);
}

#[test]
fn transitivity_refuses_a_graph_with_no_triples() {
    // 0/0. Answering 0 would make "no triples" and "triples that never close"
    // the same number, and they are different graphs.
    let (s, _) = graph(2, &[(0, 1)]);
    let e = err(&s, "CALL algo.transitivity() YIELD transitivity RETURN transitivity");
    assert!(e.contains("0/0"), "{e}");
    let tri = triangle();
    assert!((scalar(&tri, "CALL algo.transitivity() YIELD transitivity RETURN transitivity",
                    "transitivity") - 1.0).abs() < 1e-9);
}

#[test]
fn global_efficiency_stays_finite_where_average_distance_would_not() {
    let (s, _) = graph(3, &[(0, 1)]);
    let e = scalar(&s, "CALL algo.globalEfficiency() YIELD efficiency RETURN efficiency",
                   "efficiency");
    assert!(e > 0.0 && e < 1.0, "{e}");
}

#[test]
fn square_clustering_and_rich_club_and_biconnected_return_rows() {
    let (cycle, _) = graph(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
    assert!(rows(&cycle, "CALL algo.squareClustering() YIELD node, coefficient RETURN node") > 0);
    assert!(scalar(&cycle, "CALL algo.richClub({k: 1}) YIELD coefficient RETURN coefficient",
                   "coefficient") >= 0.0);
    // Two triangles sharing node 2: two biconnected components, and `bridges`
    // finds nothing here because no single *edge* disconnects them.
    let (bow, _) = graph(5, &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 2)]);
    assert_eq!(rows(&bow, "CALL algo.biconnectedComponents() YIELD node, componentId RETURN node"), 6);
}

// ---- Similarity

#[test]
fn node_similarity_includes_pairs_that_are_already_connected() {
    // Link prediction excludes joined pairs by construction. "Who else is like
    // this" must not.
    let (s, _) = graph(3, &[(0, 1), (0, 2), (1, 2)]);
    assert!(rows(&s, "CALL algo.nodeSimilarity({topK: 3}) YIELD node, other, similarity RETURN node") > 0);
}

#[test]
fn overlap_and_cosine_and_structural_holes_return_rows() {
    let (s, _) = graph(4, &[(0, 2), (1, 2), (1, 3)]);
    assert!(rows(&s, "CALL algo.overlap() YIELD node, other, similarity RETURN node") > 0);
    assert!(rows(&s, "CALL algo.cosine() YIELD node, other, similarity RETURN node") > 0);
    assert!(rows(&s, "CALL algo.effectiveSize() YIELD node, value RETURN node") > 0);
    assert!(rows(&s, "CALL algo.constraint() YIELD node, value RETURN node") > 0);
}

#[test]
fn reciprocity_counts_edges_whose_reverse_exists() {
    // 0<->1 mutual, 1->2 one-way: 2 of 3.
    let (s, _) = graph(3, &[(0, 1), (1, 0), (1, 2)]);
    let r = scalar(&s, "CALL algo.reciprocity() YIELD reciprocity RETURN reciprocity", "reciprocity");
    assert!((r - 2.0 / 3.0).abs() < 1e-9, "{r}");
}

#[test]
fn every_new_procedure_is_reachable_by_at_least_one_spelling() {
    // The dispatch table and `KNOWN_FUNCTIONS` are separate lists, and a name
    // in one and not the other is refused at a different layer with a
    // different message. This asserts the pair agree for every alias shipped.
    let (s, _) = graph(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
    for call in [
        "CALL algo.katzCentrality({alpha: 0.05}) YIELD node RETURN count(*)",
        "CALL algo.hubsAndAuthorities() YIELD node RETURN count(*)",
        "CALL algo.personalisedPageRank([]) YIELD node RETURN count(*)",
        "CALL algo.allPairs() YIELD source RETURN count(*)",
        "CALL algo.matching() YIELD source RETURN count(*)",
        "CALL algo.coloring() YIELD node RETURN count(*)",
        "CALL algo.truss({k: 3}) YIELD node RETURN count(*)",
        "CALL algo.richClubCoefficient({k: 1}) YIELD coefficient RETURN coefficient",
        "CALL algo.biconnected() YIELD node RETURN count(*)",
        "CALL algo.overlapCoefficient() YIELD node RETURN count(*)",
        "CALL algo.cosineSimilarity() YIELD node RETURN count(*)",
        "CALL algo.burtConstraint() YIELD node RETURN count(*)",
    ] {
        let q = parse_query(call).unwrap_or_else(|e| panic!("{call}: {e:?}"));
        assert!(QueryExecutor::new(&s).execute(&q).is_ok(), "alias not reachable: {call}");
    }
}
