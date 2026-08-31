//! Which graph algorithms are callable from Cypher, by execution (ALGO-01).
//!
//! ALGO-01 asks for a *count* of production algorithms callable from Cypher —
//! 12 at baseline, 40 at H1. Counting them by reading the source is exactly
//! the mistake that put a false claim in ADR-037: a `match` arm can name an
//! algorithm the executor never reaches, and a filtered search cannot show
//! what it filtered out.
//!
//! So each name is *called*. A name is counted only if `CALL <name>(…)` plans
//! and executes against a real graph. Three outcomes are distinguished, and
//! the distinction is the useful part:
//!
//! * **callable** — planned and ran.
//! * **rejected** — reached the operator and was refused, which means the
//!   dispatcher knows the name but the algorithm is not there.
//! * **unknown** — no dispatch at all.
//!
//! The candidate list deliberately includes algorithms we do **not** have, so
//! the output measures the distance to 40 rather than confirming what is
//! already known. An empty gap list would mean the list was written from the
//! implementation, which would make the measurement circular.
//!
//! ```bash
//! cargo run --release --example algo_coverage -- --json out.json
//! ```

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;

/// A small weighted, directed graph with triangles and two components, so an
/// algorithm that runs has something to find and cannot trivially return
/// nothing.
fn fixture() -> (GraphStore, Vec<u64>) {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..6 {
        let n = s.create_node_with_labels([Label::new("Person")]);
        s.set_node_property("default", n, "name", PropertyValue::String(format!("p{i}")))
            .unwrap();
        ids.push(n.as_u64());
        let _ = i;
    }
    let nodes: Vec<_> = ids.iter().map(|i| samyama::graph::NodeId(*i)).collect();
    // A triangle, a tail, and a disconnected pair.
    for (a, b) in [(0, 1), (1, 2), (2, 0), (2, 3), (4, 5)] {
        let e = s.create_edge(nodes[a], nodes[b], "KNOWS").unwrap();
        s.set_edge_property(e, "capacity", PropertyValue::Integer(3)).unwrap();
        s.set_edge_property(e, "weight", PropertyValue::Float(1.5)).unwrap();
    }
    (s, ids)
}

/// Aliases: a second spelling of an algorithm already counted.
///
/// ALGO-01 counts **algorithms**, not names. `algo.betweenness` and
/// `algo.betweennessCentrality` are one algorithm with two spellings, and
/// counting both would inflate the number against a target of 40 by however
/// many aliases we felt like adding -- a measurement that improves when
/// nothing is built.
const ALIASES: &[&str] = &[
    "betweennessCentrality", "closenessCentrality", "commonNeighbours", "coreNumber", "degreeCentrality", "eigenvectorCentrality", "findCycle", "harmonicCentrality",
];

/// Every candidate, with a call that would work if the algorithm existed.
/// `{a}` and `{b}` are node ids.
const CANDIDATES: &[(&str, &str)] = &[
    // Dispatched today.
    ("pageRank", "CALL algo.pageRank('Person', 'KNOWS') YIELD node, score RETURN count(*)"),
    ("shortestPath", "CALL algo.shortestPath({a}, {b}) YIELD path, cost RETURN count(*)"),
    ("wcc", "CALL algo.wcc() YIELD node, component RETURN count(*)"),
    ("scc", "CALL algo.scc() YIELD node, component RETURN count(*)"),
    ("weightedPath", "CALL algo.weightedPath({a}, {b}, 'weight') YIELD path, cost RETURN count(*)"),
    ("maxFlow", "CALL algo.maxFlow({a}, {b}, 'capacity') YIELD max_flow RETURN count(*)"),
    ("mst", "CALL algo.mst('weight') YIELD source, target, weight RETURN count(*)"),
    ("triangleCount", "CALL algo.triangleCount() YIELD count RETURN count(*)"),
    ("cdlp", "CALL algo.cdlp() YIELD node, community RETURN count(*)"),
    ("lcc", "CALL algo.lcc() YIELD node, coefficient RETURN count(*)"),
    ("or.solve", "CALL algo.or.solve({}) YIELD result RETURN count(*)"),
    // In the algorithms crate but not obviously reachable from Cypher.
    ("pca", "CALL algo.pca() YIELD node, component RETURN count(*)"),
    ("bfs", "CALL algo.bfs({a}) YIELD node RETURN count(*)"),
    ("dijkstra", "CALL algo.dijkstra({a}, {b}, 'weight') YIELD path RETURN count(*)"),
    // Common in other engines and named by the spec's 40. Expected absent --
    // the list is written from what a user would reach for, not from ours.
    ("betweenness", "CALL algo.betweenness() YIELD node, score RETURN count(*)"),
    ("closeness", "CALL algo.closeness() YIELD node, score RETURN count(*)"),
    ("degree", "CALL algo.degree() YIELD node, score RETURN count(*)"),
    ("betweennessCentrality", "CALL algo.betweennessCentrality() YIELD node, score RETURN count(*)"),
    ("closenessCentrality", "CALL algo.closenessCentrality() YIELD node, score RETURN count(*)"),
    ("degreeCentrality", "CALL algo.degreeCentrality() YIELD node, score RETURN count(*)"),
    ("eigenvector", "CALL algo.eigenvector() YIELD node, score RETURN count(*)"),
    ("harmonic", "CALL algo.harmonic() YIELD node, score RETURN count(*)"),
    ("harmonicCentrality", "CALL algo.harmonicCentrality() YIELD node, score RETURN count(*)"),
    ("eigenvectorCentrality", "CALL algo.eigenvectorCentrality() YIELD node, score RETURN count(*)"),
    ("coreNumber", "CALL algo.coreNumber() YIELD node, score RETURN count(*)"),
    // Whole-graph shape. Two of these return a single scalar rather than a
    // score per node, which is why they are separate algorithms rather than
    // readings of one -- a caller wants a number or a column, not both.
    ("eccentricity", "CALL algo.eccentricity() YIELD node, eccentricity RETURN count(*)"),
    ("diameter", "CALL algo.diameter() YIELD diameter RETURN count(*)"),
    ("radius", "CALL algo.radius() YIELD radius RETURN count(*)"),
    ("averageNeighborDegree", "CALL algo.averageNeighborDegree() YIELD node, score RETURN count(*)"),
    ("degreeAssortativity", "CALL algo.degreeAssortativity() YIELD assortativity RETURN count(*)"),
    ("articleRank", "CALL algo.articleRank() YIELD node, score RETURN count(*)"),
    ("louvain", "CALL algo.louvain() YIELD node, community RETURN count(*)"),
    ("labelPropagation", "CALL algo.labelPropagation() YIELD node, community RETURN count(*)"),
    ("modularity", "CALL algo.modularity() YIELD community, score RETURN count(*)"),
    ("kCore", "CALL algo.kCore() YIELD node, core RETURN count(*)"),
    ("kMeans", "CALL algo.kMeans() YIELD node, cluster RETURN count(*)"),
    ("nodeSimilarity", "CALL algo.nodeSimilarity() YIELD node1, node2, similarity RETURN count(*)"),
    ("jaccard", "CALL algo.jaccard({a}, {b}) YIELD node1, node2, score RETURN count(*)"),
    ("adamicAdar", "CALL algo.adamicAdar({a}, {b}) YIELD node1, node2, score RETURN count(*)"),
    ("commonNeighbors", "CALL algo.commonNeighbors({a}, {b}) YIELD node1, node2, score RETURN count(*)"),
    ("commonNeighbours", "CALL algo.commonNeighbours({a}, {b}) YIELD node1, node2, score RETURN count(*)"),
    ("allShortestPaths", "CALL algo.allShortestPaths({a}, {b}) YIELD path RETURN count(*)"),
    ("aStar", "CALL algo.aStar({a}, {b}, 'weight') YIELD path RETURN count(*)"),
    ("yens", "CALL algo.yens({a}, {b}, 3) YIELD path RETURN count(*)"),
    ("randomWalk", "CALL algo.randomWalk({a}, 5) YIELD node RETURN count(*)"),
    ("node2vec", "CALL algo.node2vec() YIELD node, embedding RETURN count(*)"),
    ("fastRP", "CALL algo.fastRP() YIELD node, embedding RETURN count(*)"),
    ("graphSage", "CALL algo.graphSage() YIELD node, embedding RETURN count(*)"),
    ("topologicalSort", "CALL algo.topologicalSort() YIELD node, position RETURN count(*)"),
    ("cycleDetection", "CALL algo.cycleDetection() YIELD node, position RETURN count(*)"),
    ("findCycle", "CALL algo.findCycle() YIELD node, position RETURN count(*)"),
    ("bridges", "CALL algo.bridges() YIELD source, target RETURN count(*)"),
    ("articulationPoints", "CALL algo.articulationPoints() YIELD node RETURN count(*)"),
    // The four temporal/causal primitives ALGO-15 names, and which ALGO-01's
    // target of 40 counts explicitly.
    ("temporalReachability", "CALL algo.temporalReachability({a}) YIELD node RETURN count(*)"),
    ("temporalShortestPath", "CALL algo.temporalShortestPath({a}, {b}) YIELD path RETURN count(*)"),
    ("propagationRanking", "CALL algo.propagationRanking({a}) YIELD node, rank RETURN count(*)"),
    ("symptomExplanation", "CALL algo.symptomExplanation([[{b}, 9999]]) YIELD node, explains RETURN count(*)"),
    // Plausible aliases for the two above that we do *not* provide. Kept in
    // the list because the point of it is to name what a user would reach for,
    // and someone coming from an RCA tool reaches for these.
    ("causalAncestors", "CALL algo.causalAncestors({a}) YIELD node RETURN count(*)"),
    ("causalDescendants", "CALL algo.causalDescendants({a}) YIELD node RETURN count(*)"),
];

fn main() {
    let (mut store, ids) = fixture();
    let (a, b) = (ids[0], ids[3]);

    let mut callable = Vec::new();
    let mut rejected = Vec::new();
    let mut unknown = Vec::new();

    for (name, template) in CANDIDATES {
        let cypher = template.replace("{a}", &a.to_string()).replace("{b}", &b.to_string());
        let outcome = match parse_query(&cypher) {
            Err(e) => format!("parse: {e}"),
            // The *mutating* executor, so a write-requiring algorithm is
            // measured on the same footing as the rest. Under the read
            // executor `algo.or.solve` failed with "requires write access",
            // which is a fact about the probe rather than about coverage.
            Ok(q) => match MutQueryExecutor::new(&mut store, "default".to_string()).execute(&q) {
                Ok(_) => String::new(),
                Err(e) => format!("{e:?}"),
            },
        };
        if outcome.is_empty() {
            callable.push(*name);
        } else if outcome.contains("Unknown procedure") || outcome.contains("Unknown algorithm") {
            // Both mean the same thing to a caller. The two messages exist
            // because an `algo.*` prefix routes to the operator whatever
            // follows it, so an unrecognised name reaches the operator and is
            // refused there rather than at the planner. Classifying on the
            // planner's message alone put every missing algorithm in the
            // "known" bucket, which flattered the count.
            unknown.push(*name);
        } else {
            // Known to the dispatcher and not callable *here* -- a write-only
            // algorithm under a read executor, or one that this fixture does
            // not suit. Kept separate because it is not a coverage gap.
            rejected.push(format!("{name}: {}", &outcome[..outcome.len().min(90)]));
        }
    }

    let distinct: Vec<&&str> = callable.iter().filter(|n| !ALIASES.contains(n)).collect();
    let json = serde_json::json!({
        "target_h1": 40,
        "callable_count": distinct.len(),
        "callable_names_including_aliases": callable.len(),
        "aliases_not_counted": callable.iter().filter(|n| ALIASES.contains(n)).collect::<Vec<_>>(),
        "callable": callable,
        "known_but_not_callable": rejected,
        "unknown_to_the_dispatcher": unknown,
        "candidates_probed": CANDIDATES.len(),
    });
    let out = std::env::args().collect::<Vec<_>>();
    let path = out.iter().position(|a| a == "--json").and_then(|i| out.get(i + 1));
    let text = serde_json::to_string_pretty(&json).unwrap();
    match path {
        Some(p) => std::fs::write(p, &text).unwrap(),
        None => println!("{text}"),
    }
    eprintln!(
        "{} distinct algorithms callable from Cypher ({} names incl. aliases), \
         of {} probed. ALGO-01 H1 target: 40",
        distinct.len(), callable.len(), CANDIDATES.len()
    );
}
