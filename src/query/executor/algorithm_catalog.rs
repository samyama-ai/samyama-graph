//! The algorithms `CALL algo.*` runs, as data a client can read (#2013).
//!
//! A workbench rendering an algorithms page had no way to ask the engine what
//! it can run, so samyama-insight hard-coded eight names while the operator
//! dispatches sixty-eight. This list is what `GET /api/algorithms/catalog`
//! serves, and `AlgorithmOperator::is_algorithm` reads it, so a name cannot be
//! callable without being listed or listed without being callable.
//!
//! `yields` is the set of columns each algorithm's rows carry, measured by
//! running it (see `every_catalogued_algorithm_yields_its_listed_columns`), not
//! copied from doc comments. Parameters are not described yet.

use serde::Serialize;

/// What kind of question an algorithm answers; groups the catalogue for a UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmCategory {
    Centrality,
    Community,
    Path,
    LinkPrediction,
    Similarity,
    Structure,
    GraphMetric,
    Temporal,
    Embedding,
    Optimization,
}

/// One callable algorithm.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct AlgorithmEntry {
    /// The spelling to show; any case and the `samyama.` prefix also resolve.
    pub name: &'static str,
    /// Other names that dispatch to the same implementation.
    pub aliases: &'static [&'static str],
    pub category: AlgorithmCategory,
    /// Every column its rows carry. Where rows differ in shape -- `algo.mst`'s
    /// summary and edge rows, `algo.or.solve`'s single- and multi-objective
    /// results -- this is the union (see those entries).
    pub yields: &'static [&'static str],
    /// Writes to the graph, so it needs write access to call.
    pub writes: bool,
}

use AlgorithmCategory::*;

const fn read(
    name: &'static str,
    aliases: &'static [&'static str],
    category: AlgorithmCategory,
    yields: &'static [&'static str],
) -> AlgorithmEntry {
    AlgorithmEntry { name, aliases, category, yields, writes: false }
}

const fn write(
    name: &'static str,
    category: AlgorithmCategory,
    yields: &'static [&'static str],
) -> AlgorithmEntry {
    AlgorithmEntry { name, aliases: &[], category, yields, writes: true }
}

/// Every algorithm `AlgorithmOperator` runs, in display order.
pub const ALGORITHM_CATALOG: &[AlgorithmEntry] = &[
    // Centrality and ranking.
    read("algo.pageRank", &[], Centrality, &["node", "score"]),
    read("algo.articleRank", &[], Centrality, &["node", "score"]),
    read("algo.personalizedPageRank", &["algo.personalisedPageRank"], Centrality, &["node", "score"]),
    read("algo.katz", &["algo.katzCentrality"], Centrality, &["node", "score"]),
    read("algo.hits", &["algo.hubsAndAuthorities"], Centrality, &["node", "hub", "authority"]),
    read("algo.voteRank", &[], Centrality, &["node", "rank"]),
    read("algo.degree", &["algo.degreeCentrality"], Centrality, &["node", "score"]),
    read("algo.closeness", &["algo.closenessCentrality"], Centrality, &["node", "score"]),
    read("algo.betweenness", &["algo.betweennessCentrality"], Centrality, &["node", "score"]),
    read("algo.harmonic", &["algo.harmonicCentrality"], Centrality, &["node", "score"]),
    read("algo.eigenvector", &["algo.eigenvectorCentrality"], Centrality, &["node", "score"]),
    read("algo.kCore", &["algo.coreNumber"], Centrality, &["node", "score"]),
    // Community detection and components. `labelPropagation` is cdlp: that
    // *is* label propagation, LDBC's, and a second deterministic one collapses
    // every connected graph to a single community.
    read("algo.wcc", &[], Community, &["node", "componentId"]),
    read("algo.scc", &[], Community, &["node", "componentId"]),
    read("algo.cdlp", &["algo.labelPropagation"], Community, &["node", "communityId"]),
    read("algo.louvain", &[], Community, &["node", "communityId", "modularity"]),
    read("algo.modularity", &[], Community, &["modularity"]),
    read("algo.biconnectedComponents", &["algo.biconnected"], Community, &["node", "componentId"]),
    read("algo.kTruss", &["algo.truss"], Community, &["node"]),
    // Paths and flows.
    read("algo.shortestPath", &[], Path, &["path", "cost"]),
    read("algo.weightedPath", &[], Path, &["path", "cost"]),
    read("algo.allShortestPaths", &[], Path, &["path", "cost", "rank"]),
    read("algo.aStar", &[], Path, &["path", "cost", "rank"]),
    read("algo.yens", &[], Path, &["path", "cost", "rank"]),
    read("algo.bellmanFord", &[], Path, &["node", "distance"]),
    read("algo.allPairsShortestPath", &["algo.allPairs"], Path, &["source", "target", "hops"]),
    read("algo.dagLongestPath", &["algo.longestPath"], Path, &["node", "position"]),
    read("algo.transitiveClosure", &[], Path, &["source", "target"]),
    read("algo.randomWalk", &[], Path, &["node", "step"]),
    read("algo.maxFlow", &[], Path, &["max_flow"]),
    // A summary row (`total_weight`, `components`), then one row per tree
    // edge (`source`, `target`, `weight`).
    read("algo.mst", &[], Path, &["total_weight", "components", "source", "target", "weight"]),
    // Link prediction: a score over a *pair*, so these yield `node1, node2,
    // score` rather than a column beside the nodes.
    read("algo.commonNeighbors", &["algo.commonNeighbours"], LinkPrediction, &["node1", "node2", "score"]),
    read("algo.jaccard", &[], LinkPrediction, &["node1", "node2", "score"]),
    read("algo.adamicAdar", &[], LinkPrediction, &["node1", "node2", "score"]),
    // Similarity.
    read("algo.nodeSimilarity", &[], Similarity, &["node", "other", "similarity"]),
    read("algo.overlap", &["algo.overlapCoefficient"], Similarity, &["node", "other", "similarity"]),
    read("algo.cosine", &["algo.cosineSimilarity"], Similarity, &["node", "other", "similarity"]),
    // Structure.
    read("algo.topologicalSort", &["algo.topoSort"], Structure, &["node", "position"]),
    read("algo.cycleDetection", &["algo.findCycle"], Structure, &["node", "position"]),
    read("algo.bridges", &[], Structure, &["source", "target"]),
    read("algo.articulationPoints", &[], Structure, &["node"]),
    read("algo.bipartite", &["algo.bipartiteSets"], Structure, &["node", "side"]),
    read("algo.maximalMatching", &["algo.matching"], Structure, &["source", "target"]),
    read("algo.colouring", &["algo.coloring", "algo.greedyColouring"], Structure, &["node", "colour"]),
    read("algo.dominatingSet", &[], Structure, &["node"]),
    read("algo.effectiveSize", &[], Structure, &["node", "value"]),
    read("algo.constraint", &["algo.burtConstraint"], Structure, &["node", "value"]),
    // Whole-graph and per-node metrics.
    read("algo.triangleCount", &[], GraphMetric, &["triangles"]),
    read("algo.lcc", &[], GraphMetric, &["node", "coefficient"]),
    read("algo.squareClustering", &[], GraphMetric, &["node", "coefficient"]),
    read("algo.eccentricity", &[], GraphMetric, &["node", "eccentricity"]),
    read("algo.diameter", &[], GraphMetric, &["diameter"]),
    read("algo.radius", &[], GraphMetric, &["radius"]),
    read("algo.averageNeighborDegree", &["algo.averageNeighbourDegree"], GraphMetric, &["node", "score"]),
    read("algo.degreeAssortativity", &[], GraphMetric, &["assortativity"]),
    read("algo.wienerIndex", &[], GraphMetric, &["wienerIndex"]),
    read("algo.transitivity", &[], GraphMetric, &["transitivity"]),
    read("algo.globalEfficiency", &[], GraphMetric, &["efficiency"]),
    read("algo.richClub", &["algo.richClubCoefficient"], GraphMetric, &["coefficient"]),
    read("algo.reciprocity", &[], GraphMetric, &["reciprocity"]),
    // Temporal and causal (ALGO-15). Reachability in a temporal graph is not
    // transitive -- an edge that fired before you arrived is not traversable --
    // so none of these can be expressed with the static algorithms above.
    read("algo.temporalReachability", &[], Temporal, &["node", "time", "path", "times"]),
    read("algo.propagationRanking", &[], Temporal, &["node", "rank", "time", "path", "times"]),
    read("algo.temporalShortestPath", &[], Temporal, &["arrival", "path", "times"]),
    read("algo.symptomExplanation", &[], Temporal, &["node", "explains", "onset", "path", "times"]),
    // Embeddings and dimensionality reduction. fastRP and node2vec write the
    // vector as a node property (ML-06); see `execute_fastrp`/`execute_node2vec`
    // for what each does and does not do.
    read("algo.pca", &[], Embedding, &["node", "projection"]),
    write("algo.fastRP", Embedding, &["node", "embedding"]),
    write("algo.node2vec", Embedding, &["node", "embedding"]),
    // Optimisation. A single-objective solve also yields `history`,
    // `iterations` and `max_iterations`; a multi-objective one yields
    // `front_size` instead.
    write(
        "algo.or.solve",
        Optimization,
        &[
            "algorithm", "objective", "fitness", "feasible", "gap", "total_time_ms",
            "history", "iterations", "max_iterations", "front_size",
        ],
    ),
    // Negative sampling for link prediction (ML-04, #1725): pairs
    // that are NOT edges, to train against. It yields the pair and
    // nothing else -- there is no score to report for a non-edge.
    read(
        "algo.negativeSampling",
        &["algo.negativeSamples"],
        LinkPrediction,
        &["node1", "node2"],
    ),
];

/// The entry a procedure name resolves to, by the same canonical form dispatch
/// uses (any case, optional `algo.`/`samyama.` prefix, GDS spellings).
pub fn find(name: &str) -> Option<&'static AlgorithmEntry> {
    use super::operator::AlgorithmOperator;
    let wanted = AlgorithmOperator::canonical_name(name);
    ALGORITHM_CATALOG.iter().find(|e| {
        std::iter::once(e.name)
            .chain(e.aliases.iter().copied())
            .any(|n| AlgorithmOperator::canonical_name(n) == wanted)
    })
}
