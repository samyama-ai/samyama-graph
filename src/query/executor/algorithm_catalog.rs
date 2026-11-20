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
//! copied from doc comments. `parameters` are read from each `execute_*`
//! function; where the function checks its config keys against an allow-list,
//! `config_keys_match_what_each_algorithm_reads` holds the two together.

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

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
    /// Retrieval for an AI caller: vector search, then the graph around the
    /// matches (AI-07).
    Retrieval,
}

/// One callable algorithm.
#[derive(Debug, Clone, Copy)]
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
    // vector as a node property (ML-06), or stream it on `write: false`
    // (#1736); see `execute_fastrp`/`execute_node2vec`
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
    // The cheapest path within a limit on a second edge property
    // (OPT-06, #1770). Two names, because both are what a caller
    // reaches for. A read: it returns the path it found and both of
    // its totals, and writes nothing.
    read(
        "algo.or.constrainedShortestPath",
        &["algo.or.shortestPathWithConstraints"],
        Optimization,
        &["path", "cost", "resource"],
    ),
    // GraphRAG retrieval (AI-07): seeds by vector search, then the graph.
    read("algo.graphRag", &["algo.retrieve"], Retrieval, &["node", "score", "hops", "seed", "community", "path"]),
];

/// How an argument is passed: by position, or as a key of the config map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParamKind {
    Positional,
    Config,
}

/// The value an argument takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    Integer,
    Float,
    Boolean,
    String,
    /// A node id, as `id(n)` returns it.
    NodeId,
    List,
}

/// The value used when an optional argument is left out.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ParamDefault {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(&'static str),
    FloatList(&'static [f64]),
}

/// One argument an algorithm reads.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Param {
    pub name: &'static str,
    pub kind: ParamKind,
    #[serde(rename = "type")]
    pub ty: ParamType,
    pub required: bool,
    /// `None` when the argument is required, or when leaving it out means
    /// "not applied" (no label filter, no weight) rather than a value.
    pub default: Option<ParamDefault>,
    /// A second spelling the engine also accepts for this key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub also: Option<&'static str>,
    /// The smallest value the engine accepts as given, where it enforces one
    /// (#2013). Absent means none is enforced, not that any value is sensible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound: Option<Bound>,
}

/// What the engine does with a value below a parameter's minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BelowMin {
    /// The call fails with an error naming the parameter.
    Refused,
    /// The value is raised to the minimum and the call runs.
    Clamped,
    /// The value is replaced by the default and the call runs.
    Replaced,
}

/// A lower bound the engine enforces on a numeric parameter (#2013).
///
/// Only bounds the code checks are listed, each verified by calling the
/// algorithm below it (`every_listed_bound_is_enforced`). Every one is a
/// minimum; no parameter has an enforced maximum.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Bound {
    pub min: f64,
    /// `false` when `min` itself is out of range (`returnFactor > 0`).
    pub inclusive: bool,
    pub below: BelowMin,
}

use ParamDefault as D;
use ParamType as T;

/// A positional argument.
const fn pos(name: &'static str, ty: ParamType, required: bool) -> Param {
    Param { name, kind: ParamKind::Positional, ty, required, default: None, also: None, bound: None }
}

/// An optional config-map key with a default.
const fn key(name: &'static str, ty: ParamType, default: ParamDefault) -> Param {
    Param { name, kind: ParamKind::Config, ty, required: false, default: Some(default), also: None, bound: None }
}

/// An optional config-map key that is simply not applied when left out.
const fn opt(name: &'static str, ty: ParamType) -> Param {
    Param { name, kind: ParamKind::Config, ty, required: false, default: None, also: None, bound: None }
}

/// A config key read under a second spelling as well.
const fn aka(p: Param, also: &'static str) -> Param {
    Param { also: Some(also), ..p }
}

/// `p`, which the engine holds at `min` or above.
const fn at_least(p: Param, min: f64, below: BelowMin) -> Param {
    Param { bound: Some(Bound { min, inclusive: true, below }), ..p }
}

/// `p`, which the engine holds strictly above `min`.
const fn above(p: Param, min: f64, below: BelowMin) -> Param {
    Param { bound: Some(Bound { min, inclusive: false, below }), ..p }
}

use BelowMin as B;

const LABEL: Param = opt("label", T::String);
const EDGE_TYPE: Param = opt("edgeType", T::String);
const UNDIRECTED: Param = key("undirected", T::Boolean, D::Boolean(true));
const SOURCE: Param = pos("source", T::NodeId, true);
const TARGET: Param = pos("target", T::NodeId, true);
/// `label` and `edgeType` from the config map, for every algorithm that builds
/// its graph with `structural_view` (which also takes a leading string as the
/// label).
const FILTERS: &[Param] = &[LABEL, EDGE_TYPE];
const METRIC: &[Param] = &[LABEL, EDGE_TYPE, UNDIRECTED];
const PATH_ENUM: &[Param] = &[
    SOURCE,
    TARGET,
    pos("k", T::Integer, false),
    LABEL,
    EDGE_TYPE,
    opt("weightProperty", T::String),
    opt("heuristicProperty", T::String),
    at_least(key("k", T::Integer, D::Integer(3)), 1.0, B::Clamped),
    at_least(key("limit", T::Integer, D::Integer(1000)), 1.0, B::Clamped),
];

/// The arguments each catalogued algorithm reads, by catalogue name.
///
/// Every entry has a row, an empty one included, so a new algorithm cannot be
/// added without saying what it takes (`every_algorithm_has_a_parameter_row`).
pub const PARAMETERS: &[(&str, &[Param])] = &[
    ("algo.pageRank", &[
        pos("label", T::String, false),
        pos("edgeType", T::String, false),
        key("damping", T::Float, D::Float(0.85)),
        key("iterations", T::Integer, D::Integer(20)),
    ]),
    ("algo.articleRank", &[LABEL, EDGE_TYPE, key("dampingFactor", T::Float, D::Float(0.85)), at_least(key("iterations", T::Integer, D::Integer(20)), 1.0, B::Clamped)]),
    ("algo.personalizedPageRank", &[
        pos("sources", T::List, false),
        LABEL,
        EDGE_TYPE,
        key("dampingFactor", T::Float, D::Float(0.85)),
        at_least(key("iterations", T::Integer, D::Integer(100)), 1.0, B::Clamped),
    ]),
    ("algo.katz", &[
        LABEL,
        EDGE_TYPE,
        key("alpha", T::Float, D::Float(0.1)),
        key("beta", T::Float, D::Float(1.0)),
        at_least(key("iterations", T::Integer, D::Integer(1000)), 1.0, B::Clamped),
        key("tolerance", T::Float, D::Float(1e-6)),
    ]),
    ("algo.hits", &[LABEL, EDGE_TYPE, at_least(key("iterations", T::Integer, D::Integer(500)), 1.0, B::Clamped), key("tolerance", T::Float, D::Float(1e-8))]),
    ("algo.voteRank", &[at_least(pos("k", T::Integer, false), 0.0, B::Clamped), LABEL, EDGE_TYPE, at_least(key("k", T::Integer, D::Integer(10)), 0.0, B::Clamped)]),
    ("algo.degree", METRIC),
    ("algo.closeness", METRIC),
    ("algo.betweenness", METRIC),
    ("algo.harmonic", METRIC),
    ("algo.eigenvector", METRIC),
    ("algo.kCore", METRIC),
    ("algo.wcc", &[pos("label", T::String, false), pos("edgeType", T::String, false), LABEL, EDGE_TYPE]),
    ("algo.scc", &[]),
    ("algo.cdlp", &[pos("label", T::String, false), pos("edgeType", T::String, false), key("maxIterations", T::Integer, D::Integer(100))]),
    ("algo.louvain", FILTERS),
    ("algo.modularity", &[pos("partition", T::List, true), LABEL, EDGE_TYPE]),
    ("algo.biconnectedComponents", FILTERS),
    ("algo.kTruss", &[at_least(pos("k", T::Integer, false), 0.0, B::Clamped), LABEL, EDGE_TYPE, at_least(key("k", T::Integer, D::Integer(3)), 0.0, B::Clamped)]),
    ("algo.shortestPath", &[SOURCE, TARGET, opt("weight_property", T::String)]),
    ("algo.weightedPath", &[SOURCE, TARGET, pos("weightProperty", T::String, true)]),
    ("algo.allShortestPaths", PATH_ENUM),
    ("algo.aStar", PATH_ENUM),
    ("algo.yens", PATH_ENUM),
    ("algo.bellmanFord", &[SOURCE, LABEL, EDGE_TYPE]),
    ("algo.allPairsShortestPath", FILTERS),
    ("algo.dagLongestPath", FILTERS),
    ("algo.transitiveClosure", FILTERS),
    ("algo.randomWalk", &[SOURCE, LABEL, EDGE_TYPE, at_least(key("steps", T::Integer, D::Integer(10)), 0.0, B::Clamped), key("seed", T::Integer, D::Integer(42))]),
    ("algo.maxFlow", &[SOURCE, pos("sink", T::NodeId, true), pos("capacityProperty", T::String, false)]),
    ("algo.mst", &[pos("weightProperty", T::String, false)]),
    ("algo.commonNeighbors", &[pos("node1", T::NodeId, false), pos("node2", T::NodeId, false), LABEL, EDGE_TYPE, at_least(key("limit", T::Integer, D::Integer(100)), 0.0, B::Clamped)]),
    ("algo.jaccard", &[pos("node1", T::NodeId, false), pos("node2", T::NodeId, false), LABEL, EDGE_TYPE, at_least(key("limit", T::Integer, D::Integer(100)), 0.0, B::Clamped)]),
    ("algo.adamicAdar", &[pos("node1", T::NodeId, false), pos("node2", T::NodeId, false), LABEL, EDGE_TYPE, at_least(key("limit", T::Integer, D::Integer(100)), 0.0, B::Clamped)]),
    ("algo.nodeSimilarity", &[
        at_least(pos("topK", T::Integer, false), 0.0, B::Clamped),
        LABEL,
        EDGE_TYPE,
        at_least(key("topK", T::Integer, D::Integer(10)), 0.0, B::Clamped),
        key("cutoff", T::Float, D::Float(0.0)),
    ]),
    ("algo.overlap", FILTERS),
    ("algo.cosine", FILTERS),
    ("algo.topologicalSort", FILTERS),
    ("algo.cycleDetection", FILTERS),
    ("algo.bridges", FILTERS),
    ("algo.articulationPoints", FILTERS),
    ("algo.bipartite", FILTERS),
    ("algo.maximalMatching", FILTERS),
    ("algo.colouring", FILTERS),
    ("algo.dominatingSet", FILTERS),
    ("algo.effectiveSize", FILTERS),
    ("algo.constraint", FILTERS),
    ("algo.triangleCount", &[]),
    ("algo.lcc", &[pos("label", T::String, false), pos("edgeType", T::String, false)]),
    ("algo.squareClustering", FILTERS),
    ("algo.eccentricity", METRIC),
    ("algo.diameter", METRIC),
    ("algo.radius", METRIC),
    ("algo.averageNeighborDegree", METRIC),
    ("algo.degreeAssortativity", METRIC),
    ("algo.wienerIndex", FILTERS),
    ("algo.transitivity", FILTERS),
    ("algo.globalEfficiency", FILTERS),
    ("algo.richClub", &[at_least(pos("k", T::Integer, false), 0.0, B::Clamped), LABEL, EDGE_TYPE, at_least(key("k", T::Integer, D::Integer(1)), 0.0, B::Clamped)]),
    ("algo.reciprocity", FILTERS),
    ("algo.temporalReachability", &[SOURCE, LABEL, EDGE_TYPE, opt("timeProperty", T::String), opt("startTime", T::Integer)]),
    ("algo.propagationRanking", &[SOURCE, LABEL, EDGE_TYPE, opt("timeProperty", T::String), opt("startTime", T::Integer)]),
    ("algo.temporalShortestPath", &[SOURCE, TARGET, LABEL, EDGE_TYPE, opt("timeProperty", T::String), opt("startTime", T::Integer)]),
    ("algo.symptomExplanation", &[pos("symptoms", T::List, true), LABEL, EDGE_TYPE, opt("timeProperty", T::String)]),
    ("algo.pca", &[pos("label", T::String, true), pos("properties", T::List, true), at_least(pos("nComponents", T::Integer, false), 1.0, B::Refused)]),
    ("algo.fastRP", &[
        LABEL,
        EDGE_TYPE,
        at_least(key("embeddingDimension", T::Integer, D::Integer(128)), 1.0, B::Refused),
        key("iterationWeights", T::List, D::FloatList(&[1.0, 1.0, 1.0])),
        key("seed", T::Integer, D::Integer(387_844_754_753)),
        key("normalize", T::Boolean, D::Boolean(true)),
        // Writes by default; `write: false` streams the vectors instead (#1736).
        key("write", T::Boolean, D::Boolean(true)),
        key("writeProperty", T::String, D::String("embedding")),
    ]),
    ("algo.node2vec", &[
        LABEL,
        EDGE_TYPE,
        at_least(key("embeddingDimension", T::Integer, D::Integer(128)), 1.0, B::Refused),
        at_least(key("walkLength", T::Integer, D::Integer(40)), 2.0, B::Refused),
        at_least(key("walksPerNode", T::Integer, D::Integer(10)), 1.0, B::Refused),
        above(key("returnFactor", T::Float, D::Float(1.0)), 0.0, B::Replaced),
        above(key("inOutFactor", T::Float, D::Float(1.0)), 0.0, B::Replaced),
        at_least(key("windowSize", T::Integer, D::Integer(5)), 1.0, B::Refused),
        key("seed", T::Integer, D::Integer(336_337_323_314)),
        // Writes by default; `write: false` streams the vectors instead (#1736).
        key("write", T::Boolean, D::Boolean(true)),
        key("writeProperty", T::String, D::String("embedding")),
    ]),
    ("algo.or.solve", &[
        Param { name: "label", kind: ParamKind::Config, ty: T::String, required: true, default: None, also: None, bound: None },
        Param { name: "property", kind: ParamKind::Config, ty: T::String, required: true, default: None, also: None, bound: None },
        key("algorithm", T::String, D::String("Jaya")),
        aka(opt("costProperty", T::String), "cost_property"),
        aka(opt("costProperties", T::List), "cost_properties"),
        aka(key("lowerBound", T::Float, D::Float(0.0)), "min"),
        aka(key("upperBound", T::Float, D::Float(100.0)), "max"),
        aka(opt("minTotal", T::Float), "min_total"),
        opt("budget", T::Float),
        aka(opt("bestKnown", T::Float), "best_known"),
        // `penalty`, `repair` or `feasibility` (OPT-10, #2266). Its default
        // depends on the solver -- `penalty` for a single-objective one,
        // `feasibility` for a multi-objective one -- so none is listed.
        aka(opt("constraintHandling", T::String), "constraint_handling"),
        aka(key("populationSize", T::Integer, D::Integer(50)), "population_size"),
        aka(key("maxIterations", T::Integer, D::Integer(100)), "max_iterations"),
        // Not applied when left out: an unseeded solve draws from entropy (#2061).
        at_least(opt("seed", T::Integer), 0.0, B::Refused),
    ]),
    ("algo.negativeSampling", &[
        LABEL,
        EDGE_TYPE,
        key("strategy", T::String, D::String("random")),
        at_least(key("count", T::Integer, D::Integer(100)), 1.0, B::Refused),
        key("k", T::Integer, D::Integer(2)),
        key("seed", T::Integer, D::Integer(42)),
    ]),
    ("algo.or.constrainedShortestPath", &[
        SOURCE,
        TARGET,
        Param { name: "resourceProperty", kind: ParamKind::Config, ty: T::String, required: true, default: None, also: None, bound: None },
        Param { name: "maxResource", kind: ParamKind::Config, ty: T::Float, required: true, default: None, also: None, bound: None },
        opt("costProperty", T::String),
        LABEL,
        EDGE_TYPE,
    ]),
    ("algo.graphRag", &[
        Param { name: "query", kind: ParamKind::Config, ty: T::List, required: true, default: None, also: None },
        Param { name: "label", kind: ParamKind::Config, ty: T::String, required: true, default: None, also: None },
        Param { name: "property", kind: ParamKind::Config, ty: T::String, required: true, default: None, also: None },
        key("strategy", T::String, D::String("local")),
        key("k", T::Integer, D::Integer(5)),
        key("hops", T::Integer, D::Integer(2)),
        key("decay", T::Float, D::Float(0.5)),
        key("communities", T::Integer, D::Integer(3)),
        key("limit", T::Integer, D::Integer(50)),
        EDGE_TYPE,
        opt("timeProperty", T::String),
        opt("since", T::Float),
        opt("until", T::Float),
        opt("hierarchy", T::String),
        opt("under", T::NodeId),
    ]),
];

/// What each algorithm computes, one sentence, by catalogue name (#2013).
///
/// Taken from the doc comments on each implementation (the `execute_*`
/// functions and `samyama-graph-algorithms`) or the GDS compatibility notes,
/// not written from general knowledge; `algo.or.solve`, which has no doc
/// comment, is described from its parameters and its objective.
pub const DESCRIPTIONS: &[(&str, &str)] = &[
    ("algo.pageRank", "PageRank: each node's score from the scores of the nodes linking to it."),
    ("algo.articleRank", "PageRank that discounts votes from nodes that link to many others, by adding the average out-degree to each denominator."),
    ("algo.personalizedPageRank", "PageRank biased toward a set of source nodes; with no reachable sources it is plain PageRank."),
    ("algo.katz", "Katz centrality: influence counted over all walks, decaying with their length."),
    ("algo.hits", "HITS: a hub and an authority score for each node, computed together."),
    ("algo.voteRank", "Elects influential nodes one at a time, weakening each winner's neighbours so the seeds are spread out; returns them in election order."),
    ("algo.degree", "Degree centrality: a node's neighbours divided by the most it could have."),
    ("algo.closeness", "Closeness centrality, with NetworkX's convention for disconnected graphs."),
    ("algo.betweenness", "Betweenness centrality by Brandes' algorithm, normalised as NetworkX does."),
    ("algo.harmonic", "Harmonic centrality: the sum of reciprocal distances to every other node."),
    ("algo.eigenvector", "Eigenvector centrality by power iteration; an error if it does not converge in 1000 iterations."),
    ("algo.kCore", "Core number: the largest k for which a node is in the k-core."),
    ("algo.wcc", "Weakly connected components: the disjoint subgraphs, ignoring edge direction."),
    ("algo.scc", "Strongly connected components, by Tarjan's algorithm."),
    ("algo.cdlp", "Community detection by synchronous label propagation (LDBC CDLP): each node takes the most frequent label among its neighbours."),
    ("algo.louvain", "Louvain community detection: a community per node, and the partition's modularity."),
    ("algo.modularity", "Scores a given partition: the fraction of edges inside communities, minus what random wiring would give."),
    ("algo.biconnectedComponents", "Biconnected components: maximal subgraphs with no articulation point."),
    ("algo.kTruss", "The nodes of the k-truss, where every edge lies in at least k-2 triangles."),
    ("algo.shortestPath", "Shortest path from source to target: breadth-first, or Dijkstra when a weight property is given."),
    ("algo.weightedPath", "Weighted shortest path (Dijkstra) on a weight property; negative weights are refused."),
    ("algo.allShortestPaths", "Every unweighted shortest path from source to target, one row per path."),
    ("algo.aStar", "A* search: Dijkstra guided by a per-node estimate of the remaining distance."),
    ("algo.yens", "Yen's algorithm: the k shortest loopless paths from source to target, cheapest first."),
    ("algo.bellmanFord", "Single-source shortest paths that allow negative weights; refuses a reachable negative cycle."),
    ("algo.allPairsShortestPath", "Hop distance for every reachable ordered pair of nodes."),
    ("algo.dagLongestPath", "The longest path in a directed acyclic graph, by edge count."),
    ("algo.transitiveClosure", "Every reachable ordered pair of nodes."),
    ("algo.randomWalk", "A seeded random walk of a given number of steps from a source node."),
    ("algo.maxFlow", "Maximum flow from source to sink by Edmonds-Karp; capacity from a property, else 1."),
    ("algo.mst", "Minimum spanning tree by Prim's algorithm, undirected; a spanning forest on a disconnected graph."),
    ("algo.commonNeighbors", "Link prediction by the number of shared neighbours, for one pair or the top unconnected pairs."),
    ("algo.jaccard", "Link prediction by shared neighbours divided by all neighbours of the pair."),
    ("algo.adamicAdar", "Link prediction weighting each shared neighbour by 1/ln(its degree)."),
    ("algo.nodeSimilarity", "The k most similar nodes to each node, by Jaccard similarity of their neighbours."),
    ("algo.overlap", "Overlap coefficient of neighbourhoods, |A∩B|/min(|A|,|B|), for every pair that has one."),
    ("algo.cosine", "Cosine similarity of the nodes' neighbourhoods."),
    ("algo.topologicalSort", "Nodes in dependency order (Kahn's algorithm); a cycle is an error."),
    ("algo.cycleDetection", "The nodes of one directed cycle in order, or no rows when there is none."),
    ("algo.bridges", "Edges whose removal increases the number of connected components."),
    ("algo.articulationPoints", "Nodes whose removal increases the number of connected components."),
    ("algo.bipartite", "Whether the graph is two-colourable, and which side each node is on."),
    ("algo.maximalMatching", "A maximal matching: greedily chosen edges that share no endpoint."),
    ("algo.colouring", "A greedy proper colouring, largest degree first."),
    ("algo.dominatingSet", "A set of nodes that every node is in or adjacent to."),
    ("algo.effectiveSize", "Burt's effective size: a node's neighbours minus the redundancy among them."),
    ("algo.constraint", "Burt's constraint: how far a node's contacts are tied up in each other."),
    ("algo.triangleCount", "The total number of triangles in the graph, ignoring direction."),
    ("algo.lcc", "Local clustering coefficient of each node (LDBC LCC)."),
    ("algo.squareClustering", "How often a node's neighbours close a four-cycle."),
    ("algo.eccentricity", "Each node's distance to the furthest node it can reach; null if it cannot reach all."),
    ("algo.diameter", "The largest eccentricity; an error if the graph is not connected."),
    ("algo.radius", "The smallest eccentricity; an error if the graph is not connected."),
    ("algo.averageNeighborDegree", "The average degree of each node's neighbours."),
    ("algo.degreeAssortativity", "Degree assortativity: the Pearson correlation of degree across edges."),
    ("algo.wienerIndex", "The Wiener index: the sum of shortest-path distances over all pairs."),
    ("algo.transitivity", "Global clustering coefficient: three times the triangles over the connected triples."),
    ("algo.globalEfficiency", "The average of 1/distance over all ordered pairs."),
    ("algo.richClub", "Rich-club coefficient at degree k: how densely the high-degree nodes connect to each other."),
    ("algo.reciprocity", "The fraction of directed node pairs whose reverse edge also exists."),
    ("algo.temporalReachability", "Which nodes a source can reach respecting edge times, when, and by what walk."),
    ("algo.propagationRanking", "What a fault at the source reaches respecting edge times, ranked by how soon."),
    ("algo.temporalShortestPath", "The earliest-arriving time-respecting path from source to target."),
    ("algo.symptomExplanation", "Ranks candidate causes for a set of observed (node, time) symptoms."),
    ("algo.pca", "Principal component analysis over numeric node properties: each node's coordinates on the first components."),
    ("algo.fastRP", "FastRP node embeddings: random vectors repeatedly averaged over neighbours; written to a node property unless write is false."),
    ("algo.node2vec", "node2vec embeddings from biased random walks, embedded by random projection of co-occurrence counts."),
    ("algo.or.solve", "Optimises a numeric property on each node of a label with a metaheuristic solver (Jaya by default), each value within the bounds, minimising the sum of value times its cost property."),
    ("algo.negativeSampling", "Node pairs that are not edges, for training link prediction (random, degree-matched or k-hop)."),
    ("algo.or.constrainedShortestPath", "The cheapest source-to-target path whose resource property sums to at most maxResource; cost is the hop count without a cost property."),
];

impl AlgorithmEntry {
    /// What this algorithm computes, from [`DESCRIPTIONS`].
    pub fn description(&self) -> &'static str {
        DESCRIPTIONS
            .iter()
            .find(|(name, _)| *name == self.name)
            .map(|(_, d)| *d)
            .unwrap_or("")
    }

    /// The arguments this algorithm reads, from [`PARAMETERS`].
    pub fn parameters(&self) -> &'static [Param] {
        PARAMETERS
            .iter()
            .find(|(name, _)| *name == self.name)
            .map(|(_, params)| *params)
            .unwrap_or(&[])
    }

    /// Whether `graph` may name a projection to run on (ALGO-05), from the
    /// operator's own refusal list.
    pub fn runs_on_projection(&self) -> bool {
        super::operator::AlgorithmOperator::runs_on_projection(self.name)
    }

    /// The column `writeProperty` stores on each node, when this algorithm can
    /// write its result back (ALGO-06), from the operator's own table.
    pub fn write_back_column(&self) -> Option<&'static str> {
        super::operator::AlgorithmOperator::write_back_column_for(self.name)
    }
}

impl Serialize for AlgorithmEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("AlgorithmEntry", 9)?;
        s.serialize_field("name", self.name)?;
        s.serialize_field("aliases", self.aliases)?;
        s.serialize_field("category", &self.category)?;
        s.serialize_field("description", self.description())?;
        s.serialize_field("parameters", self.parameters())?;
        s.serialize_field("yields", self.yields)?;
        s.serialize_field("writes", &self.writes)?;
        s.serialize_field("runs_on_projection", &self.runs_on_projection())?;
        s.serialize_field("write_back_column", &self.write_back_column())?;
        s.end()
    }
}

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
