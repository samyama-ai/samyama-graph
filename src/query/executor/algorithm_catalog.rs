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
}

use ParamDefault as D;
use ParamType as T;

/// A positional argument.
const fn pos(name: &'static str, ty: ParamType, required: bool) -> Param {
    Param { name, kind: ParamKind::Positional, ty, required, default: None, also: None }
}

/// An optional config-map key with a default.
const fn key(name: &'static str, ty: ParamType, default: ParamDefault) -> Param {
    Param { name, kind: ParamKind::Config, ty, required: false, default: Some(default), also: None }
}

/// An optional config-map key that is simply not applied when left out.
const fn opt(name: &'static str, ty: ParamType) -> Param {
    Param { name, kind: ParamKind::Config, ty, required: false, default: None, also: None }
}

/// A config key read under a second spelling as well.
const fn aka(p: Param, also: &'static str) -> Param {
    Param { also: Some(also), ..p }
}

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
    key("k", T::Integer, D::Integer(3)),
    key("limit", T::Integer, D::Integer(1000)),
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
    ("algo.articleRank", &[LABEL, EDGE_TYPE, key("dampingFactor", T::Float, D::Float(0.85)), key("iterations", T::Integer, D::Integer(20))]),
    ("algo.personalizedPageRank", &[
        pos("sources", T::List, false),
        LABEL,
        EDGE_TYPE,
        key("dampingFactor", T::Float, D::Float(0.85)),
        key("iterations", T::Integer, D::Integer(100)),
    ]),
    ("algo.katz", &[
        LABEL,
        EDGE_TYPE,
        key("alpha", T::Float, D::Float(0.1)),
        key("beta", T::Float, D::Float(1.0)),
        key("iterations", T::Integer, D::Integer(1000)),
        key("tolerance", T::Float, D::Float(1e-6)),
    ]),
    ("algo.hits", &[LABEL, EDGE_TYPE, key("iterations", T::Integer, D::Integer(500)), key("tolerance", T::Float, D::Float(1e-8))]),
    ("algo.voteRank", &[pos("k", T::Integer, false), LABEL, EDGE_TYPE, key("k", T::Integer, D::Integer(10))]),
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
    ("algo.kTruss", &[pos("k", T::Integer, false), LABEL, EDGE_TYPE, key("k", T::Integer, D::Integer(3))]),
    ("algo.shortestPath", &[SOURCE, TARGET, opt("weight_property", T::String)]),
    ("algo.weightedPath", &[SOURCE, TARGET, pos("weightProperty", T::String, true)]),
    ("algo.allShortestPaths", PATH_ENUM),
    ("algo.aStar", PATH_ENUM),
    ("algo.yens", PATH_ENUM),
    ("algo.bellmanFord", &[SOURCE, LABEL, EDGE_TYPE]),
    ("algo.allPairsShortestPath", FILTERS),
    ("algo.dagLongestPath", FILTERS),
    ("algo.transitiveClosure", FILTERS),
    ("algo.randomWalk", &[SOURCE, LABEL, EDGE_TYPE, key("steps", T::Integer, D::Integer(10)), key("seed", T::Integer, D::Integer(42))]),
    ("algo.maxFlow", &[SOURCE, pos("sink", T::NodeId, true), pos("capacityProperty", T::String, false)]),
    ("algo.mst", &[pos("weightProperty", T::String, false)]),
    ("algo.commonNeighbors", &[pos("node1", T::NodeId, false), pos("node2", T::NodeId, false), LABEL, EDGE_TYPE, key("limit", T::Integer, D::Integer(100))]),
    ("algo.jaccard", &[pos("node1", T::NodeId, false), pos("node2", T::NodeId, false), LABEL, EDGE_TYPE, key("limit", T::Integer, D::Integer(100))]),
    ("algo.adamicAdar", &[pos("node1", T::NodeId, false), pos("node2", T::NodeId, false), LABEL, EDGE_TYPE, key("limit", T::Integer, D::Integer(100))]),
    ("algo.nodeSimilarity", &[
        pos("topK", T::Integer, false),
        LABEL,
        EDGE_TYPE,
        key("topK", T::Integer, D::Integer(10)),
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
    ("algo.richClub", &[pos("k", T::Integer, false), LABEL, EDGE_TYPE, key("k", T::Integer, D::Integer(1))]),
    ("algo.reciprocity", FILTERS),
    ("algo.temporalReachability", &[SOURCE, LABEL, EDGE_TYPE, opt("timeProperty", T::String), opt("startTime", T::Integer)]),
    ("algo.propagationRanking", &[SOURCE, LABEL, EDGE_TYPE, opt("timeProperty", T::String), opt("startTime", T::Integer)]),
    ("algo.temporalShortestPath", &[SOURCE, TARGET, LABEL, EDGE_TYPE, opt("timeProperty", T::String), opt("startTime", T::Integer)]),
    ("algo.symptomExplanation", &[pos("symptoms", T::List, true), LABEL, EDGE_TYPE, opt("timeProperty", T::String)]),
    ("algo.pca", &[pos("label", T::String, true), pos("properties", T::List, true), pos("nComponents", T::Integer, false)]),
    ("algo.fastRP", &[
        LABEL,
        EDGE_TYPE,
        key("embeddingDimension", T::Integer, D::Integer(128)),
        key("iterationWeights", T::List, D::FloatList(&[1.0, 1.0, 1.0])),
        key("seed", T::Integer, D::Integer(387_844_754_753)),
        key("normalize", T::Boolean, D::Boolean(true)),
        key("writeProperty", T::String, D::String("embedding")),
    ]),
    ("algo.node2vec", &[
        LABEL,
        EDGE_TYPE,
        key("embeddingDimension", T::Integer, D::Integer(128)),
        key("walkLength", T::Integer, D::Integer(40)),
        key("walksPerNode", T::Integer, D::Integer(10)),
        key("returnFactor", T::Float, D::Float(1.0)),
        key("inOutFactor", T::Float, D::Float(1.0)),
        key("windowSize", T::Integer, D::Integer(5)),
        key("seed", T::Integer, D::Integer(336_337_323_314)),
        key("writeProperty", T::String, D::String("embedding")),
    ]),
    ("algo.or.solve", &[
        Param { name: "label", kind: ParamKind::Config, ty: T::String, required: true, default: None, also: None },
        Param { name: "property", kind: ParamKind::Config, ty: T::String, required: true, default: None, also: None },
        key("algorithm", T::String, D::String("Jaya")),
        aka(opt("costProperty", T::String), "cost_property"),
        aka(opt("costProperties", T::List), "cost_properties"),
        aka(key("lowerBound", T::Float, D::Float(0.0)), "min"),
        aka(key("upperBound", T::Float, D::Float(100.0)), "max"),
        aka(opt("minTotal", T::Float), "min_total"),
        opt("budget", T::Float),
        aka(opt("bestKnown", T::Float), "best_known"),
        aka(key("populationSize", T::Integer, D::Integer(50)), "population_size"),
        aka(key("maxIterations", T::Integer, D::Integer(100)), "max_iterations"),
        // Not applied when left out: an unseeded solve draws from entropy (#2061).
        opt("seed", T::Integer),
    ]),
    ("algo.negativeSampling", &[
        LABEL,
        EDGE_TYPE,
        key("strategy", T::String, D::String("random")),
        key("count", T::Integer, D::Integer(100)),
        key("k", T::Integer, D::Integer(2)),
        key("seed", T::Integer, D::Integer(42)),
    ]),
    ("algo.or.constrainedShortestPath", &[
        SOURCE,
        TARGET,
        Param { name: "resourceProperty", kind: ParamKind::Config, ty: T::String, required: true, default: None, also: None },
        Param { name: "maxResource", kind: ParamKind::Config, ty: T::Float, required: true, default: None, also: None },
        opt("costProperty", T::String),
        LABEL,
        EDGE_TYPE,
    ]),
];

impl AlgorithmEntry {
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
        let mut s = serializer.serialize_struct("AlgorithmEntry", 8)?;
        s.serialize_field("name", self.name)?;
        s.serialize_field("aliases", self.aliases)?;
        s.serialize_field("category", &self.category)?;
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
