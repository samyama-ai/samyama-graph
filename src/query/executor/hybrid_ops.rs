//! Hybrid retrieval (#1279): `db.hybrid.search`.
//!
//! Keyword search and vector search each miss what the other finds: BM25
//! misses a paraphrase, an embedding misses an exact identifier. This
//! procedure runs both, fuses the two ranked lists into a few seeds, walks a
//! bounded neighbourhood of those seeds, and reranks what it found -- the
//! retrieval half of a retrieval-augmented answer, in one call.
//!
//! ```cypher
//! CALL db.hybrid.search('doc_text', 'statin myopathy', 'doc_emb', $vector,
//!                       {k: 10, hops: 1, relTypes: ['CITES'], maxExpanded: 50})
//! YIELD node, score, bm25Rank, vectorRank, foundBy, timings
//! ```
//!
//! The stages, each timed separately (`timings`):
//!
//! 1. **bm25** -- the full-text index's BM25 search for `queryText`, the top
//!    `candidates`, exactly what `db.index.fulltext.queryNodes` returns.
//! 2. **vector** -- the vector index's nearest `candidates` to `queryVector`,
//!    scored as `db.index.vector.queryNodes` scores them.
//! 3. **fusion** -- Reciprocal Rank Fusion by default:
//!    `fused(n) = Σ_s w_s / (rrfK + rank_s(n))` over the lists `n` is in, with
//!    1-based ranks, `rrfK = 60` and both weights 1. `fusion: 'weighted'`
//!    instead sums `w_s * score_s(n) / max_s`, each list's scores divided by
//!    its best. The `k` best fused candidates are the seeds.
//! 4. **expansion** -- breadth-first from every seed, at most `hops` steps,
//!    over `relTypes` (default any) in `direction` (default both). Nodes are
//!    admitted nearest level first, higher-ranked seed first within a level,
//!    and at most `maxExpanded` nodes that are not seeds are admitted; the walk
//!    continues only through admitted nodes and seeds.
//! 5. **rerank** -- `score(n) = fused(n) + graphWeight * support(n)`, where
//!    `support(n) = Σ fused(s) * decay^d(s, n)` over the seeds `s ≠ n` that
//!    reach `n` within `hops`. A node is rewarded for being close to other
//!    good seeds. With `hops: 0` (the default) `score` is the fused score.
//!
//! What is not here: the query embedding and the answer. The caller embeds
//! the question (the engine calls no model) and hands the rows to whatever
//! writes the answer; the LLM stage of #1279's flow is outside the engine and
//! its time is the caller's to measure.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::graph::{GraphStore, NodeId, PropertyValue};
use crate::query::ast::Expression;
use crate::query::executor::operator::{eval_expression, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// The procedure's name, as messages spell it.
pub const PROCEDURE: &str = "db.hybrid.search";

/// Is `name` this procedure? Case-insensitive, as `db.checkIntegrity` is.
pub fn is_procedure(name: &str) -> bool {
    name.eq_ignore_ascii_case(PROCEDURE)
}

/// The config keys the procedure reads; any other is an error.
pub const CONFIG_KEYS: &[&str] = &[
    "k",
    "candidates",
    "fusion",
    "rrfK",
    "bm25Weight",
    "vectorWeight",
    "ef",
    "hops",
    "relTypes",
    "direction",
    "maxExpanded",
    "graphWeight",
    "decay",
];

/// The columns it yields.
pub const COLUMNS: &[&str] = &[
    "node",
    "score",
    "fusedScore",
    "graphScore",
    "bm25Rank",
    "bm25Score",
    "vectorRank",
    "vectorScore",
    "foundBy",
    "hops",
    "seed",
    "timings",
];

/// The furthest `hops` may reach. The expansion is meant to be local; a
/// larger radius is a traversal, which `MATCH` already writes.
pub const MAX_HOPS: usize = 5;

/// How the two ranked lists are combined.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fusion {
    /// Reciprocal Rank Fusion: ranks only, so the two lists' score scales
    /// never have to be reconciled.
    Rrf,
    /// Each list's scores divided by its best, then a weighted sum.
    Weighted,
}

/// Which way the expansion follows a relationship.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

/// The vector index searched: by name, or by the `(label, property)` it is on.
#[derive(Clone, Debug, PartialEq)]
pub enum VectorIndexRef {
    Name(String),
    On { label: String, property: String },
}

/// What a call asks for, once its arguments are evaluated.
#[derive(Clone, Debug)]
pub struct HybridRequest {
    pub fulltext_index: Option<String>,
    pub query_text: Option<String>,
    pub vector_index: Option<VectorIndexRef>,
    pub query_vector: Option<Vec<f32>>,
    /// How many seeds the fusion keeps.
    pub k: usize,
    /// How many candidates each list contributes to the fusion.
    pub candidates: usize,
    pub fusion: Fusion,
    pub rrf_k: f64,
    pub bm25_weight: f64,
    pub vector_weight: f64,
    pub ef: Option<usize>,
    pub hops: usize,
    /// Relationship types the expansion follows; empty is any.
    pub rel_types: Vec<String>,
    pub direction: Direction,
    pub max_expanded: usize,
    pub graph_weight: f64,
    pub decay: f64,
}

impl Default for HybridRequest {
    fn default() -> Self {
        Self {
            fulltext_index: None,
            query_text: None,
            vector_index: None,
            query_vector: None,
            k: 10,
            candidates: 50,
            fusion: Fusion::Rrf,
            rrf_k: 60.0,
            bm25_weight: 1.0,
            vector_weight: 1.0,
            ef: None,
            hops: 0,
            rel_types: Vec::new(),
            direction: Direction::Both,
            max_expanded: 100,
            graph_weight: 1.0,
            decay: 0.5,
        }
    }
}

/// One returned node and how it was found.
#[derive(Clone, Debug, PartialEq)]
pub struct HybridHit {
    pub node: NodeId,
    /// `fused + graphWeight * graph`, what the rows are ordered by.
    pub score: f64,
    /// The fusion's score; 0 for a node only the expansion found.
    pub fused: f64,
    /// `support(n)` before `graphWeight` is applied.
    pub graph: f64,
    /// 1-based rank and BM25 score, when the node was a BM25 candidate.
    pub bm25: Option<(usize, f64)>,
    /// 1-based rank and similarity, when the node was a vector candidate.
    pub vector: Option<(usize, f64)>,
    /// Whether the fusion kept it as a seed.
    pub is_seed: bool,
    /// Steps from the nearest seed; 0 for a seed.
    pub hops: usize,
    /// The seed contributing most to `graph`, or the node itself for a seed.
    pub seed: NodeId,
}

impl HybridHit {
    /// `bm25`, `vector` and `graph`, for each way the node was found.
    pub fn found_by(&self) -> Vec<&'static str> {
        let mut by = Vec::with_capacity(3);
        if self.bm25.is_some() {
            by.push("bm25");
        }
        if self.vector.is_some() {
            by.push("vector");
        }
        // An expanded node was found by the walk even when `decay: 0`
        // leaves it no graph score.
        if !self.is_seed || self.graph > 0.0 {
            by.push("graph");
        }
        by
    }
}

/// Wall-clock time spent in each stage, in milliseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StageTimings {
    pub bm25_ms: f64,
    pub vector_ms: f64,
    pub fusion_ms: f64,
    pub expansion_ms: f64,
    pub rerank_ms: f64,
    pub total_ms: f64,
}

impl StageTimings {
    /// As the `timings` column carries them.
    pub fn to_map(&self) -> HashMap<String, PropertyValue> {
        [
            ("bm25Ms", self.bm25_ms),
            ("vectorMs", self.vector_ms),
            ("fusionMs", self.fusion_ms),
            ("expansionMs", self.expansion_ms),
            ("rerankMs", self.rerank_ms),
            ("totalMs", self.total_ms),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), PropertyValue::Float(v)))
        .collect()
    }
}

/// The rows, best first, and what each stage cost.
#[derive(Clone, Debug)]
pub struct HybridResult {
    pub hits: Vec<HybridHit>,
    pub timings: StageTimings,
}

fn ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

fn bad(msg: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::bad_argument(format!("{PROCEDURE}: {msg}"))
}

/// Order by score descending, ties by node id, so a run is reproducible.
fn by_score_then_id(a: &(NodeId, f64), b: &(NodeId, f64)) -> std::cmp::Ordering {
    b.1.total_cmp(&a.1).then(a.0.cmp(&b.0))
}

/// Run the search. The procedure's operator calls this; so can a harness that
/// wants the stages' timings without going through Cypher.
pub fn hybrid_search(store: &GraphStore, req: &HybridRequest) -> ExecutionResult<HybridResult> {
    let total = Instant::now();
    let mut timings = StageTimings::default();

    let text = req.query_text.as_deref().filter(|t| !t.trim().is_empty());
    let vector = req.query_vector.as_deref().filter(|v| !v.is_empty());

    // An index that is named must exist, whether or not its query is given:
    // a misspelt name and a search that matched nothing look the same.
    if let Some(name) = &req.fulltext_index {
        if store.fulltext.covers(name).is_none() {
            let known = store.fulltext.names();
            return Err(bad(format!(
                "no full-text index named '{name}'. {}",
                if known.is_empty() {
                    "None has been created; CREATE FULLTEXT INDEX <name> FOR (n:Label) \
                     ON EACH [n.property] makes one."
                        .to_string()
                } else {
                    format!("Known index names: {}", known.join(", "))
                }
            )));
        }
    }
    let vector_target = match &req.vector_index {
        None => None,
        Some(VectorIndexRef::Name(name)) => {
            Some(store.resolve_vector_index(name).ok_or_else(|| {
                let known = store.vector_index_names();
                bad(format!(
                    "no vector index named '{name}'. {}",
                    if known.is_empty() {
                        "No vector index has been created with a name; pass \
                     {label: 'L', property: 'p'} to address an unnamed one."
                            .to_string()
                    } else {
                        format!("Known index names: {}", known.join(", "))
                    }
                ))
            })?)
        }
        Some(VectorIndexRef::On { label, property }) => {
            if store.vector_index.get_index(label, property).is_none() {
                return Err(bad(format!(
                    "no vector index on :{label}({property}). CREATE VECTOR INDEX FOR \
                     (n:{label}) ON (n.{property}) OPTIONS {{dimensions: <d>}} creates one."
                )));
            }
            Some((label.clone(), property.clone()))
        }
    };

    if text.is_none() && vector.is_none() {
        return Err(bad(
            "needs a query text, a query vector, or both; both are null or empty",
        ));
    }
    if text.is_some() && req.fulltext_index.is_none() {
        return Err(bad("a query text needs a full-text index to search"));
    }
    if vector.is_some() && vector_target.is_none() {
        return Err(bad("a query vector needs a vector index to search"));
    }

    // 1. BM25.
    let started = Instant::now();
    let bm25: Vec<(NodeId, f64)> = match (text, &req.fulltext_index) {
        (Some(text), Some(name)) => store
            .fulltext
            .search(name, text, req.candidates)
            .unwrap_or_default()
            .into_iter()
            .filter(|h| store.has_node(h.node))
            .map(|h| (h.node, h.score))
            .collect(),
        _ => Vec::new(),
    };
    timings.bm25_ms = ms(started);

    // 2. Vector.
    let started = Instant::now();
    let vec_hits: Vec<(NodeId, f64)> = match (vector, &vector_target) {
        (Some(v), Some((label, property))) => {
            let metric = store
                .vector_index
                .metric(label, property)
                .ok_or_else(|| bad(format!("no vector index on :{label}({property})")))?;
            let dimensions = store
                .vector_index
                .get_index(label, property)
                .and_then(|index| index.read().ok().map(|i| i.dimensions()));
            if let Some(d) = dimensions.filter(|d| *d != v.len()) {
                return Err(bad(format!(
                    "dimension mismatch: the vector index on :{label}({property}) holds \
                     {d}-dimensional vectors, the query vector has {}",
                    v.len()
                )));
            }
            let mut hits: Vec<(NodeId, f64)> = store
                .vector_search_where(label, property, v, req.candidates, req.ef, |id| {
                    store.has_node(id)
                })
                .map_err(bad)?
                .into_iter()
                .map(|(id, d)| (id, metric.score(d)))
                .collect();
            hits.sort_by(by_score_then_id);
            hits.truncate(req.candidates);
            hits
        }
        _ => Vec::new(),
    };
    timings.vector_ms = ms(started);

    // 3. Fusion.
    let started = Instant::now();
    let mut bm25_of: HashMap<NodeId, (usize, f64)> = HashMap::with_capacity(bm25.len());
    for (i, (id, s)) in bm25.iter().enumerate() {
        bm25_of.insert(*id, (i + 1, *s));
    }
    let mut vector_of: HashMap<NodeId, (usize, f64)> = HashMap::with_capacity(vec_hits.len());
    for (i, (id, s)) in vec_hits.iter().enumerate() {
        vector_of.insert(*id, (i + 1, *s));
    }
    let best = |list: &[(NodeId, f64)]| list.first().map_or(0.0, |(_, s)| s.max(0.0));
    let (bm25_best, vector_best) = (best(&bm25), best(&vec_hits));
    let contribution = |entry: Option<&(usize, f64)>, weight: f64, max: f64| -> f64 {
        match entry {
            None => 0.0,
            Some((rank, score)) => match req.fusion {
                Fusion::Rrf => weight / (req.rrf_k + *rank as f64),
                Fusion::Weighted if max > 0.0 => weight * score.max(0.0) / max,
                Fusion::Weighted => 0.0,
            },
        }
    };
    let mut fused: HashMap<NodeId, f64> = HashMap::with_capacity(bm25.len() + vec_hits.len());
    for id in bm25_of.keys().chain(vector_of.keys()) {
        fused.entry(*id).or_insert_with(|| {
            contribution(bm25_of.get(id), req.bm25_weight, bm25_best)
                + contribution(vector_of.get(id), req.vector_weight, vector_best)
        });
    }
    let mut ranked: Vec<(NodeId, f64)> = fused.iter().map(|(id, s)| (*id, *s)).collect();
    ranked.sort_by(by_score_then_id);
    ranked.truncate(req.k);
    let seeds: Vec<NodeId> = ranked.iter().map(|(id, _)| *id).collect();
    timings.fusion_ms = ms(started);

    // 4. Expansion: each seed's distance to everything it reaches, through
    // admitted nodes only.
    let started = Instant::now();
    let seed_set: HashSet<NodeId> = seeds.iter().copied().collect();
    let mut admitted: Vec<NodeId> = Vec::new();
    let mut admitted_set: HashSet<NodeId> = HashSet::new();
    let mut reached: Vec<HashMap<NodeId, usize>> = seeds
        .iter()
        .map(|s| HashMap::from([(*s, 0usize)]))
        .collect();
    if req.hops > 0 {
        let wanted = |t: &crate::graph::EdgeType| {
            req.rel_types.is_empty() || req.rel_types.iter().any(|r| r == t.as_str())
        };
        let mut frontier: Vec<(usize, NodeId)> = seeds.iter().copied().enumerate().collect();
        for depth in 1..=req.hops {
            let mut next = Vec::new();
            for (si, node) in frontier {
                let mut neighbours: Vec<NodeId> = Vec::new();
                if req.direction != Direction::Incoming {
                    neighbours.extend(
                        store
                            .get_outgoing_edge_targets(node)
                            .into_iter()
                            .filter(|(_, _, _, t)| wanted(t))
                            .map(|(_, _, target, _)| target),
                    );
                }
                if req.direction != Direction::Outgoing {
                    neighbours.extend(
                        store
                            .get_incoming_edge_sources(node)
                            .into_iter()
                            .filter(|(_, _, _, t)| wanted(t))
                            .map(|(_, source, _, _)| source),
                    );
                }
                neighbours.sort_unstable();
                neighbours.dedup();
                for nb in neighbours {
                    if reached[si].contains_key(&nb) || !store.has_node(nb) {
                        continue;
                    }
                    let open = seed_set.contains(&nb)
                        || admitted_set.contains(&nb)
                        || (admitted.len() < req.max_expanded && {
                            admitted.push(nb);
                            admitted_set.insert(nb);
                            true
                        });
                    if open {
                        reached[si].insert(nb, depth);
                        next.push((si, nb));
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
    }
    timings.expansion_ms = ms(started);

    // 5. Rerank.
    let started = Instant::now();
    let fused_of = |id: &NodeId| fused.get(id).copied().unwrap_or(0.0);
    let mut hits: Vec<HybridHit> = seeds
        .iter()
        .copied()
        .chain(admitted.iter().copied())
        .map(|node| {
            let is_seed = seed_set.contains(&node);
            let mut graph = 0.0;
            // (contribution, seed rank, hops) of the seed contributing most.
            let mut strongest: Option<(f64, usize, usize)> = None;
            for (si, seed) in seeds.iter().enumerate() {
                if *seed == node {
                    continue;
                }
                if let Some(d) = reached[si].get(&node) {
                    let c = fused_of(seed) * req.decay.powi(*d as i32);
                    graph += c;
                    if strongest.is_none_or(|(best, _, _)| c > best) {
                        strongest = Some((c, si, *d));
                    }
                }
            }
            let (seed, hops) = match (is_seed, strongest) {
                (true, _) | (false, None) => (node, 0),
                (false, Some((_, si, d))) => (seeds[si], d),
            };
            let fused = fused_of(&node);
            HybridHit {
                node,
                score: fused + req.graph_weight * graph,
                fused,
                graph,
                bm25: bm25_of.get(&node).copied(),
                vector: vector_of.get(&node).copied(),
                is_seed,
                hops,
                seed,
            }
        })
        .collect();
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then(b.fused.total_cmp(&a.fused))
            .then(a.node.cmp(&b.node))
    });
    timings.rerank_ms = ms(started);
    timings.total_ms = ms(total);

    Ok(HybridResult { hits, timings })
}

/// A value an argument evaluated to, as a property, where it is one.
fn to_property(value: Value) -> Option<PropertyValue> {
    match value {
        Value::Property(p) => Some(p),
        Value::Null => Some(PropertyValue::Null),
        Value::List(items) => items
            .into_iter()
            .map(to_property)
            .collect::<Option<Vec<_>>>()
            .map(PropertyValue::Array),
        Value::Map(entries) => entries
            .into_iter()
            .map(|(k, v)| to_property(v).map(|p| (k, p)))
            .collect::<Option<HashMap<_, _>>>()
            .map(PropertyValue::Map),
        _ => None,
    }
}

/// Read the call's five arguments into a request.
pub fn request_from_args(
    args: &[Expression],
    store: &GraphStore,
) -> ExecutionResult<HybridRequest> {
    if args.len() < 4 || args.len() > 5 {
        return Err(bad(
            "takes (fulltextIndex, queryText, vectorIndex, queryVector) and an optional \
             config map",
        ));
    }
    let empty = Record::new();
    let mut values = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        let value = eval_expression(arg, &empty, store)?;
        values.push(to_property(value).ok_or_else(|| {
            bad(format!(
                "argument {} must be a value, not a node, relationship or path",
                i + 1
            ))
        })?);
    }
    let fulltext_index = match &values[0] {
        PropertyValue::Null => None,
        PropertyValue::String(s) if !s.trim().is_empty() => Some(s.clone()),
        other => {
            return Err(bad(format!(
                "the full-text index must be an index name or null, got {other:?}"
            )))
        }
    };
    let query_text = match &values[1] {
        PropertyValue::Null => None,
        PropertyValue::String(s) => Some(s.clone()),
        other => {
            return Err(bad(format!(
                "the query text must be a string or null, got {other:?}"
            )))
        }
    };
    let vector_index = match &values[2] {
        PropertyValue::Null => None,
        PropertyValue::String(s) if !s.trim().is_empty() => Some(VectorIndexRef::Name(s.clone())),
        PropertyValue::Map(m) => match (m.get("label"), m.get("property"), m.len()) {
            (Some(PropertyValue::String(l)), Some(PropertyValue::String(p)), 2) => {
                Some(VectorIndexRef::On {
                    label: l.clone(),
                    property: p.clone(),
                })
            }
            _ => {
                return Err(bad(
                    "a vector index given as a map must be {label: 'L', property: 'p'}",
                ))
            }
        },
        other => {
            return Err(bad(format!(
                "the vector index must be an index name, {{label, property}} or null, got {other:?}"
            )))
        }
    };
    let query_vector = match &values[3] {
        PropertyValue::Null => None,
        PropertyValue::Array(a) if a.is_empty() => Some(Vec::new()),
        v => Some(v.to_vector().ok_or_else(|| {
            bad(format!(
                "the query vector must be a list of numbers or null, got {v:?}"
            ))
        })?),
    };

    let mut req = HybridRequest {
        fulltext_index,
        query_text,
        vector_index,
        query_vector,
        ..HybridRequest::default()
    };

    let config = match values.get(4) {
        None | Some(PropertyValue::Null) => HashMap::new(),
        Some(PropertyValue::Map(m)) => m.clone(),
        Some(other) => {
            return Err(bad(format!(
                "the last argument must be a config map, got {other:?}"
            )))
        }
    };
    let mut unknown: Vec<&String> = config
        .keys()
        .filter(|k| !CONFIG_KEYS.contains(&k.as_str()))
        .collect();
    unknown.sort();
    if let Some(first) = unknown.first() {
        return Err(bad(format!(
            "unknown config key `{first}`. It reads: {}",
            CONFIG_KEYS.join(", ")
        )));
    }

    let integer = |key: &str, min: i64| -> ExecutionResult<Option<usize>> {
        match config.get(key) {
            None | Some(PropertyValue::Null) => Ok(None),
            Some(PropertyValue::Integer(n)) if *n >= min => Ok(Some(*n as usize)),
            Some(other) => Err(bad(format!(
                "`{key}` must be an integer of at least {min}, got {other:?}"
            ))),
        }
    };
    let number = |key: &str| -> ExecutionResult<Option<f64>> {
        match config.get(key) {
            None | Some(PropertyValue::Null) => Ok(None),
            Some(PropertyValue::Integer(n)) if *n >= 0 => Ok(Some(*n as f64)),
            Some(PropertyValue::Float(f)) if f.is_finite() && *f >= 0.0 => Ok(Some(*f)),
            Some(other) => Err(bad(format!(
                "`{key}` must be a non-negative number, got {other:?}"
            ))),
        }
    };

    if let Some(k) = integer("k", 1)? {
        req.k = k;
    }
    req.candidates = integer("candidates", 1)?.unwrap_or(req.candidates.max(req.k));
    req.ef = integer("ef", 1)?;
    if let Some(h) = integer("hops", 0)? {
        if h > MAX_HOPS {
            return Err(bad(format!(
                "`hops` is at most {MAX_HOPS}: the expansion is local. Write a MATCH for a \
                 longer traversal"
            )));
        }
        req.hops = h;
    }
    if let Some(m) = integer("maxExpanded", 0)? {
        req.max_expanded = m;
    }
    if let Some(r) = number("rrfK")? {
        req.rrf_k = r;
    }
    if let Some(w) = number("bm25Weight")? {
        req.bm25_weight = w;
    }
    if let Some(w) = number("vectorWeight")? {
        req.vector_weight = w;
    }
    if let Some(w) = number("graphWeight")? {
        req.graph_weight = w;
    }
    if let Some(d) = number("decay")? {
        if d > 1.0 {
            return Err(bad(format!(
                "`decay` is a factor per hop in [0, 1], got {d}"
            )));
        }
        req.decay = d;
    }
    req.fusion = match config.get("fusion") {
        None | Some(PropertyValue::Null) => Fusion::Rrf,
        Some(PropertyValue::String(s)) if s.eq_ignore_ascii_case("rrf") => Fusion::Rrf,
        Some(PropertyValue::String(s)) if s.eq_ignore_ascii_case("weighted") => Fusion::Weighted,
        Some(other) => {
            return Err(bad(format!(
                "`fusion` is 'rrf' or 'weighted', got {other:?}"
            )))
        }
    };
    req.direction = match config.get("direction") {
        None | Some(PropertyValue::Null) => Direction::Both,
        Some(PropertyValue::String(s)) => match s.to_ascii_lowercase().as_str() {
            "both" => Direction::Both,
            "out" | "outgoing" => Direction::Outgoing,
            "in" | "incoming" => Direction::Incoming,
            _ => {
                return Err(bad(format!(
                    "`direction` is 'out', 'in' or 'both', got '{s}'"
                )))
            }
        },
        Some(other) => {
            return Err(bad(format!(
                "`direction` is 'out', 'in' or 'both', got {other:?}"
            )))
        }
    };
    req.rel_types = match config.get("relTypes") {
        None | Some(PropertyValue::Null) => Vec::new(),
        Some(PropertyValue::String(s)) => vec![s.clone()],
        Some(PropertyValue::Array(items)) => items
            .iter()
            .map(|t| match t {
                PropertyValue::String(s) => Ok(s.clone()),
                other => Err(bad(format!(
                    "`relTypes` must be a list of type names, got {other:?}"
                ))),
            })
            .collect::<ExecutionResult<Vec<_>>>()?,
        Some(other) => {
            return Err(bad(format!(
                "`relTypes` must be a list of type names, got {other:?}"
            )))
        }
    };
    Ok(req)
}

/// `db.hybrid.search` as a physical operator.
pub struct HybridSearchOperator {
    args: Vec<Expression>,
    /// Output variable per column, after `YIELD ... AS`.
    columns: Vec<(&'static str, String)>,
    rows: Option<std::vec::IntoIter<HybridHit>>,
    timings: StageTimings,
}

impl HybridSearchOperator {
    /// Built by the planner. `yields` are `(column, alias)` pairs; an empty
    /// list yields every column under its own name.
    pub fn new(
        args: Vec<Expression>,
        yields: &[(String, Option<String>)],
    ) -> ExecutionResult<Self> {
        if args.len() < 4 || args.len() > 5 {
            return Err(ExecutionError::PlanningError(format!(
                "{PROCEDURE} takes (fulltextIndex, queryText, vectorIndex, queryVector) and an \
                 optional config map"
            )));
        }
        let columns = if yields.is_empty() {
            COLUMNS.iter().map(|c| (*c, c.to_string())).collect()
        } else {
            let mut columns = Vec::with_capacity(yields.len());
            for (name, alias) in yields {
                let Some(column) = COLUMNS.iter().find(|c| **c == name.as_str()) else {
                    return Err(ExecutionError::PlanningError(format!(
                        "{PROCEDURE} yields {}; it has no `{name}` column",
                        COLUMNS.join(", ")
                    )));
                };
                columns.push((*column, alias.clone().unwrap_or_else(|| name.clone())));
            }
            columns
        };
        Ok(Self {
            args,
            columns,
            rows: None,
            timings: StageTimings::default(),
        })
    }

    fn row(&self, hit: HybridHit) -> Record {
        let int_or_null = |x: Option<usize>| {
            x.map_or(Value::Null, |n| {
                Value::Property(PropertyValue::Integer(n as i64))
            })
        };
        let float_or_null =
            |x: Option<f64>| x.map_or(Value::Null, |f| Value::Property(PropertyValue::Float(f)));
        let mut r = Record::new();
        for (column, var) in &self.columns {
            let value = match *column {
                "node" => Value::NodeRef(hit.node),
                "score" => Value::Property(PropertyValue::Float(hit.score)),
                "fusedScore" => Value::Property(PropertyValue::Float(hit.fused)),
                "graphScore" => Value::Property(PropertyValue::Float(hit.graph)),
                "bm25Rank" => int_or_null(hit.bm25.map(|b| b.0)),
                "bm25Score" => float_or_null(hit.bm25.map(|b| b.1)),
                "vectorRank" => int_or_null(hit.vector.map(|v| v.0)),
                "vectorScore" => float_or_null(hit.vector.map(|v| v.1)),
                "foundBy" => Value::Property(PropertyValue::Array(
                    hit.found_by()
                        .into_iter()
                        .map(|s| PropertyValue::String(s.to_string()))
                        .collect(),
                )),
                "hops" => Value::Property(PropertyValue::Integer(hit.hops as i64)),
                "seed" => Value::NodeRef(hit.seed),
                _ => Value::Property(PropertyValue::Map(self.timings.to_map())),
            };
            r.bind(var.clone(), value);
        }
        r
    }
}

impl PhysicalOperator for HybridSearchOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            let req = request_from_args(&self.args, store)?;
            let result = hybrid_search(store, &req)?;
            self.timings = result.timings;
            self.rows = Some(result.hits.into_iter());
        }
        Ok(self
            .rows
            .as_mut()
            .and_then(|r| r.next())
            .map(|hit| self.row(hit)))
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "HybridSearch".to_string(),
            details: "bm25 + vector -> fusion -> expansion -> rerank".to_string(),
            children: Vec::new(),
        }
    }
}
