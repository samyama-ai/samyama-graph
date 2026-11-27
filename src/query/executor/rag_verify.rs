//! `ai.rag.verify_or_refuse`: check an answer's claims against the evidence
//! it was written from, and refuse when they are not grounded (part C of
//! #2098, #2234; AI-08 / TRUST-06).
//!
//! ```cypher
//! CALL ai.rag.verify_or_refuse($query, $claims, $evidence, {minScore: 0.8})
//! YIELD status, verified_claims, unsupported_claims, abstain_reason,
//!       confidence, coverage, detail
//! ```
//!
//! The decision is a pure function of the arguments and the graph: no model
//! is called, nothing is sampled, and the same inputs give the same verdict.
//!
//! # Arguments
//!
//! | | |
//! |---|---|
//! | `query` | the question, a string or null. Not used by the decision; accepted so the call has #2098's shape |
//! | `candidate_answer` | the answer's claims: a list of `{subject, predicate, object}` (or `[subject, predicate, object]`), a map with such a list under `claims`, or null / `[]` to check the evidence alone |
//! | `evidence_envelope` | what [`ai.rag.retrieve`](super::rag_ops) returned, as either form below |
//! | `config` | thresholds, a map or null; see [`GateConfig`] |
//!
//! Free text is refused as `candidate_answer`. Turning prose into claims needs
//! an extractor, and an extractor is the non-deterministic step this gate
//! exists to keep out of the decision; the caller extracts, the gate checks.
//!
//! # The envelope
//!
//! Either the rows of `ai.rag.retrieve`, one map per row with `node`, `seed`,
//! `score` and `path` (`path` a path value, or `{nodes, edges}` of ids, which is
//! how a path comes back as JSON), or the `samyama-mcp` envelope
//! `{sources, lineage_paths}`, whose paths carry `node_id`, `seed_node_id`,
//! `seed_vector_score` and `hop_sequence: [{src, edge_type, dst}]`. Ids may be
//! integers, numeric strings, nodes, or maps with an `id`.
//!
//! **Nothing in the envelope is taken on trust.** Every walk is re-checked
//! against the graph: each edge must exist and join the nodes the walk says it
//! joins. An item whose walk is not in the graph is dropped and counted in
//! `detail`, so an envelope edited after retrieval cannot ground a claim the
//! graph does not. Sources are read from the graph, not from the envelope.
//!
//! An item marked `redacted: true`, or a `redacted_count` above zero on the
//! envelope map, records context a policy withheld (part B, #2233). It grounds
//! nothing, and it changes why the gate refuses: a claim that may have been
//! supported by withheld context is `POLICY_REDACTED_CONTEXT`, not
//! `INSUFFICIENT_SUBGRAPH_EVIDENCE`.
//!
//! # When a claim is supported
//!
//! Subject and object name nodes: an id names that node, a string names every
//! node in the evidence whose `matchProperties` (default `name`) equals it,
//! ignoring case and surrounding space. Then, first that holds:
//!
//! 1. **Relation.** A hop of some evidence walk is an edge of type `predicate`
//!    (ignoring case) from a subject node to an object node, either way round
//!    when `directed` is false.
//! 2. **Induced relation**, only with `induced: true`. Such an edge exists in
//!    the graph between two nodes of the evidence, though no walk took it.
//!    Retrieval reports each node once, by one walk, so an edge between two
//!    retrieved nodes is often on no walk.
//! 3. **Property.** A subject node in the evidence has property `predicate`
//!    and its value equals `object` (numbers by value, strings ignoring case and
//!    surrounding space). A different value **contradicts** the claim.
//!
//! # Confidence
//!
//! A hop walked `h` edges from its seed has path confidence
//! `score * hopDecay^h`, `score` being the seed's similarity. A claim's
//! confidence is that of the evidence supporting it, the best when several do;
//! an induced relation takes the lower of its two ends', one hop further. The
//! verdict's `confidence` is the lowest over the verified claims, or with no
//! claims the best evidence item's. An unknown score gives an unknown (null)
//! confidence, which no positive `minConfidence` accepts.
//!
//! # The verdict
//!
//! `status` is `VERIFIED` or `REFUSED`. The first of these that holds is the
//! `abstain_reason`:
//!
//! 1. No usable evidence: `POLICY_REDACTED_CONTEXT` when some was redacted,
//!    otherwise `INSUFFICIENT_SUBGRAPH_EVIDENCE`.
//! 2. `minScore` is set and the best seed score is below it, or unknown:
//!    `BELOW_SIMILARITY_FLOOR`.
//! 3. A claim is contradicted: `CONTRADICTORY_PATHS`, whatever `minCoverage`
//!    allows. A claim the evidence says is false is not a gap.
//! 4. The supported share of the claims is below `minCoverage`:
//!    `POLICY_REDACTED_CONTEXT` when some evidence was redacted, otherwise
//!    `INSUFFICIENT_SUBGRAPH_EVIDENCE`.
//! 5. `confidence` is below `minConfidence`: `LOW_PATH_CONFIDENCE`.
//!
//! The gate never returns the answer. A caller shows it only on `VERIFIED`,
//! citing `verified_claims`.

use std::collections::{BTreeMap, BTreeSet};

use crate::graph::{EdgeId, GraphStore, NodeId, PropertyValue};
use crate::query::ast::Expression;
use crate::query::executor::operator::{eval_expression, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// The procedure's name.
pub const NAME: &str = "ai.rag.verify_or_refuse";

/// The columns, in the order a call without `YIELD` binds them.
pub const COLUMNS: [&str; 7] = [
    "status",
    "verified_claims",
    "unsupported_claims",
    "abstain_reason",
    "confidence",
    "coverage",
    "detail",
];

/// The keys `config` reads.
pub const CONFIG_KEYS: [&str; 7] = [
    "minCoverage",
    "minConfidence",
    "minScore",
    "hopDecay",
    "directed",
    "induced",
    "matchProperties",
];

/// Whether the answer may be shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Verified,
    Refused,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Verified => "VERIFIED",
            Status::Refused => "REFUSED",
        }
    }
}

/// Why the gate refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbstainReason {
    /// The evidence does not support enough of the answer, or there is none.
    InsufficientSubgraphEvidence,
    /// As above, and a policy withheld some of the context (#2233).
    PolicyRedactedContext,
    /// The evidence says a claim is false.
    ContradictoryPaths,
    /// The best seed is less similar to the question than `minScore`.
    BelowSimilarityFloor,
    /// The weakest supporting path is below `minConfidence`.
    LowPathConfidence,
}

impl AbstainReason {
    pub fn as_str(self) -> &'static str {
        match self {
            AbstainReason::InsufficientSubgraphEvidence => "INSUFFICIENT_SUBGRAPH_EVIDENCE",
            AbstainReason::PolicyRedactedContext => "POLICY_REDACTED_CONTEXT",
            AbstainReason::ContradictoryPaths => "CONTRADICTORY_PATHS",
            AbstainReason::BelowSimilarityFloor => "BELOW_SIMILARITY_FLOOR",
            AbstainReason::LowPathConfidence => "LOW_PATH_CONFIDENCE",
        }
    }
}

/// The thresholds, read from the `config` map.
#[derive(Debug, Clone, PartialEq)]
pub struct GateConfig {
    /// Share of the claims that must be supported, `0..=1`. Default `1`: all.
    pub min_coverage: f64,
    /// Lowest path confidence accepted. Default `0`: not checked.
    pub min_confidence: f64,
    /// Lowest best-seed score accepted. Default none: not checked, because
    /// what a good score is depends on the index's metric.
    pub min_score: Option<f64>,
    /// What each hop from the seed multiplies confidence by, `0..=1`.
    /// Default `0.5`.
    pub hop_decay: f64,
    /// Whether a relation claim must follow the edge's direction. Default true.
    pub directed: bool,
    /// Whether an edge between two evidence nodes that no walk took counts.
    /// Default false.
    pub induced: bool,
    /// The properties a string subject or object is matched against.
    /// Default `["name"]`.
    pub match_properties: Vec<String>,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            min_coverage: 1.0,
            min_confidence: 0.0,
            min_score: None,
            hop_decay: 0.5,
            directed: true,
            induced: false,
            match_properties: vec!["name".to_string()],
        }
    }
}

fn err(msg: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::RuntimeError(format!("{NAME}: {msg}"))
}

/// A list, whichever way the value holds one.
fn as_list(v: &Value) -> Option<Vec<Value>> {
    match v {
        Value::List(items) => Some(items.clone()),
        Value::Property(PropertyValue::Array(items)) => {
            Some(items.iter().cloned().map(Value::Property).collect())
        }
        _ => None,
    }
}

/// A map, whichever way the value holds one.
fn as_map(v: &Value) -> Option<BTreeMap<String, Value>> {
    match v {
        Value::Map(m) => Some(m.clone()),
        Value::Property(PropertyValue::Map(m)) => Some(
            m.iter()
                .map(|(k, v)| (k.clone(), Value::Property(v.clone())))
                .collect(),
        ),
        _ => None,
    }
}

fn is_null(v: &Value) -> bool {
    matches!(v, Value::Null | Value::Property(PropertyValue::Null))
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Property(PropertyValue::Float(f)) => Some(*f),
        Value::Property(PropertyValue::Integer(i)) => Some(*i as f64),
        _ => None,
    }
}

fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Property(PropertyValue::Boolean(b)) => Some(*b),
        _ => None,
    }
}

/// An id: an integer, a numeric string, or a map with an `id`.
fn raw_id(v: &Value) -> Option<u64> {
    match v {
        Value::Property(PropertyValue::Integer(i)) if *i >= 0 => Some(*i as u64),
        Value::Property(PropertyValue::String(s)) => s.trim().parse().ok(),
        _ => as_map(v).and_then(|m| m.get("id").and_then(raw_id)),
    }
}

fn node_id(v: &Value) -> Option<NodeId> {
    match v {
        Value::NodeRef(id) | Value::Node(id, _) => Some(*id),
        _ => raw_id(v).map(NodeId::new),
    }
}

fn edge_id(v: &Value) -> Option<EdgeId> {
    match v {
        Value::EdgeRef(id, ..) | Value::Edge(id, _) => Some(*id),
        _ => raw_id(v).map(EdgeId::new),
    }
}

impl GateConfig {
    /// The configuration in `v`, a map or null. An unknown key is refused:
    /// a misspelt threshold silently left at its default is a gate that
    /// checks less than its caller thinks.
    pub fn from_value(v: &Value) -> ExecutionResult<Self> {
        let mut c = Self::default();
        if is_null(v) {
            return Ok(c);
        }
        let map = as_map(v).ok_or_else(|| err("config must be a map or null"))?;
        let unit = |key: &str, x: &Value| -> ExecutionResult<f64> {
            match as_f64(x) {
                Some(f) if (0.0..=1.0).contains(&f) => Ok(f),
                _ => Err(err(format!("`{key}` must be a number between 0 and 1"))),
            }
        };
        let flag = |key: &str, x: &Value| -> ExecutionResult<bool> {
            as_bool(x).ok_or_else(|| err(format!("`{key}` must be true or false")))
        };
        for (key, x) in &map {
            if is_null(x) {
                continue;
            }
            match key.as_str() {
                "minCoverage" => c.min_coverage = unit(key, x)?,
                "minConfidence" => c.min_confidence = unit(key, x)?,
                "hopDecay" => c.hop_decay = unit(key, x)?,
                "minScore" => {
                    c.min_score = Some(
                        as_f64(x)
                            .filter(|f| f.is_finite())
                            .ok_or_else(|| err("`minScore` must be a number"))?,
                    )
                }
                "directed" => c.directed = flag(key, x)?,
                "induced" => c.induced = flag(key, x)?,
                "matchProperties" => {
                    let names: Option<Vec<String>> = as_list(x).and_then(|items| {
                        items
                            .iter()
                            .map(|i| match i {
                                Value::Property(PropertyValue::String(s)) => Some(s.clone()),
                                _ => None,
                            })
                            .collect()
                    });
                    c.match_properties = names
                        .ok_or_else(|| err("`matchProperties` must be a list of property names"))?;
                }
                other => {
                    return Err(err(format!(
                        "config has no `{other}`; it reads {}",
                        CONFIG_KEYS.join(", ")
                    )))
                }
            }
        }
        Ok(c)
    }
}

/// One edge of a walk, in the edge's own direction.
#[derive(Debug, Clone)]
struct Hop {
    src: NodeId,
    dst: NodeId,
    edge_type: String,
    edge: EdgeId,
}

/// One evidence item whose walk is in the graph.
#[derive(Debug, Clone)]
struct Item {
    /// Its position in the envelope, for citing it.
    index: usize,
    seed: NodeId,
    score: Option<f64>,
    /// The walk's nodes, seed first.
    walk: Vec<NodeId>,
    /// `hops[i]` joins `walk[i]` and `walk[i + 1]`.
    hops: Vec<Hop>,
}

/// The envelope, checked against the graph.
#[derive(Debug, Default)]
struct Evidence {
    items: Vec<Item>,
    /// Items dropped because their walk is not in the graph or they name no node.
    rejected: usize,
    /// Items, or a count, a policy withheld.
    redacted: usize,
}

/// The walk `nodes`/`edges`, if every edge exists and joins its two nodes.
fn walk_from_path(store: &GraphStore, nodes: &[NodeId], edges: &[EdgeId]) -> Option<Vec<Hop>> {
    if nodes.len() != edges.len() + 1 || !nodes.iter().all(|n| store.has_node(*n)) {
        return None;
    }
    edges
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let e = store.get_edge(*id)?;
            let (a, b) = (nodes[i], nodes[i + 1]);
            let joins = (e.source == a && e.target == b) || (e.source == b && e.target == a);
            joins.then(|| Hop {
                src: e.source,
                dst: e.target,
                edge_type: e.edge_type.as_str().to_string(),
                edge: e.id,
            })
        })
        .collect()
}

/// The walk from `seed` along `hops` as `(src, type, dst)`, if each such edge
/// exists and each continues from where the last one ended.
fn walk_from_hops(
    store: &GraphStore,
    seed: NodeId,
    hops: &[(NodeId, String, NodeId)],
) -> Option<(Vec<NodeId>, Vec<Hop>)> {
    if !store.has_node(seed) {
        return None;
    }
    let mut walk = vec![seed];
    let mut out = Vec::new();
    for (src, edge_type, dst) in hops {
        let at = *walk.last()?;
        let next = if at == *src {
            *dst
        } else if at == *dst {
            *src
        } else {
            return None;
        };
        let mut found: Vec<_> = store
            .get_outgoing_edges(*src)
            .into_iter()
            .filter(|e| e.target == *dst && e.edge_type.as_str() == edge_type)
            .map(|e| e.id)
            .collect();
        found.sort();
        let edge = *found.first()?;
        out.push(Hop {
            src: *src,
            dst: *dst,
            edge_type: edge_type.clone(),
            edge,
        });
        walk.push(next);
    }
    Some((walk, out))
}

/// `[{src, edge_type, dst}]` or `[[src, edge_type, dst]]`.
fn hop_triples(v: &Value) -> Option<Vec<(NodeId, String, NodeId)>> {
    as_list(v)?
        .iter()
        .map(|h| {
            let (s, t, d) = if let Some(m) = as_map(h) {
                (
                    m.get("src")?.clone(),
                    m.get("edge_type")?.clone(),
                    m.get("dst")?.clone(),
                )
            } else {
                let l = as_list(h)?;
                if l.len() != 3 {
                    return None;
                }
                (l[0].clone(), l[1].clone(), l[2].clone())
            };
            let t = match t {
                Value::Property(PropertyValue::String(s)) => s,
                _ => return None,
            };
            Some((node_id(&s)?, t, node_id(&d)?))
        })
        .collect()
}

/// One envelope item, checked; `None` when it names no node or its walk is
/// not in the graph.
fn item(store: &GraphStore, index: usize, m: &BTreeMap<String, Value>) -> Option<Item> {
    let get = |keys: &[&str]| keys.iter().find_map(|k| m.get(*k)).filter(|v| !is_null(v));
    let node = node_id(get(&["node", "node_id"])?)?;
    let seed = match get(&["seed", "seed_node_id"]) {
        Some(v) => Some(node_id(v)?),
        None => None,
    };
    let score = match get(&["score", "seed_vector_score"]) {
        Some(v) => Some(as_f64(v).filter(|f| f.is_finite())?),
        None => None,
    };
    let (walk, hops) = if let Some(p) = get(&["path"]) {
        let (nodes, edges) = match p {
            Value::Path { nodes, edges } => (nodes.clone(), edges.clone()),
            other => {
                let pm = as_map(other)?;
                let nodes = as_list(pm.get("nodes")?)?
                    .iter()
                    .map(node_id)
                    .collect::<Option<Vec<_>>>()?;
                let edges = match pm.get("edges") {
                    Some(e) => as_list(e)?
                        .iter()
                        .map(edge_id)
                        .collect::<Option<Vec<_>>>()?,
                    None => Vec::new(),
                };
                (nodes, edges)
            }
        };
        let hops = walk_from_path(store, &nodes, &edges)?;
        (nodes, hops)
    } else if let Some(h) = get(&["hop_sequence"]) {
        walk_from_hops(store, seed.unwrap_or(node), &hop_triples(h)?)?
    } else {
        // No walk given: only a seed, which is its own walk.
        if !store.has_node(node) {
            return None;
        }
        (vec![node], Vec::new())
    };
    // The walk must start at the seed and end at the node the item is about.
    let seed = seed.unwrap_or(walk[0]);
    if walk[0] != seed || *walk.last()? != node {
        return None;
    }
    Some(Item {
        index,
        seed,
        score,
        walk,
        hops,
    })
}

fn evidence(store: &GraphStore, envelope: &Value) -> ExecutionResult<Evidence> {
    let mut ev = Evidence::default();
    if is_null(envelope) {
        return Ok(ev);
    }
    let items = if let Some(list) = as_list(envelope) {
        list
    } else if let Some(m) = as_map(envelope) {
        if let Some(n) = m.get("redacted_count").and_then(as_f64) {
            ev.redacted += n.max(0.0) as usize;
        }
        match m.get("lineage_paths") {
            Some(l) => as_list(l).ok_or_else(|| err("`lineage_paths` must be a list"))?,
            None => {
                return Err(err(
                    "the evidence envelope is a list of ai.rag.retrieve rows or a map with `lineage_paths`",
                ))
            }
        }
    } else {
        return Err(err(
            "the evidence envelope is a list of ai.rag.retrieve rows or a map with `lineage_paths`",
        ));
    };
    for (index, raw) in items.iter().enumerate() {
        let Some(m) = as_map(raw) else {
            ev.rejected += 1;
            continue;
        };
        if m.get("redacted").and_then(as_bool) == Some(true) {
            ev.redacted += 1;
            continue;
        }
        match item(store, index, &m) {
            Some(i) => ev.items.push(i),
            None => ev.rejected += 1,
        }
    }
    Ok(ev)
}

/// A known confidence beats an unknown one; a higher beats a lower.
fn better(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => x > y,
        (Some(_), None) => true,
        _ => false,
    }
}

/// Where a node is best reached: confidence, item, edges from the seed.
#[derive(Debug, Clone, Copy)]
struct Reach {
    confidence: Option<f64>,
    item: usize,
    hops: usize,
}

/// What supports a claim.
#[derive(Debug, Clone)]
struct Support {
    kind: &'static str,
    confidence: Option<f64>,
    /// Index into `Evidence::items`.
    item: usize,
    hops: usize,
    edge: Option<EdgeId>,
}

/// One claim after checking.
#[derive(Debug, Clone)]
pub struct CheckedClaim {
    /// The claim as given, with what supports it or why nothing does.
    pub value: Value,
    pub confidence: Option<f64>,
}

/// The gate's decision.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub status: Status,
    pub abstain_reason: Option<AbstainReason>,
    pub verified: Vec<CheckedClaim>,
    pub unsupported: Vec<CheckedClaim>,
    pub confidence: Option<f64>,
    /// Supported share of the claims; `None` when there are none.
    pub coverage: Option<f64>,
    pub detail: String,
}

struct Gate<'a> {
    store: &'a GraphStore,
    config: &'a GateConfig,
    ev: Evidence,
    reach: BTreeMap<NodeId, Reach>,
    /// Lower-cased, trimmed `matchProperties` value -> evidence nodes.
    names: BTreeMap<String, BTreeSet<NodeId>>,
}

fn norm(s: &str) -> String {
    s.trim().to_lowercase()
}

/// Whether a stored property value says what the claim's object says.
fn same_value(stored: &PropertyValue, claimed: &PropertyValue) -> bool {
    use PropertyValue as P;
    match (stored, claimed) {
        (P::String(a), P::String(b)) => norm(a) == norm(b),
        (P::Integer(a), P::Integer(b)) => a == b,
        // `as_float` is `None` for an integer, so it cannot compare across the
        // two numeric types: widen the integer instead.
        (P::Integer(i), P::Float(f)) | (P::Float(f), P::Integer(i)) => *i as f64 == *f,
        (P::Float(a), P::Float(b)) => a == b,
        (P::Array(a), P::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| same_value(x, y))
        }
        _ => stored == claimed,
    }
}

impl<'a> Gate<'a> {
    fn new(store: &'a GraphStore, config: &'a GateConfig, ev: Evidence) -> Self {
        let mut reach: BTreeMap<NodeId, Reach> = BTreeMap::new();
        for (k, it) in ev.items.iter().enumerate() {
            for (h, n) in it.walk.iter().enumerate() {
                let r = Reach {
                    confidence: it.score.map(|s| s * config.hop_decay.powi(h as i32)),
                    item: k,
                    hops: h,
                };
                match reach.get(n) {
                    Some(old) if !better(r.confidence, old.confidence) => {}
                    _ => {
                        reach.insert(*n, r);
                    }
                }
            }
        }
        let mut names: BTreeMap<String, BTreeSet<NodeId>> = BTreeMap::new();
        for n in reach.keys() {
            let props = store.node_properties_merged(*n);
            for p in &config.match_properties {
                if let Some(PropertyValue::String(s)) = props.get(p) {
                    names.entry(norm(s)).or_default().insert(*n);
                }
            }
        }
        Self {
            store,
            config,
            ev,
            reach,
            names,
        }
    }

    /// The evidence nodes `v` names.
    fn resolve(&self, v: &Value) -> BTreeSet<NodeId> {
        match v {
            Value::Property(PropertyValue::String(s)) => {
                self.names.get(&norm(s)).cloned().unwrap_or_default()
            }
            _ => node_id(v)
                .filter(|n| self.reach.contains_key(n))
                .into_iter()
                .collect(),
        }
    }

    fn relation(&self, s: &BTreeSet<NodeId>, p: &str, o: &BTreeSet<NodeId>) -> Option<Support> {
        let joins = |hop: &Hop| {
            hop.edge_type.eq_ignore_ascii_case(p)
                && ((s.contains(&hop.src) && o.contains(&hop.dst))
                    || (!self.config.directed && o.contains(&hop.src) && s.contains(&hop.dst)))
        };
        let mut best: Option<Support> = None;
        for (k, it) in self.ev.items.iter().enumerate() {
            if let Some((i, hop)) = it.hops.iter().enumerate().find(|(_, h)| joins(h)) {
                let c = it
                    .score
                    .map(|sc| sc * self.config.hop_decay.powi(i as i32 + 1));
                if best.as_ref().is_none_or(|b| better(c, b.confidence)) {
                    best = Some(Support {
                        kind: "relation",
                        confidence: c,
                        item: k,
                        hops: i + 1,
                        edge: Some(hop.edge),
                    });
                }
            }
        }
        if best.is_some() || !self.config.induced {
            return best;
        }
        // An edge in the graph between two evidence nodes that no walk took.
        for a in s {
            for e in self.store.get_outgoing_edges(*a) {
                let forward = o.contains(&e.target);
                if !forward || !e.edge_type.as_str().eq_ignore_ascii_case(p) {
                    continue;
                }
                best = self.induced(best, *a, e.target, e.id);
            }
            if !self.config.directed {
                for e in self.store.get_incoming_edges(*a) {
                    if o.contains(&e.source) && e.edge_type.as_str().eq_ignore_ascii_case(p) {
                        best = self.induced(best, *a, e.source, e.id);
                    }
                }
            }
        }
        best
    }

    fn induced(
        &self,
        best: Option<Support>,
        a: NodeId,
        b: NodeId,
        edge: EdgeId,
    ) -> Option<Support> {
        let (ra, rb) = (self.reach[&a], self.reach[&b]);
        let c = match (ra.confidence, rb.confidence) {
            (Some(x), Some(y)) => Some(x.min(y) * self.config.hop_decay),
            _ => None,
        };
        let near = if ra.hops <= rb.hops { ra } else { rb };
        let candidate = Support {
            kind: "induced",
            confidence: c,
            item: near.item,
            hops: near.hops + 1,
            edge: Some(edge),
        };
        match best {
            Some(b) if !better(c, b.confidence) => Some(b),
            _ => Some(candidate),
        }
    }

    /// `Ok(support)`, or `Err(value found)` when a subject's property
    /// contradicts the claim, or `Err(None)` when no subject has it.
    fn property(
        &self,
        s: &BTreeSet<NodeId>,
        p: &str,
        object: &Value,
    ) -> Result<Support, Option<PropertyValue>> {
        let Value::Property(claimed) = object else {
            return Err(None);
        };
        let mut best: Option<Support> = None;
        let mut contradiction = None;
        for n in s {
            let props = self.store.node_properties_merged(*n);
            let stored = props.get(p).or_else(|| {
                let mut keys: Vec<&String> =
                    props.keys().filter(|k| k.eq_ignore_ascii_case(p)).collect();
                keys.sort();
                keys.first().and_then(|k| props.get(*k))
            });
            let Some(stored) = stored else { continue };
            if same_value(stored, claimed) {
                let r = self.reach[n];
                if best
                    .as_ref()
                    .is_none_or(|b| better(r.confidence, b.confidence))
                {
                    best = Some(Support {
                        kind: "property",
                        confidence: r.confidence,
                        item: r.item,
                        hops: r.hops,
                        edge: None,
                    });
                }
            } else if contradiction.is_none() {
                contradiction = Some(stored.clone());
            }
        }
        best.ok_or(contradiction)
    }

    fn cite(&self, claim: BTreeMap<String, Value>, s: &Support) -> CheckedClaim {
        let it = &self.ev.items[s.item];
        let mut m = claim;
        let int = |i: u64| Value::Property(PropertyValue::Integer(i as i64));
        let num =
            |f: Option<f64>| f.map_or(Value::Null, |f| Value::Property(PropertyValue::Float(f)));
        m.insert(
            "kind".into(),
            Value::Property(PropertyValue::String(s.kind.into())),
        );
        m.insert("confidence".into(), num(s.confidence));
        m.insert("evidence".into(), int(it.index as u64));
        m.insert("seed".into(), int(it.seed.as_u64()));
        m.insert("score".into(), num(it.score));
        m.insert("hops".into(), int(s.hops as u64));
        m.insert(
            "edge".into(),
            s.edge.map_or(Value::Null, |e| int(e.as_u64())),
        );
        m.insert(
            "source".into(),
            super::rag_ops::source(self.store, it.seed).map_or(Value::Null, Value::Property),
        );
        CheckedClaim {
            value: Value::Map(m),
            confidence: s.confidence,
        }
    }
}

fn reject(
    claim: BTreeMap<String, Value>,
    reason: &str,
    found: Option<PropertyValue>,
) -> CheckedClaim {
    let mut m = claim;
    m.insert(
        "reason".into(),
        Value::Property(PropertyValue::String(reason.into())),
    );
    m.insert("found".into(), found.map_or(Value::Null, Value::Property));
    CheckedClaim {
        value: Value::Map(m),
        confidence: None,
    }
}

/// The claims in `answer`, as given.
fn claims(answer: &Value) -> ExecutionResult<Vec<Value>> {
    if is_null(answer) {
        return Ok(Vec::new());
    }
    if let Value::Property(PropertyValue::String(_)) = answer {
        return Err(err(
            "candidate_answer is free text; pass its claims as a list of \
             {subject, predicate, object}. Extracting claims from prose is not \
             deterministic, so it is the caller's step, not the gate's",
        ));
    }
    if let Some(list) = as_list(answer) {
        return Ok(list);
    }
    if let Some(l) = as_map(answer).and_then(|m| m.get("claims").and_then(as_list)) {
        return Ok(l);
    }
    Err(err(
        "candidate_answer must be a list of {subject, predicate, object}, a map with `claims`, or null",
    ))
}

/// A claim's subject, predicate and object.
type Triple = (Value, String, Value);

/// `(subject, predicate, object)` of a claim, and the claim as a map to echo.
fn parse_claim(v: &Value) -> (BTreeMap<String, Value>, Option<Triple>) {
    let (s, p, o) = if let Some(m) = as_map(v) {
        (
            m.get("subject").cloned(),
            m.get("predicate").cloned(),
            m.get("object").cloned(),
        )
    } else if let Some(l) = as_list(v).filter(|l| l.len() == 3) {
        (Some(l[0].clone()), Some(l[1].clone()), Some(l[2].clone()))
    } else {
        (None, None, None)
    };
    let mut echo = BTreeMap::new();
    for (k, x) in [("subject", &s), ("predicate", &p), ("object", &o)] {
        echo.insert(k.to_string(), x.clone().unwrap_or(Value::Null));
    }
    let parsed = match (s, p, o) {
        (Some(s), Some(Value::Property(PropertyValue::String(p))), Some(o))
            if !is_null(&s) && !is_null(&o) && !p.trim().is_empty() =>
        {
            Some((s, p.trim().to_string(), o))
        }
        _ => None,
    };
    (echo, parsed)
}

/// Check `answer`'s claims against `envelope` under `config`.
pub fn verify(
    store: &GraphStore,
    answer: &Value,
    envelope: &Value,
    config: &GateConfig,
) -> ExecutionResult<Verdict> {
    let claims = claims(answer)?;
    let ev = evidence(store, envelope)?;
    let gate = Gate::new(store, config, ev);

    let mut verified = Vec::new();
    let mut unsupported = Vec::new();
    let mut contradicted = 0usize;
    for c in &claims {
        let (echo, parsed) = parse_claim(c);
        let Some((s, p, o)) = parsed else {
            unsupported.push(reject(echo, "MALFORMED", None));
            continue;
        };
        let subjects = gate.resolve(&s);
        if subjects.is_empty() {
            unsupported.push(reject(echo, "NOT_IN_EVIDENCE", None));
            continue;
        }
        if let Some(sup) = gate.relation(&subjects, &p, &gate.resolve(&o)) {
            verified.push(gate.cite(echo, &sup));
            continue;
        }
        match gate.property(&subjects, &p, &o) {
            Ok(sup) => verified.push(gate.cite(echo, &sup)),
            Err(Some(found)) => {
                contradicted += 1;
                unsupported.push(reject(echo, "CONTRADICTED", Some(found)));
            }
            Err(None) => unsupported.push(reject(echo, "NOT_IN_EVIDENCE", None)),
        }
    }

    let best_score = gate
        .ev
        .items
        .iter()
        .filter_map(|i| i.score)
        .fold(None, |a: Option<f64>, s| Some(a.map_or(s, |a| a.max(s))));
    let coverage = (!claims.is_empty()).then(|| verified.len() as f64 / claims.len() as f64);
    let confidence = if claims.is_empty() {
        best_score
    } else if verified.is_empty() {
        None
    } else {
        verified
            .iter()
            .map(|c| c.confidence)
            .try_fold(f64::INFINITY, |a, c| c.map(|c| a.min(c)))
    };

    let withheld = |r| {
        if gate.ev.redacted > 0 {
            AbstainReason::PolicyRedactedContext
        } else {
            r
        }
    };
    let (reason, why) = if gate.ev.items.is_empty() {
        (
            Some(withheld(AbstainReason::InsufficientSubgraphEvidence)),
            "no evidence item is usable".to_string(),
        )
    } else if let Some(floor) = config
        .min_score
        .filter(|f| best_score.is_none_or(|b| b < *f))
    {
        (
            Some(AbstainReason::BelowSimilarityFloor),
            match best_score {
                Some(b) => format!("the best seed scores {b}, below {floor}"),
                None => format!("no seed score is known, so none clears {floor}"),
            },
        )
    } else if contradicted > 0 {
        (
            Some(AbstainReason::ContradictoryPaths),
            format!("{contradicted} claim(s) contradicted by the evidence"),
        )
    } else if coverage.is_some_and(|c| c < config.min_coverage) {
        (
            Some(withheld(AbstainReason::InsufficientSubgraphEvidence)),
            format!(
                "{} of {} claim(s) supported, below {}",
                verified.len(),
                claims.len(),
                config.min_coverage
            ),
        )
    } else if config.min_confidence > 0.0 && confidence.is_none_or(|c| c < config.min_confidence) {
        (
            Some(AbstainReason::LowPathConfidence),
            match confidence {
                Some(c) => format!("path confidence {c}, below {}", config.min_confidence),
                None => format!(
                    "path confidence unknown, so it does not clear {}",
                    config.min_confidence
                ),
            },
        )
    } else {
        (
            None,
            format!("{} of {} claim(s) supported", verified.len(), claims.len()),
        )
    };
    let mut detail = why;
    detail.push_str(&format!(
        "; {} evidence item(s) used, {} rejected as not in the graph, {} redacted",
        gate.ev.items.len(),
        gate.ev.rejected,
        gate.ev.redacted
    ));
    Ok(Verdict {
        status: if reason.is_some() {
            Status::Refused
        } else {
            Status::Verified
        },
        abstain_reason: reason,
        verified,
        unsupported,
        confidence,
        coverage,
        detail,
    })
}

/// `CALL ai.rag.verify_or_refuse(query, candidate_answer, evidence_envelope, config)`.
///
/// The arguments are read once, without input rows: literals and parameters.
pub struct RagVerifyOperator {
    args: Vec<Expression>,
    yields: Vec<(&'static str, String)>,
    done: bool,
}

impl RagVerifyOperator {
    pub fn new(
        args: Vec<Expression>,
        yields: &[(String, Option<String>)],
    ) -> ExecutionResult<Self> {
        if !(3..=4).contains(&args.len()) {
            return Err(ExecutionError::PlanningError(format!(
                "{NAME} takes (query, candidate_answer, evidence_envelope, config)"
            )));
        }
        let yields = if yields.is_empty() {
            COLUMNS.iter().map(|c| (*c, c.to_string())).collect()
        } else {
            yields
                .iter()
                .map(|(name, alias)| {
                    let column = COLUMNS.iter().find(|c| **c == name).ok_or_else(|| {
                        ExecutionError::PlanningError(format!(
                            "{NAME} yields {}; there is no `{name}`",
                            COLUMNS.join(", ")
                        ))
                    })?;
                    Ok((*column, alias.clone().unwrap_or_else(|| name.clone())))
                })
                .collect::<ExecutionResult<_>>()?
        };
        Ok(Self {
            args,
            yields,
            done: false,
        })
    }
}

impl PhysicalOperator for RagVerifyOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let empty = Record::new();
        let mut args = Vec::with_capacity(4);
        for a in &self.args {
            args.push(eval_expression(a, &empty, store)?);
        }
        if !is_null(&args[0]) && !matches!(args[0], Value::Property(PropertyValue::String(_))) {
            return Err(err("query must be a string or null"));
        }
        let config = GateConfig::from_value(args.get(3).unwrap_or(&Value::Null))?;
        let v = verify(store, &args[1], &args[2], &config)?;
        let num =
            |f: Option<f64>| f.map_or(Value::Null, |f| Value::Property(PropertyValue::Float(f)));
        let text = |s: &str| Value::Property(PropertyValue::String(s.to_string()));
        let mut record = Record::new();
        for (column, var) in &self.yields {
            let value = match *column {
                "status" => text(v.status.as_str()),
                "verified_claims" => {
                    Value::List(v.verified.iter().map(|c| c.value.clone()).collect())
                }
                "unsupported_claims" => {
                    Value::List(v.unsupported.iter().map(|c| c.value.clone()).collect())
                }
                "abstain_reason" => v.abstain_reason.map_or(Value::Null, |r| text(r.as_str())),
                "confidence" => num(v.confidence),
                "coverage" => num(v.coverage),
                "detail" => text(&v.detail),
                _ => unreachable!("checked in new"),
            };
            record.bind(var.clone(), value);
        }
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.done = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "RagVerifyOrRefuse".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}
