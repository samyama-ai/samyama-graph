//! `ai.rag.retrieve`: hybrid retrieval that says why each row is there (#2098,
//! AI-08 / AI-12 / TRUST-06).
//!
//! ```cypher
//! CALL ai.rag.retrieve('Chunk', 'embedding', $vector, 5, 2)
//! YIELD node, seed, score, hops, path, source
//! ```
//!
//! The same retrieval was already possible as plain Cypher --
//! `CALL db.index.vector.queryNodes(...) YIELD node, score MATCH (node)-[*1..2]-(m)`
//! -- and its answer carried nothing about how a row got there. A node two
//! hops out could not be told apart from one the vector search found, nor
//! traced back to the seed and the edges that reached it, and the source its
//! facts came from stayed in properties nobody returned. That is the evidence
//! a GraphRAG answer needs to cite, and the input a check-or-refuse gate needs
//! to check against.
//!
//! # What each row is
//!
//! One row per node in the retrieved context:
//!
//! | yield | |
//! |---|---|
//! | `node` | the context node |
//! | `seed` | the vector match it was reached from (itself, for a seed) |
//! | `score` | that seed's similarity, as `queryNodes` yields it |
//! | `hops` | edges walked from the seed, `0` for a seed |
//! | `path` | the walk itself, seed first; edges may be walked either way |
//! | `source` | the node's provenance as a map, or null when it has none |
//!
//! `source` holds the reserved keys of [`crate::provenance`] that are set, and
//! three more a chunking pipeline may set: `__chunk_id`, `__span` and
//! `__extractor_version`. Nothing is inferred: a node without them has a null
//! source, which is "unknown", not "none needed".
//!
//! # Deterministic
//!
//! The same graph and the same arguments give the same rows in the same
//! order, so an envelope can be stored and checked later. Seeds come first,
//! best score first, ties by node id. Then each seed's neighbourhood, breadth
//! first, edges taken in edge-id order. A node is reported once, from the
//! first seed that reaches it -- the best-scoring one -- by its shortest walk
//! from that seed. A seed is always reported as a seed, even when a better
//! seed also reaches it.
//!
//! # Bounded
//!
//! `hops` is at most [`MAX_HOPS`]. Two hops from a hub is already most of a
//! small graph; the limit keeps a mistyped `20` from assembling all of it.

use std::collections::{HashSet, VecDeque};

use crate::graph::{EdgeId, GraphStore, NodeId, PropertyValue};
use crate::provenance;
use crate::query::executor::operator::{OperatorBox, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// The procedure's name.
pub const NAME: &str = "ai.rag.retrieve";

/// The furthest a retrieval walks from a seed.
pub const MAX_HOPS: usize = 3;

/// Provenance a chunking pipeline may record beyond [`provenance::KEYS`].
pub const CHUNK_KEYS: [&str; 3] = ["__chunk_id", "__span", "__extractor_version"];

/// The columns, in the order a bare `YIELD *`-less call binds them.
pub const COLUMNS: [&str; 6] = ["node", "seed", "score", "hops", "path", "source"];

/// One row of the envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct Evidence {
    pub node: NodeId,
    pub seed: NodeId,
    pub score: Option<PropertyValue>,
    /// The walk from `seed` to `node`; `nodes.len() == edges.len() + 1`.
    pub nodes: Vec<NodeId>,
    pub edges: Vec<EdgeId>,
}

/// Every edge at `node`, either direction, as `(edge, other end)`, in
/// edge-id order. A self-loop is listed once.
fn neighbours(store: &GraphStore, node: NodeId) -> Vec<(EdgeId, NodeId)> {
    let mut out: Vec<(EdgeId, NodeId)> = store
        .get_outgoing_edges(node)
        .into_iter()
        .map(|e| (e.id, e.target))
        .chain(
            store
                .get_incoming_edges(node)
                .into_iter()
                .map(|e| (e.id, e.source)),
        )
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The envelope for `seeds`, ranked as the vector search ranked them.
pub fn expand(
    store: &GraphStore,
    seeds: &[(NodeId, Option<PropertyValue>)],
    hops: usize,
) -> Vec<Evidence> {
    let mut seen: HashSet<NodeId> = HashSet::new();
    let mut out = Vec::new();
    // A node found twice by the search (it cannot be, today) is one seed.
    let seeds: Vec<_> = seeds
        .iter()
        .filter(|(n, _)| seen.insert(*n))
        .cloned()
        .collect();
    for (seed, score) in &seeds {
        out.push(Evidence {
            node: *seed,
            seed: *seed,
            score: score.clone(),
            nodes: vec![*seed],
            edges: vec![],
        });
    }
    for (seed, score) in &seeds {
        // Breadth first from this seed, over nodes no better seed has claimed.
        // Seeds are claimed already, so a walk does not pass through another
        // seed: what lies beyond it is that seed's to report.
        let mut queue: VecDeque<(NodeId, Vec<NodeId>, Vec<EdgeId>)> = VecDeque::new();
        queue.push_back((*seed, vec![*seed], vec![]));
        while let Some((at, nodes, edges)) = queue.pop_front() {
            if edges.len() == hops {
                continue;
            }
            for (edge, next) in neighbours(store, at) {
                if !seen.insert(next) {
                    continue;
                }
                let mut nodes = nodes.clone();
                let mut edges = edges.clone();
                nodes.push(next);
                edges.push(edge);
                out.push(Evidence {
                    node: next,
                    seed: *seed,
                    score: score.clone(),
                    nodes: nodes.clone(),
                    edges: edges.clone(),
                });
                queue.push_back((next, nodes, edges));
            }
        }
    }
    out
}

/// The provenance recorded on `node`, or `None` when it has none.
///
/// Read from the merged property view, as [`provenance::of`] reads it: a value
/// set after the node was created lives on the columnar side, where the
/// node's own map does not see it (#554).
pub fn source(store: &GraphStore, node: NodeId) -> Option<PropertyValue> {
    let props = store.node_properties_merged(node);
    let map: std::collections::HashMap<String, PropertyValue> = provenance::KEYS
        .iter()
        .chain(CHUNK_KEYS.iter())
        .filter_map(|k| props.get(*k).map(|v| (k.to_string(), v.clone())))
        .collect();
    (!map.is_empty()).then_some(PropertyValue::Map(map))
}

/// `CALL ai.rag.retrieve(label, property, queryVector, k, hops)`.
pub struct RagRetrieveOperator {
    /// The vector search, binding `node` and `score`.
    search: OperatorBox,
    hops: usize,
    /// `(column, variable)` for each yielded column.
    yields: Vec<(&'static str, String)>,
    rows: Option<std::vec::IntoIter<Evidence>>,
}

impl RagRetrieveOperator {
    /// `search` must bind `node` and `score`. `yields` are `(name, alias)`;
    /// none yields every column under its own name.
    pub fn new(
        search: OperatorBox,
        hops: usize,
        yields: &[(String, Option<String>)],
    ) -> ExecutionResult<Self> {
        if hops > MAX_HOPS {
            return Err(ExecutionError::PlanningError(format!(
                "{NAME}: hops must be between 0 and {MAX_HOPS}, got {hops}"
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
            search,
            hops,
            yields,
            rows: None,
        })
    }
}

impl PhysicalOperator for RagRetrieveOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            let mut seeds = Vec::new();
            while let Some(r) = self.search.next(store)? {
                let node = match r.get("node") {
                    Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => *id,
                    _ => continue,
                };
                let score = match r.get("score") {
                    Some(Value::Property(p)) => Some(p.clone()),
                    _ => None,
                };
                seeds.push((node, score));
            }
            // Best first, ties by id: the index's own order for equal scores
            // is not promised to be stable across builds.
            seeds.sort_by(|(a, sa), (b, sb)| {
                let f = |s: &Option<PropertyValue>| match s {
                    Some(PropertyValue::Float(f)) => *f,
                    Some(PropertyValue::Integer(i)) => *i as f64,
                    _ => f64::NEG_INFINITY,
                };
                f(sb).total_cmp(&f(sa)).then(a.cmp(b))
            });
            self.rows = Some(expand(store, &seeds, self.hops).into_iter());
        }
        let Some(e) = self.rows.as_mut().and_then(|r| r.next()) else {
            return Ok(None);
        };
        let mut record = Record::new();
        for (column, var) in &self.yields {
            let value = match *column {
                "node" => Value::NodeRef(e.node),
                "seed" => Value::NodeRef(e.seed),
                "score" => e.score.clone().map_or(Value::Null, Value::Property),
                "hops" => Value::Property(PropertyValue::Integer(e.edges.len() as i64)),
                "path" => Value::Path {
                    nodes: e.nodes.clone(),
                    edges: e.edges.clone(),
                },
                "source" => source(store, e.node).map_or(Value::Null, Value::Property),
                _ => unreachable!("checked in new"),
            };
            record.bind(var.clone(), value);
        }
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.search.reset();
        self.rows = None;
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.search]
    }

    fn amplifies_rows(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "RagRetrieve".to_string(),
            details: format!("hops={}", self.hops),
            children: vec![self.search.describe()],
        }
    }
}
