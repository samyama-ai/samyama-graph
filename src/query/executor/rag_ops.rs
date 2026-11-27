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

//!
//! # Answer-time policy
//!
//! Every retrieval runs under the asking principal's [`rag_policy`] (#2233):
//! withheld nodes and edges are left out of the walk, so they are neither
//! rows nor steps of any row's `path`, and withheld properties are removed
//! from `properties` and `source`. The `withheld` column says how much was
//! left out and by which rules, and `ai.rag.withheld` -- same arguments --
//! says it in one row, which an empty context also gets.
//!
//! | further yield | |
//! |---|---|
//! | `node_id`, `seed_id` | the ids of `node` and `seed` |
//! | `labels` | `node`'s labels, sorted |
//! | `hop_sequence` | each edge of `path` as `[start id, type, end id]`, in the edge's own direction |
//! | `properties` | `node`'s properties, less the withheld ones |
//! | `withheld` | `{nodes, edges, properties, rules}` for the whole retrieval |

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use super::rag_policy::{self, RagPolicy, Withheld};
use crate::graph::{Edge, EdgeId, GraphStore, NodeId, PropertyMap, PropertyValue};
use crate::provenance;
use crate::query::executor::operator::{OperatorBox, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// The procedure's name.
pub const NAME: &str = "ai.rag.retrieve";

/// The same retrieval, reporting only what its policy withheld, in one row.
pub const WITHHELD_NAME: &str = "ai.rag.withheld";

/// The furthest a retrieval walks from a seed.
pub const MAX_HOPS: usize = 3;

/// Provenance a chunking pipeline may record beyond [`provenance::KEYS`].
pub const CHUNK_KEYS: [&str; 3] = ["__chunk_id", "__span", "__extractor_version"];

/// The columns, in the order a bare `YIELD *`-less call binds them.
pub const COLUMNS: [&str; 12] = [
    "node",
    "seed",
    "score",
    "hops",
    "path",
    "source",
    "node_id",
    "seed_id",
    "labels",
    "hop_sequence",
    "properties",
    "withheld",
];

/// Columns that carry an entity, which a client renders with every property
/// read back from the store. Closed while a property rule is in force.
pub const ENTITY_COLUMNS: [&str; 3] = ["node", "seed", "path"];

/// `ai.rag.withheld`'s columns.
pub const WITHHELD_COLUMNS: [&str; 4] = ["nodes", "edges", "properties", "rules"];

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
fn neighbours(store: &GraphStore, node: NodeId) -> Vec<(EdgeId, NodeId, Edge)> {
    let mut out: Vec<(EdgeId, NodeId, Edge)> = store
        .get_outgoing_edges(node)
        .into_iter()
        .map(|e| (e.id, e.target, e))
        .chain(
            store
                .get_incoming_edges(node)
                .into_iter()
                .map(|e| (e.id, e.source, e)),
        )
        .collect();
    out.sort_by_key(|(id, other, _)| (*id, *other));
    out.dedup_by_key(|(id, other, _)| (*id, *other));
    out
}

/// The envelope for `seeds`, ranked as the vector search ranked them, with
/// nothing withheld.
pub fn expand(
    store: &GraphStore,
    seeds: &[(NodeId, Option<PropertyValue>)],
    hops: usize,
) -> Vec<Evidence> {
    expand_under(store, seeds, hops, &RagPolicy::unrestricted(), &mut Withheld::default())
}

/// The envelope for `seeds` under `policy`: a withheld seed is dropped, and
/// the walk neither takes a withheld edge nor enters a withheld node. What
/// it met and left out is added to `withheld`.
pub fn expand_under(
    store: &GraphStore,
    seeds: &[(NodeId, Option<PropertyValue>)],
    hops: usize,
    policy: &RagPolicy,
    withheld: &mut Withheld,
) -> Vec<Evidence> {
    let mut seen: HashSet<NodeId> = HashSet::new();
    let mut out = Vec::new();
    // A node found twice by the search (it cannot be, today) is one seed.
    let mut kept = Vec::new();
    for (n, score) in seeds {
        if !seen.insert(*n) {
            continue;
        }
        if let Some(rule) = rag_policy::node_rule(store, policy, *n) {
            withheld.node(*n, rule);
            continue;
        }
        kept.push((*n, score.clone()));
    }
    for (seed, score) in &kept {
        out.push(Evidence {
            node: *seed,
            seed: *seed,
            score: score.clone(),
            nodes: vec![*seed],
            edges: vec![],
        });
    }
    for (seed, score) in &kept {
        // Breadth first from this seed, over nodes no better seed has claimed.
        // Seeds are claimed already, so a walk does not pass through another
        // seed: what lies beyond it is that seed's to report.
        let mut queue: VecDeque<(NodeId, Vec<NodeId>, Vec<EdgeId>)> = VecDeque::new();
        queue.push_back((*seed, vec![*seed], vec![]));
        while let Some((at, nodes, edges)) = queue.pop_front() {
            if edges.len() == hops {
                continue;
            }
            for (edge, next, e) in neighbours(store, at) {
                if !policy.is_empty() {
                    if let Some(rule) = policy.edge_rule(&e) {
                        withheld.edge(edge, rule);
                        continue;
                    }
                    if let Some(rule) = rag_policy::node_rule(store, policy, next) {
                        withheld.node(next, rule);
                        continue;
                    }
                }
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
    source_from(&store.node_properties_merged(node))
}

/// The provenance among `props`, or `None` when there is none.
pub fn source_from(props: &PropertyMap) -> Option<PropertyValue> {
    let map: std::collections::HashMap<String, PropertyValue> = provenance::KEYS
        .iter()
        .chain(CHUNK_KEYS.iter())
        .filter_map(|k| props.get(*k).map(|v| (k.to_string(), v.clone())))
        .collect();
    (!map.is_empty()).then_some(PropertyValue::Map(map))
}

/// `[start id, type, end id]` for each edge, in the edge's own direction.
fn hop_sequence(store: &GraphStore, edges: &[EdgeId]) -> PropertyValue {
    PropertyValue::Array(
        edges
            .iter()
            .filter_map(|id| store.get_edge(*id))
            .map(|e| {
                PropertyValue::Array(vec![
                    PropertyValue::Integer(e.source.as_u64() as i64),
                    PropertyValue::String(e.edge_type.as_str().to_string()),
                    PropertyValue::Integer(e.target.as_u64() as i64),
                ])
            })
            .collect(),
    )
}

fn labels_of(store: &GraphStore, node: NodeId) -> PropertyValue {
    let mut labels: Vec<String> = store
        .get_node(node)
        .map(|n| n.labels.iter().map(|l| l.as_str().to_string()).collect())
        .unwrap_or_default();
    labels.sort();
    PropertyValue::Array(labels.into_iter().map(PropertyValue::String).collect())
}

/// `CALL ai.rag.retrieve(label, property, queryVector, k, hops)`, or
/// `ai.rag.withheld` with the same arguments.
pub struct RagRetrieveOperator {
    /// The vector search, binding `node` and `score`.
    search: OperatorBox,
    hops: usize,
    /// `ai.rag.withheld`: one summary row instead of the context.
    summary: bool,
    /// No YIELD: every column the policy leaves open.
    bare: bool,
    /// `(column, variable)` for each yielded column.
    yields: Vec<(&'static str, String)>,
    rows: Option<std::vec::IntoIter<Record>>,
}

impl RagRetrieveOperator {
    /// `search` must bind `node` and `score`. `yields` are `(name, alias)`;
    /// none yields every column under its own name.
    pub fn new(
        search: OperatorBox,
        hops: usize,
        yields: &[(String, Option<String>)],
    ) -> ExecutionResult<Self> {
        Self::build(search, hops, yields, false)
    }

    /// `ai.rag.withheld`: the same retrieval, one row of [`WITHHELD_COLUMNS`].
    pub fn withheld(
        search: OperatorBox,
        hops: usize,
        yields: &[(String, Option<String>)],
    ) -> ExecutionResult<Self> {
        Self::build(search, hops, yields, true)
    }

    fn build(
        search: OperatorBox,
        hops: usize,
        yields: &[(String, Option<String>)],
        summary: bool,
    ) -> ExecutionResult<Self> {
        let name = if summary { WITHHELD_NAME } else { NAME };
        if hops > MAX_HOPS {
            return Err(ExecutionError::PlanningError(format!(
                "{name}: hops must be between 0 and {MAX_HOPS}, got {hops}"
            )));
        }
        let columns: &[&'static str] = if summary { &WITHHELD_COLUMNS } else { &COLUMNS };
        let bare = yields.is_empty();
        let yields = if bare {
            columns.iter().map(|c| (*c, c.to_string())).collect()
        } else {
            yields
                .iter()
                .map(|(y, alias)| {
                    let column = columns.iter().find(|c| **c == y).ok_or_else(|| {
                        ExecutionError::PlanningError(format!(
                            "{name} yields {}; there is no `{y}`",
                            columns.join(", ")
                        ))
                    })?;
                    Ok((*column, alias.clone().unwrap_or_else(|| y.clone())))
                })
                .collect::<ExecutionResult<_>>()?
        };
        Ok(Self {
            search,
            hops,
            summary,
            bare,
            yields,
            rows: None,
        })
    }

    /// Run the retrieval under the active policy and build every row.
    fn assemble(&mut self, store: &GraphStore) -> ExecutionResult<Vec<Record>> {
        let policy: Arc<RagPolicy> = rag_policy::active().map_err(ExecutionError::RuntimeError)?;
        if !self.summary && policy.hides_properties() {
            if self.bare {
                self.yields.retain(|(c, _)| !ENTITY_COLUMNS.contains(c));
            } else if let Some((c, _)) = self.yields.iter().find(|(c, _)| ENTITY_COLUMNS.contains(c)) {
                return Err(ExecutionError::RuntimeError(format!(
                    "{NAME}: `{c}` is closed while this principal's retrieval rules withhold \
                     properties, because an entity is returned with all of its properties; \
                     yield node_id, seed_id, labels, hop_sequence and properties instead"
                )));
            }
        }

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
        let mut withheld = Withheld::default();
        let evidence = expand_under(store, &seeds, self.hops, &policy, &mut withheld);
        // Properties are filtered for every row before any row is built, so
        // the `withheld` each row carries counts all of them.
        let props: Vec<PropertyMap> = evidence
            .iter()
            .map(|e| rag_policy::visible_properties(store, &policy, e.node, &mut withheld))
            .collect();

        if self.summary {
            let mut record = Record::new();
            for (column, var) in &self.yields {
                let value = match *column {
                    "nodes" => PropertyValue::Integer(withheld.nodes.len() as i64),
                    "edges" => PropertyValue::Integer(withheld.edges.len() as i64),
                    "properties" => PropertyValue::Integer(withheld.properties as i64),
                    "rules" => withheld.rules_map(),
                    _ => unreachable!("checked in build"),
                };
                record.bind(var.clone(), Value::Property(value));
            }
            return Ok(vec![record]);
        }

        let summary = withheld.to_map();
        let mut out = Vec::with_capacity(evidence.len());
        for (e, props) in evidence.into_iter().zip(props) {
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
                    "source" => source_from(&props).map_or(Value::Null, Value::Property),
                    "node_id" => Value::Property(PropertyValue::Integer(e.node.as_u64() as i64)),
                    "seed_id" => Value::Property(PropertyValue::Integer(e.seed.as_u64() as i64)),
                    "labels" => Value::Property(labels_of(store, e.node)),
                    "hop_sequence" => Value::Property(hop_sequence(store, &e.edges)),
                    "properties" => Value::Property(PropertyValue::Map(props.clone())),
                    "withheld" => Value::Property(summary.clone()),
                    _ => unreachable!("checked in build"),
                };
                record.bind(var.clone(), value);
            }
            out.push(record);
        }
        Ok(out)
    }
}

impl PhysicalOperator for RagRetrieveOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            let rows = self.assemble(store)?;
            self.rows = Some(rows.into_iter());
        }
        Ok(self.rows.as_mut().and_then(|r| r.next()))
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
            name: if self.summary { "RagWithheld" } else { "RagRetrieve" }.to_string(),
            details: format!("hops={}", self.hops),
            children: vec![self.search.describe()],
        }
    }
}
