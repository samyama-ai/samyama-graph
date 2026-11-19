//! The property graph read as RDF where it is, with no copy (#2100, part 2).
//!
//! Read-only: it only reads nodes and edges, and changes nothing.
//!
//! SPARQL asks one question of its data: which triples match this pattern?
//! [`TripleSource`] is that question. [`RdfStore`] answers it from triples it
//! holds; [`GraphAsRdf`] answers it from a [`GraphStore`] as it is now, so a
//! SPARQL query sees the graph a Cypher query would, and nothing is exported
//! first.
//!
//! # Same triples as the export, by construction
//!
//! `GraphAsRdf` does not restate the mapping. For each node or edge it visits it
//! calls the export's own [`map_node`] and [`map_edge`], then keeps the triples
//! that match the pattern. What it adds is the choice of *which* nodes and
//! edges to visit: a pattern that names a subject, a class or a relationship
//! type reads only those, and anything else reads everything, exactly as
//! [`sync_to_rdf`] would have. So the answer to every pattern is the answer the
//! exported copy gives, including the parts that are easy to get wrong:
//! reified edge properties, nodes with their own IRI, and parallel edges, which
//! RDF holds once.
//!
//! [`map_node`]: super::mapping_impl::map_node
//! [`map_edge`]: super::mapping_impl::map_edge
//! [`sync_to_rdf`]: super::mapping_impl::sync_to_rdf

use std::cell::OnceCell;
use std::collections::{HashMap, HashSet};

use super::mapping::MappingConfig;
use super::mapping_impl::{self, IRI_PROPERTY, RDF_TYPE};
use super::{RdfObject, RdfStore, RdfSubject, Triple, TriplePattern};
use crate::graph::{Edge, EdgeId, EdgeType, GraphStore, Label, NodeId, PropertyValue};

/// Something SPARQL can read: the triples matching a pattern.
///
/// An error is a graph that cannot be expressed as RDF (an IRI that does not
/// parse, an edge whose endpoint is missing), reported as the export reports
/// it rather than skipped, because a skipped triple is a wrong answer.
pub trait TripleSource {
    /// Every triple matching `pattern`, each once.
    fn matching(&self, pattern: &TriplePattern) -> Result<Vec<Triple>, String>;
}

impl TripleSource for RdfStore {
    fn matching(&self, pattern: &TriplePattern) -> Result<Vec<Triple>, String> {
        Ok(self.query(pattern))
    }
}

/// A [`GraphStore`] read as RDF under a [`MappingConfig`], without a copy.
pub struct GraphAsRdf<'g> {
    graph: &'g GraphStore,
    config: MappingConfig,
    /// Nodes that carry their own IRI, by that IRI. Built on the first lookup
    /// that needs it: a node's IRI is a property, and finding the node for one
    /// means reading them all once.
    own_iris: OnceCell<HashMap<String, NodeId>>,
}

/// The nodes and edges whose triples can match a pattern.
#[derive(Default)]
struct Visit {
    nodes: Vec<NodeId>,
    edges: Vec<EdgeId>,
}

impl<'g> GraphAsRdf<'g> {
    /// `graph` as RDF, with IRIs under `config.base_iri`.
    pub fn new(graph: &'g GraphStore, config: MappingConfig) -> Self {
        Self { graph, config, own_iris: OnceCell::new() }
    }

    fn base(&self, kind: &str) -> String {
        format!("{}{kind}/", self.config.base_iri)
    }

    /// `<base>{kind}/<n>` as `n`.
    fn numbered(&self, iri: &str, kind: &str) -> Option<u64> {
        iri.strip_prefix(&self.base(kind))?.parse().ok()
    }

    fn own_iris(&self) -> &HashMap<String, NodeId> {
        self.own_iris.get_or_init(|| {
            self.graph
                .all_nodes()
                .into_iter()
                .filter_map(|n| match self.graph.node_properties_merged(n.id).get(IRI_PROPERTY) {
                    Some(PropertyValue::String(iri)) => Some((iri.clone(), n.id)),
                    _ => None,
                })
                .collect()
        })
    }

    /// The node an IRI names, whether a `<base>node/<id>` or its own.
    ///
    /// Both are tried: a node with its own IRI is not named by its numbered
    /// one, and nothing stops a node's own IRI from looking numbered. The
    /// triples decide; this only says where to look.
    fn nodes_named(&self, iri: &str) -> Vec<NodeId> {
        let mut out = Vec::new();
        if let Some(id) = self.numbered(iri, "node") {
            let id = NodeId::new(id);
            if self.graph.get_node(id).is_some() {
                out.push(id);
            }
        }
        if let Some(&id) = self.own_iris().get(iri) {
            if !out.contains(&id) {
                out.push(id);
            }
        }
        out
    }

    /// Everything about the nodes `iri` names: their own triples, and the
    /// edges at either end, whose base triple or reification can mention them.
    fn around(&self, iri: &str, v: &mut Visit) {
        for id in self.nodes_named(iri) {
            v.nodes.push(id);
            v.edges.extend(self.graph.get_outgoing_edges(id).into_iter().map(|e| e.id));
            v.edges.extend(self.graph.get_incoming_edges(id).into_iter().map(|e| e.id));
        }
        if let Some(id) = self.numbered(iri, "statement") {
            v.edges.push(EdgeId::new(id));
        }
    }

    fn everything(&self) -> Visit {
        Visit {
            nodes: self.graph.all_nodes().into_iter().map(|n| n.id).collect(),
            edges: self.graph.all_edges().into_iter().map(|e| e.id).collect(),
        }
    }

    /// Which nodes and edges to read for `pattern`. Narrowed only where the
    /// mapping says exactly which ones can match; otherwise everything.
    fn plan(&self, pattern: &TriplePattern) -> Visit {
        let mut v = Visit::default();
        if let Some(RdfSubject::NamedNode(s)) = &pattern.subject {
            self.around(s.as_str(), &mut v);
            return v;
        }
        if let Some(p) = &pattern.predicate {
            let p = p.as_named_node().as_str();
            if let Some(t) = p.strip_prefix(&self.base("rel")) {
                // A relationship predicate appears in an edge's base triple and
                // nowhere else as a predicate.
                let t = EdgeType::new(mapping_impl::decode_segment(t));
                v.edges = self.graph.get_edges_by_type(&t).into_iter().map(|e| e.id).collect();
                return v;
            }
            if p == RDF_TYPE {
                if let Some(RdfObject::NamedNode(o)) = &pattern.object {
                    if let Some(l) = o.as_str().strip_prefix(&self.base("class")) {
                        let l = Label::new(mapping_impl::decode_segment(l));
                        v.nodes = self.graph.get_nodes_by_label(&l).into_iter().map(|n| n.id).collect();
                        return v;
                    }
                }
            }
        }
        if let Some(RdfObject::NamedNode(o)) = &pattern.object {
            let o = o.as_str();
            if o.starts_with(&self.base("node")) || self.own_iris().contains_key(o) {
                self.around(o, &mut v);
                return v;
            }
        }
        self.everything()
    }
}

impl TripleSource for GraphAsRdf<'_> {
    fn matching(&self, pattern: &TriplePattern) -> Result<Vec<Triple>, String> {
        let visit = self.plan(pattern);
        let mut seen: HashSet<Triple> = HashSet::new();
        let mut out = Vec::new();
        let mut keep = |triples: Vec<Triple>| {
            for t in triples {
                // Once each: RDF is a set, so two parallel edges are one triple,
                // as they are in an exported copy.
                if pattern.matches(&t) && seen.insert(t.clone()) {
                    out.push(t);
                }
            }
        };
        let mut nodes_done = HashSet::new();
        for id in visit.nodes {
            if !nodes_done.insert(id) {
                continue;
            }
            if let Some(node) = self.graph.get_node(id) {
                keep(mapping_impl::map_node(&self.config, self.graph, node).map_err(|e| e.to_string())?);
            }
        }
        let mut edges_done = HashSet::new();
        for id in visit.edges {
            if !edges_done.insert(id) {
                continue;
            }
            if let Some(edge) = self.graph.get_edge(id) {
                keep(edge_triples(&self.config, self.graph, &edge)?);
            }
        }
        Ok(out)
    }
}

fn edge_triples(config: &MappingConfig, graph: &GraphStore, edge: &Edge) -> Result<Vec<Triple>, String> {
    mapping_impl::map_edge(config, graph, edge).map_err(|e| e.to_string())
}
