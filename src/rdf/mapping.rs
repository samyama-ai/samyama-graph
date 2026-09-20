//! Property Graph ↔ RDF mapping
//!
//! This module provides bidirectional mapping between property graphs and RDF triples.
//!
//! # Mapping Strategy
//!
//! ## Property Graph → RDF
//!
//! - Node → rdf:type for each label
//! - Node properties → property triples
//! - Edge → triple with reified edge properties
//!
//! ## RDF → Property Graph
//!
//! - rdf:type triples → node labels
//! - Property triples → node/edge properties
//! - Reified statements → edges with properties

use crate::graph::{GraphStore, Node, Edge, Label, EdgeType, PropertyValue};
use super::{RdfStore, Triple, NamedNode, RdfPredicate, RdfObject, Literal, RdfSubject};
use thiserror::Error;

/// Mapping errors
#[derive(Error, Debug)]
pub enum MappingError {
    /// Invalid IRI
    #[error("Invalid IRI: {0}")]
    InvalidIri(String),

    /// Unsupported property type
    #[error("Unsupported property type: {0}")]
    UnsupportedPropertyType(String),

    /// Missing base IRI
    #[error("Missing base IRI")]
    MissingBaseIri,

    /// The mapping exists as an API and has no implementation behind it.
    ///
    /// Its own variant because the alternative was worse: these functions used
    /// to return `Ok(())` and `Ok(Vec::new())`, so a caller importing RDF got
    /// success and an empty graph, and could not distinguish "this worked" from
    /// "this did nothing" -- indistinguishable from a successful round trip over
    /// input that happened to hold no triples. Failing loudly is strictly better
    /// than succeeding falsely, and stays correct when the bodies are written.
    #[error("{0} is not implemented: RDF mapping is declared but has no implementation (samyama-graph#1043)")]
    NotImplemented(&'static str),
}

pub type MappingResult<T> = Result<T, MappingError>;

/// Mapping configuration
#[derive(Debug, Clone)]
pub struct MappingConfig {
    /// Base IRI for generated IRIs
    pub base_iri: String,

    /// Use reification for edge properties
    pub use_reification: bool,

    /// Preserve blank nodes
    pub preserve_blank_nodes: bool,
}

impl MappingConfig {
    /// Create a new mapping configuration
    pub fn new(base_iri: impl Into<String>) -> Self {
        Self {
            base_iri: base_iri.into(),
            use_reification: true,
            preserve_blank_nodes: false,
        }
    }
}

/// Property Graph → RDF mapper
pub struct GraphToRdfMapper {
    config: MappingConfig,
}

impl GraphToRdfMapper {
    /// Create a new mapper with base IRI
    pub fn new(base_iri: impl Into<String>) -> Self {
        Self {
            config: MappingConfig::new(base_iri),
        }
    }

    /// Create a mapper with custom configuration
    pub fn with_config(config: MappingConfig) -> Self {
        Self { config }
    }

    /// A node's labels and properties as triples.
    ///
    /// See `mapping_impl` for what each part maps to and what does not survive.
    pub fn map_node(&self, graph: &GraphStore, node: &Node) -> MappingResult<Vec<Triple>> {
        super::mapping_impl::map_node(&self.config, graph, node)
    }

    /// An edge as a triple, plus reification when it carries properties.
    ///
    /// Takes the graph because an edge names its endpoints by id and a triple
    /// names them by IRI, which only the store can resolve — and an endpoint
    /// that is missing is an error rather than an invented IRI.
    pub fn map_edge(&self, graph: &GraphStore, edge: &Edge) -> MappingResult<Vec<Triple>> {
        super::mapping_impl::map_edge(&self.config, graph, edge)
    }

    /// Write the whole graph into an RDF store.
    ///
    /// Returns what was written and what collapsed — RDF has no parallel
    /// edges, so a graph that says the same thing twice says it once here.
    pub fn sync_to_rdf(
        &self,
        graph: &GraphStore,
        rdf: &mut RdfStore,
    ) -> MappingResult<super::mapping_impl::SyncReport> {
        super::mapping_impl::sync_to_rdf(&self.config, graph, rdf)
    }
}

/// RDF → Property Graph mapper
pub struct RdfToGraphMapper {
    config: MappingConfig,
}

impl RdfToGraphMapper {
    /// Create a new mapper
    pub fn new(base_iri: impl Into<String>) -> Self {
        Self {
            config: MappingConfig::new(base_iri),
        }
    }

    /// Read an RDF store into a property graph.
    ///
    /// Each subject IRI becomes a node and is kept in `IRI_PROPERTY`, so
    /// exporting the result reproduces the IRIs that came in.
    pub fn map_to_graph(&self, rdf: &RdfStore, graph: &mut GraphStore) -> MappingResult<()> {
        super::mapping_impl::map_to_graph(&self.config, rdf, graph)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapper_creation() {
        let mapper = GraphToRdfMapper::new("http://example.org/");
        assert_eq!(mapper.config.base_iri, "http://example.org/");
    }

    /// This test has been through all three states the mapping has had, and
    /// the history is the point.
    ///
    /// It first asserted `triples.is_empty()` — encoding the defect, so
    /// implementing the mapping would have looked like a regression. It then
    /// asserted the `NotImplemented` refusal, which was correct at the time and
    /// said it would fail when the mapping was written. It has, and this is
    /// that moment: the mapping now produces triples, and the assertion is that
    /// it produces the right ones.
    ///
    /// Round-trip behaviour lives in `tests/rdf_round_trip.rs`; this is the
    /// unit-level check that one node becomes its label and its properties.
    #[test]
    fn a_node_maps_to_its_type_and_its_properties() {
        let mapper = GraphToRdfMapper::new("http://example.org/");
        let mut graph = GraphStore::new();
        let node_id = graph.create_node("Person");
        graph.set_node_property("default", node_id, "name", "Ada").unwrap();
        let node = graph.get_node(node_id).expect("the node just created").clone();

        let triples = mapper.map_node(&graph, &node).expect("map_node");
        let text: Vec<String> = triples.iter().map(|t| t.to_string()).collect();
        assert_eq!(triples.len(), 2, "one rdf:type and one property: {text:?}");
        assert!(
            text.iter().any(|t| t.contains("rdf-syntax-ns#type") && t.contains("class/Person")),
            "{text:?}"
        );
        assert!(text.iter().any(|t| t.contains("prop/name") && t.contains("Ada")), "{text:?}");
    }
}
