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

    /// Map a node to RDF triples
    ///
    /// TODO: Full implementation
    /// - Convert node ID to IRI
    /// - Add rdf:type triples for labels
    /// - Add property triples
    pub fn map_node(&self, _node: &Node) -> MappingResult<Vec<Triple>> {
        Err(MappingError::NotImplemented("map_node"))
    }

    /// Map an edge to RDF triples
    ///
    /// TODO: Full implementation
    /// - Create triple for edge relationship
    /// - Optionally reify edge properties
    pub fn map_edge(&self, _edge: &Edge) -> MappingResult<Vec<Triple>> {
        Err(MappingError::NotImplemented("map_edge"))
    }

    /// Synchronize property graph to RDF store
    ///
    /// TODO: Full implementation
    pub fn sync_to_rdf(&self, _graph: &GraphStore, _rdf: &mut RdfStore) -> MappingResult<()> {
        Err(MappingError::NotImplemented("sync_to_rdf"))
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

    /// Map RDF triples to property graph
    ///
    /// TODO: Full implementation
    pub fn map_to_graph(&self, _rdf: &RdfStore, _graph: &mut GraphStore) -> MappingResult<()> {
        Err(MappingError::NotImplemented("map_to_graph"))
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

    /// The previous version of this test asserted `triples.is_empty()` -- it
    /// encoded the defect, so implementing the mapping correctly would have
    /// broken it and the failure would have looked like a regression. It now
    /// asserts the refusal, which is the behaviour we actually want today, and
    /// it will fail when the mapping is implemented, which is the right moment
    /// to be told.
    #[test]
    fn mapping_refuses_rather_than_silently_returning_nothing() {
        let mapper = GraphToRdfMapper::new("http://example.org/");
        let mut graph = GraphStore::new();
        let node_id = graph.create_node("Person");
        let node = graph.get_node(node_id).expect("the node just created");

        let err = mapper.map_node(node).expect_err(
            "an unimplemented mapping must not report success: a caller cannot \
             tell an empty result from a no-op",
        );
        assert!(matches!(err, MappingError::NotImplemented("map_node")), "{err}");
        assert!(err.to_string().contains("not implemented"), "{err}");
    }
}
