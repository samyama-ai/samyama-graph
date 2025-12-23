//! RDF (Resource Description Framework) support for Samyama Graph Database
//!
//! This module implements RDF triple/quad store functionality with support for:
//! - RDF triples (subject-predicate-object)
//! - Named graphs (quads)
//! - RDF serialization formats (Turtle, RDF/XML, N-Triples, JSON-LD)
//! - Property graph ↔ RDF mapping
//! - RDFS basic reasoning
//!
//! # Requirements Coverage
//!
//! - REQ-RDF-001: RDF data model
//! - REQ-RDF-002: RDF triples
//! - REQ-RDF-003: RDF serialization formats
//! - REQ-RDF-004: Named graphs (quads)
//! - REQ-RDF-005: RDFS semantics
//! - REQ-RDF-006: Property graph ↔ RDF mapping
//!
//! # Example
//!
//! ```rust
//! use samyama::rdf::{RdfStore, Triple, NamedNode, Literal, RdfPredicate};
//!
//! let mut store = RdfStore::new();
//!
//! // Create a triple
//! let subject = NamedNode::new("http://example.org/alice").unwrap();
//! let predicate = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
//! let object = Literal::new_simple_literal("Alice");
//!
//! let triple = Triple::new(subject.clone().into(), predicate, object.into());
//! store.insert(triple.clone()).unwrap();
//!
//! // Query triples
//! let results = store.get_triples_with_subject(&subject.into());
//! assert_eq!(results.len(), 1);
//! ```

mod types;
mod store;
mod mapping;
mod namespace;
mod serialization;
mod schema;

pub use types::{
    RdfTerm, RdfSubject, RdfPredicate, RdfObject,
    NamedNode, BlankNode, Literal, Triple, Quad,
    TriplePattern, QuadPattern,
};

pub use store::{
    RdfStore, RdfStoreError, RdfStoreResult,
    TripleIterator,
};

pub use mapping::{
    GraphToRdfMapper, RdfToGraphMapper,
    MappingConfig, MappingError, MappingResult,
};

pub use namespace::{
    NamespaceManager, Namespace,
    PrefixError, PrefixResult,
};

pub use serialization::{
    RdfFormat, RdfParser, RdfSerializer,
    ParseError, ParseResult,
    SerializeError, SerializeResult,
};

pub use schema::{
    RdfsReasoner, InferenceRule,
    ReasoningError, ReasoningResult,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdf_module_exports() {
        // Verify all main types are exported
        let _store: RdfStore = RdfStore::new();
        let _mapper = GraphToRdfMapper::new("http://example.org/");
        let _ns_mgr = NamespaceManager::new();
    }
}
