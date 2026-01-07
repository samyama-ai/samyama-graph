//! RDF serialization formats
//!
//! Supports:
//! - Turtle (TTL)
//! - N-Triples (NT)
//! - RDF/XML
//! - JSON-LD

use super::{Triple, RdfStore};
use thiserror::Error;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub mod turtle;
pub mod ntriples;
pub mod rdfxml;
pub mod jsonld;

use turtle::{TurtleParserWrapper, TurtleSerializerWrapper};
use ntriples::{NTriplesParserWrapper, NTriplesSerializerWrapper};
use rdfxml::{RdfXmlParserWrapper, RdfXmlSerializerWrapper};
use jsonld::{JsonLdParserWrapper, JsonLdSerializerWrapper};

/// RDF serialization format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdfFormat {
    /// Turtle format (.ttl)
    Turtle,
    /// N-Triples format (.nt)
    NTriples,
    /// RDF/XML format (.rdf)
    RdfXml,
    /// JSON-LD format (.jsonld)
    JsonLd,
}

impl RdfFormat {
    /// Guess format from file extension
    pub fn from_extension(path: &Path) -> Option<Self> {
        path.extension()
            .and_then(|ext| ext.to_str())
            .and_then(|ext| match ext.to_lowercase().as_str() {
                "ttl" => Some(RdfFormat::Turtle),
                "nt" => Some(RdfFormat::NTriples),
                "rdf" | "xml" => Some(RdfFormat::RdfXml),
                "jsonld" | "json" => Some(RdfFormat::JsonLd),
                _ => None,
            })
    }
}

/// Parse errors
#[derive(Error, Debug)]
pub enum ParseError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Parse error
    #[error("Parse error: {0}")]
    Parse(String),

    /// Unsupported format
    #[error("Unsupported format: {0:?}")]
    UnsupportedFormat(RdfFormat),
}

pub type ParseResult<T> = Result<T, ParseError>;

/// Serialization errors
#[derive(Error, Debug)]
pub enum SerializeError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialize(String),

    /// Unsupported format
    #[error("Unsupported format: {0:?}")]
    UnsupportedFormat(RdfFormat),
}

pub type SerializeResult<T> = Result<T, SerializeError>;

/// RDF parser
pub struct RdfParser;

impl RdfParser {
    /// Parse RDF data from a string
    pub fn parse(input: &str, format: RdfFormat) -> ParseResult<Vec<Triple>> {
        match format {
            RdfFormat::Turtle => TurtleParserWrapper::parse(input),
            RdfFormat::NTriples => NTriplesParserWrapper::parse(input),
            RdfFormat::RdfXml => RdfXmlParserWrapper::parse(input),
            RdfFormat::JsonLd => JsonLdParserWrapper::parse(input),
        }
    }

    /// Parse RDF data from a file
    pub fn parse_file(path: &Path, format: Option<RdfFormat>) -> ParseResult<Vec<Triple>> {
        let format = format
            .or_else(|| RdfFormat::from_extension(path))
            .ok_or_else(|| ParseError::Parse("Could not determine format from extension".to_string()))?;

        let mut file = File::open(path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;

        Self::parse(&content, format)
    }
}

/// RDF serializer
pub struct RdfSerializer;

impl RdfSerializer {
    /// Serialize triples to a string
    pub fn serialize(triples: &[Triple], format: RdfFormat) -> SerializeResult<String> {
        match format {
            RdfFormat::Turtle => TurtleSerializerWrapper::serialize(triples),
            RdfFormat::NTriples => NTriplesSerializerWrapper::serialize(triples),
            RdfFormat::RdfXml => RdfXmlSerializerWrapper::serialize(triples),
            RdfFormat::JsonLd => JsonLdSerializerWrapper::serialize(triples),
        }
    }

    /// Serialize RDF store to a string
    pub fn serialize_store(store: &RdfStore, format: RdfFormat) -> SerializeResult<String> {
        // Collect all triples from the store
        // Note: This collects into memory, which is fine for MVP but could be optimized
        let triples: Vec<Triple> = store.iter().cloned().collect();
        Self::serialize(&triples, format)
    }

    /// Serialize triples to a file
    pub fn serialize_file(
        triples: &[Triple],
        path: &Path,
        format: RdfFormat,
    ) -> SerializeResult<()> {
        let content = Self::serialize(triples, format)?;
        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdf::{NamedNode, RdfPredicate, Literal};

    fn create_test_triples() -> Vec<Triple> {
        let subject = NamedNode::new("http://example.org/alice").unwrap();
        let predicate = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
        let object = Literal::new_simple_literal("Alice");
        
        vec![Triple::new(subject.into(), predicate, object.into())]
    }

    #[test]
    fn test_turtle() {
        let triples = create_test_triples();
        let output = RdfSerializer::serialize(&triples, RdfFormat::Turtle).unwrap();
        let parsed = RdfParser::parse(&output, RdfFormat::Turtle).unwrap();
        assert_eq!(triples.len(), parsed.len());
    }

    #[test]
    fn test_ntriples() {
        let triples = create_test_triples();
        let output = RdfSerializer::serialize(&triples, RdfFormat::NTriples).unwrap();
        let parsed = RdfParser::parse(&output, RdfFormat::NTriples).unwrap();
        assert_eq!(triples.len(), parsed.len());
    }

    #[test]
    fn test_rdfxml() {
        let triples = create_test_triples();
        let output = RdfSerializer::serialize(&triples, RdfFormat::RdfXml).unwrap();
        let parsed = RdfParser::parse(&output, RdfFormat::RdfXml).unwrap();
        assert_eq!(triples.len(), parsed.len());
    }

    #[test]
    fn test_jsonld_serialization() {
        let triples = create_test_triples();
        let output = RdfSerializer::serialize(&triples, RdfFormat::JsonLd).unwrap();
        assert!(output.contains("@id"));
    }
    
    #[test]
    fn test_format_detection() {
        assert_eq!(RdfFormat::from_extension(Path::new("test.ttl")), Some(RdfFormat::Turtle));
        assert_eq!(RdfFormat::from_extension(Path::new("test.nt")), Some(RdfFormat::NTriples));
        assert_eq!(RdfFormat::from_extension(Path::new("test.rdf")), Some(RdfFormat::RdfXml));
        assert_eq!(RdfFormat::from_extension(Path::new("test.jsonld")), Some(RdfFormat::JsonLd));
    }
}