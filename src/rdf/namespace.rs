//! RDF namespace and prefix management
//!
//! This module handles namespace prefixes for compact IRI notation.

use std::collections::HashMap;
use thiserror::Error;

/// Prefix errors
#[derive(Error, Debug)]
pub enum PrefixError {
    /// Unknown prefix
    #[error("Unknown prefix: {0}")]
    UnknownPrefix(String),

    /// Invalid IRI
    #[error("Invalid IRI: {0}")]
    InvalidIri(String),
}

pub type PrefixResult<T> = Result<T, PrefixError>;

/// Namespace (prefix → IRI mapping)
#[derive(Debug, Clone)]
pub struct Namespace {
    /// Prefix
    pub prefix: String,
    /// IRI
    pub iri: String,
}

impl Namespace {
    /// Create a new namespace
    pub fn new(prefix: impl Into<String>, iri: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            iri: iri.into(),
        }
    }
}

/// Namespace manager with common prefixes
pub struct NamespaceManager {
    /// Prefix → IRI mappings
    prefixes: HashMap<String, String>,
}

impl NamespaceManager {
    /// Create a new namespace manager with common prefixes
    pub fn new() -> Self {
        let mut mgr = Self {
            prefixes: HashMap::new(),
        };

        // Add common RDF/RDFS/OWL prefixes
        mgr.add_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        mgr.add_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        mgr.add_prefix("xsd", "http://www.w3.org/2001/XMLSchema#");
        mgr.add_prefix("owl", "http://www.w3.org/2002/07/owl#");
        mgr.add_prefix("foaf", "http://xmlns.com/foaf/0.1/");
        mgr.add_prefix("dc", "http://purl.org/dc/elements/1.1/");
        mgr.add_prefix("dcterms", "http://purl.org/dc/terms/");

        mgr
    }

    /// Add a prefix
    pub fn add_prefix(&mut self, prefix: impl Into<String>, iri: impl Into<String>) {
        self.prefixes.insert(prefix.into(), iri.into());
    }

    /// Get IRI for a prefix
    pub fn get_iri(&self, prefix: &str) -> PrefixResult<&str> {
        self.prefixes
            .get(prefix)
            .map(|s| s.as_str())
            .ok_or_else(|| PrefixError::UnknownPrefix(prefix.to_string()))
    }

    /// Expand a compact IRI (prefix:local) to full IRI
    pub fn expand(&self, compact_iri: &str) -> PrefixResult<String> {
        if let Some(pos) = compact_iri.find(':') {
            let prefix = &compact_iri[..pos];
            let local = &compact_iri[pos + 1..];
            let iri = self.get_iri(prefix)?;
            Ok(format!("{}{}", iri, local))
        } else {
            Err(PrefixError::InvalidIri(compact_iri.to_string()))
        }
    }

    /// Compact an IRI using known prefixes
    pub fn compact(&self, iri: &str) -> Option<String> {
        for (prefix, namespace_iri) in &self.prefixes {
            if iri.starts_with(namespace_iri) {
                let local = &iri[namespace_iri.len()..];
                return Some(format!("{}:{}", prefix, local));
            }
        }
        None
    }

    /// Get all registered prefixes
    pub fn prefixes(&self) -> Vec<Namespace> {
        self.prefixes
            .iter()
            .map(|(prefix, iri)| Namespace::new(prefix.clone(), iri.clone()))
            .collect()
    }
}

impl Default for NamespaceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_prefixes() {
        let mgr = NamespaceManager::new();

        assert_eq!(
            mgr.get_iri("rdf").unwrap(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        );
        assert_eq!(
            mgr.get_iri("rdfs").unwrap(),
            "http://www.w3.org/2000/01/rdf-schema#"
        );
        assert_eq!(mgr.get_iri("xsd").unwrap(), "http://www.w3.org/2001/XMLSchema#");
    }

    #[test]
    fn test_expand() {
        let mgr = NamespaceManager::new();

        let expanded = mgr.expand("foaf:name").unwrap();
        assert_eq!(expanded, "http://xmlns.com/foaf/0.1/name");

        let expanded = mgr.expand("rdf:type").unwrap();
        assert_eq!(expanded, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    }

    #[test]
    fn test_compact() {
        let mgr = NamespaceManager::new();

        let compacted = mgr.compact("http://xmlns.com/foaf/0.1/name");
        assert_eq!(compacted, Some("foaf:name".to_string()));

        let compacted = mgr.compact("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        assert_eq!(compacted, Some("rdf:type".to_string()));
    }

    #[test]
    fn test_custom_prefix() {
        let mut mgr = NamespaceManager::new();
        mgr.add_prefix("ex", "http://example.org/");

        let expanded = mgr.expand("ex:alice").unwrap();
        assert_eq!(expanded, "http://example.org/alice");
    }
}
