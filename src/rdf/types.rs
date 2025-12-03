//! RDF type definitions
//!
//! This module provides wrapper types around the oxrdf library for RDF primitives.

use oxrdf::{
    NamedNode as OxNamedNode,
    BlankNode as OxBlankNode,
    Literal as OxLiteral,
    Subject as OxSubject,
    Term as OxTerm,
    Triple as OxTriple,
    Quad as OxQuad,
    NamedOrBlankNode,
};
use std::fmt;
use thiserror::Error;

/// RDF errors
#[derive(Error, Debug)]
pub enum RdfError {
    /// Invalid IRI
    #[error("Invalid IRI: {0}")]
    InvalidIri(String),

    /// Invalid blank node
    #[error("Invalid blank node: {0}")]
    InvalidBlankNode(String),

    /// Invalid literal
    #[error("Invalid literal: {0}")]
    InvalidLiteral(String),
}

pub type RdfResult<T> = Result<T, RdfError>;

/// Named node (IRI)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamedNode(OxNamedNode);

impl NamedNode {
    /// Create a new named node from an IRI string
    pub fn new(iri: &str) -> RdfResult<Self> {
        OxNamedNode::new(iri)
            .map(Self)
            .map_err(|e| RdfError::InvalidIri(e.to_string()))
    }

    /// Get the IRI string
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Get the inner oxrdf NamedNode
    pub fn inner(&self) -> &OxNamedNode {
        &self.0
    }
}

impl fmt::Display for NamedNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>", self.as_str())
    }
}

impl From<OxNamedNode> for NamedNode {
    fn from(node: OxNamedNode) -> Self {
        Self(node)
    }
}

impl From<NamedNode> for OxNamedNode {
    fn from(node: NamedNode) -> Self {
        node.0
    }
}

/// Blank node (anonymous node)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlankNode(OxBlankNode);

impl BlankNode {
    /// Create a new blank node with a unique identifier
    pub fn new() -> Self {
        Self(OxBlankNode::default())
    }

    /// Create a blank node from a string identifier
    pub fn from_str(s: &str) -> RdfResult<Self> {
        OxBlankNode::new(s)
            .map(Self)
            .map_err(|e| RdfError::InvalidBlankNode(e.to_string()))
    }

    /// Get the blank node identifier
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Get the inner oxrdf BlankNode
    pub fn inner(&self) -> &OxBlankNode {
        &self.0
    }
}

impl Default for BlankNode {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BlankNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "_:{}", self.as_str())
    }
}

impl From<OxBlankNode> for BlankNode {
    fn from(node: OxBlankNode) -> Self {
        Self(node)
    }
}

impl From<BlankNode> for OxBlankNode {
    fn from(node: BlankNode) -> Self {
        node.0
    }
}

/// RDF literal value
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Literal(OxLiteral);

impl Literal {
    /// Create a simple literal (plain string)
    pub fn new_simple_literal(value: impl Into<String>) -> Self {
        Self(OxLiteral::new_simple_literal(value))
    }

    /// Create a literal with language tag
    pub fn new_language_tagged_literal(value: impl Into<String>, language: impl Into<String>) -> RdfResult<Self> {
        OxLiteral::new_language_tagged_literal(value, language)
            .map(Self)
            .map_err(|e| RdfError::InvalidLiteral(e.to_string()))
    }

    /// Create a typed literal
    pub fn new_typed_literal(value: impl Into<String>, datatype: NamedNode) -> Self {
        Self(OxLiteral::new_typed_literal(value, datatype.0))
    }

    /// Get the lexical value
    pub fn value(&self) -> &str {
        self.0.value()
    }

    /// Get the language tag if present
    pub fn language(&self) -> Option<&str> {
        self.0.language()
    }

    /// Get the datatype
    pub fn datatype(&self) -> NamedNode {
        NamedNode(self.0.datatype().into_owned())
    }

    /// Get the inner oxrdf Literal
    pub fn inner(&self) -> &OxLiteral {
        &self.0
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(lang) = self.language() {
            write!(f, "\"{}\"@{}", self.value(), lang)
        } else {
            write!(f, "\"{}\"^^{}", self.value(), self.datatype())
        }
    }
}

impl From<OxLiteral> for Literal {
    fn from(lit: OxLiteral) -> Self {
        Self(lit)
    }
}

impl From<Literal> for OxLiteral {
    fn from(lit: Literal) -> Self {
        lit.0
    }
}

/// RDF subject (NamedNode or BlankNode)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RdfSubject {
    /// Named node (IRI)
    NamedNode(NamedNode),
    /// Blank node
    BlankNode(BlankNode),
}

impl RdfSubject {
    /// Check if this is a named node
    pub fn is_named_node(&self) -> bool {
        matches!(self, RdfSubject::NamedNode(_))
    }

    /// Check if this is a blank node
    pub fn is_blank_node(&self) -> bool {
        matches!(self, RdfSubject::BlankNode(_))
    }
}

impl fmt::Display for RdfSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdfSubject::NamedNode(n) => write!(f, "{}", n),
            RdfSubject::BlankNode(b) => write!(f, "{}", b),
        }
    }
}

impl From<NamedNode> for RdfSubject {
    fn from(node: NamedNode) -> Self {
        RdfSubject::NamedNode(node)
    }
}

impl From<BlankNode> for RdfSubject {
    fn from(node: BlankNode) -> Self {
        RdfSubject::BlankNode(node)
    }
}

impl From<OxSubject> for RdfSubject {
    fn from(subject: OxSubject) -> Self {
        match subject {
            OxSubject::NamedNode(n) => RdfSubject::NamedNode(n.into()),
            OxSubject::BlankNode(b) => RdfSubject::BlankNode(b.into()),
            #[allow(unreachable_patterns)]
            _ => panic!("RDF-star triples not yet supported"),
        }
    }
}

impl From<RdfSubject> for OxSubject {
    fn from(subject: RdfSubject) -> Self {
        match subject {
            RdfSubject::NamedNode(n) => OxSubject::NamedNode(n.0),
            RdfSubject::BlankNode(b) => OxSubject::BlankNode(b.0),
        }
    }
}

/// RDF predicate (always a NamedNode)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RdfPredicate(NamedNode);

impl RdfPredicate {
    /// Create a new predicate from an IRI
    pub fn new(iri: &str) -> RdfResult<Self> {
        Ok(Self(NamedNode::new(iri)?))
    }

    /// Get the underlying named node
    pub fn as_named_node(&self) -> &NamedNode {
        &self.0
    }
}

impl fmt::Display for RdfPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<NamedNode> for RdfPredicate {
    fn from(node: NamedNode) -> Self {
        RdfPredicate(node)
    }
}

impl From<RdfPredicate> for NamedNode {
    fn from(pred: RdfPredicate) -> Self {
        pred.0
    }
}

/// RDF object (NamedNode, BlankNode, or Literal)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RdfObject {
    /// Named node (IRI)
    NamedNode(NamedNode),
    /// Blank node
    BlankNode(BlankNode),
    /// Literal value
    Literal(Literal),
}

impl RdfObject {
    /// Check if this is a named node
    pub fn is_named_node(&self) -> bool {
        matches!(self, RdfObject::NamedNode(_))
    }

    /// Check if this is a blank node
    pub fn is_blank_node(&self) -> bool {
        matches!(self, RdfObject::BlankNode(_))
    }

    /// Check if this is a literal
    pub fn is_literal(&self) -> bool {
        matches!(self, RdfObject::Literal(_))
    }
}

impl fmt::Display for RdfObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdfObject::NamedNode(n) => write!(f, "{}", n),
            RdfObject::BlankNode(b) => write!(f, "{}", b),
            RdfObject::Literal(l) => write!(f, "{}", l),
        }
    }
}

impl From<NamedNode> for RdfObject {
    fn from(node: NamedNode) -> Self {
        RdfObject::NamedNode(node)
    }
}

impl From<BlankNode> for RdfObject {
    fn from(node: BlankNode) -> Self {
        RdfObject::BlankNode(node)
    }
}

impl From<Literal> for RdfObject {
    fn from(lit: Literal) -> Self {
        RdfObject::Literal(lit)
    }
}

impl From<OxTerm> for RdfObject {
    fn from(term: OxTerm) -> Self {
        match term {
            OxTerm::NamedNode(n) => RdfObject::NamedNode(n.into()),
            OxTerm::BlankNode(b) => RdfObject::BlankNode(b.into()),
            OxTerm::Literal(l) => RdfObject::Literal(l.into()),
            #[allow(unreachable_patterns)]
            _ => panic!("RDF-star triples not yet supported"),
        }
    }
}

impl From<RdfObject> for OxTerm {
    fn from(object: RdfObject) -> Self {
        match object {
            RdfObject::NamedNode(n) => OxTerm::NamedNode(n.0),
            RdfObject::BlankNode(b) => OxTerm::BlankNode(b.0),
            RdfObject::Literal(l) => OxTerm::Literal(l.0),
        }
    }
}

/// RDF term (any RDF value)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RdfTerm {
    /// Named node (IRI)
    NamedNode(NamedNode),
    /// Blank node
    BlankNode(BlankNode),
    /// Literal value
    Literal(Literal),
}

impl From<RdfSubject> for RdfTerm {
    fn from(subject: RdfSubject) -> Self {
        match subject {
            RdfSubject::NamedNode(n) => RdfTerm::NamedNode(n),
            RdfSubject::BlankNode(b) => RdfTerm::BlankNode(b),
        }
    }
}

impl From<RdfObject> for RdfTerm {
    fn from(object: RdfObject) -> Self {
        match object {
            RdfObject::NamedNode(n) => RdfTerm::NamedNode(n),
            RdfObject::BlankNode(b) => RdfTerm::BlankNode(b),
            RdfObject::Literal(l) => RdfTerm::Literal(l),
        }
    }
}

/// RDF triple (subject-predicate-object)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Triple {
    /// Subject
    pub subject: RdfSubject,
    /// Predicate
    pub predicate: RdfPredicate,
    /// Object
    pub object: RdfObject,
}

impl Triple {
    /// Create a new triple
    pub fn new(subject: RdfSubject, predicate: RdfPredicate, object: RdfObject) -> Self {
        Self {
            subject,
            predicate,
            object,
        }
    }

    /// Convert to oxrdf Triple
    pub fn to_oxrdf(&self) -> OxTriple {
        let subject: OxSubject = self.subject.clone().into();
        let predicate: OxNamedNode = self.predicate.clone().0.into();
        let object: OxTerm = self.object.clone().into();

        OxTriple::new(subject, predicate, object)
    }
}

impl fmt::Display for Triple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {} .", self.subject, self.predicate, self.object)
    }
}

impl From<OxTriple> for Triple {
    fn from(triple: OxTriple) -> Self {
        Self {
            subject: triple.subject.into(),
            predicate: RdfPredicate(triple.predicate.into()),
            object: triple.object.into(),
        }
    }
}

/// RDF quad (triple + named graph)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Quad {
    /// Subject
    pub subject: RdfSubject,
    /// Predicate
    pub predicate: RdfPredicate,
    /// Object
    pub object: RdfObject,
    /// Named graph (None = default graph)
    pub graph: Option<NamedNode>,
}

impl Quad {
    /// Create a new quad
    pub fn new(
        subject: RdfSubject,
        predicate: RdfPredicate,
        object: RdfObject,
        graph: Option<NamedNode>,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            graph,
        }
    }

    /// Create a quad from a triple (default graph)
    pub fn from_triple(triple: Triple) -> Self {
        Self {
            subject: triple.subject,
            predicate: triple.predicate,
            object: triple.object,
            graph: None,
        }
    }

    /// Get the triple part (without graph)
    pub fn as_triple(&self) -> Triple {
        Triple {
            subject: self.subject.clone(),
            predicate: self.predicate.clone(),
            object: self.object.clone(),
        }
    }
}

impl fmt::Display for Quad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(graph) = &self.graph {
            write!(
                f,
                "{} {} {} {} .",
                self.subject, self.predicate, self.object, graph
            )
        } else {
            write!(f, "{} {} {} .", self.subject, self.predicate, self.object)
        }
    }
}

/// Triple pattern for queries (with optional variables)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriplePattern {
    /// Subject (None = variable)
    pub subject: Option<RdfSubject>,
    /// Predicate (None = variable)
    pub predicate: Option<RdfPredicate>,
    /// Object (None = variable)
    pub object: Option<RdfObject>,
}

impl TriplePattern {
    /// Create a new triple pattern
    pub fn new(
        subject: Option<RdfSubject>,
        predicate: Option<RdfPredicate>,
        object: Option<RdfObject>,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
        }
    }

    /// Check if a triple matches this pattern
    pub fn matches(&self, triple: &Triple) -> bool {
        if let Some(ref s) = self.subject {
            if s != &triple.subject {
                return false;
            }
        }
        if let Some(ref p) = self.predicate {
            if p != &triple.predicate {
                return false;
            }
        }
        if let Some(ref o) = self.object {
            if o != &triple.object {
                return false;
            }
        }
        true
    }
}

/// Quad pattern for queries (with optional variables)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuadPattern {
    /// Subject (None = variable)
    pub subject: Option<RdfSubject>,
    /// Predicate (None = variable)
    pub predicate: Option<RdfPredicate>,
    /// Object (None = variable)
    pub object: Option<RdfObject>,
    /// Graph (None = variable, Some(None) = default graph)
    pub graph: Option<Option<NamedNode>>,
}

impl QuadPattern {
    /// Check if a quad matches this pattern
    pub fn matches(&self, quad: &Quad) -> bool {
        if let Some(ref s) = self.subject {
            if s != &quad.subject {
                return false;
            }
        }
        if let Some(ref p) = self.predicate {
            if p != &quad.predicate {
                return false;
            }
        }
        if let Some(ref o) = self.object {
            if o != &quad.object {
                return false;
            }
        }
        if let Some(ref g) = self.graph {
            if g != &quad.graph {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_named_node() {
        let node = NamedNode::new("http://example.org/alice").unwrap();
        assert_eq!(node.as_str(), "http://example.org/alice");
        assert_eq!(node.to_string(), "<http://example.org/alice>");
    }

    #[test]
    fn test_blank_node() {
        let node1 = BlankNode::new();
        let node2 = BlankNode::new();
        assert_ne!(node1, node2); // Should have unique identifiers
    }

    #[test]
    fn test_literal() {
        // Simple literal
        let lit = Literal::new_simple_literal("Alice");
        assert_eq!(lit.value(), "Alice");

        // Language-tagged literal
        let lit = Literal::new_language_tagged_literal("Alice", "en").unwrap();
        assert_eq!(lit.value(), "Alice");
        assert_eq!(lit.language(), Some("en"));
    }

    #[test]
    fn test_triple() {
        let subject = NamedNode::new("http://example.org/alice").unwrap();
        let predicate = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
        let object = Literal::new_simple_literal("Alice");

        let triple = Triple::new(
            subject.into(),
            predicate,
            object.into(),
        );

        assert!(triple.subject.is_named_node());
        assert!(triple.object.is_literal());
    }

    #[test]
    fn test_triple_pattern_matching() {
        let subject = NamedNode::new("http://example.org/alice").unwrap();
        let predicate = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
        let object = Literal::new_simple_literal("Alice");

        let triple = Triple::new(
            subject.clone().into(),
            predicate.clone(),
            object.into(),
        );

        // Pattern with subject
        let pattern = TriplePattern::new(
            Some(subject.into()),
            None,
            None,
        );
        assert!(pattern.matches(&triple));

        // Pattern with wrong subject
        let wrong_subject = NamedNode::new("http://example.org/bob").unwrap();
        let pattern = TriplePattern::new(
            Some(wrong_subject.into()),
            None,
            None,
        );
        assert!(!pattern.matches(&triple));

        // Pattern with all variables
        let pattern = TriplePattern::new(None, None, None);
        assert!(pattern.matches(&triple));
    }

    #[test]
    fn test_quad() {
        let subject = NamedNode::new("http://example.org/alice").unwrap();
        let predicate = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
        let object = Literal::new_simple_literal("Alice");
        let graph = NamedNode::new("http://example.org/graph/social").unwrap();

        let quad = Quad::new(
            subject.into(),
            predicate,
            object.into(),
            Some(graph),
        );

        assert!(quad.graph.is_some());

        let triple = quad.as_triple();
        assert!(triple.subject.is_named_node());
    }
}
