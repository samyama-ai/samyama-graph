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

    // ========== Batch 6: Additional RDF Types Tests ==========

    #[test]
    fn test_named_node_inner() {
        let node = NamedNode::new("http://example.org/foo").unwrap();
        let inner = node.inner();
        assert_eq!(inner.as_str(), "http://example.org/foo");
    }

    #[test]
    fn test_blank_node_from_str() {
        let bn = BlankNode::from_str("b1").unwrap();
        assert_eq!(bn.as_str(), "b1");
        let inner = bn.inner();
        assert_eq!(inner.as_str(), "b1");
    }

    #[test]
    fn test_literal_language_tagged() {
        let lit = Literal::new_language_tagged_literal("hello", "en").unwrap();
        assert_eq!(lit.value(), "hello");
        assert_eq!(lit.language(), Some("en"));
    }

    #[test]
    fn test_literal_typed() {
        let dt = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
        let lit = Literal::new_typed_literal("42", dt.clone());
        assert_eq!(lit.value(), "42");
        let dt2 = lit.datatype();
        assert_eq!(dt2.as_str(), dt.as_str());
    }

    #[test]
    fn test_literal_inner() {
        let lit = Literal::new_simple_literal("test");
        let inner = lit.inner();
        assert_eq!(inner.value(), "test");
    }

    #[test]
    fn test_rdf_subject_type_checks() {
        let named = NamedNode::new("http://example.org/s").unwrap();
        let subj = RdfSubject::from(named);
        assert!(subj.is_named_node());
        assert!(!subj.is_blank_node());

        let blank = BlankNode::new();
        let subj2 = RdfSubject::from(blank);
        assert!(!subj2.is_named_node());
        assert!(subj2.is_blank_node());
    }

    #[test]
    fn test_rdf_predicate() {
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let nn = pred.as_named_node();
        assert_eq!(nn.as_str(), "http://example.org/p");
    }

    #[test]
    fn test_rdf_object_type_checks() {
        let named = NamedNode::new("http://example.org/o").unwrap();
        let obj = RdfObject::from(named);
        assert!(obj.is_named_node());
        assert!(!obj.is_blank_node());
        assert!(!obj.is_literal());

        let lit = Literal::new_simple_literal("text");
        let obj2 = RdfObject::from(lit);
        assert!(!obj2.is_named_node());
        assert!(!obj2.is_blank_node());
        assert!(obj2.is_literal());

        let blank = BlankNode::new();
        let obj3 = RdfObject::from(blank);
        assert!(!obj3.is_named_node());
        assert!(obj3.is_blank_node());
        assert!(!obj3.is_literal());
    }

    #[test]
    fn test_triple_to_oxrdf() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("value"));
        let triple = Triple::new(subj, pred, obj);

        let ox = triple.to_oxrdf();
        assert_eq!(ox.subject.to_string(), "<http://example.org/s>");
    }

    #[test]
    fn test_quad_from_triple() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("value"));
        let triple = Triple::new(subj, pred, obj);

        let quad = Quad::from_triple(triple.clone());
        assert!(quad.graph.is_none());
        let back = quad.as_triple();
        assert!(back.subject.is_named_node());
    }

    #[test]
    fn test_triple_pattern_new() {
        let subj = Some(RdfSubject::from(NamedNode::new("http://example.org/s").unwrap()));
        let pattern = TriplePattern::new(subj, None, None);
        assert!(pattern.subject.is_some());
        assert!(pattern.predicate.is_none());
        assert!(pattern.object.is_none());
    }

    #[test]
    fn test_quad_pattern_matches() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("value"));
        let triple = Triple::new(subj.clone(), pred.clone(), obj.clone());
        let quad = Quad::from_triple(triple);

        // Wildcard pattern matches everything
        let pattern = QuadPattern {
            subject: None,
            predicate: None,
            object: None,
            graph: None,
        };
        assert!(pattern.matches(&quad));

        // Specific subject match
        let pattern2 = QuadPattern {
            subject: Some(subj),
            predicate: None,
            object: None,
            graph: None,
        };
        assert!(pattern2.matches(&quad));
    }

    #[test]
    fn test_triple_display() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("hello"));
        let triple = Triple::new(subj, pred, obj);

        let display = format!("{}", triple);
        assert!(display.contains("http://example.org/s"));
        assert!(display.contains("http://example.org/p"));
    }

    // ========== Mop-up: From/Display/conversion coverage ==========

    #[test]
    fn test_named_node_from_ox() {
        let ox_node = OxNamedNode::new("http://example.org/test").unwrap();
        let node: NamedNode = ox_node.into();
        assert_eq!(node.as_str(), "http://example.org/test");
    }

    #[test]
    fn test_named_node_into_ox() {
        let node = NamedNode::new("http://example.org/test").unwrap();
        let ox_node: OxNamedNode = node.into();
        assert_eq!(ox_node.as_str(), "http://example.org/test");
    }

    #[test]
    fn test_blank_node_default() {
        let bn = BlankNode::default();
        assert!(!bn.as_str().is_empty());
    }

    #[test]
    fn test_blank_node_display() {
        let bn = BlankNode::from_str("b42").unwrap();
        let display = format!("{}", bn);
        assert!(display.starts_with("_:"));
        assert!(display.contains("b42"));
    }

    #[test]
    fn test_blank_node_from_ox() {
        let ox_bn = OxBlankNode::default();
        let bn: BlankNode = ox_bn.into();
        assert!(!bn.as_str().is_empty());
    }

    #[test]
    fn test_blank_node_into_ox() {
        let bn = BlankNode::from_str("b1").unwrap();
        let ox_bn: OxBlankNode = bn.into();
        assert_eq!(ox_bn.as_str(), "b1");
    }

    #[test]
    fn test_literal_display_simple() {
        let lit = Literal::new_simple_literal("hello");
        let display = format!("{}", lit);
        assert!(display.contains("hello"));
    }

    #[test]
    fn test_literal_display_language_tagged() {
        let lit = Literal::new_language_tagged_literal("hello", "en").unwrap();
        let display = format!("{}", lit);
        assert!(display.contains("hello"));
        assert!(display.contains("@en"));
    }

    #[test]
    fn test_literal_display_typed() {
        let dt = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
        let lit = Literal::new_typed_literal("42", dt);
        let display = format!("{}", lit);
        assert!(display.contains("42"));
    }

    #[test]
    fn test_literal_from_ox() {
        let ox_lit = OxLiteral::new_simple_literal("test");
        let lit: Literal = ox_lit.into();
        assert_eq!(lit.value(), "test");
    }

    #[test]
    fn test_literal_into_ox() {
        let lit = Literal::new_simple_literal("test");
        let ox_lit: OxLiteral = lit.into();
        assert_eq!(ox_lit.value(), "test");
    }

    #[test]
    fn test_rdf_subject_display() {
        let named = NamedNode::new("http://example.org/s").unwrap();
        let subj = RdfSubject::from(named);
        let display = format!("{}", subj);
        assert!(display.contains("http://example.org/s"));

        let blank = BlankNode::from_str("b1").unwrap();
        let subj2 = RdfSubject::from(blank);
        let display2 = format!("{}", subj2);
        assert!(display2.starts_with("_:"));
    }

    #[test]
    fn test_rdf_subject_from_ox() {
        let ox_nn = OxNamedNode::new("http://example.org/s").unwrap();
        let ox_subj = OxSubject::NamedNode(ox_nn);
        let subj: RdfSubject = ox_subj.into();
        assert!(subj.is_named_node());

        let ox_bn = OxBlankNode::default();
        let ox_subj2 = OxSubject::BlankNode(ox_bn);
        let subj2: RdfSubject = ox_subj2.into();
        assert!(subj2.is_blank_node());
    }

    #[test]
    fn test_rdf_predicate_from_named_node() {
        let nn = NamedNode::new("http://example.org/p").unwrap();
        let pred: RdfPredicate = nn.into();
        assert_eq!(pred.as_named_node().as_str(), "http://example.org/p");
    }

    #[test]
    fn test_rdf_predicate_into_named_node() {
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let nn: NamedNode = pred.into();
        assert_eq!(nn.as_str(), "http://example.org/p");
    }

    #[test]
    fn test_rdf_predicate_display() {
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let display = format!("{}", pred);
        assert!(display.contains("http://example.org/p"));
    }

    #[test]
    fn test_rdf_object_display() {
        let named = NamedNode::new("http://example.org/o").unwrap();
        let obj = RdfObject::from(named);
        let display = format!("{}", obj);
        assert!(display.contains("http://example.org/o"));

        let blank = BlankNode::from_str("b2").unwrap();
        let obj2 = RdfObject::from(blank);
        let display2 = format!("{}", obj2);
        assert!(display2.starts_with("_:"));

        let lit = Literal::new_simple_literal("value");
        let obj3 = RdfObject::from(lit);
        let display3 = format!("{}", obj3);
        assert!(display3.contains("value"));
    }

    #[test]
    fn test_rdf_object_from_ox_term() {
        let ox_nn = OxNamedNode::new("http://example.org/o").unwrap();
        let term = OxTerm::NamedNode(ox_nn);
        let obj: RdfObject = term.into();
        assert!(obj.is_named_node());

        let ox_bn = OxBlankNode::default();
        let term2 = OxTerm::BlankNode(ox_bn);
        let obj2: RdfObject = term2.into();
        assert!(obj2.is_blank_node());

        let ox_lit = OxLiteral::new_simple_literal("test");
        let term3 = OxTerm::Literal(ox_lit);
        let obj3: RdfObject = term3.into();
        assert!(obj3.is_literal());
    }

    #[test]
    fn test_rdf_error_display() {
        let e1 = RdfError::InvalidIri("bad iri".to_string());
        let s1 = format!("{}", e1);
        assert!(s1.contains("Invalid IRI"));

        let e2 = RdfError::InvalidBlankNode("bad bn".to_string());
        let s2 = format!("{}", e2);
        assert!(s2.contains("Invalid blank node"));

        let e3 = RdfError::InvalidLiteral("bad lit".to_string());
        let s3 = format!("{}", e3);
        assert!(s3.contains("Invalid literal"));
    }

    #[test]
    fn test_named_node_invalid_iri() {
        let result = NamedNode::new("not a valid iri");
        assert!(result.is_err());
    }

    // ========== Coverage batch: additional RDF types tests ==========

    #[test]
    fn test_blank_node_invalid_str() {
        // Blank node IDs with spaces are invalid
        let result = BlankNode::from_str("invalid blank node id!");
        assert!(result.is_err());
    }

    #[test]
    fn test_literal_language_tag_invalid() {
        // Invalid language tag
        let result = Literal::new_language_tagged_literal("hello", "invalid tag with spaces");
        assert!(result.is_err());
    }

    #[test]
    fn test_literal_simple_no_language() {
        let lit = Literal::new_simple_literal("test value");
        assert_eq!(lit.value(), "test value");
        assert!(lit.language().is_none());
    }

    #[test]
    fn test_literal_datatype_for_simple() {
        let lit = Literal::new_simple_literal("hello");
        let dt = lit.datatype();
        // Simple literals have xsd:string datatype
        assert!(dt.as_str().contains("string"));
    }

    #[test]
    fn test_rdf_subject_into_ox_named() {
        let nn = NamedNode::new("http://example.org/s").unwrap();
        let subj = RdfSubject::NamedNode(nn);
        let ox_subj: OxSubject = subj.into();
        match ox_subj {
            OxSubject::NamedNode(n) => assert_eq!(n.as_str(), "http://example.org/s"),
            _ => panic!("Expected NamedNode"),
        }
    }

    #[test]
    fn test_rdf_subject_into_ox_blank() {
        let bn = BlankNode::from_str("b99").unwrap();
        let subj = RdfSubject::BlankNode(bn);
        let ox_subj: OxSubject = subj.into();
        match ox_subj {
            OxSubject::BlankNode(b) => assert_eq!(b.as_str(), "b99"),
            _ => panic!("Expected BlankNode"),
        }
    }

    #[test]
    fn test_rdf_object_into_ox_term_named() {
        let nn = NamedNode::new("http://example.org/o").unwrap();
        let obj = RdfObject::NamedNode(nn);
        let term: OxTerm = obj.into();
        match term {
            OxTerm::NamedNode(n) => assert_eq!(n.as_str(), "http://example.org/o"),
            _ => panic!("Expected NamedNode"),
        }
    }

    #[test]
    fn test_rdf_object_into_ox_term_blank() {
        let bn = BlankNode::from_str("b1").unwrap();
        let obj = RdfObject::BlankNode(bn);
        let term: OxTerm = obj.into();
        match term {
            OxTerm::BlankNode(b) => assert_eq!(b.as_str(), "b1"),
            _ => panic!("Expected BlankNode"),
        }
    }

    #[test]
    fn test_rdf_object_into_ox_term_literal() {
        let lit = Literal::new_simple_literal("val");
        let obj = RdfObject::Literal(lit);
        let term: OxTerm = obj.into();
        match term {
            OxTerm::Literal(l) => assert_eq!(l.value(), "val"),
            _ => panic!("Expected Literal"),
        }
    }

    #[test]
    fn test_rdf_term_from_subject_named() {
        let nn = NamedNode::new("http://example.org/t").unwrap();
        let subj = RdfSubject::NamedNode(nn);
        let term: RdfTerm = subj.into();
        assert!(matches!(term, RdfTerm::NamedNode(_)));
    }

    #[test]
    fn test_rdf_term_from_subject_blank() {
        let bn = BlankNode::new();
        let subj = RdfSubject::BlankNode(bn);
        let term: RdfTerm = subj.into();
        assert!(matches!(term, RdfTerm::BlankNode(_)));
    }

    #[test]
    fn test_rdf_term_from_object_named() {
        let nn = NamedNode::new("http://example.org/t2").unwrap();
        let obj = RdfObject::NamedNode(nn);
        let term: RdfTerm = obj.into();
        assert!(matches!(term, RdfTerm::NamedNode(_)));
    }

    #[test]
    fn test_rdf_term_from_object_blank() {
        let bn = BlankNode::new();
        let obj = RdfObject::BlankNode(bn);
        let term: RdfTerm = obj.into();
        assert!(matches!(term, RdfTerm::BlankNode(_)));
    }

    #[test]
    fn test_rdf_term_from_object_literal() {
        let lit = Literal::new_simple_literal("val");
        let obj = RdfObject::Literal(lit);
        let term: RdfTerm = obj.into();
        assert!(matches!(term, RdfTerm::Literal(_)));
    }

    #[test]
    fn test_triple_from_ox_triple() {
        let ox_subj = OxSubject::NamedNode(OxNamedNode::new("http://example.org/s").unwrap());
        let ox_pred = OxNamedNode::new("http://example.org/p").unwrap();
        let ox_obj = OxTerm::Literal(OxLiteral::new_simple_literal("value"));
        let ox_triple = OxTriple::new(ox_subj, ox_pred, ox_obj);

        let triple: Triple = ox_triple.into();
        assert!(triple.subject.is_named_node());
        assert!(triple.object.is_literal());
    }

    #[test]
    fn test_triple_display_full() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/alice").unwrap());
        let pred = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("Alice"));
        let triple = Triple::new(subj, pred, obj);
        let display = format!("{}", triple);
        assert!(display.contains("<http://example.org/alice>"));
        assert!(display.contains("<http://xmlns.com/foaf/0.1/name>"));
        assert!(display.contains("Alice"));
        assert!(display.ends_with("."));
    }

    #[test]
    fn test_quad_display_with_graph() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("v"));
        let graph = NamedNode::new("http://example.org/g").unwrap();
        let quad = Quad::new(subj, pred, obj, Some(graph));
        let display = format!("{}", quad);
        assert!(display.contains("<http://example.org/g>"));
        assert!(display.ends_with("."));
    }

    #[test]
    fn test_quad_display_default_graph() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("v"));
        let quad = Quad::new(subj, pred, obj, None);
        let display = format!("{}", quad);
        assert!(display.ends_with("."));
        assert!(!display.contains("graph"));
    }

    #[test]
    fn test_triple_pattern_predicate_match() {
        let subj = NamedNode::new("http://example.org/s").unwrap();
        let pred = RdfPredicate::new("http://example.org/p1").unwrap();
        let obj = Literal::new_simple_literal("val");
        let triple = Triple::new(subj.into(), pred.clone(), obj.into());

        // Pattern matching on predicate
        let pattern = TriplePattern::new(None, Some(pred), None);
        assert!(pattern.matches(&triple));

        // Non-matching predicate
        let other_pred = RdfPredicate::new("http://example.org/p2").unwrap();
        let pattern2 = TriplePattern::new(None, Some(other_pred), None);
        assert!(!pattern2.matches(&triple));
    }

    #[test]
    fn test_triple_pattern_object_match() {
        let subj = NamedNode::new("http://example.org/s").unwrap();
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = Literal::new_simple_literal("target_value");
        let triple = Triple::new(subj.into(), pred, obj.clone().into());

        // Pattern matching on object
        let pattern = TriplePattern::new(None, None, Some(obj.into()));
        assert!(pattern.matches(&triple));

        // Non-matching object
        let other_obj = RdfObject::from(Literal::new_simple_literal("other_value"));
        let pattern2 = TriplePattern::new(None, None, Some(other_obj));
        assert!(!pattern2.matches(&triple));
    }

    #[test]
    fn test_quad_pattern_graph_match() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("v"));
        let graph = NamedNode::new("http://example.org/g").unwrap();
        let quad = Quad::new(subj, pred, obj, Some(graph.clone()));

        // Match specific graph
        let pattern = QuadPattern {
            subject: None,
            predicate: None,
            object: None,
            graph: Some(Some(graph)),
        };
        assert!(pattern.matches(&quad));

        // Match default graph (None) against quad with named graph => no match
        let pattern_default = QuadPattern {
            subject: None,
            predicate: None,
            object: None,
            graph: Some(None),
        };
        assert!(!pattern_default.matches(&quad));
    }

    #[test]
    fn test_quad_pattern_subject_mismatch_and_full_match() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("v"));
        let quad = Quad::from_triple(Triple::new(subj.clone(), pred.clone(), obj.clone()));

        let other = RdfSubject::from(NamedNode::new("http://example.org/other").unwrap());
        let mismatch = QuadPattern { subject: Some(other), predicate: None, object: None, graph: None };
        assert!(!mismatch.matches(&quad));

        let all = QuadPattern {
            subject: Some(subj),
            predicate: Some(pred),
            object: Some(obj),
            graph: Some(None),
        };
        assert!(all.matches(&quad));
    }

    #[test]
    fn test_quad_pattern_predicate_mismatch() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p1").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("v"));
        let quad = Quad::from_triple(Triple::new(subj, pred, obj));

        let other_pred = RdfPredicate::new("http://example.org/p2").unwrap();
        let pattern = QuadPattern {
            subject: None,
            predicate: Some(other_pred),
            object: None,
            graph: None,
        };
        assert!(!pattern.matches(&quad));
    }

    #[test]
    fn test_quad_pattern_object_mismatch() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("val1"));
        let quad = Quad::from_triple(Triple::new(subj, pred, obj));

        let other_obj = RdfObject::from(Literal::new_simple_literal("val2"));
        let pattern = QuadPattern {
            subject: None,
            predicate: None,
            object: Some(other_obj),
            graph: None,
        };
        assert!(!pattern.matches(&quad));
    }

    #[test]
    fn test_named_node_equality() {
        let n1 = NamedNode::new("http://example.org/same").unwrap();
        let n2 = NamedNode::new("http://example.org/same").unwrap();
        let n3 = NamedNode::new("http://example.org/different").unwrap();
        assert_eq!(n1, n2);
        assert_ne!(n1, n3);
    }

    #[test]
    fn test_blank_node_clone_and_hash() {
        let bn = BlankNode::from_str("b1").unwrap();
        let bn_clone = bn.clone();
        assert_eq!(bn, bn_clone);

        // Hash test - same blank node should have same hash
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(bn.clone());
        assert!(set.contains(&bn_clone));
    }

    #[test]
    fn test_literal_clone_and_eq() {
        let lit1 = Literal::new_simple_literal("same");
        let lit2 = lit1.clone();
        assert_eq!(lit1, lit2);
    }

    #[test]
    fn test_quad_as_triple_preserves_content() {
        let subj = RdfSubject::from(NamedNode::new("http://example.org/s").unwrap());
        let pred = RdfPredicate::new("http://example.org/p").unwrap();
        let obj = RdfObject::from(Literal::new_simple_literal("val"));
        let graph = NamedNode::new("http://example.org/graph").unwrap();
        let quad = Quad::new(subj.clone(), pred.clone(), obj.clone(), Some(graph));

        let triple = quad.as_triple();
        assert_eq!(triple.subject, subj);
        assert_eq!(triple.predicate, pred);
        assert_eq!(triple.object, obj);
    }
}
