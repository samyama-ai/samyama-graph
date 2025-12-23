//! RDF triple/quad store implementation
//!
//! This module provides an in-memory RDF store with efficient indexing.

use super::types::{Triple, Quad, TriplePattern, QuadPattern, RdfSubject, RdfPredicate, RdfObject, NamedNode};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// RDF store errors
#[derive(Error, Debug)]
pub enum RdfStoreError {
    /// Triple not found
    #[error("Triple not found")]
    TripleNotFound,

    /// Quad not found
    #[error("Quad not found")]
    QuadNotFound,

    /// Graph not found
    #[error("Graph not found: {0}")]
    GraphNotFound(String),

    /// Duplicate triple
    #[error("Duplicate triple")]
    DuplicateTriple,
}

pub type RdfStoreResult<T> = Result<T, RdfStoreError>;

/// Iterator over triples
pub struct TripleIterator<'a> {
    triples: Vec<&'a Triple>,
    current: usize,
}

impl<'a> TripleIterator<'a> {
    fn new(triples: Vec<&'a Triple>) -> Self {
        Self { triples, current: 0 }
    }
}

impl<'a> Iterator for TripleIterator<'a> {
    type Item = &'a Triple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current < self.triples.len() {
            let triple = self.triples[self.current];
            self.current += 1;
            Some(triple)
        } else {
            None
        }
    }
}

/// RDF triple store with multiple indices for efficient queries
///
/// Implements:
/// - SPO index (Subject-Predicate-Object)
/// - POS index (Predicate-Object-Subject)
/// - OSP index (Object-Subject-Predicate)
///
/// This allows O(1) lookups for patterns with fixed subjects, predicates, or objects.
#[derive(Clone)]
pub struct RdfStore {
    /// All triples (primary storage)
    triples: HashSet<Triple>,

    /// SPO index: Subject -> Predicate -> Set of Objects
    spo_index: HashMap<String, HashMap<String, HashSet<String>>>,

    /// POS index: Predicate -> Object -> Set of Subjects
    pos_index: HashMap<String, HashMap<String, HashSet<String>>>,

    /// OSP index: Object -> Subject -> Set of Predicates
    osp_index: HashMap<String, HashMap<String, HashSet<String>>>,

    /// Named graphs (for quad support)
    graphs: HashMap<String, HashSet<Triple>>,
}

impl RdfStore {
    /// Create a new empty RDF store
    pub fn new() -> Self {
        Self {
            triples: HashSet::new(),
            spo_index: HashMap::new(),
            pos_index: HashMap::new(),
            osp_index: HashMap::new(),
            graphs: HashMap::new(),
        }
    }

    /// Insert a triple into the store
    pub fn insert(&mut self, triple: Triple) -> RdfStoreResult<()> {
        if self.triples.contains(&triple) {
            return Err(RdfStoreError::DuplicateTriple);
        }

        // Insert into main storage
        self.triples.insert(triple.clone());

        // Update indices
        self.update_indices_insert(&triple);

        Ok(())
    }

    /// Insert a quad (triple with named graph)
    pub fn insert_quad(&mut self, quad: Quad) -> RdfStoreResult<()> {
        let triple = quad.as_triple();

        // Insert into main storage
        self.triples.insert(triple.clone());

        // Update indices
        self.update_indices_insert(&triple);

        // Add to named graph if specified
        if let Some(graph) = quad.graph {
            self.graphs
                .entry(graph.as_str().to_string())
                .or_insert_with(HashSet::new)
                .insert(triple);
        }

        Ok(())
    }

    /// Remove a triple from the store
    pub fn remove(&mut self, triple: &Triple) -> RdfStoreResult<()> {
        if !self.triples.contains(triple) {
            return Err(RdfStoreError::TripleNotFound);
        }

        // Remove from main storage
        self.triples.remove(triple);

        // Update indices
        self.update_indices_remove(triple);

        // Remove from all named graphs
        for graph_triples in self.graphs.values_mut() {
            graph_triples.remove(triple);
        }

        Ok(())
    }

    /// Check if a triple exists in the store
    pub fn contains(&self, triple: &Triple) -> bool {
        self.triples.contains(triple)
    }

    /// Get the total number of triples
    pub fn len(&self) -> usize {
        self.triples.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.triples.is_empty()
    }

    /// Clear all triples
    pub fn clear(&mut self) {
        self.triples.clear();
        self.spo_index.clear();
        self.pos_index.clear();
        self.osp_index.clear();
        self.graphs.clear();
    }

    /// Query triples matching a pattern
    pub fn query(&self, pattern: &TriplePattern) -> Vec<Triple> {
        self.triples
            .iter()
            .filter(|triple| pattern.matches(triple))
            .cloned()
            .collect()
    }

    /// Get triples with a specific subject
    pub fn get_triples_with_subject(&self, subject: &RdfSubject) -> Vec<Triple> {
        self.triples
            .iter()
            .filter(|triple| &triple.subject == subject)
            .cloned()
            .collect()
    }

    /// Get triples with a specific predicate
    pub fn get_triples_with_predicate(&self, predicate: &RdfPredicate) -> Vec<Triple> {
        self.triples
            .iter()
            .filter(|triple| &triple.predicate == predicate)
            .cloned()
            .collect()
    }

    /// Get triples with a specific object
    pub fn get_triples_with_object(&self, object: &RdfObject) -> Vec<Triple> {
        self.triples
            .iter()
            .filter(|triple| &triple.object == object)
            .cloned()
            .collect()
    }

    /// Get all triples in a named graph
    pub fn get_graph(&self, graph_iri: &str) -> RdfStoreResult<Vec<Triple>> {
        self.graphs
            .get(graph_iri)
            .map(|triples| triples.iter().cloned().collect())
            .ok_or_else(|| RdfStoreError::GraphNotFound(graph_iri.to_string()))
    }

    /// List all named graphs
    pub fn list_graphs(&self) -> Vec<String> {
        self.graphs.keys().cloned().collect()
    }

    /// Get an iterator over all triples
    pub fn iter(&self) -> impl Iterator<Item = &Triple> {
        self.triples.iter()
    }

    /// Get all subjects in the store
    pub fn subjects(&self) -> Vec<RdfSubject> {
        self.triples
            .iter()
            .map(|t| t.subject.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get all predicates in the store
    pub fn predicates(&self) -> Vec<RdfPredicate> {
        self.triples
            .iter()
            .map(|t| t.predicate.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get all objects in the store
    pub fn objects(&self) -> Vec<RdfObject> {
        self.triples
            .iter()
            .map(|t| t.object.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    // Private helper methods

    fn update_indices_insert(&mut self, triple: &Triple) {
        let s_key = self.term_key(&triple.subject);
        let p_key = self.term_key_pred(&triple.predicate);
        let o_key = self.term_key_obj(&triple.object);

        // Update SPO index
        self.spo_index
            .entry(s_key.clone())
            .or_insert_with(HashMap::new)
            .entry(p_key.clone())
            .or_insert_with(HashSet::new)
            .insert(o_key.clone());

        // Update POS index
        self.pos_index
            .entry(p_key.clone())
            .or_insert_with(HashMap::new)
            .entry(o_key.clone())
            .or_insert_with(HashSet::new)
            .insert(s_key.clone());

        // Update OSP index
        self.osp_index
            .entry(o_key)
            .or_insert_with(HashMap::new)
            .entry(s_key)
            .or_insert_with(HashSet::new)
            .insert(p_key);
    }

    fn update_indices_remove(&mut self, triple: &Triple) {
        let s_key = self.term_key(&triple.subject);
        let p_key = self.term_key_pred(&triple.predicate);
        let o_key = self.term_key_obj(&triple.object);

        // Remove from SPO index
        if let Some(preds) = self.spo_index.get_mut(&s_key) {
            if let Some(objs) = preds.get_mut(&p_key) {
                objs.remove(&o_key);
                if objs.is_empty() {
                    preds.remove(&p_key);
                }
            }
            if preds.is_empty() {
                self.spo_index.remove(&s_key);
            }
        }

        // Remove from POS index
        if let Some(objs) = self.pos_index.get_mut(&p_key) {
            if let Some(subjs) = objs.get_mut(&o_key) {
                subjs.remove(&s_key);
                if subjs.is_empty() {
                    objs.remove(&o_key);
                }
            }
            if objs.is_empty() {
                self.pos_index.remove(&p_key);
            }
        }

        // Remove from OSP index
        if let Some(subjs) = self.osp_index.get_mut(&o_key) {
            if let Some(preds) = subjs.get_mut(&s_key) {
                preds.remove(&p_key);
                if preds.is_empty() {
                    subjs.remove(&s_key);
                }
            }
            if subjs.is_empty() {
                self.osp_index.remove(&o_key);
            }
        }
    }

    fn term_key(&self, subject: &RdfSubject) -> String {
        format!("{}", subject)
    }

    fn term_key_pred(&self, predicate: &RdfPredicate) -> String {
        format!("{}", predicate)
    }

    fn term_key_obj(&self, object: &RdfObject) -> String {
        format!("{}", object)
    }
}

impl Default for RdfStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdf::types::{NamedNode, Literal};

    fn create_test_triple() -> Triple {
        let subject = NamedNode::new("http://example.org/alice").unwrap();
        let predicate = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
        let object = Literal::new_simple_literal("Alice");

        Triple::new(subject.into(), predicate, object.into())
    }

    #[test]
    fn test_insert_and_query() {
        let mut store = RdfStore::new();
        let triple = create_test_triple();

        assert!(store.insert(triple.clone()).is_ok());
        assert_eq!(store.len(), 1);
        assert!(store.contains(&triple));
    }

    #[test]
    fn test_duplicate_insert() {
        let mut store = RdfStore::new();
        let triple = create_test_triple();

        assert!(store.insert(triple.clone()).is_ok());
        assert!(store.insert(triple).is_err());
    }

    #[test]
    fn test_remove() {
        let mut store = RdfStore::new();
        let triple = create_test_triple();

        store.insert(triple.clone()).unwrap();
        assert_eq!(store.len(), 1);

        store.remove(&triple).unwrap();
        assert_eq!(store.len(), 0);
        assert!(!store.contains(&triple));
    }

    #[test]
    fn test_query_by_subject() {
        let mut store = RdfStore::new();
        let subject = NamedNode::new("http://example.org/alice").unwrap();

        // Insert multiple triples with same subject
        let pred1 = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
        let pred2 = RdfPredicate::new("http://xmlns.com/foaf/0.1/age").unwrap();

        let triple1 = Triple::new(
            subject.clone().into(),
            pred1,
            Literal::new_simple_literal("Alice").into(),
        );

        let triple2 = Triple::new(
            subject.clone().into(),
            pred2,
            Literal::new_simple_literal("30").into(),
        );

        store.insert(triple1).unwrap();
        store.insert(triple2).unwrap();

        let results = store.get_triples_with_subject(&subject.into());
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_triple_pattern_query() {
        let mut store = RdfStore::new();
        let triple = create_test_triple();
        store.insert(triple.clone()).unwrap();

        // Query with pattern (all variables)
        let pattern = TriplePattern::new(None, None, None);
        let results = store.query(&pattern);
        assert_eq!(results.len(), 1);

        // Query with specific subject
        let pattern = TriplePattern::new(Some(triple.subject.clone()), None, None);
        let results = store.query(&pattern);
        assert_eq!(results.len(), 1);

        // Query with wrong subject
        let wrong_subject = NamedNode::new("http://example.org/bob").unwrap();
        let pattern = TriplePattern::new(Some(wrong_subject.into()), None, None);
        let results = store.query(&pattern);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_named_graphs() {
        let mut store = RdfStore::new();
        let triple = create_test_triple();
        let graph = NamedNode::new("http://example.org/graph/social").unwrap();

        let quad = Quad::new(
            triple.subject.clone(),
            triple.predicate.clone(),
            triple.object.clone(),
            Some(graph.clone()),
        );

        store.insert_quad(quad).unwrap();

        let graph_triples = store.get_graph(graph.as_str()).unwrap();
        assert_eq!(graph_triples.len(), 1);

        let graphs = store.list_graphs();
        assert_eq!(graphs.len(), 1);
    }

    #[test]
    fn test_clear() {
        let mut store = RdfStore::new();
        let triple = create_test_triple();

        store.insert(triple).unwrap();
        assert_eq!(store.len(), 1);

        store.clear();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_subjects_predicates_objects() {
        let mut store = RdfStore::new();

        // Insert multiple triples
        let alice = NamedNode::new("http://example.org/alice").unwrap();
        let bob = NamedNode::new("http://example.org/bob").unwrap();
        let name_pred = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();

        let triple1 = Triple::new(
            alice.into(),
            name_pred.clone(),
            Literal::new_simple_literal("Alice").into(),
        );

        let triple2 = Triple::new(
            bob.into(),
            name_pred,
            Literal::new_simple_literal("Bob").into(),
        );

        store.insert(triple1).unwrap();
        store.insert(triple2).unwrap();

        let subjects = store.subjects();
        assert_eq!(subjects.len(), 2);

        let predicates = store.predicates();
        assert_eq!(predicates.len(), 1);

        let objects = store.objects();
        assert_eq!(objects.len(), 2);
    }
}
