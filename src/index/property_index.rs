//! B-Tree based property index for fast lookups
//!
//! Implements REQ-OPT-001: Property Indices

use crate::graph::{NodeId, PropertyValue};
use std::collections::{BTreeMap, HashSet};

/// Index for a specific property on a specific label
#[derive(Debug, Clone)]
pub struct PropertyIndex {
    /// Value -> Set of NodeIds
    index: BTreeMap<PropertyValue, HashSet<NodeId>>,
}

impl PropertyIndex {
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, value: PropertyValue, node_id: NodeId) {
        self.index.entry(value).or_default().insert(node_id);
    }

    pub fn remove(&mut self, value: &PropertyValue, node_id: NodeId) {
        if let Some(nodes) = self.index.get_mut(value) {
            nodes.remove(&node_id);
            if nodes.is_empty() {
                self.index.remove(value);
            }
        }
    }

    pub fn get(&self, value: &PropertyValue) -> Vec<NodeId> {
        self.index.get(value)
            .map(|nodes| nodes.iter().cloned().collect())
            .unwrap_or_default()
    }
    
    pub fn range<R>(&self, range: R) -> Vec<NodeId>
    where
        R: std::ops::RangeBounds<PropertyValue>,
    {
        let mut result = Vec::new();
        for (_, nodes) in self.index.range(range) {
            result.extend(nodes.iter().cloned());
        }
        result
    }
}

impl Default for PropertyIndex {
    fn default() -> Self {
        Self::new()
    }
}
