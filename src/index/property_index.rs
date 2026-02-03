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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_index_ops() {
        let mut index = PropertyIndex::new();
        let n1 = NodeId::new(1);
        let n2 = NodeId::new(2);
        let val = PropertyValue::Integer(100);

        // Insert
        index.insert(val.clone(), n1);
        index.insert(val.clone(), n2);

        // Get
        let results = index.get(&val);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&n1));
        assert!(results.contains(&n2));

        // Remove
        index.remove(&val, n1);
        let results = index.get(&val);
        assert_eq!(results.len(), 1);
        assert!(results.contains(&n2));
    }

    #[test]
    fn test_property_index_range() {
        let mut index = PropertyIndex::new();
        for i in 1..=10 {
            index.insert(PropertyValue::Integer(i), NodeId::new(i as u64));
        }

        // Range 3..=7
        use std::ops::Bound;
        let range = (Bound::Included(PropertyValue::Integer(3)), Bound::Included(PropertyValue::Integer(7)));
        let results = index.range(range);
        
        assert_eq!(results.len(), 5); // 3, 4, 5, 6, 7
        for i in 3..=7 {
            assert!(results.contains(&NodeId::new(i)));
        }
    }
}
