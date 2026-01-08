//! Manager for property indices
//!
//! Handles creation, deletion, and access to property indices.

use crate::graph::{Label, NodeId, PropertyValue};
use super::property_index::PropertyIndex;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Key for identifying a property index
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PropertyIndexKey {
    pub label: Label,
    pub property: String,
}

/// Manager for all property indices
#[derive(Debug)]
pub struct IndexManager {
    indices: RwLock<HashMap<PropertyIndexKey, Arc<RwLock<PropertyIndex>>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indices: RwLock::new(HashMap::new()),
        }
    }

    /// Create an index for a label and property
    pub fn create_index(&self, label: Label, property: String) {
        let key = PropertyIndexKey { label, property };
        let mut indices = self.indices.write().unwrap();
        indices.entry(key).or_insert_with(|| Arc::new(RwLock::new(PropertyIndex::new())));
    }

    /// Drop an index
    pub fn drop_index(&self, label: &Label, property: &str) {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        let mut indices = self.indices.write().unwrap();
        indices.remove(&key);
    }

    /// Update index when a node property is set/changed
    pub fn index_insert(&self, label: &Label, property: &str, value: PropertyValue, node_id: NodeId) {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        let indices = self.indices.read().unwrap();
        if let Some(index) = indices.get(&key) {
            index.write().unwrap().insert(value, node_id);
        }
    }

    /// Update index when a node property is removed (or old value replaced)
    pub fn index_remove(&self, label: &Label, property: &str, value: &PropertyValue, node_id: NodeId) {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        let indices = self.indices.read().unwrap();
        if let Some(index) = indices.get(&key) {
            index.write().unwrap().remove(value, node_id);
        }
    }

    /// Check if an index exists
    pub fn has_index(&self, label: &Label, property: &str) -> bool {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        self.indices.read().unwrap().contains_key(&key)
    }

    /// Get index for querying
    pub fn get_index(&self, label: &Label, property: &str) -> Option<Arc<RwLock<PropertyIndex>>> {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        self.indices.read().unwrap().get(&key).cloned()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}
