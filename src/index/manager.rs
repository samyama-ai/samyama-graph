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
    /// Unique constraints (label, property) pairs
    unique_constraints: RwLock<HashMap<PropertyIndexKey, Arc<RwLock<PropertyIndex>>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indices: RwLock::new(HashMap::new()),
            unique_constraints: RwLock::new(HashMap::new()),
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

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<(Label, String)> {
        self.indices.read().unwrap().keys()
            .map(|k| (k.label.clone(), k.property.clone()))
            .collect()
    }

    /// Create a unique constraint (also creates an index)
    pub fn create_unique_constraint(&self, label: Label, property: String) {
        let key = PropertyIndexKey { label: label.clone(), property: property.clone() };
        let mut constraints = self.unique_constraints.write().unwrap();
        constraints.entry(key).or_insert_with(|| Arc::new(RwLock::new(PropertyIndex::new())));
        // Also create a regular index for query performance
        self.create_index(label, property);
    }

    /// Check if a unique constraint exists
    pub fn has_unique_constraint(&self, label: &Label, property: &str) -> bool {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        self.unique_constraints.read().unwrap().contains_key(&key)
    }

    /// Check unique constraint before insert. Returns Ok if unique or no constraint.
    pub fn check_unique_constraint(&self, label: &Label, property: &str, value: &PropertyValue) -> Result<(), String> {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        let constraints = self.unique_constraints.read().unwrap();
        if let Some(index) = constraints.get(&key) {
            let idx = index.read().unwrap();
            let existing = idx.get(value);
            if !existing.is_empty() {
                return Err(format!(
                    "Unique constraint violation: :{}({}) already has value {:?}",
                    label.as_str(), property, value
                ));
            }
        }
        Ok(())
    }

    /// Insert into unique constraint index
    pub fn constraint_insert(&self, label: &Label, property: &str, value: PropertyValue, node_id: NodeId) {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        let constraints = self.unique_constraints.read().unwrap();
        if let Some(index) = constraints.get(&key) {
            index.write().unwrap().insert(value, node_id);
        }
    }

    /// List all constraints
    pub fn list_constraints(&self) -> Vec<(Label, String)> {
        self.unique_constraints.read().unwrap().keys()
            .map(|k| (k.label.clone(), k.property.clone()))
            .collect()
    }

    /// Create a composite index on multiple properties (creates individual indexes for each)
    pub fn create_composite_index(&self, label: Label, properties: Vec<String>) {
        for prop in &properties {
            self.create_index(label.clone(), prop.clone());
        }
    }

    /// Get all indexed properties for a label
    pub fn get_indexed_properties(&self, label: &Label) -> Vec<String> {
        self.indices.read().unwrap().keys()
            .filter(|k| &k.label == label)
            .map(|k| k.property.clone())
            .collect()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}
