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

    /// Exact number of nodes an indexed equality would return, if such an index exists.
    ///
    /// Lets the planner cost an anchored lookup from the index itself instead of a
    /// selectivity estimate that may be a placeholder (#303).
    pub fn indexed_equality_count(
        &self,
        label: &Label,
        property: &str,
        value: &PropertyValue,
    ) -> Option<usize> {
        let index = self.get_index(label, property)?;
        let count = index.read().unwrap().count(value);
        Some(count)
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

    /// Are there any unique constraints at all?
    ///
    /// The write path checks this first so that graphs without constraints -- the common
    /// case, and every bulk load -- pay only one lock-free-ish map read per property set
    /// rather than a per-label lookup.
    pub fn has_any_unique_constraints(&self) -> bool {
        !self.unique_constraints.read().unwrap().is_empty()
    }

    /// The node currently holding `value` for a constrained `label`.`property`, if any.
    ///
    /// Distinct from `check_unique_constraint`, which cannot tell "another node holds this"
    /// from "this same node already holds this" -- the latter must stay legal so that
    /// re-setting a property to its current value is not a violation.
    pub fn unique_constraint_holder(
        &self,
        label: &Label,
        property: &str,
        value: &PropertyValue,
    ) -> Option<NodeId> {
        let key = PropertyIndexKey {
            label: label.clone(),
            property: property.to_string(),
        };
        let constraints = self.unique_constraints.read().unwrap();
        let index = constraints.get(&key)?;
        let holders = index.read().unwrap().get(value);
        holders.first().copied()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_manager_new() {
        let mgr = IndexManager::new();
        assert!(mgr.list_indexes().is_empty());
    }

    #[test]
    fn test_index_manager_default() {
        let mgr = IndexManager::default();
        assert!(mgr.list_indexes().is_empty());
    }

    #[test]
    fn test_create_and_has_index() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());
        assert!(mgr.has_index(&label, "name"));
        assert!(!mgr.has_index(&label, "age"));
    }

    #[test]
    fn test_drop_index() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());
        assert!(mgr.has_index(&label, "name"));

        mgr.drop_index(&label, "name");
        assert!(!mgr.has_index(&label, "name"));
    }

    #[test]
    fn test_get_index() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());

        let index = mgr.get_index(&label, "name");
        assert!(index.is_some());

        let no_index = mgr.get_index(&label, "missing");
        assert!(no_index.is_none());
    }

    #[test]
    fn test_index_insert_and_lookup() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());

        mgr.index_insert(&label, "name", PropertyValue::String("Alice".to_string()), NodeId::new(1));
        mgr.index_insert(&label, "name", PropertyValue::String("Bob".to_string()), NodeId::new(2));

        let idx = mgr.get_index(&label, "name").unwrap();
        let idx_guard = idx.read().unwrap();
        let results = idx_guard.get(&PropertyValue::String("Alice".to_string()));
        assert!(results.contains(&NodeId::new(1)));
    }

    #[test]
    fn test_index_remove() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());

        mgr.index_insert(&label, "name", PropertyValue::String("Alice".to_string()), NodeId::new(1));
        mgr.index_remove(&label, "name", &PropertyValue::String("Alice".to_string()), NodeId::new(1));

        let idx = mgr.get_index(&label, "name").unwrap();
        let idx_guard = idx.read().unwrap();
        let results = idx_guard.get(&PropertyValue::String("Alice".to_string()));
        assert!(results.is_empty());
    }

    #[test]
    fn test_list_indexes() {
        let mgr = IndexManager::new();
        mgr.create_index(Label::new("Person"), "name".to_string());
        mgr.create_index(Label::new("Person"), "age".to_string());
        mgr.create_index(Label::new("Company"), "name".to_string());

        let indexes = mgr.list_indexes();
        assert_eq!(indexes.len(), 3);
    }

    #[test]
    fn test_unique_constraint() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_unique_constraint(label.clone(), "email".to_string());
        assert!(mgr.has_unique_constraint(&label, "email"));
        assert!(!mgr.has_unique_constraint(&label, "name"));
        // Also creates a regular index
        assert!(mgr.has_index(&label, "email"));
    }

    #[test]
    fn test_check_unique_constraint() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_unique_constraint(label.clone(), "email".to_string());

        // First insert should pass
        let val = PropertyValue::String("alice@example.com".to_string());
        assert!(mgr.check_unique_constraint(&label, "email", &val).is_ok());

        // Insert the value
        mgr.constraint_insert(&label, "email", val.clone(), NodeId::new(1));

        // Duplicate should fail
        assert!(mgr.check_unique_constraint(&label, "email", &val).is_err());

        // Different value should pass
        let val2 = PropertyValue::String("bob@example.com".to_string());
        assert!(mgr.check_unique_constraint(&label, "email", &val2).is_ok());
    }

    #[test]
    fn test_list_constraints() {
        let mgr = IndexManager::new();
        mgr.create_unique_constraint(Label::new("Person"), "email".to_string());
        mgr.create_unique_constraint(Label::new("Company"), "name".to_string());

        let constraints = mgr.list_constraints();
        assert_eq!(constraints.len(), 2);
    }

    #[test]
    fn test_composite_index() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_composite_index(label.clone(), vec!["name".to_string(), "age".to_string()]);

        assert!(mgr.has_index(&label, "name"));
        assert!(mgr.has_index(&label, "age"));
    }

    #[test]
    fn test_get_indexed_properties() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());
        mgr.create_index(label.clone(), "age".to_string());
        mgr.create_index(Label::new("Company"), "name".to_string());

        let props = mgr.get_indexed_properties(&label);
        assert_eq!(props.len(), 2);
        assert!(props.contains(&"name".to_string()));
        assert!(props.contains(&"age".to_string()));
    }

    // ========== Coverage batch: additional IndexManager tests ==========

    #[test]
    fn test_create_index_idempotent() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());
        mgr.create_index(label.clone(), "name".to_string()); // Duplicate
        let indexes = mgr.list_indexes();
        let count = indexes.iter().filter(|(l, p)| l == &label && p == "name").count();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_drop_nonexistent_index() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.drop_index(&label, "nonexistent");
        assert!(!mgr.has_index(&label, "nonexistent"));
    }

    #[test]
    fn test_index_insert_without_existing_index() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.index_insert(&label, "name", PropertyValue::String("Alice".to_string()), NodeId::new(1));
        assert!(mgr.get_index(&label, "name").is_none());
    }

    #[test]
    fn test_index_remove_without_existing_index() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.index_remove(&label, "name", &PropertyValue::String("Alice".to_string()), NodeId::new(1));
    }

    #[test]
    fn test_index_insert_multiple_values_same_key() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());

        mgr.index_insert(&label, "name", PropertyValue::String("Alice".to_string()), NodeId::new(1));
        mgr.index_insert(&label, "name", PropertyValue::String("Alice".to_string()), NodeId::new(2));

        let idx = mgr.get_index(&label, "name").unwrap();
        let idx_guard = idx.read().unwrap();
        let results = idx_guard.get(&PropertyValue::String("Alice".to_string()));
        assert!(results.contains(&NodeId::new(1)));
        assert!(results.contains(&NodeId::new(2)));
    }

    #[test]
    fn test_unique_constraint_violation_message() {
        let mgr = IndexManager::new();
        let label = Label::new("User");
        mgr.create_unique_constraint(label.clone(), "email".to_string());

        let val = PropertyValue::String("alice@test.com".to_string());
        mgr.constraint_insert(&label, "email", val.clone(), NodeId::new(1));

        let result = mgr.check_unique_constraint(&label, "email", &val);
        assert!(result.is_err());
        let err_msg = result.err().unwrap();
        assert!(err_msg.contains("Unique constraint violation"));
        assert!(err_msg.contains("User"));
        assert!(err_msg.contains("email"));
    }

    #[test]
    fn test_check_unique_constraint_no_constraint() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        let val = PropertyValue::String("anything".to_string());
        assert!(mgr.check_unique_constraint(&label, "name", &val).is_ok());
    }

    #[test]
    fn test_constraint_insert_without_constraint() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.constraint_insert(&label, "name", PropertyValue::String("test".to_string()), NodeId::new(1));
    }

    #[test]
    fn test_composite_index_creates_all() {
        let mgr = IndexManager::new();
        let label = Label::new("Product");
        mgr.create_composite_index(label.clone(), vec!["name".to_string(), "price".to_string(), "category".to_string()]);

        assert!(mgr.has_index(&label, "name"));
        assert!(mgr.has_index(&label, "price"));
        assert!(mgr.has_index(&label, "category"));
        assert_eq!(mgr.get_indexed_properties(&label).len(), 3);
    }

    #[test]
    fn test_get_indexed_properties_no_indexes() {
        let mgr = IndexManager::new();
        let label = Label::new("Empty");
        let props = mgr.get_indexed_properties(&label);
        assert!(props.is_empty());
    }

    #[test]
    fn test_get_indexed_properties_different_labels() {
        let mgr = IndexManager::new();
        mgr.create_index(Label::new("A"), "prop1".to_string());
        mgr.create_index(Label::new("B"), "prop2".to_string());

        let props_a = mgr.get_indexed_properties(&Label::new("A"));
        assert_eq!(props_a.len(), 1);
        assert!(props_a.contains(&"prop1".to_string()));

        let props_b = mgr.get_indexed_properties(&Label::new("B"));
        assert_eq!(props_b.len(), 1);
        assert!(props_b.contains(&"prop2".to_string()));
    }

    #[test]
    fn test_list_constraints_empty() {
        let mgr = IndexManager::new();
        assert!(mgr.list_constraints().is_empty());
    }

    #[test]
    fn test_has_unique_constraint_false_for_different_prop() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_unique_constraint(label.clone(), "email".to_string());
        assert!(!mgr.has_unique_constraint(&label, "name"));
        assert!(!mgr.has_unique_constraint(&Label::new("Other"), "email"));
    }

    #[test]
    fn test_unique_constraint_also_creates_regular_index() {
        let mgr = IndexManager::new();
        let label = Label::new("Account");
        mgr.create_unique_constraint(label.clone(), "username".to_string());
        assert!(mgr.has_unique_constraint(&label, "username"));
        assert!(mgr.has_index(&label, "username"));
        let indexes = mgr.list_indexes();
        assert!(indexes.iter().any(|(l, p)| l == &label && p == "username"));
    }

    #[test]
    fn test_property_index_key_equality() {
        let key1 = PropertyIndexKey {
            label: Label::new("Person"),
            property: "name".to_string(),
        };
        let key2 = PropertyIndexKey {
            label: Label::new("Person"),
            property: "name".to_string(),
        };
        let key3 = PropertyIndexKey {
            label: Label::new("Person"),
            property: "age".to_string(),
        };
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_property_index_key_hash() {
        use std::collections::HashSet;
        let key1 = PropertyIndexKey {
            label: Label::new("Person"),
            property: "name".to_string(),
        };
        let key2 = PropertyIndexKey {
            label: Label::new("Person"),
            property: "name".to_string(),
        };
        let mut set = HashSet::new();
        set.insert(key1);
        assert!(set.contains(&key2));
    }

    #[test]
    fn test_index_with_integer_values() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "age".to_string());

        mgr.index_insert(&label, "age", PropertyValue::Integer(30), NodeId::new(1));
        mgr.index_insert(&label, "age", PropertyValue::Integer(25), NodeId::new(2));

        let idx = mgr.get_index(&label, "age").unwrap();
        let idx_guard = idx.read().unwrap();
        let results = idx_guard.get(&PropertyValue::Integer(30));
        assert!(results.contains(&NodeId::new(1)));
        assert!(!results.contains(&NodeId::new(2)));
    }

    #[test]
    fn test_drop_index_then_insert() {
        let mgr = IndexManager::new();
        let label = Label::new("Person");
        mgr.create_index(label.clone(), "name".to_string());
        mgr.drop_index(&label, "name");

        mgr.index_insert(&label, "name", PropertyValue::String("Alice".to_string()), NodeId::new(1));
        assert!(mgr.get_index(&label, "name").is_none());
    }
}
