//! Columnar storage implementation for node and edge properties.
//!
//! Sparse HashMap-based columns: only entries that exist are stored.
//! No memory waste for multi-label graphs where properties differ per label.
//! E.g., "title" only stored for Article nodes, not for Author/MeSH/Chemical nodes.

use crate::graph::PropertyValue;
use crate::graph::types::NodeId;
use std::collections::HashMap;

/// A single property column — sparse HashMap indexed by node/edge index.
/// Only entries that are explicitly set consume memory.
#[derive(Debug, Clone)]
pub enum Column {
    Int(HashMap<usize, i64>),
    Float(HashMap<usize, f64>),
    String(HashMap<usize, String>),
    Bool(HashMap<usize, bool>),
}

impl Column {
    pub fn new_int() -> Self { Column::Int(HashMap::new()) }
    pub fn new_float() -> Self { Column::Float(HashMap::new()) }
    pub fn new_string() -> Self { Column::String(HashMap::new()) }
    pub fn new_bool() -> Self { Column::Bool(HashMap::new()) }

    pub fn set(&mut self, idx: usize, value: PropertyValue) {
        match (self, value) {
            (Column::Int(m), PropertyValue::Integer(val)) => { m.insert(idx, val); }
            (Column::Float(m), PropertyValue::Float(val)) => { m.insert(idx, val); }
            (Column::String(m), PropertyValue::String(val)) => { m.insert(idx, val); }
            (Column::Bool(m), PropertyValue::Boolean(val)) => { m.insert(idx, val); }
            _ => {
                // Type mismatch or unsupported columnar type (Map/Array/Vector)
            }
        }
    }

    pub fn get(&self, idx: usize) -> PropertyValue {
        match self {
            Column::Int(m) => m.get(&idx).map(|&v| PropertyValue::Integer(v)).unwrap_or(PropertyValue::Null),
            Column::Float(m) => m.get(&idx).map(|&v| PropertyValue::Float(v)).unwrap_or(PropertyValue::Null),
            Column::Bool(m) => m.get(&idx).map(|&v| PropertyValue::Boolean(v)).unwrap_or(PropertyValue::Null),
            Column::String(m) => m.get(&idx).map(|s| PropertyValue::String(s.clone())).unwrap_or(PropertyValue::Null),
        }
    }

    /// Check if a value exists at the given index.
    pub fn has(&self, idx: usize) -> bool {
        match self {
            Column::Int(m) => m.contains_key(&idx),
            Column::Float(m) => m.contains_key(&idx),
            Column::String(m) => m.contains_key(&idx),
            Column::Bool(m) => m.contains_key(&idx),
        }
    }

    /// Number of entries in this column.
    pub fn len(&self) -> usize {
        match self {
            Column::Int(m) => m.len(),
            Column::Float(m) => m.len(),
            Column::String(m) => m.len(),
            Column::Bool(m) => m.len(),
        }
    }
}

/// Manages multiple property columns.
#[derive(Debug, Default, Clone)]
pub struct ColumnStore {
    /// Mapping from property key -> Column
    columns: HashMap<String, Column>,
}

impl ColumnStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_property(&mut self, idx: usize, key: &str, value: PropertyValue) {
        if let Some(col) = self.columns.get_mut(key) {
            col.set(idx, value);
        } else {
            // Create new column based on type
            let mut col = match value {
                PropertyValue::Integer(_) => Column::new_int(),
                PropertyValue::Float(_) => Column::new_float(),
                PropertyValue::String(_) => Column::new_string(),
                PropertyValue::Boolean(_) => Column::new_bool(),
                _ => return, // Don't index complex types in columns for now
            };
            col.set(idx, value);
            self.columns.insert(key.to_string(), col);
        }
    }

    pub fn get_property(&self, idx: usize, key: &str) -> PropertyValue {
        self.columns.get(key).map(|col| col.get(idx)).unwrap_or(PropertyValue::Null)
    }

    /// Optimized batch read for a single property
    pub fn get_column(&self, key: &str) -> Option<&Column> {
        self.columns.get(key)
    }

    /// Get all property keys that have a non-null value for a given node index.
    /// Used by `keys()` function to discover column-store-only properties.
    pub fn get_property_keys(&self, idx: usize) -> Vec<String> {
        self.columns.iter()
            .filter(|(_, col)| col.has(idx))
            .map(|(key, _)| key.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparse_column_string() {
        let mut col = Column::new_string();
        // Set at index 1_000_000 — should NOT allocate 1M entries
        col.set(1_000_000, PropertyValue::String("hello".to_string()));
        assert_eq!(col.get(1_000_000), PropertyValue::String("hello".to_string()));
        assert_eq!(col.get(0), PropertyValue::Null);
        assert_eq!(col.get(999_999), PropertyValue::Null);
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_sparse_column_int() {
        let mut col = Column::new_int();
        col.set(42, PropertyValue::Integer(100));
        col.set(99, PropertyValue::Integer(200));
        assert_eq!(col.get(42), PropertyValue::Integer(100));
        assert_eq!(col.get(99), PropertyValue::Integer(200));
        assert_eq!(col.get(50), PropertyValue::Null);
        assert_eq!(col.len(), 2);
    }

    #[test]
    fn test_column_store_sparse() {
        let mut store = ColumnStore::new();
        // Set property at high index — should not waste memory
        store.set_property(10_000_000, "name", PropertyValue::String("test".to_string()));
        assert_eq!(
            store.get_property(10_000_000, "name"),
            PropertyValue::String("test".to_string())
        );
        assert_eq!(store.get_property(0, "name"), PropertyValue::Null);
        assert_eq!(store.get_property(10_000_000, "other"), PropertyValue::Null);
    }

    #[test]
    fn test_get_property_keys() {
        let mut store = ColumnStore::new();
        store.set_property(5, "name", PropertyValue::String("Alice".to_string()));
        store.set_property(5, "age", PropertyValue::Integer(30));
        store.set_property(10, "name", PropertyValue::String("Bob".to_string()));

        let keys5 = store.get_property_keys(5);
        assert!(keys5.contains(&"name".to_string()));
        assert!(keys5.contains(&"age".to_string()));
        assert_eq!(keys5.len(), 2);

        let keys10 = store.get_property_keys(10);
        assert_eq!(keys10, vec!["name".to_string()]);

        let keys99 = store.get_property_keys(99);
        assert!(keys99.is_empty());
    }
}
