//! Columnar storage implementation for node and edge properties.
//!
//! Sparse hash-map-based columns: only entries that exist are stored.
//! No memory waste for multi-label graphs where properties differ per label.
//! E.g., "title" only stored for Article nodes, not for Author/MeSH/Chemical nodes.
//!
//! # Cost of a read, and what it is not
//!
//! A property read is **two hash lookups**: the property name to a column, then
//! the row index to a value. Measured over a million integer reads in id order
//! (#531):
//!
//! | access | ns |
//! |---|---:|
//! | `get_property` with `std::collections::HashMap` | 222.4 |
//! | `get_property` with `FxHashMap` (this) | see the bench |
//! | a dense `Vec<i64>` index | 1.3 |
//!
//! The hash function is the part this module can cheaply control, and that is
//! what `FxHashMap` addresses here — `std`'s `SipHash` is a DoS-resistant hash
//! being asked to hash a `usize`, which is not the threat model of a row index.
//!
//! **Most of the remaining cost is not the hash.** At a million rows the inner
//! table is tens of megabytes and the hash scatters access, so a scan in id
//! order — the friendliest order there is — still misses cache on nearly every
//! row. Fixing that means a dense array with a presence bitmap for the columns
//! that are dense enough to deserve one, which is #531's Phase 2 and a real
//! design task: the sparsity argument above is genuine, and a dense array over
//! a rare property in a 66M-node graph would be worse than what is here.
//!
//! So: this is the cheap half, it is measured, and it does not close #531.

use crate::graph::PropertyValue;
use crate::graph::types::NodeId;
use rustc_hash::FxHashMap;
use std::collections::HashMap;

/// A single property column — sparse map indexed by node/edge index.
/// Only entries that are explicitly set consume memory.
///
/// Keyed with `FxHashMap`: the key is a row index, and `std`'s `SipHash` costs
/// more to compute than the lookup it protects (#531).
#[derive(Debug, Clone)]
pub enum Column {
    Int(FxHashMap<usize, i64>),
    Float(FxHashMap<usize, f64>),
    String(FxHashMap<usize, String>),
    Bool(FxHashMap<usize, bool>),
}

impl Column {
    pub fn new_int() -> Self { Column::Int(FxHashMap::default()) }
    pub fn new_float() -> Self { Column::Float(FxHashMap::default()) }
    pub fn new_string() -> Self { Column::String(FxHashMap::default()) }
    pub fn new_bool() -> Self { Column::Bool(FxHashMap::default()) }

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

    /// Remove this row's value from the column, if present.
    pub fn remove(&mut self, idx: usize) {
        match self {
            Column::Int(m) => { m.remove(&idx); }
            Column::Float(m) => { m.remove(&idx); }
            Column::String(m) => { m.remove(&idx); }
            Column::Bool(m) => { m.remove(&idx); }
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
    /// Mapping from property key -> Column.
    ///
    /// Also `FxHashMap`: property names are short, the set of them is small
    /// and fixed after load, and this lookup happens on every property read.
    columns: FxHashMap<String, Column>,
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

    /// Drop every property stored for one row.
    ///
    /// Node ids are recycled through a free list, so a deleted node's slot is handed to the
    /// next `create_node`. Without this the new node inherits whatever the previous
    /// occupant left in each column — values from deleted data reappearing on new data
    /// (#364). Deletion has to clear the columns as well as the sparse map.
    pub fn clear_row(&mut self, idx: usize) {
        for col in self.columns.values_mut() {
            col.remove(idx);
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
