//! Columnar storage implementation for node and edge properties.
//!
//! Provides high-performance, cache-friendly property access by storing 
//! property values in contiguous arrays rather than individual objects.

use crate::graph::PropertyValue;
use crate::graph::types::NodeId;
use std::collections::HashMap;

/// A single property column.
#[derive(Debug, Clone)]
pub enum Column {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    String(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
}

impl Column {
    pub fn new_int() -> Self { Column::Int(Vec::new()) }
    pub fn new_float() -> Self { Column::Float(Vec::new()) }
    pub fn new_string() -> Self { Column::String(Vec::new()) }
    pub fn new_bool() -> Self { Column::Bool(Vec::new()) }

    pub fn set(&mut self, idx: usize, value: PropertyValue) {
        match (self, value) {
            (Column::Int(v), PropertyValue::Integer(val)) => {
                if idx >= v.len() { v.resize(idx + 1, None); }
                v[idx] = Some(val);
            }
            (Column::Float(v), PropertyValue::Float(val)) => {
                if idx >= v.len() { v.resize(idx + 1, None); }
                v[idx] = Some(val);
            }
            (Column::String(v), PropertyValue::String(val)) => {
                if idx >= v.len() { v.resize(idx + 1, None); }
                v[idx] = Some(val);
            }
            (Column::Bool(v), PropertyValue::Boolean(val)) => {
                if idx >= v.len() { v.resize(idx + 1, None); }
                v[idx] = Some(val);
            }
            _ => {
                // Type mismatch or unsupported columnar type (Map/Array/Vector)
                // In a production system, we might handle promotion or error
            }
        }
    }

    pub fn get(&self, idx: usize) -> PropertyValue {
        match self {
            Column::Int(v) => v.get(idx).and_then(|&o| o).map(PropertyValue::Integer).unwrap_or(PropertyValue::Null),
            Column::Float(v) => v.get(idx).and_then(|&o| o).map(PropertyValue::Float).unwrap_or(PropertyValue::Null),
            Column::Bool(v) => v.get(idx).and_then(|&o| o).map(PropertyValue::Boolean).unwrap_or(PropertyValue::Null),
            Column::String(v) => v.get(idx).and_then(|o| o.as_ref()).map(|s| PropertyValue::String(s.clone())).unwrap_or(PropertyValue::Null),
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
}
