//! Property Indexing module
//!
//! Provides B-Tree indices for optimizing property lookups.

pub mod property_index;
pub mod manager;
pub mod sketch;
pub mod hierarchy;

pub use property_index::PropertyIndex;
pub use manager::{IndexManager, PropertyIndexKey};
