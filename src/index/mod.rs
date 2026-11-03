//! Property Indexing module
//!
//! Provides B-Tree indices for optimizing property lookups.

pub mod property_index;
// Not beside `hierarchy`: samyama-graph#1397 inserts `pub mod fulltext;` at
// that anchor, and two insertions at one line conflict for no reason either
// change is about.
pub mod catalog;
pub mod fulltext;
pub mod hierarchy;
pub mod manager;
pub mod point;
pub mod sketch;
pub mod structure;

pub use catalog::{IndexCatalog, IndexDefinition, RestoredIndexes};
pub use manager::{IndexManager, IndexMemory, PropertyIndexKey};
pub use property_index::PropertyIndex;
