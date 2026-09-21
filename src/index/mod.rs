//! Property Indexing module
//!
//! Provides B-Tree indices for optimizing property lookups.

pub mod property_index;
// Not beside `hierarchy`: samyama-graph#1397 inserts `pub mod fulltext;` at
// that anchor, and two insertions at one line conflict for no reason either
// change is about.
pub mod sketch;
pub mod manager;
pub mod hierarchy;

pub use property_index::PropertyIndex;
pub use manager::{IndexManager, PropertyIndexKey};
