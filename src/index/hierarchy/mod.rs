//! Hierarchy indexing — OEH (Order-Embedded Hierarchy), ADR-035.
//!
//! Time, geography and ontology all carry a hierarchy, and all three reduce to the same
//! object: a **subsumption poset** with two dominant operations — order testing (`is x
//! under y?`) and hierarchical roll-up (aggregate a measure over everything under `y`).
//! This module implements a single index that answers both, selecting its encoding from a
//! cheap structural probe, with the roll-up answered *from* the index.
//!
//! Module map:
//!
//! | Module | Role |
//! |---|---|
//! | [`poset`] | the subsumption poset built from graph edges; acyclicity, topo orders, probe input |
//! | [`oracle`] | brute-force ground truth every encoding is validated against |
//! | [`monoid`] | roll-up monoids and the Fenwick / sparse-table range structures |
//! | [`oeh`] | the index: structural probe, nested-set and chain encodings, index-resident roll-up |
//! | [`manager`] | registry, declaration, staleness, decline diagnostics |
//!
//! See `docs/ADR/ADR-035-oeh-hierarchy-index.md` and arXiv:2606.24677.

pub mod manager;
pub mod monoid;
pub mod oeh;
pub mod oracle;
pub mod poset;

pub use manager::{
    HierarchyEntry, HierarchyIndexManager, HierarchyInfo, HierarchySpec, MeasureSpec, Unusable,
};
pub use monoid::{RollupOp, RollupValue};
pub use oeh::{Encoding, HierarchyNodeCue, OehIndex, Probe};
pub use poset::{HierarchyError, HierarchyResult, Poset};
