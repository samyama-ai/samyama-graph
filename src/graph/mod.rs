//! # Property Graph Model -- Core Graph Database Implementation
//!
//! This module implements the **property graph data model**, the most expressive
//! of the three major graph data models used in practice.
//!
//! ## What is a property graph?
//!
//! A property graph consists of:
//! - **Nodes** (vertices) that carry zero or more **labels** (e.g., `:Person`, `:Employee`)
//!   and a set of key-value **properties** (e.g., `name: "Alice"`, `age: 30`).
//! - **Edges** (relationships) that are **directed**, carry exactly one **type**
//!   (e.g., `:KNOWS`, `:WORKS_AT`), and may also carry properties
//!   (e.g., `since: 2020`, `strength: 0.95`).
//! - Multiple edges may exist between the same pair of nodes (a **multigraph**),
//!   even with the same type -- this is essential for modeling real-world data
//!   where, for instance, two people can have multiple distinct interactions.
//!
//! ## How does this differ from a relational database?
//!
//! In an RDBMS, relationships are represented via foreign keys and resolved at
//! query time through **JOIN operations**, which become increasingly expensive
//! as the number of hops grows (each hop is another JOIN). A property graph
//! stores relationships as first-class objects with direct pointers between
//! adjacent nodes (**index-free adjacency**), making multi-hop traversals O(k)
//! where k is the number of edges traversed -- independent of total graph size.
//!
//! ## How does this differ from RDF triple stores?
//!
//! RDF models data as (subject, predicate, object) triples -- a simpler but
//! less expressive model. RDF edges cannot carry properties (you need
//! **reification** to attach metadata to a relationship, which is verbose and
//! breaks query ergonomics). RDF also lacks the concept of node identity beyond
//! URIs, and has no native multi-label support. The property graph model trades
//! RDF's global-web-of-data orientation for richer local modeling, which is why
//! most application-facing graph databases (Neo4j, TigerGraph, Samyama) choose it.
//!
//! ## Rust patterns used in this module
//!
//! - **Newtype wrappers** ([`NodeId`], [`EdgeId`], [`Label`], [`EdgeType`]):
//!   wrap `u64` or `String` in single-field structs for compile-time type safety
//!   at zero runtime cost (see [`types`]).
//! - **Algebraic data types**: [`PropertyValue`] is a Rust `enum` (tagged union)
//!   whose variants carry different payload types, with exhaustive `match`
//!   enforced by the compiler (see [`property`]).
//! - **HashMap-based indices**: secondary indices in [`GraphStore`] map labels
//!   and edge types to entity sets for O(1) filtered lookups (see [`store`]).
//!
//! ## Requirements coverage
//!
//! - REQ-GRAPH-001 through REQ-GRAPH-006: property graph data model
//! - REQ-GRAPH-007: directed edges
//! - REQ-GRAPH-008: multiple edges between same nodes
//! - REQ-MEM-001, REQ-MEM-003: in-memory storage with optimized data structures

pub mod catalog;
pub mod edge;
pub mod node;
pub mod property;
pub mod store;
pub mod types;
pub mod event;
pub mod storage;

// Re-export main types
pub use edge::Edge;
pub use node::Node;
pub use property::{PropertyMap, PropertyValue};
pub use store::{GraphError, GraphResult, GraphStore, GraphStatistics, PropertyStats};
pub use types::{EdgeId, EdgeType, Label, NodeId};
pub use catalog::GraphCatalog;
pub use event::IndexEvent;
pub use storage::{Column, ColumnStore};
