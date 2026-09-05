//! Graph events for async processing
//!
//! Captures changes to the graph for indexing, replication, etc.

use super::types::{Label, NodeId};
use super::property::{PropertyMap, PropertyValue};

#[derive(Debug, Clone)]
pub enum IndexEvent {
    NodeCreated {
        tenant_id: String,
        id: NodeId,
        labels: Vec<Label>,
        properties: PropertyMap,
    },
    NodeDeleted {
        tenant_id: String,
        id: NodeId,
        labels: Vec<Label>,
        properties: PropertyMap,
    },
    PropertySet {
        tenant_id: String,
        id: NodeId,
        labels: Vec<Label>,
        key: String,
        old_value: Option<PropertyValue>,
        new_value: PropertyValue,
    },
    LabelAdded {
        tenant_id: String,
        id: NodeId,
        label: Label,
        properties: PropertyMap,
    },
}

/// A change to the graph, recorded so that durability does not depend on what a
/// query happened to `RETURN` (#1094).
///
/// Only the id is carried. The payload is read back from the store when the
/// mutation is applied, so a node written and then updated twice in one statement
/// costs three entries and one read of the final state, and no clone of the
/// properties along the way. An `Upserted` id that no longer exists at apply time
/// was deleted later in the same statement; the `Deleted` entry that follows it
/// carries the real outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutation {
    NodeUpserted(NodeId),
    NodeDeleted(NodeId),
    EdgeUpserted(super::types::EdgeId),
    EdgeDeleted(super::types::EdgeId),
}
