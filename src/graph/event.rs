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
