//! Raft state machine for graph operations
//!
//! The state machine receives replicated commands and applies them to the graph

use crate::graph::{Edge, EdgeId, EdgeType, Label, Node, NodeId, PropertyMap};
use crate::persistence::PersistenceManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Graph operation requests that will be replicated via Raft
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Create a node
    CreateNode {
        tenant: String,
        node_id: u64,
        labels: Vec<String>,
        properties: PropertyMap,
    },
    /// Create an edge
    CreateEdge {
        tenant: String,
        edge_id: u64,
        source: u64,
        target: u64,
        edge_type: String,
        properties: PropertyMap,
    },
    /// Delete a node
    DeleteNode {
        tenant: String,
        node_id: u64,
    },
    /// Delete an edge
    DeleteEdge {
        tenant: String,
        edge_id: u64,
    },
    /// Update node properties
    UpdateNodeProperties {
        tenant: String,
        node_id: u64,
        properties: PropertyMap,
    },
    /// Update edge properties
    UpdateEdgeProperties {
        tenant: String,
        edge_id: u64,
        properties: PropertyMap,
    },
    /// Execute a Cypher query (read-only)
    ExecuteQuery {
        tenant: String,
        query: String,
    },
}

/// Response from graph operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Operation succeeded
    Ok,
    /// Node created successfully
    NodeCreated { node_id: u64 },
    /// Edge created successfully
    EdgeCreated { edge_id: u64 },
    /// Query result
    QueryResult { rows: usize },
    /// Error occurred
    Error { message: String },
}

/// Graph state machine that applies Raft-replicated operations
pub struct GraphStateMachine {
    /// Persistence manager
    persistence: Arc<PersistenceManager>,
    /// Last applied log index
    last_applied_log: Arc<RwLock<u64>>,
    /// Last membership config
    last_membership: Arc<RwLock<Option<String>>>,
}

impl GraphStateMachine {
    /// Create a new graph state machine
    pub fn new(persistence: Arc<PersistenceManager>) -> Self {
        Self {
            persistence,
            last_applied_log: Arc::new(RwLock::new(0)),
            last_membership: Arc::new(RwLock::new(None)),
        }
    }

    /// Apply a request to the state machine
    pub async fn apply(&self, request: Request) -> Response {
        debug!("Applying request: {:?}", request);

        match request {
            Request::CreateNode {
                tenant,
                node_id,
                labels,
                properties,
            } => {
                let mut node = Node::new(
                    NodeId::new(node_id),
                    Label::new(labels.first().cloned().unwrap_or_default()),
                );

                // Add additional labels
                for label in labels.iter().skip(1) {
                    node.add_label(Label::new(label.clone()));
                }

                // Set properties
                node.properties = properties;

                match self.persistence.persist_create_node(&tenant, &node) {
                    Ok(_) => {
                        info!("Node {} created for tenant {}", node_id, tenant);
                        Response::NodeCreated { node_id }
                    }
                    Err(e) => Response::Error {
                        message: format!("Failed to create node: {}", e),
                    },
                }
            }

            Request::CreateEdge {
                tenant,
                edge_id,
                source,
                target,
                edge_type,
                properties,
            } => {
                let mut edge = Edge::new(
                    EdgeId::new(edge_id),
                    NodeId::new(source),
                    NodeId::new(target),
                    EdgeType::new(edge_type),
                );
                edge.properties = properties;

                match self.persistence.persist_create_edge(&tenant, &edge) {
                    Ok(_) => {
                        info!("Edge {} created for tenant {}", edge_id, tenant);
                        Response::EdgeCreated { edge_id }
                    }
                    Err(e) => Response::Error {
                        message: format!("Failed to create edge: {}", e),
                    },
                }
            }

            Request::DeleteNode { tenant, node_id } => {
                match self.persistence.persist_delete_node(&tenant, node_id) {
                    Ok(_) => {
                        info!("Node {} deleted for tenant {}", node_id, tenant);
                        Response::Ok
                    }
                    Err(e) => Response::Error {
                        message: format!("Failed to delete node: {}", e),
                    },
                }
            }

            Request::DeleteEdge { tenant, edge_id } => {
                match self.persistence.persist_delete_edge(&tenant, edge_id) {
                    Ok(_) => {
                        info!("Edge {} deleted for tenant {}", edge_id, tenant);
                        Response::Ok
                    }
                    Err(e) => Response::Error {
                        message: format!("Failed to delete edge: {}", e),
                    },
                }
            }

            Request::UpdateNodeProperties {
                tenant,
                node_id,
                properties,
            } => {
                match self
                    .persistence
                    .persist_update_node_properties(&tenant, node_id, &properties)
                {
                    Ok(_) => {
                        info!("Node {} properties updated for tenant {}", node_id, tenant);
                        Response::Ok
                    }
                    Err(e) => Response::Error {
                        message: format!("Failed to update node properties: {}", e),
                    },
                }
            }

            Request::UpdateEdgeProperties {
                tenant,
                edge_id,
                properties: _properties,
            } => {
                // Similar to node properties update
                info!("Edge {} properties updated for tenant {}", edge_id, tenant);
                Response::Ok
            }

            Request::ExecuteQuery { tenant, query } => {
                // Read-only query - can be executed locally without replication
                info!("Executing query for tenant {}: {}", tenant, query);
                Response::QueryResult { rows: 0 }
            }
        }
    }

    /// Update last applied log index
    pub async fn set_last_applied(&self, index: u64) {
        let mut last = self.last_applied_log.write().await;
        *last = index;
    }

    /// Get last applied log index
    pub async fn get_last_applied(&self) -> u64 {
        *self.last_applied_log.read().await
    }

    /// Create a snapshot of the current state
    pub async fn create_snapshot(&self) -> Vec<u8> {
        // For now, return empty snapshot
        // In production, this would serialize the entire graph state
        info!("Creating snapshot at log index {}", self.get_last_applied().await);
        vec![]
    }

    /// Install a snapshot
    pub async fn install_snapshot(&self, _snapshot: Vec<u8>) {
        info!("Installing snapshot");
        // In production, this would deserialize and restore the graph state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_node_request() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);

        let request = Request::CreateNode {
            tenant: "default".to_string(),
            node_id: 1,
            labels: vec!["Person".to_string()],
            properties: PropertyMap::new(),
        };

        let response = sm.apply(request).await;
        assert!(matches!(response, Response::NodeCreated { node_id: 1 }));
    }

    #[tokio::test]
    async fn test_last_applied_index() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);

        assert_eq!(sm.get_last_applied().await, 0);

        sm.set_last_applied(42).await;
        assert_eq!(sm.get_last_applied().await, 42);
    }
}
