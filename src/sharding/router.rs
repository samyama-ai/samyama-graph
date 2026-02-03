//! Request Router for Tenant Sharding
//!
//! Handles routing of requests to the correct Raft group based on Tenant ID.

use crate::raft::RaftNodeId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Result of a routing decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteResult {
    /// Request should be processed by the local node
    Local,
    /// Request should be forwarded to a remote node
    Remote(RaftNodeId),
}

/// Router manages the mapping between Tenants and Raft Nodes
#[derive(Clone)]
pub struct Router {
    /// ID of the local node
    local_node_id: RaftNodeId,
    /// Map of Tenant ID -> Leader Node ID
    /// In a real implementation, this would be synced via a metadata store or gossip.
    shard_map: Arc<RwLock<HashMap<String, RaftNodeId>>>,
}

impl Router {
    /// Create a new Router
    pub fn new(local_node_id: RaftNodeId) -> Self {
        Self {
            local_node_id,
            shard_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add or update a route for a tenant
    pub fn update_route(&self, tenant_id: String, leader_node_id: RaftNodeId) {
        let mut map = self.shard_map.write().unwrap();
        map.insert(tenant_id, leader_node_id);
    }

    /// Remove a route
    pub fn remove_route(&self, tenant_id: &str) {
        let mut map = self.shard_map.write().unwrap();
        map.remove(tenant_id);
    }

    /// Determine where to route a request for a given tenant
    pub fn route(&self, tenant_id: &str) -> Option<RouteResult> {
        let map = self.shard_map.read().unwrap();
        map.get(tenant_id).map(|&node_id| {
            if node_id == self.local_node_id {
                RouteResult::Local
            } else {
                RouteResult::Remote(node_id)
            }
        })
    }

    /// Get all known routes (for debugging/status)
    pub fn get_all_routes(&self) -> HashMap<String, RaftNodeId> {
        self.shard_map.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_routing() {
        let router = Router::new(1);
        router.update_route("tenant_a".to_string(), 1);
        
        match router.route("tenant_a") {
            Some(RouteResult::Local) => assert!(true),
            _ => panic!("Should route locally"),
        }
    }

    #[test]
    fn test_remote_routing() {
        let router = Router::new(1);
        router.update_route("tenant_b".to_string(), 2);
        
        match router.route("tenant_b") {
            Some(RouteResult::Remote(id)) => assert_eq!(id, 2),
            _ => panic!("Should route remotely"),
        }
    }

    #[test]
    fn test_unknown_route() {
        let router = Router::new(1);
        assert!(router.route("unknown").is_none());
    }

    #[test]
    fn test_router_ops() {
        let router = Router::new(1);
        router.update_route("t1".to_string(), 1);
        router.update_route("t2".to_string(), 2);

        let routes = router.get_all_routes();
        assert_eq!(routes.len(), 2);
        
        router.remove_route("t1");
        assert!(router.route("t1").is_none());
        assert!(router.route("t2").is_some());
    }
}
