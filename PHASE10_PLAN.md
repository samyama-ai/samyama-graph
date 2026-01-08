# Phase 10: Distributed Routing & Tenant Sharding - Implementation Plan

## Executive Summary

**Goal**: Enable horizontal scalability by distributing Tenants across multiple Raft Groups ("Shards").

**Current State**: All nodes store ALL data (Full Replication).
**Future State**: Nodes are divided into Shard Groups. Tenant A lives on Group 1, Tenant B on Group 2.

**Value**: Allows the cluster to host unlimited total data/tenants, as long as no *single* tenant exceeds node capacity.

## Architecture

### 1. Cluster Topology (Metadata)
We introduce a "Meta-Store" (conceptually similar to TiKV's PD or CockroachDB's Gossip).
*   **ShardMap**: `HashMap<TenantId, RaftGroupId>`
*   **GroupMap**: `HashMap<RaftGroupId, Vec<NodeAddr>>`

### 2. The Router (Distributor)
The `RespServer` will no longer process every request locally.
1.  **Parse**: Parse command to find Tenant context.
2.  **Lookup**: Check `ShardMap` cache.
3.  **Forward**: If local node is NOT the leader for that tenant, proxy the request to the correct leader.
4.  **Execute**: If local node IS leader, execute normally.

### 3. Implementation Steps

#### Week 1: Routing Infrastructure
*   Create `src/sharding/` module.
*   Implement `Router` struct.
*   Define `ShardMap` and basic placement logic (Round Robin).

#### Week 2: Proxy Layer
*   Implement internal RPC client (using `reqwest` or simple TCP) to forward RESP commands between nodes.
*   Update `RespServer` to use `Router`.

#### Week 3: Metadata Coordination
*   For MVP, we will use a static config or gossip to share the `ShardMap`.
*   (Full dynamic Raft-based metadata is Phase 11).

## New Components

```rust
// src/sharding/router.rs
pub struct Router {
    local_node_id: NodeId,
    shard_map: Arc<RwLock<HashMap<String, NodeId>>>, // Tenant -> LeaderNodeId
    network: ClusterNetwork,
}

impl Router {
    pub async fn route(&self, tenant: &str, command: Command) -> Result<Response> {
        let leader = self.get_leader(tenant);
        if leader == self.local_node_id {
            // Execute locally
            local_handler.execute(command)
        } else {
            // Forward
            self.network.forward(leader, command).await
        }
    }
}
```

## Risks
*   **Latency**: One extra network hop for mis-routed requests.
*   **Availability**: If the Metadata store is inconsistent, routing fails.

---
**Status**: Planned
**Version**: 1.0
