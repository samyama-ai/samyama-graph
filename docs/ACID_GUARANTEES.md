# Samyama ACID Guarantees

Samyama is designed to provide strong consistency and durability guarantees suitable for enterprise workloads. While it utilizes a distributed architecture, it adheres to ACID principles for transactional operations.

## Summary

| Property | Status | Implementation Mechanism |
|----------|:------:|--------------------------|
| **Atomicity** | ✅ Supported | RocksDB `WriteBatch` & WAL |
| **Consistency** | ✅ Supported | Schema Validation & Raft Consensus |
| **Isolation** | ⚠️ Partial | Single-node: Sequential (RwLock) + MVCC foundation. Distributed: Linearizable. *Interactive multi-statement transactions are planned.* |
| **Durability** | ✅ Supported | RocksDB Persistence + Raft Replication |

---

## Detailed Breakdown

### 1. Atomicity
**"All or Nothing"**

Samyama ensures that any graph modification command (e.g., `CREATE (a)-[:REL]->(b)`) is atomic.
*   **Mechanism**: We leverage RocksDB's `WriteBatch` capability.
*   **Example**: When creating a relationship, Samyama must update:
    1.  The Edge data.
    2.  The Source Node's outgoing adjacency list.
    3.  The Target Node's incoming adjacency list.
*   **Guarantee**: These writes happen atomically. If the server crashes mid-operation, the Write-Ahead Log (WAL) ensures that upon recovery, either all 3 updates exist, or none do. You will never encounter "dangling edges".

### 2. Consistency
**"Valid State Transitions"**

Samyama enforces constraints to ensure the graph remains in a valid state.
*   **Schema**: While Samyama is schema-flexible, internal identifiers and adjacency structures are strictly managed.
*   **Distributed Consistency**: We use the **Raft Consensus Protocol**. This ensures that all nodes in the cluster agree on the order of operations. A write is only acknowledged to the client once it has been replicated to a quorum of nodes.

### 3. Isolation
**"Concurrent Transaction Visibility"**

Samyama currently provides **Per-Operation Isolation** with MVCC foundation.
*   **Single Query**: A single Cypher query (e.g., a complex `MATCH ... DELETE`) runs in isolation. Readers will not see partial updates from a running query.
*   **Concurrency Control**: We utilize `RwLock` (Read-Write Locks) on the in-memory graph structure.
    *   **Writes**: Exclusive access. No other reads or writes can occur during a write operation.
    *   **Reads**: Shared access. Multiple readers can query simultaneously.
*   **MVCC Foundation**: Versioned nodes and edges with `get_node_at_version()` for snapshot reads. This lays the groundwork for full snapshot isolation.
*   **Limitation**: We do not yet support *Interactive Transactions* (e.g., `BEGIN` ... multiple queries ... `COMMIT`). This is on the roadmap for Phase 15.

### 4. Durability
**"Committed Data Survives"**

Once Samyama acknowledges a write, it is persisted.
*   **Disk Persistence**: Data is written to the RocksDB WAL immediately.
*   **Distributed Durability**: In a cluster, data is replicated to a majority of nodes before acknowledgement. Even if the leader node fails immediately after sending an "OK", the data is safe on the followers.

## Performance Trade-offs

To achieve these guarantees, Samyama accepts certain trade-offs:
1.  **Write Latency**: Because we wait for Raft replication and disk sync, write latency is higher than an eventual-consistency system (like Cassandra).
2.  **Throughput**: Exclusive locking for writes ensures safety but limits write concurrency compared to MVCC (Multi-Version Concurrency Control) systems.

## Comparison

| Feature | Samyama | RedisGraph | Neo4j |
|:---|:---:|:---:|:---:|
| **Storage** | RocksDB (Disk + Mem) | In-Memory | Native Disk |
| **Atomicity** | Operation-Level | Operation-Level | Full Transaction |
| **Clustering** | Raft (CP) | Master-Replica | Raft/Causal (CP/CA) |
| **Durability** | WAL + Replication | AOF/RDB | Transaction Log |
