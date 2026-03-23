# ADR-016: Billion-Node Distributed Architecture

## Status
**Proposed**

## Date
2026-03-23

## Context

Samyama currently handles ~7.7M nodes on a single Mac Mini M4 (16GB) and demonstrated a 3-node cluster with 426K nodes across AWS spot instances (2026-03-22). For production healthcare, financial, and social graph workloads, we need to scale to **1 billion+ nodes** on a small cluster (3-5 commodity nodes) without vertical scaling.

### Industry Benchmarks (2025-2026)

| System | Architecture | Scale Demonstrated | Notes |
|--------|-------------|-------------------|-------|
| TigerGraph | Native MPP, shared-nothing | 73B vertices, 534B edges (36TB raw) | Hybrid memory-disk, distributed GSQL |
| Neo4j | Leader-follower replication | ~100M per node, no distributed writes | Cannot maintain ACID on 2 nodes (Dan McCreary) |
| NebulaGraph | Shared-nothing, Raft per partition | 100B+ edges claimed | Storage-compute separation |
| Kuzu | Embedded, columnar | 280M nodes, 1.7B edges (LDBC SF100) | Single-node only, fastest embedded |
| Memgraph | In-memory, ACID | Single-node focus, replication only | No distributed writes |

**Key insight**: No open-source graph database does distributed ACID writes at billion scale. TigerGraph (proprietary) is the only system that has demonstrated this.

### Hardware Landscape (2026)

| Instance | vCPU | RAM | Network | Spot $/hr | Monthly |
|----------|------|-----|---------|-----------|---------|
| r8g.2xlarge (Graviton4) | 8 | 64 GB | 15 Gbps | ~$0.15 | ~$108 |
| r8g.4xlarge | 16 | 128 GB | 15 Gbps | ~$0.30 | ~$216 |
| r7i.4xlarge (Intel) | 16 | 128 GB | 12.5 Gbps | ~$0.35 | ~$252 |
| r8g.8xlarge | 32 | 256 GB | 15 Gbps | ~$0.60 | ~$432 |
| i4i.2xlarge (NVMe) | 8 | 64 GB | 12.5 Gbps | ~$0.20 | ~$144 |

**Target config**: 5× r8g.4xlarge = 80 vCPU, 640 GB RAM, 75 Gbps aggregate bandwidth. Cost: ~$1,080/month on spot ($36/day).

**1B node math**: 1B nodes × ~320 bytes/node (avg with properties) = ~300 GB. Fits in 640 GB with room for edges, indexes, and working memory.

## Decision

**We will implement a partition-based distributed architecture inspired by VoltDB's deterministic execution model, adapted for graph workloads with modern hardware capabilities.**

### Core Principles

1. **Partition to eliminate coordination** — Single-partition queries (90%+ of workload) execute locally with zero network hops
2. **Community-aware partitioning** — Use graph structure (WCC/METIS) to co-locate densely connected nodes, minimizing edge cuts
3. **Replicate hot boundaries** — Edges crossing partition boundaries are replicated to both sides (read-local, write-coordinate)
4. **Async persistence, sync replication** — Writes replicate to K replicas synchronously (VoltDB K-safety), persist to disk asynchronously
5. **io_uring for disk I/O** — Eliminate syscall overhead for persistence (60% improvement over epoll, proven by Neon/Tonbo in production)

### Architecture

```
                    ┌─────────────────────────┐
                    │    Query Router          │
                    │  (partition-aware plan)  │
                    └────────┬────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
     ┌────────────┐  ┌────────────┐  ┌────────────┐
     │  Node 1    │  │  Node 2    │  │  Node 3    │
     │            │  │            │  │            │
     │ Partition  │  │ Partition  │  │ Partition  │
     │  0, 3     │  │  1, 4     │  │  2, 5     │
     │            │  │            │  │            │
     │ Boundary   │  │ Boundary   │  │ Boundary   │
     │ Edge Cache │  │ Edge Cache │  │ Edge Cache │
     │            │  │            │  │            │
     │ RocksDB    │  │ RocksDB    │  │ RocksDB    │
     │ (NVMe)     │  │ (NVMe)     │  │ (NVMe)     │
     └────────────┘  └────────────┘  └────────────┘
           │               │               │
           └───────── Raft Consensus ──────┘
                  (metadata + partition map)
```

### Partitioning Strategy

**Phase 1: Hash partitioning (simple, even distribution)**
```
partition(node_id) = hash(node_id) % num_partitions
```
- Even distribution, simple routing
- Works well for label scans and property lookups
- Edge cuts are random — ~70% of edges cross partitions

**Phase 2: Community-aware partitioning (CUTTANA/METIS)**
- Use WCC to identify connected components
- Assign entire communities to the same partition
- Reduces edge cuts to ~10-20% (vs 70% with hash)
- Rebalance when communities grow unevenly
- Based on CUTTANA (VLDB 2024) — streaming partitioner that prevents premature vertex assignment

**Phase 3: Hybrid with boundary replication**
- Partition nodes by community
- Replicate boundary edges to both partitions (1-hop cache)
- 1-hop traversals from any node are always local
- 2+ hop traversals may cross partitions (scatter-gather)

### Query Execution Model

**Single-partition query** (most common):
```
MATCH (p:Person {id: 123})-[:KNOWS]->(friend)
→ Router determines: Person 123 is on Partition 2 (Node 2)
→ Execute entirely on Node 2, return result
→ Zero coordination, zero network hops
```

**Multi-partition query** (cross-community):
```
MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: 'Acme'})
→ Router determines: Company 'Acme' is on Partition 0 (Node 1)
→ Phase 1: Node 1 scans Company 'Acme', expands WORKS_AT (boundary cache)
→ Phase 2: If employees span partitions, scatter to relevant nodes
→ Phase 3: Gather results, merge, return
```

**Write coordination** (K-safety model):
```
CREATE (p:Person {name: 'Alice'})
→ Router determines: hash('Alice') → Partition 3 (Node 1)
→ Node 1 executes locally
→ Synchronous replicate to K=1 replica (Node 2)
→ Ack to client after K+1 confirmations
→ Async persist to RocksDB via io_uring
```

### Modern I/O Stack

**What changed since VoltDB (2012)**:

| 2012 (VoltDB era) | 2026 (Now) |
|---|---|
| 10 Gbps Ethernet | 100-200 Gbps (ENA, EFA) |
| Spinning disks / early SSDs | NVMe (i4i: 3.75 TB, 400K IOPS) |
| epoll / select | io_uring (60% less overhead) |
| TCP/IP networking | RDMA / kernel bypass available |
| 4-8 cores typical | 32-96 cores typical |
| 16-64 GB RAM typical | 128-768 GB RAM commodity |
| No cloud spot instances | 70-90% spot discount |

**io_uring for Samyama**:
- Replace RocksDB's default epoll-based I/O with io_uring
- `tokio-uring` or `monoio` as async runtime for disk operations
- Neon (Postgres storage) proved this works: Rust + tokio + io_uring + O_DIRECT
- 44x improvement on 4KB random reads vs standard tokio (Tonbo benchmark)

**Network**: AWS Elastic Fabric Adapter (EFA) provides kernel-bypass networking with ~2μs latency between instances in the same placement group. Not full RDMA but close enough for graph traversal scatter-gather.

### Partitioning Algorithms

**CUTTANA** (VLDB 2024):
- Streaming graph partitioner — processes vertices in one pass
- Buffering technique prevents premature partition assignment
- Coarsening + refinement for complete graph view
- Scales to billion-node web graphs
- Open source

**METIS** (classic, proven):
- Offline multilevel k-way partitioning
- Near-optimal edge cut minimization
- Minutes for billion-node graphs
- Well-understood, production-proven

**Samyama's WCC as pre-partitioner**:
- Already implemented, O(V+E)
- Identifies natural community boundaries
- WCC components become partition candidates
- Assign small components to same partition; split large components with METIS

## Implementation Plan

### Phase 1: Foundation (4 weeks)

1. **Partition metadata service** — Partition map (node → partition), stored in Raft consensus
2. **Partition-aware query router** — Extend query planner to annotate operators with partition info
3. **Hash partitioning** — Simple hash(node_id) % N for even distribution
4. **Inter-node RPC** — Arrow Flight (already in ADR-006 plan) or gRPC for scatter-gather
5. **Benchmark**: 100M nodes across 3 nodes, LDBC queries

### Phase 2: Smart Partitioning (4 weeks)

6. **WCC-based community detection** as pre-partitioner
7. **CUTTANA/METIS integration** for optimal partition assignment
8. **Boundary edge replication** — 1-hop cache at partition boundaries
9. **Partition rebalancing** — Move partitions when nodes join/leave
10. **Benchmark**: 500M nodes, measure edge-cut reduction vs hash

### Phase 3: Production Hardening (4 weeks)

11. **K-safety replication** (VoltDB model) — sync to K replicas, async persist
12. **io_uring persistence** — Replace epoll I/O in RocksDB path
13. **Snapshot import persistence** (HA-08) — Background flush after bulk load
14. **Jepsen testing** — Distributed consistency validation
15. **Benchmark**: 1B nodes, 5-node cluster, LDBC + failure scenarios

## Consequences

### Positive

- **1B+ nodes on 5 commodity nodes** ($36/day on spot)
- **Single-partition queries at local speed** (sub-millisecond)
- **Community-aware partitioning** minimizes network hops (10-20% edge cuts vs 70%)
- **K-safety without Raft overhead per write** (VoltDB proven model)
- **io_uring eliminates 60% of I/O syscall overhead**
- **Only open-source graph DB with distributed ACID at scale**

### Negative

- **Multi-partition queries add latency** (~1-5ms per partition hop)
- **Partition rebalancing is complex** (online migration without downtime)
- **io_uring requires Linux 5.1+** (not available on macOS — dev/test divergence)
- **Boundary edge replication increases storage** (~10-20% overhead)

### Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Hot partition (skewed data) | High | Adaptive split + merge |
| Network partition (split brain) | Critical | Raft consensus for metadata, K-safety for data |
| io_uring compatibility | Medium | Fallback to epoll on older kernels / macOS |
| Partition rebalancing complexity | High | Start with static partitioning, add online rebalancing in Phase 3 |

## Alternatives Considered

### 1. Shared-storage (Aurora/Socrates model)
- Compute-storage separation, shared NVMe pool
- Pros: Simpler consistency, elastic compute
- Cons: Storage becomes bottleneck for graph traversals (random access pattern)
- **Verdict**: Bad fit for graph — adjacency list lookups need local memory

### 2. Full Raft per write (current)
- Every write goes through Raft consensus
- Pros: Strong consistency, simple model
- Cons: Raft latency on every write (~2-5ms), consensus bottleneck
- **Verdict**: Fine for metadata, too slow for data writes at scale

### 3. Replication only (Neo4j model)
- Full copy on every node, leader handles writes
- Pros: Simple, all reads local
- Cons: Doesn't scale beyond single-node RAM
- **Verdict**: Can't reach 1B nodes without massive vertical scaling

## References

- [CUTTANA: Scalable Graph Partitioning (VLDB 2024)](https://arxiv.org/abs/2312.08356)
- [TigerGraph: Native MPP Graph Database (arXiv)](https://arxiv.org/pdf/1901.08248)
- [io_uring for High-Performance DBMSs (VLDB)](https://arxiv.org/html/2512.04859v1)
- [Neon: Rust + tokio + io_uring + O_DIRECT](https://www.slideshare.net/slideshow/reworking-the-neon-io-stack-rust-tokio-io_uring-o_direct-by-christian-schwarz/283707568)
- [METIS Graph Partitioning](http://glaros.dtc.umn.edu/gkhome/metis/metis/overview)
- [VoltDB Architecture](https://www.voltdb.com/product/architecture/)
- [How to Partition a Billion-Node Graph (Microsoft Research)](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/Partition.pdf)
- [Jepsen Distributed Systems Testing](https://jepsen.io/)
- [A Wake-Up Call for Kernel-Bypass on Modern Hardware (DaMoN 2025)](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/damon25_wake_up_call.pdf)
- [Distributed Shared-Memory Databases (VLDB)](https://www.vldb.org/pvldb/vol16/p15-wang.pdf)
- [Tonbo: Exploring Better Async Rust Disk I/O](https://tonbo.io/blog/exploring-better-async-rust-disk-io)

## Decision Makers

- Sandeep Kunkunuru (Architecture)
- Madhulatha Mandarapu (Engineering)

## Approval

**Proposed**: 2026-03-23

---

**Last Updated**: 2026-03-23
**Status**: Proposed
