# Samyama ACID Guarantees

**Last Updated:** 2026-05-19 · **Engine version:** v1.0.0 (shipped 2026-04-11)

Samyama provides ACID guarantees for both single-statement Cypher and multi-statement transactions. The MVCC transaction layer landed in v1.0.0 (ADR-020); the storage path is RocksDB + Samyama's logical WAL (ADR-023).

## Summary

| Property | Status | Mechanism |
|----------|:------:|-----------|
| **Atomicity** | ✅ | RocksDB `WriteBatch` + WAL — see ADR-023 |
| **Consistency** | ✅ | Schema-flexible with internal-identifier integrity; distributed = Raft quorum |
| **Isolation** | ✅ | **MVCC** in v1.0.0: Read Committed + Snapshot Isolation + write/write conflict detection (ADR-020) |
| **Durability** | ✅ | RocksDB persistence + Samyama logical WAL + Raft replication in HA |

---

## Detailed Breakdown

### 1. Atomicity — "all or nothing"

Any Cypher mutation (`CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, and combined patterns) is atomic across all the secondary structures it touches:

- Edge endpoints (`edge_endpoints` Vec — the post-DS-07c layout, ADR-024)
- Outgoing and incoming adjacency (CSR + segment buffer)
- Property columns (columnar property store, ADR-021)
- Indexes (`IndexManager`, ADR-029): label, property, unique, composite
- Label count and edge-type count caches

Persistence path: writes append to the **Samyama logical WAL** (ADR-023) before in-memory state is mutated. RocksDB's internal WAL is separate; the two are not collapsed. Recovery replays from the logical WAL.

There are no dangling edges, no orphan index entries, and no half-applied multi-label changes after a crash.

### 2. Consistency — "valid state transitions"

- **Schema-flexible**, but internal invariants are enforced: a `NodeId` referenced from an edge must exist, label-interning IDs (ADR-028) are stable across reads, and the columnar property store maintains its column-aligned indexes.
- **Distributed**: Raft quorum (`openraft`) is the agreement protocol. A write is acknowledged only after a majority of nodes have logged it; this gives linearizability at the cluster level.

### 3. Isolation — MVCC in v1.0.0

v1.0.0 introduced full MVCC transaction isolation. The single-`RwLock` model of earlier versions is gone.

- **Default isolation**: Read Committed (RC). Each statement sees committed data as of statement-start time.
- **Snapshot Isolation (SI)** available for multi-statement reads; a SI transaction sees a consistent snapshot for its full lifetime.
- **Write/write conflict detection**: concurrent transactions that touch overlapping nodes/edges are detected at commit time; the loser aborts with a serialization error.
- **Versioned edges and nodes**: `get_node_at_version()` / `get_edge_at_version()` are the per-version readers behind the MVCC layer.
- **Version GC**: a background pass reclaims versions no longer reachable by any active reader.

ADR-020 documents the design and the trade-offs (RC default vs SI opt-in, conflict-detection cost, GC pacing).

**Practical implication**: long-running analytics queries can run concurrently with writers without blocking either side, and multi-statement workflows (e.g., the `algo.or.solve` write path in ADR-026) can see a consistent snapshot of the graph without holding global locks.

### 4. Durability — "committed data survives"

- **Disk persistence**: writes go through the Samyama logical WAL (ADR-023) before being applied. The current WAL "checksum" is XOR-of-bytes; the CRC32C upgrade and segment-rotation work are still open (see ADR-023 "Partially Shipped" status).
- **Snapshots**: portable `.sgsnap` format (ADR-022) — gzip-framed, importable via `import_tenant_with_dedup` (ADR-019) for cross-KG entity dedup at load time.
- **Distributed durability**: Raft replication ensures data is on a quorum before acknowledgement. Leader failure post-ACK does not lose the write.

## Performance Trade-offs

| Trade-off | Why |
|---|---|
| Write latency higher than eventual-consistency systems | WAL fsync + (in HA) Raft replication before ACK |
| MVCC GC pressure under heavy churn | Versioned nodes/edges accumulate until GC reclaims them; tunable in ADR-020 |
| Snapshot import is bulk-only | `.sgsnap` import bypasses the WAL for speed; in-flight transactions see the imported tenant only after commit |

## Comparison

| Feature | Samyama v1.0.0 | RedisGraph | Neo4j |
|:---|:---:|:---:|:---:|
| **Storage** | RocksDB + columnar property store | In-memory | Native disk |
| **Atomicity** | Multi-statement (MVCC txn) | Operation-level | Multi-statement |
| **Isolation** | RC default + SI opt-in + conflict detection | None (single-threaded) | Read Committed |
| **Clustering** | Raft (CP) | Master-replica | Raft / Causal Clustering (CP / CA) |
| **Durability** | Logical WAL + RocksDB + Raft | AOF / RDB | Transaction log |

## References

- ADR-020 — MVCC transaction isolation
- ADR-021 — Columnar property store
- ADR-022 — Snapshot format (`.sgsnap`)
- ADR-023 — WAL versioning (partially shipped; CRC32C still open)
- ADR-024 — Edge arena removal (DS-07c)
- ADR-029 — IndexManager
- Engineering Compendium: `samyama-cloud/wiki/topics/engineering-compendium.md` — §1.6 MVCC isolation, §1.7 storage layout, §3.x indexes
