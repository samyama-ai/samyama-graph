# ADR-020: MVCC Transaction Isolation (RC + SI)

## Status
**Proposed** (2026-05-05) — retroactively documenting the v1.0.0 shipped feature.

## Date
2026-05-05

## Context

Pre-v1.0 the engine had no real transaction layer. Each Cypher statement was atomic in isolation, but multi-statement client sessions had no consistency guarantee, no snapshot read, no conflict detection. Two Cypher writers updating the same property could silently produce a last-writer-wins outcome that neither client expected.

Use cases that forced a transaction layer:
- **Agentic enrichment.** A Claude-or-GPT loop that reads a node, decides on a property update, and writes it back must guarantee the read-then-write is consistent — at minimum, it must catch the case where another writer updated the same property in between.
- **Long-running analytics.** Cypher queries that run for tens of seconds (algorithm operators, large aggregations) need a stable snapshot — a graph that mutates underneath produces nondeterministic results.
- **Multi-step Cypher.** `MATCH ... CREATE ... SET ...` in one transaction must either fully succeed or fully roll back.

Required:
- Two isolation levels (Read Committed for the "agentic write" case, Snapshot Isolation for the "long analytics" case).
- Per-entity version chains.
- Conflict detection at commit time (write-write).
- WAL versioning so that recovery rebuilds chain state correctly.

Not required (yet):
- Distributed transactions across the cluster (Raft handles single-tenant cluster-wide consistency via state-machine replication).
- Serializable level (no observed user demand to date; the gap is documented).
- Cross-tenant transactions.

## Decision

We will provide two isolation levels via an MVCC-with-version-chains design.

### Isolation levels

```rust
pub enum IsolationLevel {
    ReadCommitted,
    SnapshotIsolation,
}
```

- **Read Committed (RC)**: every read sees the latest committed version at read time.
- **Snapshot Isolation (SI)**: every read sees the version at transaction begin.

### Transaction shape

```rust
pub struct Transaction {
    pub id: TxnId,
    pub isolation: IsolationLevel,
    pub status: TxnStatus,
    pub start_version: u64,
    pub commit_version: Option<u64>,
    pub node_write_set: HashSet<NodeId>,
    pub edge_write_set: HashSet<EdgeId>,
}
```

### Version chains

- **Properties**: per-entity history list. Updates push the prior value with its prior version onto the chain; the live store holds the current value.
- **Topology** (adjacency lists, label index, edge-type index): **not version-chained at the same fidelity**. Reads at an SI snapshot may observe topology added or removed by concurrent writers. Documented as a known limitation.

### Conflict detection

First-committer-wins on write-write overlap:

```
for each id in txn.write_set:
    if any committed txn with commit_version > txn.start_version
       has id in its write_set:
        abort txn
assign commit_version = ++current_version
mark txn Committed
```

### WAL integration

`UpdateNodeProperties` / `UpdateEdgeProperties` carry the MVCC `version` field (ADR-023 §1.2). Recovery replays in version order so the chain state is rebuilt deterministically.

## Consequences

### Positive
- RC + SI cover the two practical use-case classes.
- Sparse version chains (`HashMap<EdgeId, Vec<...>>`) — unmutated entities pay zero.
- Reads under SI never block writers and vice-versa — non-blocking concurrency on the canonical OLTP path.
- WAL recovery is deterministic per version.
- Test coverage is solid (graph::store::tests covers RC vs SI, conflict, retry).

### Negative — known limitations
- **Topology is RC even under SI.** Adjacency / label-index / edge-type-index changes are visible to SI readers as soon as a writer commits. We do not version topology.
- **No Serializable (SSI).** Write-skew is not caught. We do not track read sets.
- **No version GC.** Property history grows monotonically. ADR-020 explicitly names this and lists the v1.1 plan.
- **Commit-time global mutex** serialises conflict-detection across all transactions of a tenant. Hold time short, but a contention point under heavy write concurrency.
- **Aborts surface as generic errors** at the protocol layer. Conflict-abort is not distinguished from other errors.
- **No isolation-level surface in Cypher.** Programmatic API only.

### Neutral
- Per-tenant scope. No cross-tenant transactions.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| Read Uncommitted | Permits dirty reads; correctness-fail. |
| Strict 2PL | Throughput-killing under contention. |
| MVTO (multi-version timestamp ordering) | More overhead than first-committer-wins; same correctness as SI. |
| Single-writer / multi-reader (LMDB-style) | Throughput-killing under our write workload. |
| No transactions, atomic single-statements | Forces every multi-write outside the engine. |

## Follow-ups

1. **Version GC** tied to the oldest active reader (`horizon`). Per-entity hard cap as a backstop. Surface metrics.
2. **Topology versioning** so SI is honest end-to-end. Multi-quarter work; adjacency add/remove + label/type index changes get versioned.
3. **Serializable Snapshot Isolation (SSI)** as an opt-in third level, with read-set tracking and a dangerous-structure detector.
4. **Cypher-level isolation surface**: `BEGIN [TRANSACTION] [ISOLATION LEVEL ...] / COMMIT / ROLLBACK`.
5. **Distinct conflict-abort error** at the protocol layer.
6. **`Arc<PropertyMap>` for edge version snapshots** (also called out in ADR-024 §1.5).
7. **Per-(node, edge) sub-mutex on commit path** if the global mutex shows contention at scale.

## References

- Code: `src/graph/store.rs:460,480,2240,2257,2300+,524,497`. WAL integration: `src/persistence/wal.rs:104,115`.
- Wiki: [[concurrency-mvcc-isolation.md]], [[concurrency-conflict-detection.md]], [[storage-version-gc.md]].
- Related ADRs: ADR-023 (WAL Versioning), ADR-024 (Edge Arena Removal), ADR-021 (Columnar Property Store), ADR-029 (Index Manager — also exhibits the RC-not-SI gap), ADR-007 (Volcano).
