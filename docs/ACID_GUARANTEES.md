# Samyama ACID Guarantees

**Last Updated:** 2026-09-17 (§3 Isolation re-verified; §1, §2 and §4 date from 2026-05-19 and were not re-checked)

Samyama provides ACID guarantees for both single-statement Cypher and multi-statement transactions. The MVCC transaction layer landed in v1.0.0 (ADR-020); the storage path is RocksDB + Samyama's logical WAL (ADR-023).

## Summary

| Property | Status | Mechanism |
|----------|:------:|-----------|
| **Atomicity** | ✅ | RocksDB `WriteBatch` + WAL — see ADR-023 |
| **Consistency** | ✅ | Schema-flexible with internal-identifier integrity; distributed = Raft quorum |
| **Isolation** | ✅ | Session transactions (RESP, HTTP) hold the writer lock: serializable in effect. The Rust store API offers snapshot isolation with first-committer-wins. Anomaly table in §3 |
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

### 3. Isolation — verified 2026-09-17 against v1.8.0+ main (`73e6733`)

This section replaces the v1.0.0 description, which said Read Committed was the
default and a background pass garbage-collected old versions. Neither was true:
nothing outside tests calls `gc_versions` or `gc_auto`, and a statement outside a
transaction does not run beside a writer at all. What follows is read from the
code and pinned by the tests named in the table.

**There are two transaction paths, and they isolate differently.**

| | session transaction | store transaction |
|---|---|---|
| reached through | RESP `GRAPH.BEGIN` / `GRAPH.COMMIT` / `GRAPH.ROLLBACK`; HTTP `POST /api/tx/begin`, `/api/tx/{id}/commit`, `/api/tx/{id}/rollback` | the Rust API only: `begin_transaction`, `txn_set_node_property`, `txn_create_node`, `commit_transaction`, `abort_transaction` |
| how writes are held | applied to the store at once, at a new version; the undo log records what each write replaced | buffered; applied at a new version on commit, dropped on abort |
| concurrency | holds the store's writer lock from BEGIN to COMMIT or ROLLBACK. No other reader or writer runs meanwhile | many can be open; each reads a snapshot |
| isolation | **serializable in effect** (nothing runs concurrently) | **snapshot isolation**, first-committer-wins at entity level |
| limit | rolled back after `SAMYAMA_TX_TIMEOUT_SECS` (default 30 s); over RESP also when the connection closes | none |

**A statement outside a transaction** takes the same lock: reads share it, a write
holds it alone. Each statement therefore sees every write committed before it
started and none that start after.

**The anomaly table (REL-01).** Store-transaction rows are in
`tests/mvcc_isolation_anomalies.rs`; session-transaction rows in the `tests`
modules of `src/protocol/server.rs` (RESP) and `src/http/transactions.rs` (HTTP).

| anomaly | store transaction (snapshot isolation) | test | session transaction | test |
|---|---|---|---|---|
| dirty read | prevented | `no_dirty_read` | prevented: nobody else can read while it is open | `nobody_else_can_read_while_a_connection_holds_a_transaction`, `nobody_else_can_read_while_a_transaction_is_open` |
| non-repeatable read | prevented: a snapshot keeps its value after another commit | `a_snapshot_does_not_see_a_later_commit` | prevented (no concurrent writer) | same as above |
| phantom, by creation | prevented for a lookup by id | `a_snapshot_does_not_see_a_node_created_after_it` | prevented (no concurrent writer) | same as above |
| phantom, by predicate (`MATCH (n:L)` over a snapshot) | **not tested**: Cypher does not run inside a store transaction | — | prevented (no concurrent writer) | same as above |
| lost update | prevented: the second commit on the same entity is refused | `the_second_of_two_conflicting_commits_is_refused` | prevented (no concurrent writer) | same as above |
| write skew | **allowed**, as snapshot isolation allows it | `write_skew_is_allowed_under_snapshot_isolation` | prevented (no concurrent writer) | same as above |
| rollback leaves nothing | yes | `abort_undoes_the_write`, `an_aborted_create_leaves_no_node` | yes | `a_rolled_back_transaction_leaves_nothing_and_a_committed_one_stays`, `a_rolled_back_transaction_leaves_nothing` |
| reads its own writes | yes | `a_transaction_reads_its_own_writes` | yes | `a_committed_transaction_keeps_its_writes_and_reads_them_inside` |
| commit refused part-way changes nothing | yes | `a_commit_refused_by_a_constraint_changes_nothing` | not applicable: a failing statement fails alone | — |
| abandoned transaction | not applicable | — | rolled back on disconnect | `a_connection_that_closes_mid_transaction_rolls_it_back` |

**Costs and limits:**
- An open session transaction blocks every other client. Keep them short; the timeout bounds the damage.
- Store transactions are not reachable over any protocol. Exposing them would give readers concurrency during a write transaction, at the price of write skew.
- Conflicts are detected per entity, not per property. Two transactions setting different keys on one node conflict.
- Old versions are not collected in production. Undo-log entries accumulate only while transactions write; a store with no transactions holds none (#1200 step 2).
- **Durability of COMMIT is not guaranteed by its reply.** Over RESP and HTTP, if persistence fails at commit, the server logs a warning and still reports success. The same holds for a single write statement over RESP.

### 4. Durability — "committed data survives"

- **Disk persistence**: writes go through the Samyama logical WAL (ADR-023) before being applied. The current WAL "checksum" is XOR-of-bytes; the CRC32C upgrade and segment-rotation work are still open (see ADR-023 "Partially Shipped" status).
- **Snapshots**: portable `.sgsnap` format (ADR-022) — gzip-framed, importable via `import_tenant_with_dedup` (ADR-019) for cross-KG entity dedup at load time.
- **Distributed durability**: Raft replication ensures data is on a quorum before acknowledgement. Leader failure post-ACK does not lose the write.

## Performance Trade-offs

| Trade-off | Why |
|---|---|
| Write latency higher than eventual-consistency systems | WAL fsync + (in HA) Raft replication before ACK |
| An open session transaction blocks all other clients | It holds the writer lock; bounded by `SAMYAMA_TX_TIMEOUT_SECS` |
| Snapshot import is bulk-only | `.sgsnap` import bypasses the WAL for speed; in-flight transactions see the imported tenant only after commit |

## Comparison

| Feature | Samyama v1.0.0 | RedisGraph | Neo4j |
|:---|:---:|:---:|:---:|
| **Storage** | RocksDB + columnar property store | In-memory | Native disk |
| **Atomicity** | Multi-statement (MVCC txn) | Operation-level | Multi-statement |
| **Isolation** | Serializable-in-effect sessions (RESP/HTTP); SI in the Rust API | None (single-threaded) | Read Committed |
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
