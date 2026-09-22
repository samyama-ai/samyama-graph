# Samyama ACID Guarantees

**Last Updated:** 2026-09-18 (§1, §2 and §4 re-checked against the code and corrected — see #1309; §3 Isolation re-verified 2026-09-17)

Samyama provides ACID guarantees for both single-statement Cypher and multi-statement transactions. The MVCC transaction layer landed in v1.0.0 (ADR-020); the storage path is RocksDB + Samyama's logical WAL (ADR-023).

## Summary

| Property | Status | Mechanism |
|----------|:------:|-----------|
| **Atomicity** | ✅ | RocksDB `WriteBatch` + WAL — see ADR-023 |
| **Consistency** | ✅ | Schema-flexible with internal-identifier integrity. Single node only — the distributed claim is withdrawn, see §2 |
| **Isolation** | ✅ | Session transactions (RESP, HTTP) hold the writer lock: serializable in effect. The Rust store API offers snapshot isolation with first-committer-wins. Anomaly table in §3 |
| **Durability** | ⚠️ | Written, not **synced**: a committed write reaches the OS page cache, not the platter. It survives a process crash; it may not survive power loss or a host crash. A write that fails to reach disk at all is now reported and stops further writes (#1274). See §4 |

---

## Detailed Breakdown

### 1. Atomicity — "all or nothing"

Any Cypher mutation (`CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, and combined patterns) is atomic across all the secondary structures it touches:

- Edge endpoints (`edge_endpoints` Vec — the post-DS-07c layout, ADR-024)
- Outgoing and incoming adjacency (CSR + segment buffer)
- Property columns (columnar property store, ADR-021)
- Indexes (`IndexManager`, ADR-029): label, property, unique, composite
- Label count and edge-type count caches

Persistence path: the in-memory state is mutated first, and the **Samyama logical WAL** (ADR-023) is appended after, from a write log the statement collected (`src/protocol/command.rs:238-256` → `src/persistence/mod.rs:340`). That is write-*behind*, not write-ahead — this section claimed the opposite until 2026-09-18. A crash in the window between the two loses the write entirely, because there is nothing in the log to replay. RocksDB's internal WAL is separate; the two are not collapsed.

Within the in-memory structures the all-or-nothing claim holds: no dangling edges, no orphan index entries, no half-applied multi-label changes. **A single statement is not atomic if it fails partway.** The engine has no statement rollback (LANG-07), so a `CREATE` that fails on its tenth row keeps the nine before it, in memory and on disk. Multi-statement transactions do roll back, through the undo log (§3).

### 2. Consistency — "valid state transitions"

- **Schema-flexible**, but internal invariants are enforced: a `NodeId` referenced from an edge must exist, label-interning IDs (ADR-028) are stable across reads, and the columnar property store maintains its column-aligned indexes.
- **Distributed: not implemented.** This section claimed Raft quorum before acknowledgement until 2026-09-18. `RaftNode::write` applies to the **local** state machine and increments a counter (`src/raft/node.rs:104-119`); there is no log append, no peer contact and no quorum, and the file says so itself at line 47. `openraft` supplies a `Config` type and nothing more. No protocol write path reaches it — `ClusterManager` is used for tenant routing and proxying only. Treat the cluster as a single node for every guarantee on this page.

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
- **A COMMIT that cannot be persisted is refused** (#1275). The reply is an error, the in-memory state is rolled back from the undo log and what reached disk is repaired — pinned by `a_commit_that_cannot_be_persisted_is_refused_and_rolled_back` in `src/protocol/server.rs:507` and `src/http/transactions.rs:234`. This bullet said the opposite until 2026-09-18; it was written against `73e6733`, one commit before the fix landed.
- **A single write statement outside a transaction still warns and succeeds** if persistence fails (`src/protocol/command.rs:255`, `src/http/server.rs:129`). That half of #1274 is open.

### 4. Durability — "committed data survives"

- **Nothing is fsynced by default.** A write is appended to the logical WAL and, at best, flushed out of a `BufWriter` into the OS page cache. `WalWriter::sync_mode` defaults to false (`src/persistence/wal.rs:187`) and its setter has no callers anywhere in the repository, so it is false for the life of the process; even when true the call is `file.flush()` (`wal.rs:232`), which is not a durability barrier. `sync_all`/`sync_data` appear in `src/` only in the snapshot writer. RocksDB is opened without `WriteOptions::set_sync`, so its writes are unsynced too.
  - **What survives:** the Samyama process being killed. The data is in the page cache and the kernel writes it out.
  - **What may not:** power loss, a kernel panic, a hard host reset, or a container host failure. A write acknowledged seconds earlier can be gone.
  - **`SAMYAMA_FSYNC=1` turns it on**, and it is off by default — that is
    unchanged, and this is a change to what an operator can *choose* rather
    than to what they get without asking. With it set, the WAL calls
    `sync_data()` (not `flush()`, which only reaches the page cache) and
    RocksDB is opened with `WriteOptions::set_sync(true)`. Both halves move
    together: an fsynced WAL entry describing a write still sitting in the
    store's page cache is not more durable than neither.
  - **What it costs, measured** (`cargo run --release --example fsync_cost`;
    medians of 5 runs per configuration, on two hosts):

    | host | unsynced | both barriers | WAL only |
    |---|---:|---:|---:|
    | vm-1 (i7-13620H, local NVMe) | 419,199 /s | **420×** (404–450) | 208× |
    | Vultr `voc-c-8c` (8 dedicated vCPU, virtio disk) | 174,757 /s | **120×** | 66× |

    The vm-1 both-barriers figure is the median of **five alternated pairs**,
    and the range beside it is what those five spanned. Alternation is not a
    refinement: RocksDB's `WriteOptions` are fixed when the database is opened,
    so the two configurations have to be two processes, and running one after
    the other puts every change between them into the ratio. Measured that way
    on 2026-09-21, two runs hours apart on the same idle host disagreed by
    almost a factor of two (superseded: 420.8× and 219.3×), while every arm
    measured *inside* a single process reproduced across the same two runs
    (superseded: within 2%). Interleaved, the five pairs span **1.09×**.

    **The ratio is a property of the device as much as the engine**, and a
    3.5× spread between two ordinary hosts is the evidence. Quote it with its
    host or it is a number about a disk. The synced arm is the stable half —
    1,410–1,465 writes/s across five runs on the Vultr box, ±2% — and almost
    all of the spread in the ratio comes from how fast the *unsynced* path is,
    which is what a faster disk buys.

    What does not move: the cost is two orders of magnitude on both. That is
    why the default does not change, and an operator who needs the guarantee
    now has it available rather than described.

    Both figures are ingested by `CH-RECOVER`, which gives the local run a
    verdict only when its own five ratios agree closely enough
    (threshold: 1.25×) *and* the host's load average is below its own bound
    (threshold: 2.0). The spread is the binding half: the load average is a
    one-minute decayed mean sampled before the run and cannot see a
    disturbance during it, and both of the runs that disagreed passed it
    (superseded: disagreed by 1.9×, at load averages of 1.29 and 1.89). It
    stays as context. Under real load — vm-1 is shared — three runs disagreed
    far more widely for one arm (superseded: 210×, 171× and 276×), which the
    load average did catch. The quiet-host reference is committed at
    `benchmarks/durability/fsync-quiet-host.json` in the benchmarks repo.
  - It is a request, not a proof: `sync_data` returns when the kernel says the
    device has the bytes, and whether the device lied is a property of the
    device.
- **Write order**: the log is appended after the memory mutation, not before (§1).
- **A write that does not reach disk is now reported as a failure**, and the
  process stops accepting writes (#1274). It used to be a `warn!` line and a
  success reply, so the client was told the write landed, the store kept it,
  the disk did not, and a restart threw it away.
  - A **transaction** is persisted *before* it commits in memory, so a
    persistence failure rolls it back and repairs the disk: memory and disk
    still agree afterwards.
  - A **single statement** has no rollback — the engine has no statement-level
    undo (LANG-07) — so its rows stay in memory and the disk does not have
    them. The client gets an error saying exactly that, and every later write
    is refused, because once the store is ahead of the disk each further write
    widens the gap and a restart replays a prefix that does not include the
    first failure. Reads continue; the in-memory graph is still the most
    complete thing anyone has.
  - Clearing it takes a restart, which reloads from disk and discards what
    never landed. There is deliberately no "resume" command: nothing in the
    process knows what was lost, so carrying on would be guessing.
- The current WAL "checksum" is XOR-of-bytes; the CRC32C upgrade and segment-rotation work are still open (see ADR-023 "Partially Shipped" status).
- **Snapshots**: portable `.sgsnap` format (ADR-022) — gzip-framed, importable via `import_tenant_with_dedup` (ADR-019) for cross-KG entity dedup at load time.
- **Distributed durability: none.** See §2 — the replication this claimed does not run.

## Performance Trade-offs

| Trade-off | Why |
|---|---|
| Write latency higher than a pure in-memory store | A WAL append and a RocksDB write per mutation. **Not** fsync, and not replication — neither happens (§4), so this trade-off is smaller than this table claimed until 2026-09-18 |
| An open session transaction blocks all other clients | It holds the writer lock; bounded by `SAMYAMA_TX_TIMEOUT_SECS` |
| Snapshot import is bulk-only | `.sgsnap` import bypasses the WAL for speed; in-flight transactions see the imported tenant only after commit |

## Comparison

| Feature | Samyama v1.0.0 | RedisGraph | Neo4j |
|:---|:---:|:---:|:---:|
| **Storage** | RocksDB + columnar property store | In-memory | Native disk |
| **Atomicity** | Multi-statement (MVCC txn) | Operation-level | Multi-statement |
| **Isolation** | Serializable-in-effect sessions (RESP/HTTP); SI in the Rust API | None (single-threaded) | Read Committed |
| **Clustering** | none in effect (Raft is a stub, §2) | Master-replica | Raft / Causal Clustering (CP / CA) |
| **Durability** | Logical WAL + RocksDB, **unsynced** (§4) | AOF / RDB (`appendfsync` configurable) | Transaction log, fsync per commit by default |

## References

- ADR-020 — MVCC transaction isolation
- ADR-021 — Columnar property store
- ADR-022 — Snapshot format (`.sgsnap`)
- ADR-023 — WAL versioning (partially shipped; CRC32C still open)
- ADR-024 — Edge arena removal (DS-07c)
- ADR-029 — IndexManager
- Engineering Compendium: `samyama-cloud/wiki/topics/engineering-compendium.md` — §1.6 MVCC isolation, §1.7 storage layout, §3.x indexes
