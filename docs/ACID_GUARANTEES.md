# Samyama ACID Guarantees

**Last Updated:** 2026-09-29 (§1, §2 and §4 re-checked against `main` at `28c46cf`: line references refreshed, the test that pins each claim named, and where no test pins it, said so — #1309. §3 Isolation re-verified 2026-09-17)

Samyama provides ACID guarantees for both single-statement Cypher and multi-statement transactions. The MVCC transaction layer landed in v1.0.0 (ADR-020); the storage path is RocksDB + Samyama's logical WAL (ADR-023).

## Summary

| Property | Status | Mechanism |
|----------|:------:|-----------|
| **Atomicity** | ✅ | RocksDB `WriteBatch` + WAL — see ADR-023 |
| **Consistency** | ✅ | Schema-flexible with internal-identifier integrity. Single node only — the distributed claim is withdrawn, see §2 |
| **Isolation** | ✅ | Session transactions (RESP, HTTP) hold the writer lock: serializable in effect. The Rust store API offers snapshot isolation with first-committer-wins. Anomaly table in §3 |
| **Durability** | ⚠️ | Written, not **synced** *by default*: a committed write reaches the OS page cache, not the platter. It may not survive power loss or a host crash. `SAMYAMA_FSYNC=1` puts a real barrier on both the WAL and RocksDB, at two orders of magnitude in write throughput — measured, with the host it was measured on, in §4. A write that fails to reach disk at all stops further writes (#1274); over RESP the failing statement is also told, over HTTP it is not. See §4 |

---

## Detailed Breakdown

### 1. Atomicity — "all or nothing"

Any Cypher mutation (`CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, and combined patterns) is atomic across all the secondary structures it touches:

- Edge endpoints (`edge_endpoints` Vec — the post-DS-07c layout, ADR-024)
- Outgoing and incoming adjacency (CSR + segment buffer)
- Property columns (columnar property store, ADR-021)
- Indexes (`IndexManager`, ADR-029): label, property, unique, composite
- Label count and edge-type count caches

Persistence path: the in-memory state is mutated first, and the **Samyama logical WAL** (ADR-023) and RocksDB are written after, from a write log the statement collected. Over RESP that is `enable_write_log` → `execute_mut_with_params` → `take_write_log` → `apply_mutations` (`src/protocol/command.rs:319-343`); over HTTP the same sequence runs inside `AppState::mutate` (`src/http/server.rs:551-582`). `apply_mutations` (`src/persistence/mod.rs:360`) appends each entity to the logical WAL and then puts it into RocksDB (`src/persistence/mod.rs:436-442` for a node). That is write-*behind*, not write-ahead — this section claimed the opposite until 2026-09-18. A crash in the window between the mutation and `apply_mutations` loses the write entirely.

**The logical WAL is not read at startup.** Recovery is `PersistenceManager::recover` (`src/persistence/mod.rs:561-580`), which scans nodes and edges out of RocksDB and nothing else; `Wal::replay` has no caller outside tests (the unit tests in `src/persistence/wal.rs` and `tests/wal_torn_tail.rs`). What a restart finds is what RocksDB holds. The module comment at `src/persistence/mod.rs:33-37` still describes a WAL-first write and a replay on startup; neither happens. RocksDB's internal WAL is separate; the two are not collapsed.

*Pinned by:* `a_partial_failure_leaves_nothing_in_memory_or_on_disk` (`tests/write_durability.rs`), for a statement that fails part-way. The write-behind window itself is not tested — it needs a crash between two lines — and is read from the code above.

Within the in-memory structures the all-or-nothing claim holds: no dangling edges, no orphan index entries, no half-applied multi-label changes. **A single statement is atomic: one that fails partway is undone** (#1593). `QueryEngine` runs every write statement as a transaction of its own (`GraphStore::atomically`), so a `CREATE` that fails on its tenth row, or a `SET` stopped by its deadline, leaves neither the nine rows before it nor anything in the write log; the cost is one undo-log entry per first write to an existing entity, collected when the statement commits. Until #1593 there was no statement rollback (LANG-07) and the nine rows stayed, in memory and on disk. A statement inside a transaction the client opened is undone by that transaction's ROLLBACK, not on its own. Multi-statement transactions do roll back, through the undo log (§3); `a_transaction_that_cannot_be_persisted_is_rolled_back_in_memory` (`tests/persist_failure_is_reported.rs:118`) pins that for a commit that cannot be persisted.

### 2. Consistency — "valid state transitions"

- **Schema-flexible**, but internal invariants are enforced: a `NodeId` referenced from an edge must exist, label-interning IDs (ADR-028) are stable across reads, and the columnar property store maintains its column-aligned indexes.
- **Distributed: not implemented.** This section claimed Raft quorum before acknowledgement until 2026-09-18. `RaftNode::write` applies to the **local** state machine and increments a counter (`src/raft/node.rs:104-120`); there is no log append, no peer contact and no quorum, and the file says so itself at line 47. `RaftNode::initialize` ignores its peer list and makes the node leader unconditionally (`src/raft/node.rs:91-101`). `openraft` supplies `Config` and `SnapshotPolicy` (`src/raft/mod.rs:56`, `:92`) and nothing more. No protocol write path reaches it: `RaftNode::new` is called nowhere in `src/` outside `src/raft/`, and `ClusterManager` is used for tenant routing and proxying only (`src/protocol/server.rs:162`, `:379`). The one caller is `examples/cluster_demo.rs`, whose header still says "Quorum-based writes through consensus"; what it runs is the local apply above. Treat the cluster as a single node for every guarantee on this page.

  *Pinned by:* nothing that asserts the absence of replication. The nearest test, `test_raft_node_write_after_init` (`src/raft/node.rs:244`), initialises one node with no peers and asserts only that the write returns a `QueryResult`. The claim above is read from the code.

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
- **A COMMIT that cannot be persisted is refused** (#1275). The reply is an error, the in-memory state is rolled back from the undo log and what reached disk is repaired — pinned by `a_commit_that_cannot_be_persisted_is_refused_and_rolled_back` in `src/protocol/server.rs:733` and `src/http/transactions.rs:245`. This bullet said the opposite until 2026-09-18; it was written against `73e6733`, one commit before the fix landed.
- **A single write statement outside a transaction** whose persistence fails is handled differently by the two protocols. Over RESP the reply is an error saying the write is in memory and not on disk (`src/protocol/command.rs:341-369`). Over HTTP `POST /api/query` the failing statement still gets a success reply, because `AppState::mutate` returns the body's value and has no error channel (`src/http/server.rs:565-579`); the client sees the *next* write refused with 503 (`src/http/handler.rs:476-486`). Both mark the process degraded. This bullet said "still warns and succeeds" for both until 2026-09-29. `a_failed_persist_marks_the_process_degraded_and_says_why` (`tests/persist_failure_is_reported.rs:68`) calls `mark_degraded` itself, so it pins the flag and its message, not the wiring in either server; neither reply is pinned by a test.

### 4. Durability — "committed data survives"

- **Nothing is fsynced by default.** Without `SAMYAMA_FSYNC` a WAL entry is written into a `BufWriter` and not flushed at all — the flush and the `sync_data` are both inside `if self.sync_mode` (`src/persistence/wal.rs:265-268`) — and RocksDB's `WriteOptions` carry `set_sync(false)` (`src/persistence/storage.rs:155`, from `fsync_enabled` at `:111`). The WAL's `sync_mode` is read from the environment once, at construction (`src/persistence/wal.rs:187`, `:201`), and is false unless the variable is set. So on a stock server the RocksDB write stops at the page cache and the WAL entry may still be in the process's buffer. Since the WAL is not read at startup (§1), it is the RocksDB write that recovery depends on.

  *Pinned by* `tests/durability_is_a_choice.rs`: `the_default_is_off` (`:58`) and `the_wals_own_default_is_off_too` (`:72`) for the default, `the_flag_is_read_and_is_not_over_eager` (`:94`) for which values turn it on, `a_synced_write_still_lands_and_reads_back` (`:113`), `both_halves_of_the_write_path_move_together` (`:135`) and `the_level_is_fixed_for_the_life_of_a_process` (`:154`). None of them can observe whether `sync_data` reached the device; they pin the flag, not the barrier.

  *This bullet described a stronger claim until 2026-09-27: that the sync-mode setter had no callers, that the only call even when true was `flush()`, that `sync_data` appeared in `src/` solely in the snapshot writer, and that RocksDB was opened with no `WriteOptions` at all. All four were true when #1309 was written and none are true now — the barrier below fixed them — and the bullet contradicted the one directly under it for as long as it stood. A correction goes stale the same way the claim it corrected did.*
  - **What should survive:** the Samyama process being killed. RocksDB's write is in the page cache and the kernel writes it out. This is not tested with a killed process: the restart in `tests/write_durability.rs:45-48` is a checkpoint and a `recover` inside the same process.
  - **What may not:** power loss, a kernel panic, a hard host reset, or a container host failure. A write acknowledged seconds earlier can be gone.
  - **Process kill, measured** (#1355): `scripts/crash_consistency.py` sent `kill -9` to a single-node `--data-path` server at 1000 random points mid-write; the script does not set `SAMYAMA_FSYNC`. Of 905,907 acknowledged writes — 587,488 by an auto-commit statement's 200, 318,419 by a `POST /api/tx/:id/commit` 200 — none was lost after restart, and no recovery had a gap. The kills were shallow (median 796 writes per cycle), it is a harness run by hand rather than a test in CI, and it says nothing about power loss or Raft. Numbers, limits and the command are in [FAILURE-MODES.md](./FAILURE-MODES.md#measured-outside-the-test-suite-kill--9-mid-write-rel-04-1355). One kill point per run, over RESP, is now a `cargo test` that CI runs: `every_acknowledged_write_survives_a_sigkill_mid_write`, FAILURE-MODES row 34 (#1311).
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
  - A **single statement** that succeeded and then could not be persisted is
    not undone — only a statement that *fails* is (#1593) — so its rows stay
    in memory and the disk does not have them. Over RESP the client gets an error saying exactly that; over HTTP it
    gets a success reply and only the next write is refused (§3, last bullet).
    Either way every later write is refused, because once the store is ahead of the disk each further write
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
| Write latency higher than a pure in-memory store | A WAL append and a RocksDB write per mutation. **Not** fsync unless `SAMYAMA_FSYNC=1` is set, and never replication (§2, §4), so by default this trade-off is smaller than this table claimed until 2026-09-18 |
| `SAMYAMA_FSYNC=1` costs two orders of magnitude in write throughput | A `sync_data` on the WAL and a synced RocksDB write per mutation; the measured ratio, with its hosts, is in §4 |
| An open session transaction blocks all other clients | It holds the writer lock; bounded by `SAMYAMA_TX_TIMEOUT_SECS` |
| Snapshot import is bulk-only | `.sgsnap` import bypasses the WAL for speed; in-flight transactions see the imported tenant only after commit |

## Comparison

| Feature | Samyama v1.0.0 | RedisGraph | Neo4j |
|:---|:---:|:---:|:---:|
| **Storage** | RocksDB + columnar property store | In-memory | Native disk |
| **Atomicity** | Multi-statement (MVCC txn) | Operation-level | Multi-statement |
| **Isolation** | Serializable-in-effect sessions (RESP/HTTP); SI in the Rust API | None (single-threaded) | Read Committed |
| **Clustering** | none in effect (Raft is a stub, §2) | Master-replica | Raft / Causal Clustering (CP / CA) |
| **Durability** | RocksDB, plus a logical WAL not read at startup; **unsynced by default**, `SAMYAMA_FSYNC=1` syncs both (§4) | AOF / RDB (`appendfsync` configurable) | Transaction log, fsync per commit by default |

## References

- ADR-020 — MVCC transaction isolation
- ADR-021 — Columnar property store
- ADR-022 — Snapshot format (`.sgsnap`)
- ADR-023 — WAL versioning (partially shipped; CRC32C still open)
- ADR-024 — Edge arena removal (DS-07c)
- ADR-029 — IndexManager
- Engineering Compendium: `samyama-cloud/wiki/topics/engineering-compendium.md` — §1.6 MVCC isolation, §1.7 storage layout, §3.x indexes
