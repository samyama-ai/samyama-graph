# RocksDB write stalls and concurrent read latency (#2128)

```sh
# Defaults: 100k Person nodes, 4 readers, 20 s per phase, 1,000-row write statements of 256-byte payloads
cargo bench --bench rocksdb_write_stalls

# Push RocksDB harder: bigger rows and longer phases fill memtables and L0 faster
cargo bench --bench rocksdb_write_stalls -- --value-bytes 4096 --phase-secs 60

# RocksDB on a particular disk, with every write synced
SAMYAMA_FSYNC=1 cargo bench --bench rocksdb_write_stalls -- --data-dir /mnt/bench/rocks
```

Run in `--release` (which `cargo bench` is) on a quiet host. CI compiles the
bench and runs its measurement code on a tiny workload
(`tests/rocksdb_write_stalls_harness.rs`), which checks that every phase
measures what it reports and asserts no timings. **No results have been
collected yet**: the tables below are to be filled in from a real run, not
estimated.

## The question

A write statement runs in memory and is then persisted by
`PersistenceManager::apply_mutations` *while it still holds the graph's write
lock* (`src/protocol/command.rs`, `handle_graph_query`; the HTTP path does the
same). Cypher reads are served from the in-memory `GraphStore`, but they take
the read lock, so any time RocksDB spends inside a write -- waiting on a
memtable flush, delayed or stopped behind a compaction backlog -- is time every
reader spends queued. The bench measures whether that shows up in read
latency, and whether RocksDB was actually stalling when it did.

## What it runs

One process, one `tokio::sync::RwLock<GraphStore>` (the server's lock type),
readers running `MATCH (p:Person {id: $id})-[:KNOWS]->(f) RETURN count(f)` on
an indexed id throughout, and three phases back to back:

| phase | writer | what it isolates |
|---|---|---|
| `baseline` | none | read latency with no writes; everything else is read against it |
| `memory` | `UNWIND range(..) AS i CREATE (:Event {seq: i, payload: $pad})`, not persisted | what the write lock costs readers on its own |
| `persist` | the same statements, each persisted with `apply_mutations` before the lock is released | what persistence, stalls included, adds on top |

The writer runs flat out unless `--write-pause-ms` is given. The seed graph is
built in memory and not persisted; RocksDB holds only what the `persist` phase
writes. The default tenant's quota is lifted, because a 1M-node ceiling is
passed within seconds of ingestion and every later write would be refused.

Options: `--people`, `--fanout` (seed graph), `--readers`, `--phase-secs`,
`--batch` (rows per statement), `--value-bytes`, `--write-pause-ms`,
`--sample-ms` (stall sampling interval, default 50), `--data-dir` (default: a
temporary directory).

## Reading the output

Per phase:

- **read latency** -- p50/p95/p99/max per read, from before the read lock is
  requested to after the result is built, so time queued behind the writer is
  included.
- **read lock wait** -- the queued part alone. If `persist` raises read latency
  and the lock wait accounts for the rise, the cost is the lock being held, not
  the read being slower.
- **write lock held** -- per statement, from acquiring the write lock to
  releasing it. In `persist` this includes `apply_mutations`.
- **apply_mutations** -- the persist call alone, inside the lock. Its p99 and
  max against the `memory` phase's lock hold is the share of the hold that is
  RocksDB's.
- **rocksdb** (persist only) -- what the sampler saw: samples in which writes
  were stopped (`rocksdb.is-write-stopped`) or delayed
  (`rocksdb.actual-delayed-write-rate` non-zero, with the lowest rate seen),
  and the maxima of `rocksdb.num-files-at-level0`,
  `rocksdb.estimate-pending-compaction-bytes`,
  `rocksdb.num-immutable-mem-table` and `rocksdb.num-running-compactions`.

Then read p50/p95/p99 as ratios to `baseline`, and `persist/memory`. The line
after it says whether RocksDB pushed back at all. **If it did not, the run
measured the cost of persisting under the lock, not the cost of stalls** --
raise `--value-bytes` or `--phase-secs` until it does, and say which you are
reporting. Last, the stall lines of `rocksdb.dbstats` (cumulative and interval
stall time) are printed as RocksDB formats them.

How to read the combination:

| `persist/memory` at p99 | stalls sampled | reading |
|---|---|---|
| near 1 | either | persistence does not move read tails at this load |
| above 1 | none | persisting under the lock costs readers, but not because RocksDB stalled: it is the ordinary write cost |
| above 1, rising with stall samples | yes | stalls reach readers through the write lock -- the case #2128 asks about |

Shortening the lock's scope (persisting after it is released) changes the
ordering and durability argument in #1274 and is a separate decision; this
bench only measures what the current scope costs.

## Monitoring

The same RocksDB properties are served on `/metrics` when the server runs with
persistence, as gauges read at scrape time:

| metric | property |
|---|---|
| `samyama_rocksdb_write_stopped` | `rocksdb.is-write-stopped` |
| `samyama_rocksdb_delayed_write_rate_bytes` | `rocksdb.actual-delayed-write-rate` |
| `samyama_rocksdb_l0_files` | `rocksdb.num-files-at-level0`, fullest column family |
| `samyama_rocksdb_pending_compaction_bytes` | `rocksdb.estimate-pending-compaction-bytes`, summed |
| `samyama_rocksdb_running_compactions` | `rocksdb.num-running-compactions` |
| `samyama_rocksdb_running_flushes` | `rocksdb.num-running-flushes` |
| `samyama_rocksdb_immutable_memtables` | `rocksdb.num-immutable-mem-table`, summed |

A scrape sees the state at that instant, so a short stall between scrapes is
missed. A cumulative stall-time counter needs RocksDB statistics enabled
(`Options::enable_statistics`), which costs on every write; it and a
persist-duration histogram on `/metrics` are left for a follow-up.

## Results

To be filled in from a `--release` run on a quiet host: host, disk,
`SAMYAMA_FSYNC`, the command line, and the bench's output verbatim.
