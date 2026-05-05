# ADR-023: WAL Versioning, Recovery, and Double-WAL Reconciliation

## Status
**Proposed** (2026-05-05)

## Date
2026-05-05

## Context

Samyama operates **two write-ahead logs**:

1. **Samyama's logical WAL** (`src/persistence/wal.rs`) records graph operations (`CreateNode`, `UpdateNodeProperties`, etc.).
2. **RocksDB's internal WAL** records physical KV operations against RocksDB.

In sync mode, every committed graph mutation pays two `fsync`s. The logical WAL also has a quiet correctness gap:

- The "checksum" computed at `wal.rs:147` is XOR-of-bytes, not CRC32. It misses transpositions and is much weaker than the docstring claims.
- There is no overall WAL format version: per-entry variants gained an MVCC `version` field in v1.0, but the envelope and segment layout have no version magic.
- Segment rotation is unimplemented; long uptimes accumulate one large file with slow recovery.

## Decision

We will:

1. **Add real CRC32C** (the `crc32c` crate, hardware-accelerated on x86 + ARM64). Replace the XOR fold.
2. **Add a 16-byte segment header**: 4-byte magic (`SGWL`), 4-byte format version, 8-byte sequence range start.
3. **Implement segment rotation** at 256 MB or every checkpoint, whichever first. Old segments retained until checkpoint covers them.
4. **Add envelope version field** to `WalRecord`. Existing records (without the field) read as version 0 via `#[serde(default)]`.
5. **Plan to disable RocksDB's internal WAL** (`disable_wal=true`) once Samyama's WAL is verified durable and group-commit batching is in place. **This step requires Samyama's WAL to flush before the RocksDB write, not after**, to preserve the recovery invariant. Tracked as v1.1 work.
6. **Add a sequence-monotonicity check at replay**: if a record's sequence is not exactly `prev + 1` we stop and require operator intervention.

## Consequences

### Positive
- Hardware-accelerated checksum closes the transposition gap.
- Segment rotation bounds recovery time.
- Format version on the envelope unblocks future schema evolution.
- Disabling RocksDB WAL halves sync cost in steady state.

### Negative
- Disabling RocksDB WAL is correctness-load-bearing; one ordering bug can lose committed data. We must add invariants tests around the flush ordering before flipping the flag.
- Segment rotation introduces a new failure mode: orphaned segments after a crash mid-rotation. We will rely on sequence-range gaps in segment headers for post-crash audit.

### Neutral
- Existing `.wal` files continue to read because the `serde(default)` envelope-version field treats them as v0.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| Skip Samyama's WAL, rely solely on RocksDB's | RocksDB WAL is keyed by physical KV pairs, not graph ops. Replay would need to re-derive graph operations from KV diffs. |
| Replace bincode with Avro/Parquet logs | Replay is row-by-row; columnar logs gain nothing. Adds schema-registry concept. |
| Per-tenant WAL files | File-handle pressure scales with tenant count (>1 K tenants would exhaust handle limits). |

## Follow-ups

1. Land CRC32C + envelope version (small, do first).
2. Land segment rotation (medium).
3. Land group-commit batching (medium; closes most of the sync/async gap).
4. Land `disable_wal=true` for RocksDB (large; requires invariants).

## References

- Code: `samyama-graph/src/persistence/wal.rs`, `src/persistence/storage.rs`
- Wiki: [[storage-wal.md]], [[storage-rocksdb-lsm.md]]
- Related ADRs: ADR-002 (RocksDB), ADR-020 (MVCC — provides the per-entry `version` field that already round-trips through the WAL)
