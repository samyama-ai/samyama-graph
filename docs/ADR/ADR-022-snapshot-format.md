# ADR-022: Snapshot Format (`.sgsnap`)

## Status
**Shipped** (v1.0.0, 2026-04-11)

## Date
2026-05-05

## Context

Samyama needs a portable, point-in-time, single-tenant graph export usable for:

- Distribution of pre-built KGs via GitHub Releases / S3.
- Cold-start of fresh AWS spot VMs in minutes vs hours.
- Tenant migration between clusters without reconciling Raft logs.
- Backup.

Existing options either lock us to a vendor (Neo4j dump-load), to a RocksDB major version (`BackupEngine`), or to a binary format that's hard to inspect without tooling (Cap'n Proto, Parquet).

## Decision

We will use a **gzip-compressed JSON-Lines** format with a header line and one record per node/edge.

```
gzip( header\n catalog\n [hierarchy\n ...] node\n node\n ... edge\n edge\n ... )
```

Format version is currently **v2** (see [[storage-snapshot-format.md]] §How it works for v1 → v2 migration history). The header carries: `format`, `version`, `tenant`, counts, label/edge-type lists, ISO timestamp, samyama version.

**The header carries a loss report** (`dropped`, added 2026-09-20 for INT-06).
One row per thing this export did not carry, with a count and what it means for
the restored graph — today that is edge creation timestamps. A row appears only
when there was something to lose, so an empty graph produces an empty list: a
standing list of everything the format *could* drop is a disclaimer, and nobody
reads those. The field is additive and the format version does not move.

**The file carries the index catalog** (`"t":"i"`, added 2026-09-29 for #1506).
One line right after the header, holding every property index, unique
constraint, full-text index and vector index the exporting store declared, as
the same `IndexDefinition` records the RocksDB catalog persists (ADR-029). Until
then the loss report listed these declarations as dropped, and the HTTP import
*rediscovered* vector indexes from embedding-shaped properties — which lost the
index's name and quantization, forced the metric to cosine, and invented an
index over any float list nobody had indexed.

- **Declarations only; import rebuilds.** Import re-declares each definition
  after the rows are in and builds it from them. No index contents are in the
  file, so a restored index cannot disagree with the rows it indexes.
- **Written even when empty.** Presence is the signal: a file with the line says
  exactly which indexes exist, including "none", and import declares those and
  no others. A file without it (written before #1506, or by another tool) says
  nothing, and `/api/snapshot/import` keeps the old rediscovery for it.
- **No version bump.** The line is additive, like the hierarchy declarations
  (`"t":"h"`, ADR-035): an older reader skips it and behaves exactly as it did
  before — it rediscovers vector indexes and restores nothing else — so a new
  file on an old build is no worse than an old file was.
- **Collisions keep the target's definition.** A definition identical to one on
  the target is re-declared (which rebuilds it over old and imported rows
  alike). One that differs — the same vector (label, property) at another
  dimension, metric, quantization or name, or a full-text name bound to another
  label or property — is skipped and counted in `ImportStats::index_conflicts`:
  the rows already there were indexed under the target's definition.

Properties are encoded as JSON values. Edge records ("stub edges") carry only `id, src, tgt, type, props` — endpoint/type metadata, no creation timestamps in v2.

## Consequences

### Positive
- Streamable in O(1) memory per line on both export and import.
- Human-debuggable (`zcat foo.sgsnap | head | jq`).
- Works with existing `gh release upload` / S3 tooling.
- v2 is 30–60 % smaller than v1 because edge stubs replace fully-populated edge records.

### Negative
- **2–3× larger** than a binary equivalent (Cap'n Proto). At KG sizes >100 GB this is real S3 egress money.
- **No checksum** on the body. A truncated upload decompresses cleanly to a partial graph and silently imports.
- **No explicit format-version magic** beyond the header line; if a tool strips the header (e.g., concatenation) the importer has no way to detect it.
- **Edge timestamps are dropped** in v2 (`created_at` / `updated_at` for edges).
- JSON typing flattens Cypher types (Int 32 vs 64; Date vs DateTime).

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| RocksDB `BackupEngine` | Bit-perfect but locked to RocksDB major version, blocks columnar-on-import. |
| Cap'n Proto / FlatBuffers | Faster, smaller, schema-versioned — but no human-readable debug story. Re-evaluate as `binary: true` mode in v3. |
| Parquet / Arrow | Columnar export — strong analytics fit, awkward for high-cardinality edge type. Future analytics-export candidate. |
| Neo4j-style dump-load | Closed format, vendor lock-in. |

## Follow-ups (proposed v3)

1. **SHA-256 footer line** over the body. CLI `sgsnap verify`.
2. **Optional binary mode** via header flag `binary: true` → length-prefixed Cap'n Proto frames.
3. **Optional Zstd compression** alongside gzip (~20 % ratio improvement, faster).
4. **Round-trip edge timestamps** (additive — v2 readers ignore the new fields).
5. **Magic byte segment header** to survive concatenation / corruption at the file head.

## References

- Code: `samyama-graph/src/snapshot/format.rs`, `src/snapshot/persist.rs`
- Wiki: [[storage-snapshot-format.md]], [[reference_snapshots_s3.md]]
- Related ADRs: ADR-024 (Edge Arena Removal — drove the v1 → v2 stub-edge transition), ADR-021 (Columnar Property Store — properties land in columns on import).
