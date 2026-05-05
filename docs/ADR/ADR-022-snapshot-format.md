# ADR-022: Snapshot Format (`.sgsnap`)

## Status
**Proposed** (2026-05-05)

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
gzip( header\n node\n node\n ... edge\n edge\n ... )
```

Format version is currently **v2** (see [[storage-snapshot-format.md]] §How it works for v1 → v2 migration history). The header carries: `format`, `version`, `tenant`, counts, label/edge-type lists, ISO timestamp, samyama version.

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
