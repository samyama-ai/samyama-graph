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

**The header can name its query catalog** (`queries: {file, sha256}`, added
2026-09-30 for #1154). The `.sgqueries` catalog — the executable form of KG-08
and DX-08 — ships *beside* the snapshot, not inside it: templates are edited far
more often than the data changes, and regenerating a multi-GB artifact to fix a
Cypher string is not a good trade. The header records the catalog's bare file
name (resolved against the snapshot's directory) and the SHA-256 of its bytes as
published, so `sha256sum` on the release asset gives the same string.

- **Set after export, by `samyama catalog-build … --link`.** The catalog is
  built by running queries against the snapshot, so it cannot exist when the
  snapshot is written. `--link` rewrites line 0 only; every later line is copied
  through unchanged, so the restored graph is the same. An encrypted snapshot
  is refused — link before encrypting.
- **Checked by `samyama verify`.** With a link, `--queries` may be omitted and
  the named file beside the snapshot is used; either way a catalog whose SHA-256
  differs is refused before any query runs, and a named catalog that is missing
  is an error, not a skipped check. `samyama catalog-gate <catalog> --snapshot
  <file>` refuses a pair that is unlinked or mismatched.
- **Additive, no version bump.** Absent means no catalog is promised, and the
  key is not written at all, so a snapshot without a catalog is byte-for-byte
  what it was before. Older readers ignore the key.
- **Editing the catalog means re-linking.** That rewrites the header (a
  recompression pass, not a re-import); the old digest no longer matching is
  the point of the field.
- **The catalog, not the header, says whether it may be published** (#1159).
  `catalog-build` stamps it with the snapshot's `tenant` and `publishable`
  (`true` only under `--release`), and `catalog-gate` refuses anything else.
  Stamped in the catalog because the stamp is about the questions, and the
  header's SHA-256 already binds the two; see `docs/DATA-HANDLING.md`.
- **A release catalog's samples are checked against the data's licence**
  (#1159, TRUST-03). A sample is drawn from the graph, so it is an excerpt:
  `catalog-build --release`, and `catalog-gate --snapshot`, refuse a string
  sample or enum value found only on rows marked `__redistributable = false`
  or derived from one (`samyama::provenance`).

**Each catalog entry records its work** (`work`, added 2026-10-01 for #1156):
the rows every operator produced, summed, when the entry ran with its sample
values at build time. `run_template` holds a call with caller-supplied values to
`max(10 x work, 10,000)` rows and refuses past it, naming the values. Measured
rather than read off the planner because the default planner path records no
plan cost. Optional in the file, so older catalogs still load and verify, but
`catalog-gate` refuses a release catalog with an entry that lacks it.

**Catalogs ship with three published KGs** (#1154): `case_studies/<kg>/<kg>.sgqueries`
for health-systems, dbms-research and surveillance, built from each
`questions.json` with `--release`, and checked weekly against the pinned
published snapshot by `.github/workflows/kg-catalogs.yml` (`catalog-gate
--kg08` and `verify`). `samyama queries run <catalog> --snapshot <s> --entry
<id> --param k=v` runs one template through `run_template`. The published
snapshots predate the header link, so linking them is a re-upload of each
asset with `catalog-build ... --link`; until then `verify` takes `--queries`.

**A read-only snapshot may carry materialized results** (`read_only`,
`results: {file, sha256}`, added 2026-10-01 for #1158). A second sidecar,
`<kg>.sgresults` (`samyama.results/1`), beside the catalog; the body format does
not change, and neither key is written unless set, so every existing snapshot
is byte-for-byte what it was.

- **Read-only only.** `samyama snapshot-read-only` sets the flag; `results-build`
  refuses without it, and nothing is served for a snapshot without it.
  `snapshot-read-only --off` withdraws the flag and unlinks the results.
- **Bound to the epoch.** The loaded results bind to the store's epoch right
  after restore. The first write bumps it, and from then on nothing is served
  and a warning is logged once — the result cache's coarse rule, with no
  dependency tracking.
- **Capped, smallest first.** A per-result cap (64 KiB by default) and a total
  cap: the lesser of `--max-total-bytes` and `--max-total-pct` of the snapshot
  file (5% by default). Answers are admitted smallest first, so scalars and
  aggregates go in before row sets, and what did not fit is listed with the
  reason. `results-build` and `verify` both print what was used against both caps.
- **Re-verified.** The build executes every entry and refuses the whole file
  if any answer disagrees with the catalog. `verify` re-executes every stored
  answer. Loading refuses a file whose bytes are not the linked ones, which
  answers a different catalog, or whose stored rows no longer match their own
  SHA-256.
- **Exact match, bypassable, disclosed.** An answer is served only for its own
  Cypher with exactly its sample values. `queries run --computed` bypasses it,
  and the trailer line says `"source": "materialized"` or `"computed"`
  (TRUST-06). Only `queries run` consults the file: the query engine, the
  server and every benchmark never do, so a benchmark measures the engine, not
  the file.

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
