# ADR-036: Data Load and Time-to-Ready Optimization

## Status
**Proposed**

## Date
2026-07-31

## Objective

Improve data-ingestion throughput and reduce the total time from "start loading a dataset" to "the dataset answers queries correctly".

## Context

### The problem

The load path is sequential end to end:

```text
Load all data
    ↓
Build indexes
    ↓
Validate
    ↓
Dataset ready
```

The loaders in `examples/` (`ldbc_loader`, `imdb_loader`, `football_loader`, `aact_loader`, `cricket_loader`, …) drive ingestion single-threaded — none of them use `rayon` or spawn workers — so wall-clock time to ready is the sum of every stage.

Suspected sources of avoidable overhead, none of them yet confirmed by profiling:

* Single-threaded ingestion
* Small write operations
* Frequent commits
* Repeated serialization
* Limited batching
* No overlap between loading and index construction

**This ADR should not be accepted until stage-level profiling exists.** Everything below is a hypothesis about where the time goes; the first task is to replace that guess with measurement.

### The structural constraint

`GraphStore` mutation is `&mut self` — `create_node`, `create_edge`, `set_property` all take exclusive ownership. That makes the store a single-writer structure by construction, so "parallel node writes" and "parallel relationship writes" are **not** reachable by adding worker threads to the existing API. Any parallel-write design must first answer one of:

* parse/transform in parallel, apply serially (parallelism on the expensive half only — cheapest change, and likely enough if profiling shows parsing dominates);
* shard the store and merge (see ADR-009 partitioning); or
* introduce interior mutability with per-partition locking on the hot maps.

Naming the constraint up front prevents an implementation attempt that discovers it at the type-checker.

Note also that a fast bulk path already exists: `create_node_stub` (`src/graph/store.rs:1002`) bypasses the event loop for snapshot import, and `rebuild_vector_index_full` (`:2092`) rebuilds indexes after a bulk insert. Measure against that path before building a new one — some of the wins below may already be available through snapshot import (ADR-022).

## Decision

### 1. Profile the load path first

Add stage-level timing and throughput metrics for: file reading, parsing, validation, node creation, relationship creation, transaction commits, serialization, storage writes, index construction. Publish a baseline for at least two datasets before optimizing anything.

### 2. Batch data writes

Process nodes and relationships in configurable batches, with sizes chosen by benchmark rather than assumption.

### 3. Add safe parallelism

Separate parsing from writing, with bounded queues so memory stays capped. Given the constraint above, the realistic first shape is *parallel parse → serial apply*; anything beyond that requires the ownership decision named in Context.

### 4. Reduce serialization overhead

Remove repeated JSON parsing, duplicate object conversions, repeated property normalization, and unnecessary temporaries. Use streaming parsing where the input format allows.

### 5. Overlap index construction

Investigate incremental index updates during load, per-batch/per-partition index builds, or starting index construction while the tail of the data is still loading. The dataset must only be marked ready once all required indexes are complete and validated.

### Recommended flow

```text
Read and parse input (parallel)
    ↓
Create batches
    ↓
Write nodes (serial into GraphStore)
    ↓
Write relationships
    ↓
Build indexes incrementally or in parallel
    ↓
Validate data and indexes
    ↓
Mark dataset ready
```

## Implementation Tasks

* [ ] Profile the current ingestion pipeline and publish a baseline
* [ ] Add stage-level timing and throughput metrics
* [ ] Benchmark the existing snapshot-import path as the comparison point
* [ ] Add configurable node and relationship batch sizes
* [ ] Add parallel parsing with bounded queues ahead of serial apply
* [ ] Decide and record the parallel-write ownership approach (or rule it out)
* [ ] Reduce repeated serialization and transformation
* [ ] Investigate incremental indexing
* [ ] Implement load/index overlap where safe
* [ ] Add dataset integrity validation
* [ ] Add performance and correctness tests

## Dataset States

No dataset lifecycle state exists in the engine today — this introduces new API surface (and, if it must survive restart, new persisted state). Proposed states:

```text
CREATED
LOADING_NODES
LOADING_RELATIONSHIPS
INDEXING
VALIDATING
READY
FAILED
```

A dataset must not enter `READY` until data loading is complete, required indexes are complete, and validation passes. Open questions to settle before implementing: is the state per-tenant or per-store, is it exposed over RESP and HTTP, does it survive restart, and what does a query against a non-`READY` dataset do — block, error, or serve partial results?

## Configuration

Samyama configures through environment variables and `ServerConfig` (`src/main.rs`), not a YAML file. Ingestion knobs should follow that convention, for example:

```sh
SAMYAMA_INGEST_NODE_BATCH_SIZE=5000
SAMYAMA_INGEST_REL_BATCH_SIZE=5000
SAMYAMA_INGEST_PARSER_WORKERS=4
```

Every knob defaults to current behavior, and every default is justified by a benchmark.

## Testing

Benchmarks must cover:

* At least two dataset sizes
* Multiple batch sizes
* Different worker counts
* Sequential index build vs. overlapping/incremental build

Validate node count, relationship count, property values, index-entry count, indexed query results, and the absence of missing or duplicate records.

## Consequences

**Easier:** large datasets become usable sooner; ingestion cost becomes visible per stage instead of a single opaque number; batch sizes become tunable for a given machine.

**Harder:** a staged, parallel loader has more failure modes than a sequential one — partial loads, a dataset stuck in `INDEXING`, and back-pressure tuning all become real. The `FAILED` state implies a cleanup story that does not exist yet. Bounded queues trade peak throughput for predictable memory, and that trade needs to be a documented default rather than an accident.

## Acceptance Criteria

* [ ] A profiling baseline exists and is published before optimization work lands
* [ ] Data-load throughput improves measurably against that baseline
* [ ] Batched writes reduce transaction overhead
* [ ] Parallel parsing improves throughput where supported
* [ ] Memory usage remains bounded under back-pressure
* [ ] No records are lost or duplicated
* [ ] Index construction overlaps with loading or runs incrementally
* [ ] Index correctness is preserved
* [ ] Total time-to-ready is measurably reduced
* [ ] At least two dataset sizes are benchmarked
* [ ] The optimized loader produces a byte-identical dataset to the existing loader

## Out of Scope

* Multi-hop query optimization — see [ADR-040](./ADR-040-multi-hop-query-optimization.md) (filed as ADR-035; renumbered in #1515)
* Query-planner changes
* Hardware scaling
* Distributed ingestion
* Unrelated correctness defects

## Related Decisions

* [ADR-002](./ADR-002-use-rocksdb-for-persistence.md) — RocksDB persistence
* [ADR-009](./ADR-009-graph-partitioning-strategy.md) — partitioning, if sharded ingestion is pursued
* [ADR-021](./ADR-021-columnar-property-store.md) — columnar property store (the write target for bulk scalar properties)
* [ADR-022](./ADR-022-snapshot-format.md) — snapshot import, the existing fast bulk-load path
* [ADR-029](./ADR-029-index-manager.md) — index construction
