# ADR-030: Bandwidth Accounting and Operator Observability

## Status
**Proposed** (2026-05-05) — files the consolidated metric / observability gap surfaced in §7.7 and §7.8.

## Date
2026-05-05

## Context

Several operational gaps observed across the audit are individually small but collectively undermine the operator-debug story:

- **Disk bandwidth** (Samyama WAL, RocksDB WAL, RocksDB compaction, snapshot I/O) has no unified per-flow metrics. Operators read RocksDB stats files and correlate with timestamps manually.
- **Network bandwidth** (Raft RPC, RESP client traffic, snapshot transfer, external S3/GCS) has no per-flow surface. A runaway loader can saturate the WAN link to S3 and block other tenants without any visible signal.
- **Per-tenant memory accounting** is approximate (§6.4). A noisy tenant kills the process; we cannot pre-emptively detect.
- **Plan cache hit rate, catalog generation churn, RocksDB block cache hit rate** — none surfaced as Prometheus / OTel metrics.
- **PROFILE shows row counts** per operator but not memory or wallclock per operator (§4.3 §4.4).
- **Conflict-abort distinct from other errors** — #8 on the design-debt triage; tied to this ADR because the protocol layer needs to surface it as a structured error type.
- **Compaction storms** surprise operators; no admission control, no signal.
- **NUMA-unaware** thread / allocation pinning (§7.7).
- **Container-aware tokio worker count** missing (§5.3).

## Decision

We will land a consolidated metrics + observability surface across three layers.

### Layer 1 — Per-flow bandwidth counters

Prometheus-compatible counters for every byte that crosses an interesting boundary:

- `samyama_wal_bytes_total{kind="samyama|rocksdb"}`
- `samyama_rocksdb_compaction_bytes_total{level}`
- `samyama_snapshot_bytes_total{direction="export|import"}`
- `samyama_raft_rpc_bytes_total{direction="in|out", peer}`
- `samyama_resp_bytes_total{direction="in|out", tenant}`
- `samyama_external_io_bytes_total{provider="s3|gcs|http"}`

### Layer 2 — Per-operator profiling

Extend `PROFILE` to report per-operator wallclock and peak memory in addition to row counts. Surface as:

- A new `PROFILE` text format with a column table.
- Optional structured `PROFILE` export (JSON) for tooling.

### Layer 3 — Per-tenant accounting

Allocator-tagged memory accounting per tenant (e.g., a thread-local current-tenant tag picked up by `MiMalloc` hooks, summed periodically). Surfaces:

- `samyama_tenant_memory_bytes{tenant}`
- `samyama_tenant_query_count_total{tenant}`
- `samyama_tenant_active_txns{tenant}`
- `samyama_mvcc_history_total{tenant}` (ties into ADR-020 GC follow-up)
- `samyama_mvcc_horizon{tenant}` (ties into ADR-020 GC follow-up)
- `samyama_catalog_generation{tenant}` (ties into §2.4 warmup gap)
- `samyama_plan_cache_hit_total{tenant}` / `samyama_plan_cache_miss_total{tenant}`

### Layer 4 — Structured errors

Distinct error codes at the protocol layer:

- `CONFLICT_ABORT` for MVCC conflict aborts (separate from `SYNTAX_ERROR`, `EXEC_ERROR`, `TIMEOUT`, `QUOTA_EXCEEDED`, `AUTH_DENIED`).
- RESP error format becomes `-<CODE> <message>\r\n` (RESP simple-error compatible; clients that parse the first token get the code, others get a string message).

### Layer 5 — Admission control

Two admission gates:

- **Compaction admission**: when RocksDB level-0 → level-1 compactions back up past a threshold, throttle write admission until the queue drains.
- **Per-tenant token bucket**: each tenant gets a configurable rate-limit on query admission and on bytes written; configurable per quota tier.

### Layer 6 — Operational defaults

- **Container-aware tokio worker count**: detect cgroup CPU quota (Linux) and use it as the default `TOKIO_WORKER_THREADS` value when not explicitly set.
- **NUMA-aware thread pinning**: detect multi-socket hosts; pin the Raft I/O loop and the executor pool to the same socket as the bulk of the graph state.

## Consequences

### Positive
- Single coherent operator-facing surface.
- Existing pain points (debug compaction storms, debug noisy tenants, debug WAN-link saturation) become visible.
- PROFILE becomes useful for performance investigations, not just row-count sanity checks.
- Conflict-abort errors become actionable from the client side.
- NUMA + cgroup defaults are the kind of "should-have-been-there-from-day-one" fixes that disproportionately help large customer deploys.

### Negative
- **Wide implementation surface.** Touches persistence, raft network, protocol, executor, runtime. Probably a multi-month effort.
- **Allocator-tagged accounting is invasive.** Either a custom allocator or careful instrumentation at every `Vec::with_capacity` / `HashMap::new` call site. The first is cleaner but constrains us to allocators that support tags (`MiMalloc`, `jemalloc` with profiling).
- **Admission control adds a new failure mode**: well-behaved tenants can be throttled when an unrelated compaction storm fires. Tuning the thresholds is delicate.
- **Structured error codes break clients** that match against current error strings. We will gate on a `HELLO 3 SAMYAMA_ERR_CODES` capability negotiation per §6.3 follow-up.

### Neutral
- All metrics are opt-in to scrape; the engine emits, the operator chooses to consume.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| Per-component point fixes (one PR per metric) | Drift; missed cross-cutting items; long tail of "why isn't X surfaced" tickets. |
| Push-only OpenTelemetry tracing (no Prometheus counters) | Metrics serve a different question than traces. We need both eventually; counters first. |
| Out-of-process collector reading RocksDB stats files | Doable today; doesn't help the per-tenant memory or per-operator wallclock cases. |
| Skip admission control; rely on quotas | Quotas don't catch the system-wide events (compaction storm, snapshot transfer). |

## Follow-ups (build order)

1. **Layer 4 (structured errors)** — small, unblocks conflict-abort distinguishing immediately.
2. **Layer 1 (bandwidth counters)** — pure additive; high operator value.
3. **Layer 2 (PROFILE extension)** — touches the executor but is bounded.
4. **Layer 6 (operational defaults)** — small wins.
5. **Layer 3 (per-tenant memory)** — most invasive; do last.
6. **Layer 5 (admission control)** — gated on Layer 1 metrics being in place to drive thresholds.

## References

- Wiki: [[topics/systems-cpu-ram.md]], [[topics/systems-disk-network.md]], [[topics/concurrency-tokio-runtime.md]], [[topics/concurrency-conflict-detection.md]], [[topics/protocol-resp.md]], [[topics/distributed-multitenancy.md]].
- Related ADRs: ADR-010 (Observability Stack — anchors logging / tracing; this ADR extends it with metrics + admission control), ADR-020 (MVCC — GC metrics tie in here), ADR-023 (WAL — bandwidth counters tie in), ADR-008 (Multi-Tenancy — per-tenant accounting), ADR-029 (IndexManager).
