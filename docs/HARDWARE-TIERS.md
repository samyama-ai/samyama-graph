# Hardware tiers

The five tiers Samyama is meant to run on (HW-01): what each one is, the
resources it is defined by, the features it supports, and the test suite that
has to pass for a build to claim it.

**This page defines the tiers. It does not show that every tier works.** Most
resource figures below are targets nobody has measured yet, and three tiers have
no hardware in CI. Each one says which, and which issue measures it. Read
"target" as a promise not yet checked, not as a result.

| Tier | Reference hardware | Resources (target) | Graph size (target) | Feature set | Primary use |
|---|---|---|---|---|---|
| **Server tier** | Vultr 192 GB, AWS r6i | ≥16 cores, ≥64 GB | to 1B+ edges | everything; HA when it exists | production, federation |
| **Workstation tier** | M4 Mac Mini, RTX 4050 laptop | 8–16 cores, 16–64 GB, optional GPU | to 100M edges | everything single-node, plus GPU | development, demos, papers |
| **Edge tier** | Raspberry Pi 5 8 GB, Jetson Orin Nano | 4 cores, 4–8 GB | to 10M edges | Cypher, vector, hierarchy, algorithms; no HA | on-premise gateway, factory floor, clinic |
| **Micro tier** | Pi Zero 2 W, Jetson Nano | 1 core, 512 MB | to 100K edges | read-only snapshot serving, Cypher reads | sensor-adjacent lookup, offline kiosk |
| **Browser tier** | WASM in a tab | — | to 1M edges | read-only Cypher over a fetched `.sgsnap` | exploring a published KG with no server |

The table is the one in the product spec,
`samyama-cloud/docs/product/spec/05-hardware-portability.md` §4. The spec owns
the targets. This page owns what the engine does about them.

## What has been measured

| Tier | Figure | State | Measured by |
|---|---|---|---|
| Server tier | an algorithm at 1B edges | not measured | ALGO-07, #1685 |
| Server tier | cold start under 2 s at 10M edges | not measured; restore dominates time to first answer | HW-06, #1174 |
| Server tier | binary ≤50 MB | not measured as a gate | HW-05, #1623 |
| Workstation tier | 100M edges on 16–64 GB | not measured | — |
| Edge tier | Pi 5, 1M-edge KG, p95 <100 ms on a 50-query set | not measured; no Pi reachable, no query set yet | HW-03, #1678 |
| Edge tier | feature-minimal build ≤15 MB | no such build exists | HW-05, #1623 |
| Micro tier | 100K edges read-only in 512 MB on one core, no swap | not measured; deferred to H2 by the spec | HW-04, #1679 |
| Browser tier | a `.sgsnap` queried in a tab | no WASM build exists; deferred to H2 by the spec | HW-14, #1692 |

Nothing in this table is measured yet. When one is, the row gains the number,
the commit and the hardware, and drops "not".

## Builds

Every tier runs the same binary today, built from the same features:

| Tier | Cargo features | Note |
|---|---|---|
| Server tier | `default` (`mimalloc`, `otel`) | |
| Workstation tier | `default`, plus `gpu` (wgpu) or `cuda` when a GPU is present | `cuda` needs a CUDA toolkit at build time |
| Edge tier | `default` | the feature-minimal edge build HW-05 asks for does not exist (#1623) |
| Micro tier | `default` | the same binary; nothing is cut down for 512 MB yet |
| Browser tier | none | there is no `wasm32` target (#1692) |

## Feature manifest

What each tier must do, and the tests that show it. This table is read by
`scripts/tier_suite.py`, which runs one tier's tests, and checked by
`tests/hardware_tiers_documented.rs`, which fails if a test it names does not
exist or a row breaks the rules below. Change the table and both follow.

The statuses:

- **required**: part of the tier's feature set; its tests must pass for a build to claim the tier.
- **optional**: tested when the tier's hardware has it (a GPU), skipped otherwise.
- **unsupported**: declared not to work on this tier. A tier never degrades silently: a capability it cannot offer is listed here, not left out.
- **gap**: the spec puts it in the tier's feature set and the engine does not have it yet. The note names the issue or requirement that tracks it.
- **—**: not part of this tier's feature set.

A test is either an integration test in `tests/` (`name`, run as `cargo test --test name`) or a test in another workspace crate (`crate+feature::filter`, run as `cargo test -p crate --features feature filter`).

<!-- tier-manifest:start -->
| Capability | Tests | Server tier | Workstation tier | Edge tier | Micro tier | Browser tier | Note |
|---|---|---|---|---|---|---|---|
| Cypher reads | `advanced_cypher` `traversal_from_cypher` `cypher_projection_semantics` | required | required | required | required | gap | Browser: no WASM build, #1692 |
| Cypher writes and transactions | `mvcc_isolation_anomalies` `eager_before_delete` `create_edge_allocations` | required | required | required | unsupported | unsupported | Micro and Browser serve reads only |
| Read-only queries (`GRAPH.RO_QUERY`, `read_only: true`) | `resp_audits_writes` `materialized_results` | required | required | required | required | gap | Browser: #1692 |
| Server-wide read-only serving | | — | — | — | gap | gap | no mode refuses every write; read-only is per request today. Micro: HW-04, #1679. Browser: #1692 |
| RESP and HTTP server | `server_binary` `resp_errors_are_one_line` `http_requires_a_credential` | required | required | required | required | unsupported | a browser tab opens no port |
| Snapshot import and export | `snapshot_persistence` `snapshot_verify` `snapshot_import_rollback` | required | required | required | required | gap | Micro gets its graph from a snapshot. Browser: fetch a `.sgsnap`, #1692 |
| Durable storage (RocksDB, WAL) | `recovery_reads_rocksdb_alone` `durability_is_a_choice` `snapshot_import_survives_restart` | required | required | required | — | unsupported | |
| Vector search (HNSW) | `vector_search_test` `vector_filtered_search` `vector_recall_floor` | required | required | required | — | — | |
| Hierarchy index | `hierarchy_index_test` `hier_cross_hierarchy` | required | required | required | — | — | |
| Graph algorithms | `algorithm_procedures` `centrality_from_cypher` `community_and_paths_from_cypher` | required | required | required | — | — | |
| Multi-tenancy and quotas | `tenant_registry_unification` `quota_is_admission_control` | required | required | — | — | unsupported | |
| Writes from a microcontroller over RESP | `esp32_sketch_speaks_resp` | — | — | required | — | — | see CONSTRAINED-CLIENTS.md |
| GPU acceleration | `samyama-graph-algorithms+gpu::gpu_parity` | optional | optional | — | — | unsupported | run where a GPU is present; parity, not speed (HW-08, HW-09) |
| HA failover and replication | | gap | — | unsupported | unsupported | unsupported | Raft is scaffolding only: writes apply on the local node, with no replication or quorum (REL-07) |
<!-- tier-manifest:end -->

## Running a tier's suite

```bash
python3 scripts/tier_suite.py --list                # every tier: what it runs, what it lacks
python3 scripts/tier_suite.py --tier edge --dry-run # the cargo commands, not run
python3 scripts/tier_suite.py --tier edge           # run them; exit 0 only if every required test passes
python3 scripts/tier_suite.py --tier workstation --with-optional   # also the GPU parity tests
```

It runs a debug build, as CI's `cargo test --workspace` does. With
`--with-optional` it sets `SAMYAMA_REQUIRE_GPU=1`, because the GPU parity tests
otherwise return early and pass on a host with no GPU. It exits 1 when a
required test fails and 2 when the tier has nothing to run (the Browser tier,
today), so a job for that tier cannot pass by testing nothing. The capabilities
marked gap are printed every time: a green run says "every required test
passed", never "the tier is complete".

## Where each tier is tested

| Tier | Runs in CI on | What that shows |
|---|---|---|
| Server tier | `test (ubuntu-24.04)`: the full workspace on x86-64 Linux | the suite passes; not on a 16-core, 64 GB host |
| Workstation tier | `platforms (macos-15)`, an Apple Silicon runner, and `platforms (windows-2022)` | the suite passes on macOS ARM64 and Windows. That job runs on every push and on pull requests into `main`, not on pull requests into `dev`. No GPU runner |
| Edge tier | nowhere on edge hardware. `test (ubuntu-24.04-arm)` runs the suite on ARM64 Linux, the Pi's architecture | ARM64 correctness only. It is a server-class runner: no thermal throttling, no SD-card I/O, no 4 GB limit. The spec requires a self-hosted Pi runner, not emulation |
| Micro tier | nowhere | — |
| Browser tier | nowhere; nothing to build | — |

**HW-01 asks for "CI per tier", and that does not exist yet.** No CI job runs
`tier_suite.py` for a tier, and the Edge and Micro tiers have no hardware to run
on. What exists is the full suite on four platforms, which covers every
required test above on x86-64 and ARM64 without saying which tier it was for.
Closing HW-01 needs:

1. a CI job per tier that runs `python3 scripts/tier_suite.py --tier <tier>`;
2. a self-hosted Raspberry Pi 5 runner for the Edge tier (CH-HW-TIER, spec §5);
3. a 512 MB single-core runner, or a device, for the Micro tier (deferred to H2);
4. a WASM build before the Browser tier has anything to run (deferred to H2).

The first is a workflow change. The other three need hardware.
