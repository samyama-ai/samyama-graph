# samyama-gpu — OSS port status

Scaffold for the GPU-to-OSS port (workstream `samyama-cloud/wiki/topics/gpu-oss-workstream.md`,
all-OSS decision 2026-07-22). Branch: `gpu-oss-port-phase1`.

## Landed in this scaffold ✅ (all compile; verified on RTX 4050)

- **Crate moved into OSS** — full `samyama-gpu` (kernels, wgpu shaders, **and `cuda/`** per the
  all-OSS decision), copied from SGE.
- **Relicensed** `Proprietary` → `Apache-2.0`; description de-enterprised.
- **License gate removed** — `GPU_LICENSED` / `enable_licensed()` deleted; `try_global()` now
  returns a context whenever hardware is present.
- **`SAMYAMA_GPU=off` kill-switch** — `GpuContext::is_enabled()`; forces CPU path (baseline + escape hatch).
- **Feature topology (first `[features]` in OSS):** root `gpu = [dep:samyama-gpu, algorithms/gpu]`,
  `cuda = [gpu, samyama-gpu/cuda]`; algorithms crate `gpu`/`serde` features. Verified: **default build
  pulls ZERO GPU deps**; `--features gpu` pulls `samyama-gpu`; `cudarc` only under `--features cuda`.
- **Dead scaffolding cleaned** — `benches/bench_setup.rs` rewritten (no license refs); previously-dead
  `serde` optional wired to an explicit feature.
- **`SAMYAMA_REQUIRE_GPU=1` hard gate** — `tests/gpu_smoke.rs::require_or_skip()` panics instead of
  silent-skipping when the var is set. Verified: fails correctly with GPU off + var set.

Build status: `cargo check` (default), `cargo check --features gpu --tests --benches`,
`cargo test -p samyama-gpu --test gpu_smoke` all green.

## Also landed (dispatch gates + parity — 2026-07-22) ✅

- [x] **4 dispatch gates ported** into `samyama-graph-algorithms`: `page_rank`, `cdlp`,
      `local_clustering_coefficient` (via `_directed`), `count_triangles`. `#[cfg(feature="gpu")]`
      size-threshold → `try_global()` → GPU with transparent CPU fallback. (PCA deferred per workstream.)
- [x] **`eprintln!` → `tracing::warn!`** on fallback; **`SAMYAMA_GPU_MIN_NODES`** knob via
      `gpu_dispatch::min_gpu_nodes()` (thresholds were tuned only on the 4050).
- [x] **CPU/GPU parity test** (`src/gpu_parity_tests.rs`, `#[cfg(all(test, feature="gpu"))]`):
      same `GraphView` through both paths; **PASSES on RTX 4050** — PageRank & LCC to 1e-6, triangle
      count exact, CDLP smoke. Sanity-guarded so it can't silently degrade to CPU-vs-CPU.
      Suite: **39 pass with `--features gpu`, 38 without** (gates cfg-out cleanly).

**⇒ E1 (per-operator ablation) is now runnable on SG.** `algo` calls route CPU→GPU in-engine.

## Paper 22 (B3) — unified-memory seam: **scaffolded + verified on RTX 4050 (2026-07-22)** ✅

- [x] **`samyama_gpu::unified::UnifiedBuffer<T>`** — `Host(Vec)` (default, any hardware) +
      `Managed(ManagedBuffer)` (`cuda`): one `cuMemAllocManaged` allocation addressable by both CPU
      (`as_slice`/`as_mut_slice`) and CUDA kernels (`device_ptr()`), **no host↔device copy**.
- [x] **Opt-in** via `SAMYAMA_GPU_UM=on`; host fallback so callers use it unconditionally.
- [x] **Verified on RTX 4050** (software UM): `unified::tests::managed_roundtrip_or_skip` allocates
      managed memory, CPU round-trips it, device pointer non-null. Same code path gets *coherence* on GH200.
- [x] **ADR-034** documents the design + the discrete(software-UM)→GH200(coherent) equivalence.
- [x] **All 4 operators' CSR plumbed through UM** (shared `cuda::csr_to_device`): under `SAMYAMA_GPU_UM=on`,
      CSR goes to a `ManagedBuffer` and the kernel reads it via `upgrade_device_ptr` — **no `cuMemcpyHtoD`**.
      **A/B on RTX 4050 (`e3a-um-vs-copy-...`), correctness exact for all:** UM/copy scales with
      compute-to-CSR-access ratio — PageRank/triangles ~1.18–1.25×, CDLP/LCC ~1.02–1.04×. Added `cuda`
      feature to the algorithms crate + `init_runtime`.
- [ ] **Next:** property buffers through UM; concurrent CPU-write/GPU-read sweep;
      **validate the coherent win on a GH200** (E3b — expect UM/copy < 1.0, most for memory-bound ops).

## Phase 2 + CI — **landed 2026-07-22** ✅

- [x] **`algo.cdlp` + `algo.lcc` Cypher procedures** — `execute_cdlp`/`execute_lcc` in
      `query/executor/operator.rs`, registered in both dispatch arms. `CALL algo.cdlp(label?, edge?,
      {maxIterations}?) YIELD node, communityId` and `algo.lcc(...) YIELD node, coefficient`. CPU-first
      (auto-GPU above threshold). **3 tests pass** (`test_algo_cdlp*`, `test_algo_lcc*`). This is the
      Phase 2 "biggest value item" — CDLP/LCC (where wgpu beats CUDA) were GPU-accelerated but
      unreachable from Cypher.
- [x] **GPU CI lane** — `.github/workflows/gpu-ci.yml`: self-hosted `[self-hosted, gpu]` runner,
      `SAMYAMA_REQUIRE_GPU=1`, PR runs gated behind the `gpu-ci` Environment (required reviewers).
      Runs gpu_smoke + parity + full `--features gpu` suite + CUDA unified tests. **Needs one-time
      setup:** register the vm-1 runner and configure the `gpu-ci` environment reviewers.

## Findings from the AWS L40S run (2026-07-23) — real port issues

- [x] **F1 FIXED (2026-07-23): GPU dispatch no longer no-ops on headless/CUDA-only machines.** New
      `samyama_gpu::gpu_available()` = `GpuRuntime::init().is_active()` (CUDA *or* wgpu, and it
      *initializes* the runtime). The 4 gates now use it instead of `GpuContext::try_global()` (wgpu-only),
      and the 4 entry points (`gpu_page_rank`/`gpu_cdlp`/`gpu_lcc`/`gpu_count_triangles`) dropped the
      `ctx: &GpuContext` param — the wgpu branch sources its context from `GpuRuntime::init().wgpu()`.
      **Bonus:** this also fixes GPU in the *production Cypher path* — before, nothing initialized the
      runtime for `CALL algo.*`, so CUDA was never reached. Verified: parity still passes on the 4050
      with the new gate; smoke + unified tests green; default build clean. (Vulkan loader now optional.)
- [ ] **F2: cudarc 0.12 caps at CUDA 12.5.** Fails to build against CUDA 12.8+/13.x. Pin a CUDA ≤ 12.5
      toolkit (`cuda-toolkit-12-4`) or bump cudarc. Document in the OSS build/reproduction notes.

## Remaining (minor / follow-up)

- [ ] **wgpu unit tests** for CDLP, LCC, topology, buffer (currently zero *kernel-level* coverage;
      parity + Cypher tests cover end-to-end).
- [ ] **Fix `lib.rs:6`** false "wired query operators" claim; tidy unused-import warnings in kernels.

## Workflow note
SG is GitHub-first (PR to `github.com/samyama-ai/samyama-graph`, then Gitea mirror). `main` is protected.
This branch is local/uncommitted — raise as a PR when ready.
