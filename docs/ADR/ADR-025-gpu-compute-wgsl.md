# ADR-025: GPU Compute — `samyama-gpu` crate, WGSL by default, CUDA opt-in

## Status
**Proposed** (2026-05-05) — retroactively documenting the v0.7+ shipped crate.

## Date
2026-05-05

## Context

Several graph and vector operations exhibit data-parallel structure that maps well to GPU compute:

- PageRank's edge-scatter update (the LDBC-compliance hot path).
- LCC (local clustering coefficient) — per-node triangle counting.
- CDLP (community detection via label propagation) — parallel label updates.
- Vector similarity (cosine, inner product) — the consumer of HNSW results.
- PCA / dimensionality reduction.

CPU implementations of these — even with SIMD and rayon parallelisation — cap at single-host memory bandwidth and core count. On the 1.29 B-edge hero workload, CPU PageRank takes ~10× longer than GPU PageRank on a single L40S.

The choice of GPU stack matters for portability: NVIDIA-only (CUDA) locks out our Mac Mini deployments and any deploy on AMD or Intel GPUs. Cross-vendor (WGSL via wgpu) gives portability at a small per-shader cost.

## Decision

We will operate a dedicated `samyama-gpu` crate (in the enterprise repo, `crates/samyama-gpu/`) with **WGSL via wgpu as the default backend** and **CUDA as an optional, feature-flagged backend** for benchmarking and NVIDIA-specific optimisation work.

### Crate boundaries

- `lib.rs` — typed public API per algorithm.
- `runtime.rs` / `context.rs` — `wgpu::Device` + `wgpu::Queue` lifecycle, kept alive across calls.
- `buffer.rs` — typed wrappers around `wgpu::Buffer`.
- `shaders/*.wgsl` — kernels.
- `cuda/` — opt-in feature flag; CUDA kernels for benchmarking against WGSL.

### Algorithms with GPU paths

PageRank, LCC, CDLP, cosine distance, inner product, PCA (mean / centre / covariance / power-iteration), bitonic sort, aggregate kernels, topology kernels.

### Determinism contract

GPU outputs must match CPU outputs to the LDBC tolerance (typically 6 decimal places). We do not promise bit-exact GPU/CPU agreement — floating-point reductions are order-dependent and GPU thread schedules vary.

### naga validator constraints (codified)

The WGSL validator (`naga`) enforces constraints that are not always obvious. We document them in this ADR so future shader work avoids relearning them:

1. `target` is a reserved name in storage/uniform variable contexts; use `tgt`.
2. `while` is preferred over `loop { ... if cond { break; } ... }` for naga reachability analysis.
3. No recursion in shaders (standard for compute shaders).
4. Storage buffer entries align to 16 bytes.
5. Workgroup size limits depend on backend; default to ≤ 256 threads/workgroup.
6. No dynamic-length arrays in workgroup memory.

## Consequences

### Positive
- ~30× PageRank speedup on hero scale.
- Cross-vendor: identical code runs on NVIDIA L40S, AMD, Apple Silicon (Metal).
- No driver pin: `wgpu` is bundled at install time.
- Faster shader iteration cycle (millisecond compile vs CUDA's multi-minute toolchain).

### Negative
- WGSL validator surprises (the 6 constraints above; new ones may surface).
- PCIe upload dominates first-call latency; cache invalidation is conservative.
- CUDA path is poorly maintained — feature-flagged but tested infrequently.
- No multi-GPU support; ceiling is single-GPU memory.
- No async upload — buffer transfers block initiating CPU thread.
- No deterministic-bit reductions; outputs match within tolerance, not bit-exact.

### Neutral
- The CPU paths in `samyama-graph-algorithms` remain the source of truth and the regression baseline. GPU paths must produce matching outputs.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| CUDA-only | NVIDIA lock-in; loses Mac dev story. |
| OpenCL | Moribund ecosystem in 2026. |
| rust-gpu (Rust → SPIR-V) | Not yet mature for our shader complexity. |
| CPU SIMD only | 10× slower on hero workloads. |
| External GPU graph DB (cuGraph) | Wrong layer; we use kernels, not stacks. |

## Follow-ups

1. Async buffer upload (overlap CPU prep with GPU upload).
2. Smarter dirty-tracking on cached GPU buffers (today: any graph mutation invalidates everything for the tenant).
3. Multi-GPU partitioning, gated on within-tenant partitioning (§6.5).
4. Daily CUDA-path regression test to catch silent rot.
5. Document validator constraints in a developer onboarding doc.
6. Surface a `samyama_gpu_dispatch_threshold` knob (today a constant).

## References

- Code: `samyama-graph-enterprise/crates/samyama-gpu/`, shaders in `crates/samyama-gpu/src/shaders/*.wgsl`.
- Wiki: [[compute-gpu.md]], [[algo-pagerank.md]], [[algo-centrality-community.md]].
- Related ADRs: ADR-001 (Rust), ADR-005 (Cap'n Proto — unrelated but adjacent infra).
