# ADR-025: GPU Compute — `samyama-gpu` crate, WGSL by default, CUDA opt-in

## Status
**Shipped** (v1.0.0, 2026-04-11) — retroactively documenting the v0.7+ shipped crate.

## Date
2026-05-05

## Context

Several graph and vector operations exhibit data-parallel structure that maps well to GPU compute:

- PageRank's edge-scatter update (the LDBC-compliance hot path).
- LCC (local clustering coefficient) — per-node triangle counting.
- CDLP (community detection via label propagation) — parallel label updates.
- Vector similarity (cosine, inner product) — the consumer of HNSW results.
- PCA / dimensionality reduction.

CPU implementations of these — even with SIMD and rayon parallelisation — cap at single-host memory bandwidth and core count. Measured against a 16-core rayon baseline, GPU PageRank is **2.25× faster at 1M nodes and up to 7.2× at 2M** (see [Measured results](#measured-results)).

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
- **2.25× PageRank and 2.67× LCC at 1M nodes** on an entry-level 30 W laptop GPU via the portable wgpu path; up to **7.2× PageRank at 2M** on the CUDA path. Speedup grows with graph size. Full tables in [Measured results](#measured-results).
- Cross-vendor **by construction**: WGSL/wgpu targets Vulkan, Metal and DX12, so the same kernels are not locked to NVIDIA. *(Portability of the design; **not** a claim of measured parity — see [Measured results](#measured-results) for what has actually been run.)*
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

## Measured results

Every performance number in this ADR traces to a published run. Nothing here is estimated or extrapolated.

**Testbed** — NVIDIA RTX 4050 Laptop (6 GiB, ~30 W), driver 595.71.05, wgpu 24 → Vulkan; CPU baseline 16-core rayon; deterministic random graphs at average degree 10; PageRank 20 iterations with `dangling_redistribution = false`. Methodology is CPU-first in-process on a byte-identical `GraphView`, median of several runs.

| Algorithm | Size | CPU (ms) | wgpu (ms) | Speedup |
|---|---:|---:|---:|---:|
| PageRank | 1M | 208.36 | 92.69 | **2.25×** |
| LCC | 1M | 867.83 | 324.45 | **2.67×** |
| CDLP | 250k | 176.35 | 83.36 | 2.12× |
| CDLP | 1M | 804.17 | 623.79 | 1.29× *(tapers — atomic-heavy)* |
| TriangleCount | 100k | 64.78 | 35.37 | 1.83× |

Backend comparison, PageRank (same box; speedup vs the same CPU baseline):

| Nodes | wgpu | CUDA |
|---:|---:|---:|
| 1M | 2.6× | **4.4×** |
| 2M | 4.5× | **7.2×** |
| 4M | 4.0× | 4.0× *(converged — DRAM-bandwidth-bound)* |

**7.2× (CUDA, 2M nodes) is the highest speedup this project has measured.**

The backends are **not uniformly ranked**: CUDA wins PageRank and triangle counting; wgpu wins CDLP (1.7× vs 1.5×) and LCC (~2.0× vs ~1.4×), and CUDA CDLP is *slower than CPU* at 500k (0.93×).

Where the GPU **loses**: one-shot batch vector cosine runs at **0.33–0.38× of CPU** — it re-uploads the whole candidate corpus per call and is transfer-bound. The corpus-resident `GpuVectorIndex` API turns this into 4.5–5.7×, but **it has no production caller today** — it is a benchmark result, not a shipped feature.

### Coverage and limits of these measurements

- **One GPU, one vendor, one backend.** Everything above is NVIDIA/Vulkan on a single laptop part. Metal (Apple Silicon), AMD, and Intel are **unmeasured** — the cross-vendor property is a design guarantee, not a benchmarked one.
- The only other GPU run in `results/` (`20260317-a10g/`, Enterprise) is a **correctness test run, not a benchmark** — its benchmarks were CPU-only by license, four wgpu tests hung >60 s, and it terminated on SIGTERM. It predates the July 2026 `adapter.limits()` and `triangles.wgsl` fixes and needs re-running.
- Random graphs have uniform degree, which is unusually GPU-friendly (little warp divergence). Expect **lower** speedups on skewed real-world degree distributions. An LDBC Graphalytics run is outstanding.
- Raw logs: `samyama-graph-enterprise/results/20260707-rtx4050/`; write-up: `samyama-graph-enterprise/docs/gpu-benchmark-rtx4050.md`.

> **Retraction (2026-07-16).** Revisions of this ADR from its creation (`243f78a`, 2026-05-05) until this one asserted *"~30× PageRank speedup on hero scale"* and *"on the 1.29 B-edge hero workload, CPU PageRank takes ~10× longer than GPU PageRank on a single L40S."* **Both were unsourced and are retracted.** No L40S GPU benchmark was ever run; the 1.29 B-edge hero run executed on a CPU-only `x2iedn.24xlarge` and its bench step was skipped. The two figures also contradicted each other (30× vs 10×). The ADR was written retroactively and the numbers were never traceable to a result. The measured maximum is 7.2×.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| CUDA-only | NVIDIA lock-in; loses Mac dev story. |
| OpenCL | Moribund ecosystem in 2026. |
| rust-gpu (Rust → SPIR-V) | Not yet mature for our shader complexity. |
| CPU SIMD only | 2–7× slower on the measured 1M–2M node workloads, and the gap widens with size. |
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
