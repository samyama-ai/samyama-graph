# ADR-034: Unified-memory buffer seam for zero-ETL CPU/GPU graph sharing

## Status
**Proposed / scaffolded** (2026-07-22, branch `gpu-oss-port-phase1`). Host + CUDA-managed
backings implemented and functionally verified on a discrete GPU; coherent-hardware validation
(GH200) pending. Forcing function: paper 22 (`samyama-research/papers/paper22-gpu-unified-memory`).

## Date
2026-07-22

## Context

Mixed graph workloads — vector ANN, traversal, and analytics (PageRank/CDLP/LCC) in one query —
today pay a hidden tax: every GPU point-tool requires the graph to be **copied** into GPU memory
(`cudaMemcpy` / `wgpu::Queue::write_buffer`) as a separate resident copy. For a *live, updated*
graph that copy is re-paid on every change. Our own measurement makes the tax concrete: on an
RTX 4050, GPU vector search is **0.24× (a loss) one-shot** but **4.3× resident** — an ~18× swing
that is pure host↔device transfer (`samyama-research/.../preflight-rtx4050-20260722`).

Coherent unified-memory hardware removes the copy: NVIDIA GH200 (Grace CPU + Hopper GPU over
NVLink-C2C) and AMD MI300A expose one cache-coherent address space where the CPU and GPU read and
write the *same* bytes. `cuMemAllocManaged` gives the same programming model on any CUDA GPU — as
*software* unified memory (pages migrate over PCIe) on discrete parts, and as *hardware-coherent*
memory (no migration) on GH200/MI300A. **The code path is identical; only the hardware differs.**

wgpu has no managed-memory concept (it always allocates device buffers and copies), so this
capability is inherently CUDA/Grace-Hopper. It therefore lives in the CUDA path of `samyama-gpu`.

## Decision

Introduce a narrow allocation seam — `samyama_gpu::unified::UnifiedBuffer<T>` — with two backings:

- **`Host(Vec<T>)`** — default; works on any hardware; GPU access still needs an explicit upload.
- **`Managed(ManagedBuffer<T>)`** (`cuda` feature) — one `cuMemAllocManaged` allocation that is
  addressable by both the CPU (`as_slice`/`as_mut_slice`) and CUDA kernels (`device_ptr()`), with
  **no host↔device copy**.

Selection is opt-in via `SAMYAMA_GPU_UM=on` (default off), because on a discrete GPU software UM
can be *slower* than an explicit copy — paper 22's E3 characterises exactly when it wins. Keeping
it opt-in leaves the default, fully-tested path unchanged.

The seam is deliberately small: kernels and backends stay in the `samyama-gpu` crate; the only
thing that changes elsewhere is *where the CSR/property buffers are allocated*. The next step is to
plumb `samyama-graph-algorithms`' on-demand CSR construction (`out_offsets/out_targets`, …) through
`UnifiedBuffer` so that, under `SAMYAMA_GPU_UM=on`, the graph a CPU traversal touches and the graph
a GPU PageRank reads are literally one allocation.

## Consequences

### Positive
- **Zero-ETL sharing of one live graph** between CPU and GPU on coherent hardware — the paper 22
  contribution, and a capability no other Apache-2.0 graph database has.
- Same API on discrete and coherent hardware; discrete GPUs get a functional (if migration-bound)
  path and serve as the E3a control. Verified on RTX 4050: managed alloc + CPU round-trip + non-null
  device pointer (`unified::tests::managed_roundtrip_or_skip`).
- Default behaviour is unchanged (`Host` backing) and remains the regression baseline.

### Negative / risks
- Software UM on discrete GPUs can thrash under oversubscription or high write rates — this is a
  *documented* failure mode the paper measures, not a regression to hide.
- Managed allocation requires a current CUDA context; the seam assumes `CudaGpuContext::try_global()`
  has initialised. Misuse is a soundness concern guarded by that invariant.
- `cuMemAdvise`/`cuMemPrefetchAsync` tuning hints need CUDA ≥ 12.2 (this box is 12.0), so they are
  deferred; core `cuMemAllocManaged` works on all supported versions.

### Neutral
- The CPU CSR paths remain the source of truth; UM changes *where* buffers live, not the algorithms.
- Coherent-hardware performance (the headline win) is unproven until validated on a GH200; this ADR
  scaffolds the mechanism so that validation is "book a GH200 and benchmark," not "design and build."
