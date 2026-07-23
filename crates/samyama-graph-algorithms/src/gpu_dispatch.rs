//! GPU dispatch helpers. Only compiled under `--features gpu`.
//!
//! The CPU paths in this crate remain the source of truth and the regression
//! baseline (ADR-025). Each GPU-eligible algorithm probes here for the size
//! threshold, then routes to `samyama-gpu` with transparent CPU fallback.

/// Effective minimum node count for GPU dispatch.
///
/// Reads `SAMYAMA_GPU_MIN_NODES` at call time, falling back to `samyama-gpu`'s
/// built-in default. The built-in default was tuned on a single 30 W laptop GPU
/// (RTX 4050) and is almost certainly wrong for datacenter parts, so this knob
/// lets deployments retune without a rebuild (workstream §3.5).
pub fn min_gpu_nodes() -> usize {
    std::env::var("SAMYAMA_GPU_MIN_NODES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(samyama_gpu::MIN_GPU_NODES)
}

/// Whether any GPU backend (CUDA or wgpu) is available for dispatch. Initializes the
/// runtime (idempotent). Unlike a wgpu-only probe, this is true on CUDA-only/headless
/// hosts too — the F1 fix, so GPU dispatch does not silently no-op there.
pub fn gpu_available() -> bool {
    samyama_gpu::gpu_available()
}

/// Initialize the GPU runtime (selects CUDA if available, else wgpu) and report the
/// active backend. Must be called before CUDA-backed ops so the CUDA path — and hence
/// the unified-memory path — is actually taken: `gpu_page_rank` routes to CUDA only when
/// `GpuRuntime::get()` is `Some`, and `get()` returns `None` until `init()` runs.
pub fn init_runtime() -> &'static str {
    let rt = samyama_gpu::GpuRuntime::init();
    if rt.is_cuda() {
        "CUDA"
    } else if rt.is_active() {
        "wgpu"
    } else {
        "none"
    }
}
