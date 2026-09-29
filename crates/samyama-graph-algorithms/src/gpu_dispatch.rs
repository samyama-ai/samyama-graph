//! GPU dispatch helpers. Only compiled under `--features gpu`.
//!
//! The CPU paths in this crate remain the source of truth and the regression
//! baseline (ADR-025). Each GPU-eligible algorithm probes here for the size
//! threshold, then routes to `samyama-gpu` with transparent CPU fallback.

use samyama_gpu::threshold;
use samyama_gpu::GpuBackendType;

/// Effective minimum node count for GPU dispatch on the active backend.
///
/// `SAMYAMA_GPU_MIN_NODES` (read at call time) overrides everything. Otherwise the
/// default depends on the backend (#1402): 1,000 for CUDA, 1,000,000 for wgpu on
/// Metal, and the historical 1,000 for backends nobody has measured yet (Vulkan,
/// DX12). See `samyama_gpu::threshold` for the measurements behind those numbers.
///
/// This does not initialise the GPU runtime: if it has not been initialised yet the
/// backend is unknown and the smallest per-backend default is returned.
pub fn min_gpu_nodes() -> usize {
    let raw = std::env::var(threshold::MIN_GPU_NODES_ENV).ok();
    match samyama_gpu::GpuRuntime::get() {
        Some(rt) => threshold::resolve_min_gpu_nodes(rt.backend, raw.as_deref()),
        None => threshold::parse_min_gpu_nodes_override(raw.as_deref())
            .unwrap_or_else(threshold::min_default_across_backends),
    }
}

/// The dispatch gate the algorithms use: should a graph of `n` nodes go to the GPU?
///
/// Initialises the GPU runtime only when `n` could clear some threshold, so tiny
/// graphs never pay device init.
pub fn should_dispatch_gpu(n: usize) -> bool {
    let raw = std::env::var(threshold::MIN_GPU_NODES_ENV).ok();
    dispatch_decision(n, raw.as_deref(), || {
        let rt = samyama_gpu::GpuRuntime::init();
        rt.is_active().then_some(rt.backend)
    })
}

/// Pure core of [`should_dispatch_gpu`]. `active_backend` initialises (or looks up)
/// the runtime and returns the active backend, or `None` when no GPU is available;
/// it is called at most once, and only when `n` is large enough to possibly dispatch.
fn dispatch_decision(
    n: usize,
    raw_override: Option<&str>,
    active_backend: impl FnOnce() -> Option<GpuBackendType>,
) -> bool {
    let floor = threshold::parse_min_gpu_nodes_override(raw_override)
        .unwrap_or_else(threshold::min_default_across_backends);
    if n <= floor {
        return false;
    }
    match active_backend() {
        Some(backend) => n > threshold::resolve_min_gpu_nodes(backend, raw_override),
        None => false,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn decide(n: usize, raw: Option<&str>, backend: Option<GpuBackendType>) -> (bool, bool) {
        let probed = Cell::new(false);
        let d = dispatch_decision(n, raw, || {
            probed.set(true);
            backend
        });
        (d, probed.get())
    }

    #[test]
    fn metal_keeps_mid_size_graphs_on_cpu() {
        // #1402: M4 GPU 7.5x slower at 10K, 2.3x slower at 100K.
        assert!(!decide(10_000, None, Some(GpuBackendType::Metal)).0);
        assert!(!decide(100_000, None, Some(GpuBackendType::Metal)).0);
        assert!(!decide(1_000_000, None, Some(GpuBackendType::Metal)).0);
        assert!(decide(1_000_001, None, Some(GpuBackendType::Metal)).0);
    }

    #[test]
    fn cuda_dispatches_above_1k() {
        assert!(!decide(1_000, None, Some(GpuBackendType::Cuda)).0);
        assert!(decide(1_001, None, Some(GpuBackendType::Cuda)).0);
        assert!(decide(100_000, None, Some(GpuBackendType::Cuda)).0);
    }

    #[test]
    fn override_applies_to_every_backend() {
        assert!(decide(2_000, Some("1000"), Some(GpuBackendType::Metal)).0);
        assert!(!decide(2_000, Some("5000"), Some(GpuBackendType::Cuda)).0);
    }

    #[test]
    fn no_gpu_never_dispatches() {
        assert_eq!(decide(10_000_000, None, None), (false, true));
    }

    #[test]
    fn small_graphs_do_not_initialise_the_runtime() {
        assert_eq!(
            decide(500, None, Some(GpuBackendType::Cuda)),
            (false, false)
        );
        // With an override the floor moves with it.
        assert_eq!(
            decide(4_000, Some("5000"), Some(GpuBackendType::Cuda)),
            (false, false)
        );
    }
}
