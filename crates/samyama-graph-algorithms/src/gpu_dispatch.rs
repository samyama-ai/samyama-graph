//! GPU dispatch helpers. Only compiled under `--features gpu`.
//!
//! The CPU paths in this crate remain the source of truth and the regression
//! baseline (ADR-025). Each GPU-eligible algorithm probes here for the size
//! threshold, then routes to `samyama-gpu` with transparent CPU fallback.

use crate::cpu_fallback::CpuFallbackReason;
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
/// `Ok(())` sends it to the GPU; `Err` carries the reason it stays on the CPU,
/// which the caller logs with [`crate::cpu_fallback::note_cpu_fallback`]
/// (HW-10, #1684). A gate that answered only `false` left every fallback
/// unexplained: "graph too small" and "no GPU on this host" looked the same.
///
/// Initialises the GPU runtime only when `n` could clear some threshold, so tiny
/// graphs never pay device init. Once the runtime is initialised, the reason
/// always carries the active backend's own threshold, not the cross-backend floor.
pub fn gpu_route(n: usize) -> Result<(), CpuFallbackReason> {
    let raw = std::env::var(threshold::MIN_GPU_NODES_ENV).ok();
    let known = match samyama_gpu::GpuRuntime::get() {
        Some(rt) => Runtime::Initialised(active_backend(rt)),
        None => Runtime::NotInitialised,
    };
    dispatch_decision(n, raw.as_deref(), known, || {
        active_backend(samyama_gpu::GpuRuntime::init())
    })
}

/// The backend `rt` dispatches to, or `None` when it found no GPU.
fn active_backend(rt: &samyama_gpu::GpuRuntime) -> Option<GpuBackendType> {
    rt.is_active().then_some(rt.backend)
}

/// [`gpu_route`] as a yes or no, for callers that do not report a reason.
pub fn should_dispatch_gpu(n: usize) -> bool {
    gpu_route(n).is_ok()
}

/// What [`gpu_route`] knows about the GPU runtime before deciding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Runtime {
    /// Initialised already, with its active backend (`None`: no GPU was found).
    Initialised(Option<GpuBackendType>),
    /// Not initialised yet, so the backend, and its threshold, are unknown.
    NotInitialised,
}

/// Pure core of [`gpu_route`].
///
/// With the runtime already initialised, the active backend's own threshold
/// decides and is the one reported. Before that, the backend is unknown: a graph
/// at or below the smallest threshold of any backend stays on the CPU without
/// initialising anything, and only a larger one calls `init_backend`, which
/// initialises the runtime and returns the active backend, or `None` when no GPU
/// is available. `init_backend` is called at most once.
fn dispatch_decision(
    n: usize,
    raw_override: Option<&str>,
    runtime: Runtime,
    init_backend: impl FnOnce() -> Option<GpuBackendType>,
) -> Result<(), CpuFallbackReason> {
    let backend = match runtime {
        Runtime::Initialised(backend) => backend,
        Runtime::NotInitialised => {
            let floor = threshold::parse_min_gpu_nodes_override(raw_override)
                .unwrap_or_else(threshold::min_default_across_backends);
            if n <= floor {
                return Err(CpuFallbackReason::BelowThreshold {
                    nodes: n,
                    min_nodes: floor,
                });
            }
            init_backend()
        }
    };
    let backend = backend.ok_or(CpuFallbackReason::NoDevice)?;
    let min_nodes = threshold::resolve_min_gpu_nodes(backend, raw_override);
    if n <= min_nodes {
        return Err(CpuFallbackReason::BelowThreshold {
            nodes: n,
            min_nodes,
        });
    }
    Ok(())
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
        let (d, probed) = route(n, raw, backend);
        (d.is_ok(), probed)
    }

    /// The decision for a runtime not yet initialised, which `init` would
    /// find with `backend`; and whether it was initialised.
    fn route(
        n: usize,
        raw: Option<&str>,
        backend: Option<GpuBackendType>,
    ) -> (Result<(), CpuFallbackReason>, bool) {
        let probed = Cell::new(false);
        let d = dispatch_decision(n, raw, Runtime::NotInitialised, || {
            probed.set(true);
            backend
        });
        (d, probed.get())
    }

    /// The decision for a runtime already initialised with `backend`. It must
    /// never be initialised again.
    fn route_initialised(
        n: usize,
        raw: Option<&str>,
        backend: Option<GpuBackendType>,
    ) -> Result<(), CpuFallbackReason> {
        dispatch_decision(n, raw, Runtime::Initialised(backend), || {
            panic!("an initialised runtime was initialised again")
        })
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
    fn no_gpu_is_reported_as_no_device() {
        assert_eq!(
            route(10_000_000, None, None).0,
            Err(CpuFallbackReason::NoDevice)
        );
    }

    #[test]
    fn a_small_graph_reports_the_threshold_it_missed() {
        // Below the cross-backend floor: the runtime is never asked.
        let floor = threshold::min_default_across_backends();
        assert_eq!(
            route(floor, None, Some(GpuBackendType::Cuda)),
            (
                Err(CpuFallbackReason::BelowThreshold {
                    nodes: floor,
                    min_nodes: floor
                }),
                false
            )
        );
        // Above the floor but below this backend's own threshold.
        let metal = threshold::resolve_min_gpu_nodes(GpuBackendType::Metal, None);
        assert_eq!(
            route(metal, None, Some(GpuBackendType::Metal)).0,
            Err(CpuFallbackReason::BelowThreshold {
                nodes: metal,
                min_nodes: metal
            })
        );
    }

    #[test]
    fn an_initialised_runtime_reports_its_own_threshold_for_a_small_graph() {
        // #1936 review: Metal active, 500 nodes. The reason must name Metal's
        // threshold, the one dispatch applies, not the cross-backend floor.
        let metal = threshold::resolve_min_gpu_nodes(GpuBackendType::Metal, None);
        assert!(metal > threshold::min_default_across_backends());
        assert_eq!(
            route_initialised(500, None, Some(GpuBackendType::Metal)),
            Err(CpuFallbackReason::BelowThreshold {
                nodes: 500,
                min_nodes: metal
            })
        );
        // The override still wins over the backend default.
        assert_eq!(
            route_initialised(500, Some("5000"), Some(GpuBackendType::Metal)),
            Err(CpuFallbackReason::BelowThreshold {
                nodes: 500,
                min_nodes: 5_000
            })
        );
    }

    #[test]
    fn an_initialised_runtime_dispatches_exactly_as_an_uninitialised_one() {
        // The shortcut changes what is reported, never where the graph runs.
        for backend in [
            GpuBackendType::Cuda,
            GpuBackendType::Metal,
            GpuBackendType::Vulkan,
        ] {
            for n in [500, 1_000, 1_001, 1_000_000, 1_000_001] {
                for raw in [None, Some("5000")] {
                    assert_eq!(
                        route_initialised(n, raw, Some(backend)).is_ok(),
                        route(n, raw, Some(backend)).0.is_ok(),
                        "{backend:?}, {n} nodes, override {raw:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn an_initialised_runtime_without_a_gpu_reports_no_device_at_any_size() {
        // A threshold means nothing when no device was found.
        assert_eq!(
            route_initialised(500, None, None),
            Err(CpuFallbackReason::NoDevice)
        );
        assert_eq!(
            route_initialised(10_000_000, None, None),
            Err(CpuFallbackReason::NoDevice)
        );
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
