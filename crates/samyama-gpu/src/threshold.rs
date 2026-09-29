//! Per-backend GPU dispatch thresholds for the node-count gate (issue #1402).
//!
//! Everything here is a pure function of its arguments — no GPU, no runtime init, no
//! environment reads — so it is unit-testable on a host with no GPU at all. The caller
//! (`samyama-graph-algorithms::gpu_dispatch`) reads `SAMYAMA_GPU_MIN_NODES` and the
//! active backend and passes them in.
//!
//! ## Measured crossovers (PageRank, v1.8.0, `benches/full_benchmark.rs`,
//! `dangling_redistribution: false`, steady-state ms/iter, same binary both arms,
//! only `SAMYAMA_GPU_MIN_NODES` differing — from issue #1402)
//!
//! wgpu / **Metal**, Apple M4 (10-core, 16 GB, macOS 26.2):
//!
//! | nodes     | CPU   | GPU   |                   |
//! |----------:|------:|------:|-------------------|
//! | 10,000    | 1.10  | 8.31  | GPU 7.5x slower   |
//! | 100,000   | 4.31  | 10.05 | GPU 2.3x slower   |
//! | 1,000,000 | 59.36 | 49.81 | GPU 1.19x faster  |
//!
//! **CUDA**, NVIDIA A16-8Q (8 GB, driver 550.90.07, CUDA 12.0, 3 vCPU):
//!
//! | nodes     | CPU    | CUDA   |           |
//! |----------:|-------:|-------:|-----------|
//! | 10,000    | 3.38   | 3.30   | parity    |
//! | 100,000   | 17.56  | 10.82  | GPU 1.62x |
//! | 1,000,000 | 407.53 | 150.91 | GPU 2.70x |
//!
//! So the crossover is ~1M nodes on Metal and at or below 10K on CUDA; a single
//! constant cannot serve both. Vulkan, DX12 and other wgpu backends have **not** been
//! measured, so they keep the historical default ([`crate::MIN_GPU_NODES`], originally
//! tuned on an RTX 4050 laptop GPU) until someone measures them — set
//! `SAMYAMA_GPU_MIN_NODES` deliberately on those hosts.
//!
//! Note the gate is a node count only. It does not model one-off context
//! initialisation (~230 ms on the M4, ~1.1–1.35 s on the A16), which the steady-state
//! numbers above exclude.

use crate::runtime::GpuBackendType;

/// Name of the environment variable that overrides the per-backend default.
pub const MIN_GPU_NODES_ENV: &str = "SAMYAMA_GPU_MIN_NODES";

/// Default node threshold for CUDA. The A16 measurement in #1402 shows parity at
/// 10K nodes and a GPU win from 100K up, so the historical 1,000 is kept.
pub const MIN_GPU_NODES_CUDA: usize = 1_000;

/// Default node threshold for wgpu on Metal (Apple silicon). #1402 measured the GPU
/// 2.3x slower at 100K nodes and 1.19x faster at 1M on an M4, i.e. a crossover of
/// "~1M". Dispatch requires `n > threshold`, so a graph of exactly 1M nodes stays on
/// the CPU; the measured gain given up at that size is 1.19x.
pub const MIN_GPU_NODES_METAL: usize = 1_000_000;

/// Built-in default node threshold for `backend`, before any env override.
///
/// Backends without a measurement (Vulkan, DX12, other wgpu, and `None`) return the
/// historical [`crate::MIN_GPU_NODES`] unchanged.
pub fn default_min_gpu_nodes(backend: GpuBackendType) -> usize {
    match backend {
        GpuBackendType::Cuda => MIN_GPU_NODES_CUDA,
        GpuBackendType::Metal => MIN_GPU_NODES_METAL,
        GpuBackendType::Vulkan
        | GpuBackendType::Dx12
        | GpuBackendType::WgpuOther
        | GpuBackendType::None => crate::MIN_GPU_NODES,
    }
}

/// Parse a `SAMYAMA_GPU_MIN_NODES` value. Returns `None` (meaning "use the backend
/// default") when unset, empty, non-numeric, or zero — the same acceptance rules the
/// knob has always had. Surrounding whitespace is ignored.
pub fn parse_min_gpu_nodes_override(raw: Option<&str>) -> Option<usize> {
    raw.and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&v| v > 0)
}

/// Effective node threshold: a valid override wins, otherwise the backend default.
pub fn resolve_min_gpu_nodes(backend: GpuBackendType, raw_override: Option<&str>) -> usize {
    parse_min_gpu_nodes_override(raw_override).unwrap_or_else(|| default_min_gpu_nodes(backend))
}

/// The smallest default across all backends. A graph at or below this size can never
/// dispatch without an override, whatever the backend, so callers can skip GPU runtime
/// initialisation for it.
pub fn min_default_across_backends() -> usize {
    [
        GpuBackendType::Cuda,
        GpuBackendType::Metal,
        GpuBackendType::Vulkan,
        GpuBackendType::Dx12,
        GpuBackendType::WgpuOther,
        GpuBackendType::None,
    ]
    .into_iter()
    .map(default_min_gpu_nodes)
    .min()
    .unwrap_or(crate::MIN_GPU_NODES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cuda_default_matches_a16_measurement() {
        assert_eq!(default_min_gpu_nodes(GpuBackendType::Cuda), 1_000);
    }

    #[test]
    fn metal_default_matches_m4_measurement() {
        assert_eq!(default_min_gpu_nodes(GpuBackendType::Metal), 1_000_000);
        // 100K nodes (GPU measured 2.3x slower) must stay on the CPU.
        assert!(100_000 <= default_min_gpu_nodes(GpuBackendType::Metal));
    }

    #[test]
    fn unmeasured_backends_keep_historical_default() {
        for b in [
            GpuBackendType::Vulkan,
            GpuBackendType::Dx12,
            GpuBackendType::WgpuOther,
            GpuBackendType::None,
        ] {
            assert_eq!(default_min_gpu_nodes(b), crate::MIN_GPU_NODES, "{b}");
        }
    }

    #[test]
    fn override_parsing() {
        assert_eq!(parse_min_gpu_nodes_override(None), None);
        assert_eq!(parse_min_gpu_nodes_override(Some("")), None);
        assert_eq!(parse_min_gpu_nodes_override(Some("abc")), None);
        assert_eq!(parse_min_gpu_nodes_override(Some("-5")), None);
        assert_eq!(parse_min_gpu_nodes_override(Some("0")), None);
        assert_eq!(parse_min_gpu_nodes_override(Some("1.5")), None);
        assert_eq!(parse_min_gpu_nodes_override(Some("1")), Some(1));
        assert_eq!(parse_min_gpu_nodes_override(Some(" 5000 ")), Some(5000));
        assert_eq!(
            parse_min_gpu_nodes_override(Some("1000000")),
            Some(1_000_000)
        );
    }

    #[test]
    fn override_beats_backend_default() {
        assert_eq!(
            resolve_min_gpu_nodes(GpuBackendType::Metal, Some("1000")),
            1000
        );
        assert_eq!(
            resolve_min_gpu_nodes(GpuBackendType::Cuda, Some("50000")),
            50_000
        );
    }

    #[test]
    fn invalid_override_falls_back_to_backend_default() {
        assert_eq!(
            resolve_min_gpu_nodes(GpuBackendType::Metal, Some("0")),
            1_000_000
        );
        assert_eq!(
            resolve_min_gpu_nodes(GpuBackendType::Metal, Some("junk")),
            1_000_000
        );
        assert_eq!(resolve_min_gpu_nodes(GpuBackendType::Cuda, None), 1_000);
    }

    #[test]
    fn floor_is_smallest_default() {
        assert_eq!(min_default_across_backends(), 1_000);
    }
}
