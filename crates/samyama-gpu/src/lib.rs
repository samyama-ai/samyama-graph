//! GPU-accelerated graph algorithms and vector operations for Samyama Enterprise
//!
//! This crate provides GPU acceleration for:
//! - Graph algorithms: PageRank, Triangle Counting, CDLP, LCC
//! - Vector distance computation: batch cosine, inner product
//! - Query operators: parallel aggregation, bitonic sort
//!
//! **Backends:**
//! - **CUDA** (feature `cuda`): Native NVIDIA acceleration via `cudarc`. Preferred when available.
//! - **wgpu**: Cross-platform WebGPU (Metal on macOS, Vulkan on Linux, DX12 on Windows).
//!
//! When the `cuda` feature is enabled, each function tries CUDA first and falls back to wgpu.
//! All GPU functions fall back gracefully when no GPU is available.
//!
//! This crate does NOT depend on samyama-graph-algorithms to avoid circular deps.
//! Instead, it accepts raw CSR arrays (offsets, targets as &[usize]).

pub mod aggregate;
pub mod buffer;
pub mod cdlp;
pub mod context;
pub mod error;
pub mod lcc;
pub mod pagerank;
pub mod pca;
pub mod runtime;
pub mod sort;
pub mod topology;
pub mod unified;
pub mod vector;

#[cfg(feature = "cuda")]
pub mod cuda;

pub use context::GpuContext;
pub use error::GpuError;
pub use runtime::{GpuBackendType, GpuRuntime};

/// Whether any GPU backend (CUDA or wgpu) is available for dispatch.
///
/// Initializes the GPU runtime (idempotent) — selecting CUDA if present, else wgpu — so
/// subsequent `gpu_*` calls can dispatch. This is the correct dispatch gate: unlike
/// `GpuContext::is_available()` (wgpu-only), it also returns true on a CUDA-only/headless
/// host with no Vulkan adapter (the F1 fix).
pub fn gpu_available() -> bool {
    runtime::GpuRuntime::init().is_active()
}

pub use aggregate::gpu_sum_f64;
pub use cdlp::gpu_cdlp;
#[cfg(feature = "cuda")]
pub use cuda::CudaGpuContext;
pub use lcc::gpu_lcc;
pub use pagerank::gpu_page_rank;
pub use pca::gpu_pca;
pub use sort::gpu_argsort_f64;
pub use topology::gpu_count_triangles;
pub use vector::{gpu_batch_cosine, gpu_batch_inner_product, GpuVectorIndex, VectorMetric};

/// Minimum node count to use GPU acceleration (below this, CPU is faster)
pub const MIN_GPU_NODES: usize = 1000;

/// Minimum vector batch size to use GPU (below this, CPU is faster)
pub const MIN_GPU_VECTORS: usize = 64;

/// Minimum record count for GPU aggregation/sort
pub const MIN_GPU_RECORDS: usize = 10_000;

/// Minimum samples x features for GPU PCA (below this, CPU is faster).
/// Raised from 10,000 — for small feature dimensions (< 32), GPU dispatch
/// overhead exceeds computation time and CPU is always faster.
pub const MIN_GPU_PCA: usize = 50_000;
