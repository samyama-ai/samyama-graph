//! Shared benchmark setup: GPU status reporting.
//!
//! Each benchmark includes this via `#[path = "bench_setup.rs"] mod bench_setup;`
//! and calls `bench_setup::init()` at the top of `main()`.
//!
//! In OSS there is no license gate: GPU acceleration is on by default whenever a
//! GPU is present and the `gpu` feature is compiled in. Set `SAMYAMA_GPU=off` to
//! force the CPU path (baseline measurement or driver escape hatch).

/// Report GPU availability for benchmarks. No-op in a CPU-only build.
pub fn init() {
    #[cfg(feature = "gpu")]
    {
        if !samyama_gpu::GpuContext::is_enabled() {
            println!("[bench] GPU disabled via SAMYAMA_GPU=off — running CPU path.");
        } else if samyama_gpu::GpuContext::is_available() {
            println!("[bench] GPU acceleration: ENABLED (hardware detected).");
        } else {
            println!("[bench] GPU feature built, but no GPU hardware detected — CPU path.");
        }
    }

    #[cfg(not(feature = "gpu"))]
    {
        println!("[bench] Built without the `gpu` feature — CPU only.");
        println!("[bench] Rebuild with `--features gpu` for GPU-accelerated benchmarks.");
    }
}
