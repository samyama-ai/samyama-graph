//! Unified GPU runtime — detects platform, selects best backend, provides single dispatch point.
//!
//! At startup, `GpuRuntime::init()` probes available backends in priority order:
//!   1. CUDA (NVIDIA native) — if `cuda` feature enabled and NVIDIA GPU present
//!   2. wgpu/Vulkan — Linux with any GPU
//!   3. wgpu/Metal — macOS
//!   4. wgpu/DX12 — Windows
//!   5. None — no GPU available
//!
//! The selected backend is stored once and reused for all subsequent GPU operations.
//! Call `GpuRuntime::get()` from any algorithm to get the active backend.

use crate::context::GpuContext;
use crate::error::GpuError;
use std::sync::OnceLock;

/// The active GPU backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuBackendType {
    /// Native NVIDIA CUDA (via cudarc)
    Cuda,
    /// wgpu with Vulkan backend (Linux/Android)
    Vulkan,
    /// wgpu with Metal backend (macOS/iOS)
    Metal,
    /// wgpu with DX12 backend (Windows)
    Dx12,
    /// wgpu with unknown/other backend
    WgpuOther,
    /// No GPU available
    None,
}

impl std::fmt::Display for GpuBackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GpuBackendType::Cuda => write!(f, "CUDA"),
            GpuBackendType::Vulkan => write!(f, "Vulkan"),
            GpuBackendType::Metal => write!(f, "Metal"),
            GpuBackendType::Dx12 => write!(f, "DX12"),
            GpuBackendType::WgpuOther => write!(f, "wgpu"),
            GpuBackendType::None => write!(f, "None"),
        }
    }
}

/// Unified GPU runtime state — determined once at init
pub struct GpuRuntime {
    pub backend: GpuBackendType,
    pub device_name: String,
    pub memory_bytes: usize,
    /// wgpu context (always initialized if any GPU is available, for fallback)
    wgpu_ctx: Option<GpuContext>,
    /// CUDA context (only if CUDA backend selected)
    #[cfg(feature = "cuda")]
    cuda_ctx: Option<crate::cuda::CudaGpuContext>,
}

static GPU_RUNTIME: OnceLock<GpuRuntime> = OnceLock::new();

impl GpuRuntime {
    /// Initialize the GPU runtime — probes backends in priority order.
    /// Called once at startup after license validation.
    pub fn init() -> &'static GpuRuntime {
        GPU_RUNTIME.get_or_init(|| {
            // Phase 1: Try CUDA (highest priority for NVIDIA GPUs)
            #[cfg(feature = "cuda")]
            {
                match crate::cuda::CudaGpuContext::new() {
                    Ok(cuda_ctx) => {
                        let device_name = cuda_ctx.device_name().to_string();
                        let memory = cuda_ctx.total_memory();
                        tracing::info!("GPU runtime: CUDA selected — {}", device_name);

                        // Also init wgpu as fallback (for PCA and other wgpu-only ops)
                        let wgpu_ctx = GpuContext::new().ok();

                        return GpuRuntime {
                            backend: GpuBackendType::Cuda,
                            device_name,
                            memory_bytes: memory,
                            wgpu_ctx,
                            cuda_ctx: Some(cuda_ctx),
                        };
                    }
                    Err(e) => {
                        tracing::debug!("CUDA not available: {}", e);
                    }
                }
            }

            // Phase 2: Try wgpu (cross-platform)
            match GpuContext::new() {
                Ok(ctx) => {
                    let backend = match ctx.backend() {
                        wgpu::Backend::Vulkan => GpuBackendType::Vulkan,
                        wgpu::Backend::Metal => GpuBackendType::Metal,
                        wgpu::Backend::Dx12 => GpuBackendType::Dx12,
                        _ => GpuBackendType::WgpuOther,
                    };
                    let device_name = ctx.adapter_name().to_string();
                    tracing::info!("GPU runtime: {} selected — {}", backend, device_name);

                    GpuRuntime {
                        backend,
                        device_name,
                        memory_bytes: 0, // wgpu doesn't expose total memory
                        wgpu_ctx: Some(ctx),
                        #[cfg(feature = "cuda")]
                        cuda_ctx: None,
                    }
                }
                Err(e) => {
                    tracing::info!("GPU runtime: no GPU available ({})", e);
                    GpuRuntime {
                        backend: GpuBackendType::None,
                        device_name: "None".to_string(),
                        memory_bytes: 0,
                        wgpu_ctx: None,
                        #[cfg(feature = "cuda")]
                        cuda_ctx: None,
                    }
                }
            }
        })
    }

    /// Get the initialized runtime (returns None if init() hasn't been called)
    pub fn get() -> Option<&'static GpuRuntime> {
        GPU_RUNTIME.get()
    }

    /// Check if any GPU backend is active
    pub fn is_active(&self) -> bool {
        self.backend != GpuBackendType::None
    }

    /// Check if CUDA is the active backend
    pub fn is_cuda(&self) -> bool {
        self.backend == GpuBackendType::Cuda
    }

    /// Get the wgpu context (available for all GPU backends, used as fallback for CUDA)
    pub fn wgpu(&self) -> Option<&GpuContext> {
        self.wgpu_ctx.as_ref()
    }

    /// Get the CUDA context (only available when CUDA is the active backend)
    #[cfg(feature = "cuda")]
    pub fn cuda(&self) -> Option<&crate::cuda::CudaGpuContext> {
        self.cuda_ctx.as_ref()
    }

    /// Human-readable status line for startup banner
    pub fn status_line(&self) -> String {
        match self.backend {
            GpuBackendType::None => "GPU: not available".to_string(),
            GpuBackendType::Cuda => {
                if self.memory_bytes > 0 {
                    format!(
                        "GPU: CUDA — {} ({:.1} GB)",
                        self.device_name,
                        self.memory_bytes as f64 / 1e9
                    )
                } else {
                    format!("GPU: CUDA — {}", self.device_name)
                }
            }
            other => format!("GPU: {} — {}", other, self.device_name),
        }
    }
}

impl std::fmt::Debug for GpuRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuRuntime")
            .field("backend", &self.backend)
            .field("device", &self.device_name)
            .field("memory_gb", &(self.memory_bytes as f64 / 1e9))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_init() {
        let rt = GpuRuntime::init();
        println!("{}", rt.status_line());
        // Second call returns same instance
        let rt2 = GpuRuntime::init();
        assert_eq!(rt.backend, rt2.backend);
    }

    #[test]
    fn test_backend_display() {
        assert_eq!(format!("{}", GpuBackendType::Cuda), "CUDA");
        assert_eq!(format!("{}", GpuBackendType::Metal), "Metal");
        assert_eq!(format!("{}", GpuBackendType::None), "None");
    }
}
