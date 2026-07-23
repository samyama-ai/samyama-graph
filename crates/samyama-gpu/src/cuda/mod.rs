//! CUDA backend for GPU-accelerated graph algorithms
//!
//! Provides native CUDA acceleration via the `cudarc` crate as an alternative
//! to the wgpu/Vulkan/Metal backend. CUDA is preferred when available (NVIDIA GPUs)
//! because it avoids the WebGPU abstraction layer and uses NVIDIA's native runtime.
//!
//! Kernels are compiled from CUDA C source at init time using NVRTC.
//! All functions fall back to the wgpu path if CUDA initialization fails.

pub mod aggregate;
pub mod cdlp;
pub mod kernels;
pub mod lcc;
pub mod pagerank;
pub mod sort;
pub mod topology;
pub mod vector;

use crate::error::GpuError;
use cudarc::driver::{CudaDevice, CudaSlice, LaunchAsync};
use cudarc::nvrtc::compile_ptx;
use std::sync::{Arc, OnceLock};

// Re-export for submodules
pub(crate) use cudarc::driver::LaunchAsync as _LaunchAsync;

static CUDA_CONTEXT: OnceLock<Option<CudaGpuContext>> = OnceLock::new();

/// CUDA GPU context — wraps a CudaDevice with pre-compiled kernels
pub struct CudaGpuContext {
    pub(crate) dev: Arc<CudaDevice>,
    pub(crate) device_name: String,
    pub(crate) total_mem: usize,
}

const BLOCK_SIZE: u32 = 256;

impl CudaGpuContext {
    /// Try to initialize CUDA context
    pub fn new() -> Result<Self, GpuError> {
        let dev = CudaDevice::new(0)
            .map_err(|e| GpuError::DeviceCreation(format!("CUDA device 0: {}", e)))?;

        // Get device properties via cudarc
        let device_name = format!("CUDA Device {}", dev.ordinal());
        let total_mem = 0usize; // cudarc doesn't expose total_memory directly

        tracing::info!(
            "CUDA device: {} ({:.1} GB)",
            device_name,
            total_mem as f64 / 1e9
        );

        // Pre-compile all kernels
        Self::compile_kernels(&dev)?;

        Ok(CudaGpuContext {
            dev,
            device_name,
            total_mem,
        })
    }

    /// Compile all CUDA kernels using NVRTC
    fn compile_kernels(dev: &Arc<CudaDevice>) -> Result<(), GpuError> {
        let kernel_sources = [
            ("pagerank", kernels::PAGERANK, vec!["pagerank_iter"]),
            ("triangles", kernels::TRIANGLES, vec!["count_triangles"]),
            ("cosine", kernels::COSINE_DISTANCE, vec!["cosine_batch"]),
            (
                "inner_product",
                kernels::INNER_PRODUCT,
                vec!["inner_product_batch"],
            ),
            ("reduce_sum", kernels::REDUCE_SUM, vec!["reduce_sum"]),
            ("bitonic_sort", kernels::BITONIC_SORT, vec!["bitonic_step"]),
            ("cdlp", kernels::CDLP, vec!["cdlp_iter"]),
            ("lcc", kernels::LCC, vec!["lcc_compute"]),
        ];

        for (module_name, source, func_names) in &kernel_sources {
            let ptx = compile_ptx(source).map_err(|e| {
                GpuError::ShaderError(format!("NVRTC compile {}: {}", module_name, e))
            })?;
            let func_strs: Vec<&str> = func_names.iter().map(|s| *s).collect();
            dev.load_ptx(ptx, module_name, &func_strs)
                .map_err(|e| GpuError::ShaderError(format!("PTX load {}: {}", module_name, e)))?;
        }

        tracing::info!("CUDA kernels compiled: {} modules", kernel_sources.len());
        Ok(())
    }

    /// Try to get or initialize the global CUDA context.
    /// Returns None if CUDA is not available.
    pub fn try_global() -> Option<&'static CudaGpuContext> {
        CUDA_CONTEXT
            .get_or_init(|| match Self::new() {
                Ok(ctx) => {
                    tracing::info!("CUDA acceleration enabled: {}", ctx.device_name);
                    Some(ctx)
                }
                Err(e) => {
                    tracing::debug!("CUDA unavailable (will use wgpu): {}", e);
                    None
                }
            })
            .as_ref()
    }

    /// Check if CUDA is available
    pub fn is_available() -> bool {
        CudaDevice::new(0).is_ok()
    }

    /// Get device name
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Get total device memory in bytes
    pub fn total_memory(&self) -> usize {
        self.total_mem
    }

    /// Compute grid size for n elements
    pub(crate) fn grid_size(n: u32) -> u32 {
        (n + BLOCK_SIZE - 1) / BLOCK_SIZE
    }
}

impl std::fmt::Debug for CudaGpuContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CudaGpuContext")
            .field("device", &self.device_name)
            .field("memory_gb", &(self.total_mem as f64 / 1e9))
            .finish()
    }
}

/// Helper: upload a Vec<u32> to GPU, returns CudaSlice
pub(crate) fn upload_u32(dev: &Arc<CudaDevice>, data: &[u32]) -> Result<CudaSlice<u32>, GpuError> {
    dev.htod_copy(data.to_vec())
        .map_err(|e| GpuError::BufferMapFailed(format!("upload u32: {}", e)))
}

/// Move a CSR (graph-structure) array to the device. Under `SAMYAMA_GPU_UM=on` it is
/// allocated in CUDA managed (unified) memory and handed to the kernel by pointer — **no
/// `cuMemcpyHtoD`**; otherwise the classic explicit host->device copy. This is the A/B
/// switch behind paper 22 E3 (copy barrier vs. unified memory). Apply it only to the
/// shared *graph* buffers (offsets/targets/…), not transient working buffers.
///
/// On a discrete GPU managed pages migrate on first touch (E3a); on GH200 they are
/// cache-coherent with no migration (E3b). Same code path, different hardware.
pub(crate) fn csr_to_device(
    dev: &Arc<CudaDevice>,
    data: &[u32],
) -> Result<CudaSlice<u32>, GpuError> {
    if crate::unified::um_enabled() {
        let m = crate::unified::ManagedBuffer::<u32>::from_slice(data)?;
        let (ptr, len) = m.into_device_ptr();
        // SAFETY: `ptr` is a live cuMemAllocManaged allocation of `len` valid u32s just
        // written from the CPU; the returned CudaSlice takes ownership and frees it once.
        return Ok(unsafe { dev.upgrade_device_ptr(ptr, len) });
    }
    upload_u32(dev, data)
}

/// Helper: upload a Vec<f32> to GPU, returns CudaSlice
pub(crate) fn upload_f32(dev: &Arc<CudaDevice>, data: &[f32]) -> Result<CudaSlice<f32>, GpuError> {
    dev.htod_copy(data.to_vec())
        .map_err(|e| GpuError::BufferMapFailed(format!("upload f32: {}", e)))
}

/// Helper: allocate zeroed buffer on GPU
pub(crate) fn alloc_zeros_u32(dev: &Arc<CudaDevice>, n: usize) -> Result<CudaSlice<u32>, GpuError> {
    dev.alloc_zeros::<u32>(n)
        .map_err(|e| GpuError::BufferMapFailed(format!("alloc u32: {}", e)))
}

/// Helper: allocate zeroed buffer on GPU
pub(crate) fn alloc_zeros_f32(dev: &Arc<CudaDevice>, n: usize) -> Result<CudaSlice<f32>, GpuError> {
    dev.alloc_zeros::<f32>(n)
        .map_err(|e| GpuError::BufferMapFailed(format!("alloc f32: {}", e)))
}

/// Helper: download from GPU to host
pub(crate) fn download_f32(
    dev: &Arc<CudaDevice>,
    src: &CudaSlice<f32>,
) -> Result<Vec<f32>, GpuError> {
    dev.dtoh_sync_copy(src)
        .map_err(|e| GpuError::BufferMapFailed(format!("download f32: {}", e)))
}

/// Helper: download from GPU to host
pub(crate) fn download_u32(
    dev: &Arc<CudaDevice>,
    src: &CudaSlice<u32>,
) -> Result<Vec<u32>, GpuError> {
    dev.dtoh_sync_copy(src)
        .map_err(|e| GpuError::BufferMapFailed(format!("download u32: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuda_availability() {
        match CudaGpuContext::new() {
            Ok(ctx) => println!(
                "CUDA: {} ({:.1} GB)",
                ctx.device_name(),
                ctx.total_memory() as f64 / 1e9
            ),
            Err(e) => println!("No CUDA: {}", e),
        }
    }
}
