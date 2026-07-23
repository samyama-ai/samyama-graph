//! GPU context management
//!
//! Provides a global GpuContext that lazily initializes wgpu on first use.
//! Falls back gracefully when no GPU is available.

use crate::error::GpuError;
use std::sync::OnceLock;

/// Global GPU context (lazy-initialized)
static GPU_CONTEXT: OnceLock<Option<GpuContext>> = OnceLock::new();

/// Wrapper around wgpu Device and Queue for compute operations
pub struct GpuContext {
    pub(crate) device: wgpu::Device,
    pub(crate) queue: wgpu::Queue,
    pub(crate) adapter_info: wgpu::AdapterInfo,
}

impl GpuContext {
    /// Create a new GPU context (async)
    pub async fn new_async() -> Result<Self, GpuError> {
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });

        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: None,
                force_fallback_adapter: false,
            })
            .await
            .ok_or(GpuError::NoAdapter)?;

        let adapter_info = adapter.get_info();
        tracing::info!(
            "GPU adapter: {} ({:?}, {:?})",
            adapter_info.name,
            adapter_info.backend,
            adapter_info.device_type
        );

        // Request the adapter's full supported limits rather than wgpu's
        // conservative cross-platform defaults. The defaults cap a single
        // storage-buffer binding at 128 MiB (and total buffer size at 256 MiB),
        // which throttles large batch-vector and graph kernels well below what
        // discrete NVIDIA/AMD hardware can hold. Compute-only workloads have no
        // reason to stay at the portable web baseline.
        let required_limits = adapter.limits();
        let (device, queue) = adapter
            .request_device(
                &wgpu::DeviceDescriptor {
                    label: Some("samyama-gpu"),
                    required_features: wgpu::Features::empty(),
                    required_limits,
                    memory_hints: wgpu::MemoryHints::Performance,
                },
                None,
            )
            .await
            .map_err(|e| GpuError::DeviceCreation(e.to_string()))?;

        Ok(GpuContext {
            device,
            queue,
            adapter_info,
        })
    }

    /// Create a new GPU context (blocking)
    pub fn new() -> Result<Self, GpuError> {
        pollster::block_on(Self::new_async())
    }

    /// Whether GPU acceleration is enabled.
    ///
    /// GPU is on by default whenever hardware is present. Set the environment
    /// variable `SAMYAMA_GPU=off` (or `0`/`false`) to force the CPU path — used
    /// for baseline measurement and as an escape hatch for driver problems.
    pub fn is_enabled() -> bool {
        !matches!(
            std::env::var("SAMYAMA_GPU").ok().as_deref(),
            Some("off") | Some("0") | Some("false")
        )
    }

    /// Try to get or initialize the global GPU context.
    /// Returns None if disabled via `SAMYAMA_GPU=off` or no GPU hardware is available.
    pub fn try_global() -> Option<&'static GpuContext> {
        // Kill-switch: `SAMYAMA_GPU=off` forces the CPU path (baseline + escape hatch).
        if !Self::is_enabled() {
            return None;
        }

        GPU_CONTEXT
            .get_or_init(|| match Self::new() {
                Ok(ctx) => {
                    tracing::info!("GPU acceleration enabled: {}", ctx.adapter_info.name);
                    Some(ctx)
                }
                Err(e) => {
                    tracing::debug!("GPU acceleration unavailable: {}", e);
                    None
                }
            })
            .as_ref()
    }

    /// Check if any GPU is available (without initializing)
    pub fn is_available() -> bool {
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });
        pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            compatible_surface: None,
            force_fallback_adapter: false,
        }))
        .is_some()
    }

    /// Get GPU adapter info
    pub fn adapter_name(&self) -> &str {
        &self.adapter_info.name
    }

    /// Get the backend (Metal, Vulkan, DX12, etc.)
    pub fn backend(&self) -> wgpu::Backend {
        self.adapter_info.backend
    }

    /// Create a compute pipeline from WGSL shader source
    pub(crate) fn create_compute_pipeline(
        &self,
        label: &str,
        shader_source: &str,
        entry_point: &str,
    ) -> wgpu::ComputePipeline {
        let shader_module = self
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some(label),
                source: wgpu::ShaderSource::Wgsl(shader_source.into()),
            });

        self.device
            .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some(label),
                layout: None,
                module: &shader_module,
                entry_point: Some(entry_point),
                compilation_options: Default::default(),
                cache: None,
            })
    }
}

impl std::fmt::Debug for GpuContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuContext")
            .field("adapter", &self.adapter_info.name)
            .field("backend", &self.adapter_info.backend)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_availability_check() {
        // This test just verifies the API works; may return true or false
        let _available = GpuContext::is_available();
    }

    #[test]
    fn test_gpu_context_creation() {
        // Try to create a context; it's OK if it fails (no GPU in CI)
        match GpuContext::new() {
            Ok(ctx) => {
                assert!(!ctx.adapter_name().is_empty());
                println!("GPU: {} ({:?})", ctx.adapter_name(), ctx.backend());
            }
            Err(e) => {
                println!("No GPU available: {}", e);
            }
        }
    }
}
