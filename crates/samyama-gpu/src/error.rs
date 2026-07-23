//! GPU error types

use std::fmt;

/// Errors from GPU operations
#[derive(Debug)]
pub enum GpuError {
    /// No GPU adapter found on this system
    NoAdapter,
    /// Failed to create GPU device
    DeviceCreation(String),
    /// Shader compilation error
    ShaderError(String),
    /// Buffer mapping failed
    BufferMapFailed(String),
    /// GPU computation timed out
    Timeout,
    /// Input data too large for GPU memory
    DataTooLarge { requested: usize, available: usize },
    /// CUDA-specific error
    #[cfg(feature = "cuda")]
    CudaError(String),
}

impl fmt::Display for GpuError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GpuError::NoAdapter => write!(f, "No GPU adapter found"),
            GpuError::DeviceCreation(msg) => write!(f, "GPU device creation failed: {}", msg),
            GpuError::ShaderError(msg) => write!(f, "Shader error: {}", msg),
            GpuError::BufferMapFailed(msg) => write!(f, "Buffer map failed: {}", msg),
            GpuError::Timeout => write!(f, "GPU computation timed out"),
            GpuError::DataTooLarge {
                requested,
                available,
            } => {
                write!(
                    f,
                    "Data too large for GPU: {} bytes requested, {} available",
                    requested, available
                )
            }
            #[cfg(feature = "cuda")]
            GpuError::CudaError(msg) => write!(f, "CUDA error: {}", msg),
        }
    }
}

impl std::error::Error for GpuError {}
