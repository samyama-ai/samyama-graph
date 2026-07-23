//! CUDA-accelerated parallel reduction
//!
//! Provides a GPU sum reduction: input f64 values are converted to f32 for GPU
//! processing, reduced in workgroups using shared memory, then the partial sums
//! are summed on the CPU for the final result.

use super::{alloc_zeros_f32, download_f32, upload_f32, CudaGpuContext};
use crate::error::GpuError;
use cudarc::driver::{LaunchAsync, LaunchConfig};

const BLOCK_SIZE: u32 = 256;

/// Compute the sum of f64 values using CUDA parallel reduction.
///
/// Values are converted to f32 for GPU processing. The reduction is done in two
/// stages: the first GPU pass reduces each block of 256 elements into a single
/// partial sum, then the partial sums are summed on the CPU.
///
/// This avoids a second GPU pass and is efficient for typical workloads where
/// the number of blocks is small relative to the element count.
pub fn cuda_sum_f64(ctx: &CudaGpuContext, values: &[f64]) -> Result<f64, GpuError> {
    if values.is_empty() {
        return Ok(0.0);
    }

    let dev = &ctx.dev;

    // Convert f64 -> f32 for GPU
    let values_f32: Vec<f32> = values.iter().map(|&v| v as f32).collect();

    let n = values_f32.len() as u32;
    let grid = CudaGpuContext::grid_size(n);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    // Upload input values
    let d_input = upload_f32(dev, &values_f32)?;

    // Allocate output buffer for per-block partial sums
    let d_output = alloc_zeros_f32(dev, grid as usize)?;

    let func = dev
        .get_func("reduce_sum", "reduce_sum")
        .ok_or_else(|| GpuError::ShaderError("reduce_sum not found".into()))?;

    unsafe { func.clone().launch(cfg, (&d_input, &d_output, n)) }
        .map_err(|e| GpuError::ShaderError(format!("reduce_sum launch: {}", e)))?;

    // Download partial sums and finish on CPU
    let partial_sums = download_f32(dev, &d_output)?;
    let total: f64 = partial_sums.iter().map(|&s| s as f64).sum();

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuda_sum_empty() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let result = cuda_sum_f64(&ctx, &[]).unwrap();
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_cuda_sum_small() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let result = cuda_sum_f64(&ctx, &values).unwrap();
        // Sum of 1..=100 = 5050
        assert!(
            (result - 5050.0).abs() < 1.0,
            "expected ~5050, got {}",
            result
        );
    }

    #[test]
    fn test_cuda_sum_large() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // Test with more than one block worth of data (> 256 elements)
        let values: Vec<f64> = (0..1000).map(|x| x as f64).collect();
        let result = cuda_sum_f64(&ctx, &values).unwrap();
        let expected: f64 = (0..1000).map(|x| x as f64).sum();
        // Allow some float accumulation error
        assert!(
            (result - expected).abs() < expected * 1e-4,
            "expected ~{}, got {}",
            expected,
            result
        );
    }
}
