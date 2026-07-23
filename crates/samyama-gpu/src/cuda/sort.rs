//! CUDA-accelerated bitonic sort
//!
//! Provides an argsort for f64 values: returns the indices that would sort
//! the array in ascending order. Values are converted to f32 on the GPU,
//! padded to the next power of two with f32::MAX, and sorted using a series
//! of bitonic merge steps.

use super::{download_u32, upload_f32, upload_u32, CudaGpuContext};
use crate::error::GpuError;
use cudarc::driver::{LaunchAsync, LaunchConfig};

const BLOCK_SIZE: u32 = 256;

/// Argsort f64 values in ascending order using CUDA bitonic sort.
///
/// Returns a Vec of indices such that `values[result[0]] <= values[result[1]] <= ...`.
///
/// The input is padded to the next power of 2 with `f32::MAX` sentinel values
/// and indices are filtered back to the original range after sorting.
pub fn cuda_argsort_f64(ctx: &CudaGpuContext, values: &[f64]) -> Result<Vec<usize>, GpuError> {
    let n = values.len();
    if n <= 1 {
        return Ok((0..n).collect());
    }

    let dev = &ctx.dev;

    // Pad to next power of 2
    let padded_len = n.next_power_of_two();

    // Convert f64 -> f32 and pad with MAX
    let mut keys_f32: Vec<f32> = values.iter().map(|&v| v as f32).collect();
    keys_f32.resize(padded_len, f32::MAX);

    // Create indices [0, 1, 2, ..., padded_len-1]
    let mut indices_u32: Vec<u32> = (0..padded_len as u32).collect();

    // Upload keys and indices
    let d_keys = upload_f32(dev, &keys_f32)?;
    let d_indices = upload_u32(dev, &indices_u32)?;

    let count = padded_len as u32;
    let grid = CudaGpuContext::grid_size(count);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let func = dev
        .get_func("bitonic_sort", "bitonic_step")
        .ok_or_else(|| GpuError::ShaderError("bitonic_step not found".into()))?;

    // Bitonic sort: iterate through stages and substages
    // stage = 2, 4, 8, ..., padded_len
    // substage = stage/2, stage/4, ..., 1
    let mut stage: u32 = 2;
    while stage <= count {
        let mut substage = stage >> 1;
        while substage > 0 {
            unsafe {
                func.clone()
                    .launch(cfg, (&d_keys, &d_indices, count, stage, substage))
            }
            .map_err(|e| GpuError::ShaderError(format!("bitonic_step launch: {}", e)))?;

            substage >>= 1;
        }
        stage <<= 1;
    }

    // Download sorted indices and filter to original length
    let sorted_indices = download_u32(dev, &d_indices)?;
    let result: Vec<usize> = sorted_indices
        .into_iter()
        .filter(|&idx| (idx as usize) < n)
        .take(n)
        .map(|idx| idx as usize)
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuda_argsort_empty() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let result = cuda_argsort_f64(&ctx, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_cuda_argsort_single() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let result = cuda_argsort_f64(&ctx, &[42.0]).unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_cuda_argsort_sorted() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // Already sorted
        let values = vec![1.0, 2.0, 3.0, 4.0];
        let result = cuda_argsort_f64(&ctx, &values).unwrap();
        assert_eq!(result, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_cuda_argsort_reverse() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // Reverse order
        let values = vec![4.0, 3.0, 2.0, 1.0];
        let result = cuda_argsort_f64(&ctx, &values).unwrap();
        assert_eq!(result, vec![3, 2, 1, 0]);
    }

    #[test]
    fn test_cuda_argsort_non_power_of_two() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // 5 elements (not power of 2)
        let values = vec![5.0, 1.0, 3.0, 2.0, 4.0];
        let result = cuda_argsort_f64(&ctx, &values).unwrap();
        assert_eq!(result.len(), 5);
        // Verify sorted order
        for i in 1..result.len() {
            assert!(
                values[result[i - 1]] <= values[result[i]],
                "values not sorted at index {}: {} > {}",
                i,
                values[result[i - 1]],
                values[result[i]]
            );
        }
    }
}
