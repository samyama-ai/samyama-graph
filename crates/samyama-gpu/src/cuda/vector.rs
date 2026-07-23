//! CUDA-accelerated vector distance computations
//!
//! Provides batched cosine distance and inner product distance for
//! nearest-neighbor search. Each GPU thread handles one candidate vector.

use super::{alloc_zeros_f32, download_f32, upload_f32, CudaGpuContext};
use crate::error::GpuError;
use cudarc::driver::{LaunchAsync, LaunchConfig};

const BLOCK_SIZE: u32 = 256;

/// Compute cosine distance between a query vector and a batch of candidate vectors.
///
/// - `query`: single query vector of length `dimensions`
/// - `candidates`: flattened matrix of candidate vectors (length = candidate_count * dimensions)
/// - `dimensions`: dimensionality of each vector
///
/// Returns a Vec of cosine distances (1 - cosine_similarity), one per candidate.
pub fn cuda_batch_cosine(
    ctx: &CudaGpuContext,
    query: &[f32],
    candidates: &[f32],
    dimensions: usize,
) -> Result<Vec<f32>, GpuError> {
    if dimensions == 0 || candidates.is_empty() {
        return Ok(Vec::new());
    }

    let candidate_count = candidates.len() / dimensions;
    if candidates.len() % dimensions != 0 {
        return Err(GpuError::ShaderError(
            "candidates length not a multiple of dimensions".into(),
        ));
    }
    if query.len() != dimensions {
        return Err(GpuError::ShaderError(
            "query length does not match dimensions".into(),
        ));
    }

    let dev = &ctx.dev;

    let d_query = upload_f32(dev, query)?;
    let d_candidates = upload_f32(dev, candidates)?;
    let d_distances = alloc_zeros_f32(dev, candidate_count)?;

    let n = candidate_count as u32;
    let grid = CudaGpuContext::grid_size(n);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let func = dev
        .get_func("cosine", "cosine_batch")
        .ok_or_else(|| GpuError::ShaderError("cosine_batch not found".into()))?;

    unsafe {
        func.clone().launch(
            cfg,
            (
                &d_query,
                &d_candidates,
                &d_distances,
                dimensions as u32,
                candidate_count as u32,
            ),
        )
    }
    .map_err(|e| GpuError::ShaderError(format!("cosine_batch launch: {}", e)))?;

    download_f32(dev, &d_distances)
}

/// Compute inner product distance between a query vector and a batch of candidates.
///
/// Distance is defined as `1.0 - dot(query, candidate)`, matching the convention
/// where smaller values indicate higher similarity.
///
/// - `query`: single query vector of length `dimensions`
/// - `candidates`: flattened matrix of candidate vectors (length = candidate_count * dimensions)
/// - `dimensions`: dimensionality of each vector
///
/// Returns a Vec of inner-product distances, one per candidate.
pub fn cuda_batch_inner_product(
    ctx: &CudaGpuContext,
    query: &[f32],
    candidates: &[f32],
    dimensions: usize,
) -> Result<Vec<f32>, GpuError> {
    if dimensions == 0 || candidates.is_empty() {
        return Ok(Vec::new());
    }

    let candidate_count = candidates.len() / dimensions;
    if candidates.len() % dimensions != 0 {
        return Err(GpuError::ShaderError(
            "candidates length not a multiple of dimensions".into(),
        ));
    }
    if query.len() != dimensions {
        return Err(GpuError::ShaderError(
            "query length does not match dimensions".into(),
        ));
    }

    let dev = &ctx.dev;

    let d_query = upload_f32(dev, query)?;
    let d_candidates = upload_f32(dev, candidates)?;
    let d_distances = alloc_zeros_f32(dev, candidate_count)?;

    let n = candidate_count as u32;
    let grid = CudaGpuContext::grid_size(n);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let func = dev
        .get_func("inner_product", "inner_product_batch")
        .ok_or_else(|| GpuError::ShaderError("inner_product_batch not found".into()))?;

    unsafe {
        func.clone().launch(
            cfg,
            (
                &d_query,
                &d_candidates,
                &d_distances,
                dimensions as u32,
                candidate_count as u32,
            ),
        )
    }
    .map_err(|e| GpuError::ShaderError(format!("inner_product_batch launch: {}", e)))?;

    download_f32(dev, &d_distances)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuda_batch_cosine_empty() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let result = cuda_batch_cosine(&ctx, &[1.0, 0.0], &[], 2).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_cuda_batch_cosine_identical() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let query = vec![1.0, 0.0, 0.0];
        let candidates = vec![1.0, 0.0, 0.0]; // identical to query
        let result = cuda_batch_cosine(&ctx, &query, &candidates, 3).unwrap();
        assert_eq!(result.len(), 1);
        assert!(
            (result[0]).abs() < 1e-5,
            "identical vectors should have cosine distance ~0"
        );
    }

    #[test]
    fn test_cuda_batch_inner_product_basic() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let query = vec![1.0, 0.0];
        // Two candidates: [1,0] (dot=1, dist=0) and [0,1] (dot=0, dist=1)
        let candidates = vec![1.0, 0.0, 0.0, 1.0];
        let result = cuda_batch_inner_product(&ctx, &query, &candidates, 2).unwrap();
        assert_eq!(result.len(), 2);
        assert!((result[0] - 0.0).abs() < 1e-5, "parallel vectors: dist ~0");
        assert!(
            (result[1] - 1.0).abs() < 1e-5,
            "orthogonal vectors: dist ~1"
        );
    }

    #[test]
    fn test_cuda_batch_cosine_dimension_mismatch() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // query has 2 dims but dimensions says 3
        let result = cuda_batch_cosine(&ctx, &[1.0, 0.0], &[1.0, 0.0, 0.0], 3);
        assert!(result.is_err());
    }
}
