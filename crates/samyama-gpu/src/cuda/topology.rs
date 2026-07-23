//! CUDA-accelerated triangle counting
//!
//! Uses a merge-intersection approach: one GPU thread per edge computes
//! the sorted-neighbor intersection of the edge endpoints, counting
//! common neighbors via atomicAdd.

use super::{alloc_zeros_u32, csr_to_device, download_u32, CudaGpuContext};
use crate::error::GpuError;
use cudarc::driver::{LaunchAsync, LaunchConfig};

const BLOCK_SIZE: u32 = 256;

/// Count triangles in an undirected graph using CUDA.
///
/// The graph is represented in CSR format:
/// - `edge_src` / `edge_dst`: source and destination of each edge (length = edge_count)
/// - `offsets` / `targets`: standard CSR adjacency (offsets length = node_count + 1)
///
/// Returns the total number of triangles.
pub fn cuda_count_triangles(
    ctx: &CudaGpuContext,
    edge_src: &[u32],
    edge_dst: &[u32],
    offsets: &[u32],
    targets: &[u32],
) -> Result<u32, GpuError> {
    let edge_count = edge_src.len();
    if edge_count == 0 {
        return Ok(0);
    }

    let dev = &ctx.dev;

    // Upload edge list and CSR adjacency to GPU
    let d_edge_src = csr_to_device(dev, edge_src)?;
    let d_edge_dst = csr_to_device(dev, edge_dst)?;
    let d_offsets = csr_to_device(dev, offsets)?;
    let d_targets = csr_to_device(dev, targets)?;

    // Single u32 for the atomic triangle counter (zeroed)
    let d_triangle_count = alloc_zeros_u32(dev, 1)?;

    let n = edge_count as u32;
    let grid = CudaGpuContext::grid_size(n);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let func = dev
        .get_func("triangles", "count_triangles")
        .ok_or_else(|| GpuError::ShaderError("count_triangles not found".into()))?;

    unsafe {
        func.clone().launch(
            cfg,
            (
                &d_edge_src,
                &d_edge_dst,
                &d_offsets,
                &d_targets,
                &d_triangle_count,
                n,
            ),
        )
    }
    .map_err(|e| GpuError::ShaderError(format!("count_triangles launch: {}", e)))?;

    let result = download_u32(dev, &d_triangle_count)?;
    Ok(result[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuda_count_triangles_empty() {
        // Verify empty input returns zero without touching the GPU
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let result = cuda_count_triangles(&ctx, &[], &[], &[0], &[]).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_cuda_count_triangles_single() {
        // Triangle: 0-1, 0-2, 1-2
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // Edges (both directions for undirected)
        let edge_src = vec![0, 0, 1, 1, 2, 2];
        let edge_dst = vec![1, 2, 0, 2, 0, 1];

        // CSR: node 0 -> [1,2], node 1 -> [0,2], node 2 -> [0,1]
        let offsets = vec![0, 2, 4, 6];
        let targets = vec![1, 2, 0, 2, 0, 1];

        let result = cuda_count_triangles(&ctx, &edge_src, &edge_dst, &offsets, &targets).unwrap();
        assert_eq!(result, 1, "single triangle should count as 1");
    }
}
