//! CUDA-accelerated Community Detection via Label Propagation (CDLP)
//!
//! Each node starts with its own ID as label. On each iteration every node
//! adopts the most frequent label among its neighbors (tie-break: smallest
//! label). Two label buffers are ping-ponged between iterations to avoid
//! read/write conflicts.

use super::{csr_to_device, download_u32, upload_u32, CudaGpuContext};
use crate::error::GpuError;
use cudarc::driver::{LaunchAsync, LaunchConfig};

const BLOCK_SIZE: u32 = 256;

/// Run CDLP on the GPU for a fixed number of iterations.
///
/// The graph is in CSR format:
/// - `offsets`: length = node_count + 1
/// - `targets`: neighbors for each node
///
/// Returns a Vec of community labels, one per node.
pub fn cuda_cdlp(
    ctx: &CudaGpuContext,
    offsets: &[u32],
    targets: &[u32],
    node_count: usize,
    max_iterations: usize,
) -> Result<Vec<u32>, GpuError> {
    if node_count == 0 {
        return Ok(Vec::new());
    }

    let dev = &ctx.dev;

    // Upload CSR structure
    let d_offsets = csr_to_device(dev, offsets)?;
    let d_targets = csr_to_device(dev, targets)?;

    // Initialize labels: each node starts with its own id
    let initial_labels: Vec<u32> = (0..node_count as u32).collect();
    let d_labels_a = upload_u32(dev, &initial_labels)?;
    let d_labels_b = upload_u32(dev, &initial_labels)?;

    let n = node_count as u32;
    let grid = CudaGpuContext::grid_size(n);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let func = dev
        .get_func("cdlp", "cdlp_iter")
        .ok_or_else(|| GpuError::ShaderError("cdlp_iter not found".into()))?;

    // Ping-pong iterations
    for iter in 0..max_iterations {
        let (read, write) = if iter % 2 == 0 {
            (&d_labels_a, &d_labels_b)
        } else {
            (&d_labels_b, &d_labels_a)
        };

        unsafe {
            func.clone()
                .launch(cfg, (&d_offsets, &d_targets, read, write, n))
        }
        .map_err(|e| GpuError::ShaderError(format!("cdlp_iter launch: {}", e)))?;
    }

    // Download from whichever buffer was last written to
    let final_labels = if max_iterations == 0 {
        download_u32(dev, &d_labels_a)?
    } else if max_iterations % 2 == 0 {
        download_u32(dev, &d_labels_a)?
    } else {
        download_u32(dev, &d_labels_b)?
    };

    Ok(final_labels)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuda_cdlp_empty() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let result = cuda_cdlp(&ctx, &[0], &[], 0, 10).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_cuda_cdlp_zero_iterations() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // 3 nodes, no iterations => labels stay as initial [0,1,2]
        let offsets = vec![0, 1, 2, 3];
        let targets = vec![1, 0, 0]; // 0->1, 1->0, 2->0
        let result = cuda_cdlp(&ctx, &offsets, &targets, 3, 0).unwrap();
        assert_eq!(result, vec![0, 1, 2]);
    }

    #[test]
    fn test_cuda_cdlp_disconnected() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // 3 isolated nodes (no edges)
        let offsets = vec![0, 0, 0, 0];
        let targets: Vec<u32> = vec![];
        let result = cuda_cdlp(&ctx, &offsets, &targets, 3, 5).unwrap();
        // Isolated nodes keep their own label
        assert_eq!(result, vec![0, 1, 2]);
    }

    #[test]
    fn test_cuda_cdlp_triangle() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // Triangle: 0-1, 0-2, 1-2 (undirected, each direction stored)
        // CSR: 0->[1,2], 1->[0,2], 2->[0,1]
        let offsets = vec![0, 2, 4, 6];
        let targets = vec![1, 2, 0, 2, 0, 1];
        let result = cuda_cdlp(&ctx, &offsets, &targets, 3, 10).unwrap();
        assert_eq!(result.len(), 3);
        // In a triangle, all nodes should converge to the same (smallest) label
        assert_eq!(result[0], result[1]);
        assert_eq!(result[1], result[2]);
        assert_eq!(result[0], 0);
    }
}
