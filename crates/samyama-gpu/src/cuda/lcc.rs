//! CUDA-accelerated Local Clustering Coefficient (LCC)
//!
//! For each node, computes the fraction of possible edges among its neighbors
//! that actually exist. Uses the adjacency list with binary search to check
//! neighbor connectivity on the GPU.

use super::{alloc_zeros_f32, csr_to_device, download_f32, CudaGpuContext};
use crate::error::GpuError;
use cudarc::driver::{LaunchAsync, LaunchConfig};

const BLOCK_SIZE: u32 = 256;

/// Compute the local clustering coefficient of every node using CUDA.
///
/// The graph is in CSR format:
/// - `offsets`: length = node_count + 1
/// - `targets`: sorted neighbor lists for each node
///
/// If `directed` is false, the denominator for a node with degree k is k*(k-1)/2;
/// if true, it is k*(k-1).
///
/// Returns a Vec of f32 coefficients, one per node. Nodes with degree < 2
/// receive a coefficient of 0.
pub fn cuda_lcc(
    ctx: &CudaGpuContext,
    offsets: &[u32],
    targets: &[u32],
    node_count: usize,
    directed: bool,
) -> Result<Vec<f32>, GpuError> {
    if node_count == 0 {
        return Ok(Vec::new());
    }

    let dev = &ctx.dev;

    // Upload CSR structure
    let d_offsets = csr_to_device(dev, offsets)?;
    let d_targets = csr_to_device(dev, targets)?;

    // Allocate output coefficients (zeroed)
    let d_coefficients = alloc_zeros_f32(dev, node_count)?;

    let n = node_count as u32;
    let grid = CudaGpuContext::grid_size(n);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let func = dev
        .get_func("lcc", "lcc_compute")
        .ok_or_else(|| GpuError::ShaderError("lcc_compute not found".into()))?;

    let directed_flag: u32 = if directed { 1 } else { 0 };

    unsafe {
        func.clone().launch(
            cfg,
            (&d_offsets, &d_targets, &d_coefficients, n, directed_flag),
        )
    }
    .map_err(|e| GpuError::ShaderError(format!("lcc_compute launch: {}", e)))?;

    download_f32(dev, &d_coefficients)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuda_lcc_empty() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        let result = cuda_lcc(&ctx, &[0], &[], 0, false).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_cuda_lcc_isolated_nodes() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // 3 isolated nodes
        let offsets = vec![0, 0, 0, 0];
        let targets: Vec<u32> = vec![];
        let result = cuda_lcc(&ctx, &offsets, &targets, 3, false).unwrap();
        assert_eq!(result, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn test_cuda_lcc_triangle() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // Complete triangle: 0-1, 0-2, 1-2 (undirected CSR, sorted targets)
        // 0 -> [1,2], 1 -> [0,2], 2 -> [0,1]
        let offsets = vec![0, 2, 4, 6];
        let targets = vec![1, 2, 0, 2, 0, 1];
        let result = cuda_lcc(&ctx, &offsets, &targets, 3, false).unwrap();
        assert_eq!(result.len(), 3);
        // Every node in a triangle has LCC = 1.0
        for i in 0..3 {
            assert!(
                (result[i] - 1.0).abs() < 1e-5,
                "node {} expected LCC 1.0, got {}",
                i,
                result[i]
            );
        }
    }

    #[test]
    fn test_cuda_lcc_open_triple() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // Open triple: 0-1, 0-2, but no 1-2 edge
        // 0 -> [1,2], 1 -> [0], 2 -> [0]
        let offsets = vec![0, 2, 3, 4];
        let targets = vec![1, 2, 0, 0];
        let result = cuda_lcc(&ctx, &offsets, &targets, 3, false).unwrap();
        assert_eq!(result.len(), 3);
        // Node 0 has degree 2 but neighbors 1,2 are not connected => LCC = 0
        assert!(
            (result[0] - 0.0).abs() < 1e-5,
            "node 0 expected LCC 0, got {}",
            result[0]
        );
        // Nodes 1,2 have degree 1 => LCC = 0
        assert!((result[1] - 0.0).abs() < 1e-5);
        assert!((result[2] - 0.0).abs() < 1e-5);
    }

    #[test]
    fn test_cuda_lcc_directed() {
        let ctx = match CudaGpuContext::new() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("CUDA unavailable, skipping test");
                return;
            }
        };

        // Triangle (undirected CSR) but computed as directed
        // Denominator becomes d*(d-1) instead of d*(d-1)/2
        let offsets = vec![0, 2, 4, 6];
        let targets = vec![1, 2, 0, 2, 0, 1];
        let result = cuda_lcc(&ctx, &offsets, &targets, 3, true).unwrap();
        assert_eq!(result.len(), 3);
        // With directed flag, same triangle topology gives LCC = 0.5
        for i in 0..3 {
            assert!(
                (result[i] - 0.5).abs() < 1e-5,
                "node {} expected LCC 0.5 (directed), got {}",
                i,
                result[i]
            );
        }
    }
}
