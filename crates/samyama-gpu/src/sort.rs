//! GPU-accelerated sorting (bitonic sort)
//!
//! Sorts an array of f64 values on GPU and returns the sorted indices.
//! Uses bitonic sort which is O(n log^2 n) but highly parallel.

use crate::buffer::{
    create_storage_buffer_rw, create_uniform_buffer, dispatch_compute, download_u32,
};
use crate::context::GpuContext;
use crate::error::GpuError;

const SHADER_SOURCE: &str = include_str!("shaders/bitonic_sort.wgsl");
const WORKGROUP_SIZE: u32 = 256;

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct SortParams {
    count: u32,
    stage: u32,
    substage: u32,
    _pad: u32,
}

/// Sort f64 values on GPU and return sorted indices (argsort)
///
/// The returned Vec<u32> contains original indices in sorted order.
/// Converts to f32 for GPU computation.
pub fn gpu_argsort_f64(ctx: &GpuContext, values: &[f64]) -> Result<Vec<u32>, GpuError> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    #[cfg(feature = "cuda")]
    if let Some(cuda_ctx) = crate::runtime::GpuRuntime::get().and_then(|rt| rt.cuda()) {
        match crate::cuda::sort::cuda_argsort_f64(cuda_ctx, values) {
            Ok(indices) => return Ok(indices.into_iter().map(|i| i as u32).collect()),
            Err(e) => tracing::warn!("CUDA sort failed, falling back to wgpu: {}", e),
        }
    }

    let n = values.len();

    // Pad to next power of 2 for bitonic sort
    let padded_n = n.next_power_of_two();

    let mut keys: Vec<f32> = values.iter().map(|&v| v as f32).collect();
    // Pad with f32::MAX so padded elements sort to end
    keys.resize(padded_n, f32::MAX);

    let indices: Vec<u32> = (0..padded_n as u32).collect();

    let keys_buf = create_storage_buffer_rw(ctx, "sort_keys", bytemuck::cast_slice(&keys));
    let indices_buf = create_storage_buffer_rw(ctx, "sort_indices", bytemuck::cast_slice(&indices));

    let pipeline = ctx.create_compute_pipeline("bitonic_sort", SHADER_SOURCE, "bitonic_step");
    let workgroup_count = (padded_n as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;

    // Bitonic sort stages
    let mut k: u32 = 2;
    while k <= padded_n as u32 {
        let mut j = k / 2;
        while j > 0 {
            let params = SortParams {
                count: padded_n as u32,
                stage: k,
                substage: j,
                _pad: 0,
            };
            let params_buf = create_uniform_buffer(ctx, "sort_params", bytemuck::bytes_of(&params));

            let bind_group_layout = pipeline.get_bind_group_layout(0);
            let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("sort_bind_group"),
                layout: &bind_group_layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: keys_buf.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: indices_buf.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: params_buf.as_entire_binding(),
                    },
                ],
            });

            dispatch_compute(ctx, &pipeline, &bind_group, workgroup_count);

            j /= 2;
        }
        k *= 2;
    }

    // Download sorted indices
    let sorted_indices = download_u32(ctx, &indices_buf, padded_n)?;

    // Trim to original size, filtering out padding indices
    let result: Vec<u32> = sorted_indices
        .into_iter()
        .filter(|&idx| (idx as usize) < n)
        .take(n)
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_argsort() {
        let ctx = match GpuContext::new() {
            Ok(ctx) => ctx,
            Err(_) => {
                println!("No GPU, skipping test");
                return;
            }
        };

        let values = vec![3.0, 1.0, 4.0, 1.5, 2.0];
        let sorted = gpu_argsort_f64(&ctx, &values).unwrap();

        // Expected order: 1.0(1), 1.5(3), 2.0(4), 3.0(0), 4.0(2)
        assert_eq!(sorted.len(), 5);
        assert_eq!(sorted[0], 1); // 1.0
        assert_eq!(sorted[1], 3); // 1.5
        assert_eq!(sorted[2], 4); // 2.0
        assert_eq!(sorted[3], 0); // 3.0
        assert_eq!(sorted[4], 2); // 4.0
    }
}
