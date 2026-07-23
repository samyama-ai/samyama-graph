//! GPU-accelerated aggregation (parallel reduction)
//!
//! Provides GPU SUM via tree reduction in shared memory.
//! Returns f64 result from f32 GPU computation.

use crate::buffer::{
    create_storage_buffer, create_storage_buffer_rw, create_uniform_buffer, dispatch_compute,
    download_f32,
};
use crate::context::GpuContext;
use crate::error::GpuError;

const SHADER_SOURCE: &str = include_str!("shaders/aggregate.wgsl");
const WORKGROUP_SIZE: u32 = 256;

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct AggParams {
    count: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

/// Compute SUM of f64 values on GPU
///
/// Converts to f32 for GPU, runs parallel reduction, returns f64.
pub fn gpu_sum_f64(ctx: &GpuContext, values: &[f64]) -> Result<f64, GpuError> {
    if values.is_empty() {
        return Ok(0.0);
    }

    #[cfg(feature = "cuda")]
    if let Some(cuda_ctx) = crate::runtime::GpuRuntime::get().and_then(|rt| rt.cuda()) {
        match crate::cuda::aggregate::cuda_sum_f64(cuda_ctx, values) {
            Ok(sum) => return Ok(sum),
            Err(e) => tracing::warn!("CUDA sum failed, falling back to wgpu: {}", e),
        }
    }

    let n = values.len();
    let values_f32: Vec<f32> = values.iter().map(|&v| v as f32).collect();

    let input_buf = create_storage_buffer(ctx, "agg_input", bytemuck::cast_slice(&values_f32));

    let num_workgroups = (n as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;
    let output_init: Vec<f32> = vec![0.0; num_workgroups as usize];
    let output_buf =
        create_storage_buffer_rw(ctx, "agg_output", bytemuck::cast_slice(&output_init));

    let params = AggParams {
        count: n as u32,
        _pad1: 0,
        _pad2: 0,
        _pad3: 0,
    };
    let params_buf = create_uniform_buffer(ctx, "agg_params", bytemuck::bytes_of(&params));

    let pipeline = ctx.create_compute_pipeline("reduce_sum", SHADER_SOURCE, "reduce_sum");
    let bind_group_layout = pipeline.get_bind_group_layout(0);
    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("agg_bind_group"),
        layout: &bind_group_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: input_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: output_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: params_buf.as_entire_binding(),
            },
        ],
    });

    dispatch_compute(ctx, &pipeline, &bind_group, num_workgroups);

    // Download partial sums and reduce on CPU
    let partial_sums = download_f32(ctx, &output_buf, num_workgroups as usize)?;
    let total: f64 = partial_sums.iter().map(|&s| s as f64).sum();

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_sum() {
        let ctx = match GpuContext::new() {
            Ok(ctx) => ctx,
            Err(_) => {
                println!("No GPU, skipping test");
                return;
            }
        };

        let values: Vec<f64> = (1..=1000).map(|i| i as f64).collect();
        let expected = 1000.0 * 1001.0 / 2.0; // Sum 1..=1000

        let result = gpu_sum_f64(&ctx, &values).unwrap();
        assert!(
            (result - expected).abs() / expected < 0.001,
            "GPU SUM should be ~{}, got {}",
            expected,
            result
        );
    }
}
