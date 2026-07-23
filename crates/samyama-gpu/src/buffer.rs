//! GPU buffer helpers for uploading/downloading data

use crate::context::GpuContext;
use crate::error::GpuError;

/// CSR buffers uploaded to GPU
pub struct CsrBuffers {
    pub out_offsets: wgpu::Buffer,
    pub out_targets: wgpu::Buffer,
    pub in_offsets: wgpu::Buffer,
    pub in_sources: wgpu::Buffer,
    pub out_degrees: wgpu::Buffer,
    pub node_count: u32,
    pub edge_count: u32,
}

/// Upload raw CSR arrays to GPU buffers
///
/// All arrays should be u32. out_degrees is computed from out_offsets.
pub fn upload_csr(
    ctx: &GpuContext,
    node_count: usize,
    out_offsets: &[usize],
    out_targets: &[usize],
    in_offsets: &[usize],
    in_sources: &[usize],
) -> CsrBuffers {
    let out_offsets_u32: Vec<u32> = out_offsets.iter().map(|&x| x as u32).collect();
    let out_targets_u32: Vec<u32> = out_targets.iter().map(|&x| x as u32).collect();
    let in_offsets_u32: Vec<u32> = in_offsets.iter().map(|&x| x as u32).collect();
    let in_sources_u32: Vec<u32> = in_sources.iter().map(|&x| x as u32).collect();

    let out_degrees_u32: Vec<u32> = (0..node_count)
        .map(|i| (out_offsets[i + 1] - out_offsets[i]) as u32)
        .collect();

    let out_offsets_buf =
        create_storage_buffer(ctx, "out_offsets", bytemuck::cast_slice(&out_offsets_u32));
    let out_targets_buf =
        create_storage_buffer(ctx, "out_targets", bytemuck::cast_slice(&out_targets_u32));
    let in_offsets_buf =
        create_storage_buffer(ctx, "in_offsets", bytemuck::cast_slice(&in_offsets_u32));
    let in_sources_buf =
        create_storage_buffer(ctx, "in_sources", bytemuck::cast_slice(&in_sources_u32));
    let out_degrees_buf =
        create_storage_buffer(ctx, "out_degrees", bytemuck::cast_slice(&out_degrees_u32));

    CsrBuffers {
        out_offsets: out_offsets_buf,
        out_targets: out_targets_buf,
        in_offsets: in_offsets_buf,
        in_sources: in_sources_buf,
        out_degrees: out_degrees_buf,
        node_count: node_count as u32,
        edge_count: out_targets.len() as u32,
    }
}

/// Create a uniform buffer from data
pub fn create_uniform_buffer(ctx: &GpuContext, label: &str, data: &[u8]) -> wgpu::Buffer {
    use wgpu::util::DeviceExt;
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some(label),
            contents: data,
            usage: wgpu::BufferUsages::UNIFORM,
        })
}

/// Create a read-only storage buffer from data
pub fn create_storage_buffer(ctx: &GpuContext, label: &str, data: &[u8]) -> wgpu::Buffer {
    use wgpu::util::DeviceExt;
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some(label),
            contents: data,
            usage: wgpu::BufferUsages::STORAGE,
        })
}

/// Create a read-write storage buffer from data
pub fn create_storage_buffer_rw(ctx: &GpuContext, label: &str, data: &[u8]) -> wgpu::Buffer {
    use wgpu::util::DeviceExt;
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some(label),
            contents: data,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        })
}

/// Create an empty read-write storage buffer of given size
pub fn create_empty_buffer_rw(ctx: &GpuContext, label: &str, size: u64) -> wgpu::Buffer {
    ctx.device.create_buffer(&wgpu::BufferDescriptor {
        label: Some(label),
        size,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    })
}

/// Create a staging buffer for reading back results
pub fn create_staging_buffer(ctx: &GpuContext, label: &str, size: u64) -> wgpu::Buffer {
    ctx.device.create_buffer(&wgpu::BufferDescriptor {
        label: Some(label),
        size,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    })
}

/// Download f32 data from a GPU buffer
pub fn download_f32(
    ctx: &GpuContext,
    source: &wgpu::Buffer,
    count: usize,
) -> Result<Vec<f32>, GpuError> {
    let size = (count * std::mem::size_of::<f32>()) as u64;
    let staging = create_staging_buffer(ctx, "staging_f32", size);

    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("download_f32"),
        });
    encoder.copy_buffer_to_buffer(source, 0, &staging, 0, size);
    ctx.queue.submit(Some(encoder.finish()));

    let slice = staging.slice(..);
    let (tx, rx) = std::sync::mpsc::channel();
    slice.map_async(wgpu::MapMode::Read, move |result| {
        let _ = tx.send(result);
    });
    ctx.device.poll(wgpu::Maintain::Wait);

    rx.recv()
        .map_err(|_| GpuError::BufferMapFailed("channel recv failed".into()))?
        .map_err(|e| GpuError::BufferMapFailed(e.to_string()))?;

    let data = slice.get_mapped_range();
    let result: Vec<f32> = bytemuck::cast_slice(&data).to_vec();
    drop(data);
    staging.unmap();

    Ok(result)
}

/// Download u32 data from a GPU buffer
pub fn download_u32(
    ctx: &GpuContext,
    source: &wgpu::Buffer,
    count: usize,
) -> Result<Vec<u32>, GpuError> {
    let size = (count * std::mem::size_of::<u32>()) as u64;
    let staging = create_staging_buffer(ctx, "staging_u32", size);

    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("download_u32"),
        });
    encoder.copy_buffer_to_buffer(source, 0, &staging, 0, size);
    ctx.queue.submit(Some(encoder.finish()));

    let slice = staging.slice(..);
    let (tx, rx) = std::sync::mpsc::channel();
    slice.map_async(wgpu::MapMode::Read, move |result| {
        let _ = tx.send(result);
    });
    ctx.device.poll(wgpu::Maintain::Wait);

    rx.recv()
        .map_err(|_| GpuError::BufferMapFailed("channel recv failed".into()))?
        .map_err(|e| GpuError::BufferMapFailed(e.to_string()))?;

    let data = slice.get_mapped_range();
    let result: Vec<u32> = bytemuck::cast_slice(&data).to_vec();
    drop(data);
    staging.unmap();

    Ok(result)
}

/// Dispatch a compute pipeline and wait for completion
pub fn dispatch_compute(
    ctx: &GpuContext,
    pipeline: &wgpu::ComputePipeline,
    bind_group: &wgpu::BindGroup,
    workgroup_count: u32,
) {
    let mut encoder = ctx
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("compute_dispatch"),
        });
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("compute_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(pipeline);
        pass.set_bind_group(0, bind_group, &[]);
        pass.dispatch_workgroups(workgroup_count, 1, 1);
    }
    ctx.queue.submit(Some(encoder.finish()));
}
