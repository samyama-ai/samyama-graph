// Parallel reduction kernel for SUM aggregation
//
// Two-pass reduction: each workgroup reduces its portion, then a final pass
// combines workgroup results. Uses f32 for GPU compatibility.

@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read_write> output: array<f32>;
@group(0) @binding(2) var<uniform> params: AggParams;

struct AggParams {
    count: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

var<workgroup> shared_data: array<f32, 256>;

@compute @workgroup_size(256)
fn reduce_sum(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) wg_id: vec3<u32>,
) {
    let tid = local_id.x;
    let gid = global_id.x;

    // Load data into shared memory
    if (gid < params.count) {
        shared_data[tid] = input[gid];
    } else {
        shared_data[tid] = 0.0;
    }
    workgroupBarrier();

    // Tree reduction in shared memory
    var stride: u32 = 128u;
    loop {
        if (stride == 0u) {
            break;
        }
        if (tid < stride) {
            shared_data[tid] = shared_data[tid] + shared_data[tid + stride];
        }
        workgroupBarrier();
        stride = stride / 2u;
    }

    // Write workgroup result
    if (tid == 0u) {
        output[wg_id.x] = shared_data[0];
    }
}
