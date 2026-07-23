// PCA column-wise mean computation
//
// One workgroup per feature. Each workgroup reduces across all samples
// for its assigned feature using tree reduction in shared memory.

@group(0) @binding(0) var<storage, read> data: array<f32>;
@group(0) @binding(1) var<storage, read_write> means: array<f32>;
@group(0) @binding(2) var<uniform> params: MeanParams;

struct MeanParams {
    n_samples: u32,
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
}

var<workgroup> shared_sum: array<f32, 256>;

@compute @workgroup_size(256)
fn compute_mean(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) wg_id: vec3<u32>,
) {
    let feature = wg_id.x;
    let tid = local_id.x;

    if (feature >= params.n_features) {
        return;
    }

    // Each thread accumulates a partial sum over strided samples
    var partial_sum: f32 = 0.0;
    var i = tid;
    while (i < params.n_samples) {
        partial_sum = partial_sum + data[i * params.n_features + feature];
        i = i + 256u;
    }

    shared_sum[tid] = partial_sum;
    workgroupBarrier();

    // Tree reduction in shared memory
    var stride: u32 = 128u;
    while (stride > 0u) {
        if (tid < stride) {
            shared_sum[tid] = shared_sum[tid] + shared_sum[tid + stride];
        }
        workgroupBarrier();
        stride = stride / 2u;
    }

    // Thread 0 writes the mean for this feature
    if (tid == 0u) {
        means[feature] = shared_sum[0] / f32(params.n_samples);
    }
}
