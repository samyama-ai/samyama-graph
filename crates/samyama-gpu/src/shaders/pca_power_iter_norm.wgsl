// Fused power iteration: matrix-vector multiply + parallel normalize
//
// Stage 1: w[i] = sum_j(cov[i*d + j] * v_in[j])  (mat-vec multiply)
// Stage 2: Parallel reduction to compute ||w||
// Stage 3: v_out[i] = w[i] / ||w||
//
// Eliminates the CPU round-trip for normalization that was in pca_power_iter.wgsl.

@group(0) @binding(0) var<storage, read> cov: array<f32>;
@group(0) @binding(1) var<storage, read> v_in: array<f32>;
@group(0) @binding(2) var<storage, read_write> v_out: array<f32>;
@group(0) @binding(3) var<uniform> params: PowerIterNormParams;

struct PowerIterNormParams {
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

var<workgroup> shared_sq: array<f32, 256>;

@compute @workgroup_size(256)
fn power_iter_norm(@builtin(global_invocation_id) gid: vec3<u32>,
                   @builtin(local_invocation_id) lid: vec3<u32>,
                   @builtin(workgroup_id) wgid: vec3<u32>) {
    let row = gid.x;
    let local_id = lid.x;
    let d = params.n_features;

    // Stage 1: Compute w[row] = cov[row, :] dot v_in[:]
    var dot: f32 = 0.0;
    if (row < d) {
        for (var j: u32 = 0u; j < d; j = j + 1u) {
            dot = dot + cov[row * d + j] * v_in[j];
        }
        // Store w temporarily in v_out
        v_out[row] = dot;
    }

    // Stage 2: Parallel reduction to compute sum of squares
    // Each thread contributes its w[row]^2
    if (row < d) {
        shared_sq[local_id] = dot * dot;
    } else {
        shared_sq[local_id] = 0.0;
    }
    workgroupBarrier();

    // Tree reduction within workgroup
    var stride: u32 = 128u;
    while (stride > 0u) {
        if (local_id < stride) {
            shared_sq[local_id] = shared_sq[local_id] + shared_sq[local_id + stride];
        }
        workgroupBarrier();
        stride = stride >> 1u;
    }

    // Stage 3: Normalize v_out = w / ||w||
    let norm = sqrt(shared_sq[0]);
    workgroupBarrier();

    if (row < d && norm > 1e-15) {
        v_out[row] = v_out[row] / norm;
    }
}
