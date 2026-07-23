// PCA power iteration kernel
//
// One iteration of the power method: w = C @ v
// One thread per feature (row of the covariance matrix).
// Normalization happens on the CPU after download.

@group(0) @binding(0) var<storage, read> cov: array<f32>;
@group(0) @binding(1) var<storage, read> v_in: array<f32>;
@group(0) @binding(2) var<storage, read_write> v_out: array<f32>;
@group(0) @binding(3) var<uniform> params: PowerIterParams;

struct PowerIterParams {
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

@compute @workgroup_size(256)
fn power_iter(@builtin(global_invocation_id) id: vec3<u32>) {
    let row = id.x;
    if (row >= params.n_features) {
        return;
    }

    var dot: f32 = 0.0;
    for (var j: u32 = 0u; j < params.n_features; j = j + 1u) {
        dot = dot + cov[row * params.n_features + j] * v_in[j];
    }

    v_out[row] = dot;
}
