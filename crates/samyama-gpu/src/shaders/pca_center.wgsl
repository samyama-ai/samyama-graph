// PCA data centering kernel
//
// Subtracts the column mean from each element: data[i][j] -= mean[j]
// One thread per element (samples * features).

@group(0) @binding(0) var<storage, read_write> data: array<f32>;
@group(0) @binding(1) var<storage, read> means: array<f32>;
@group(0) @binding(2) var<uniform> params: CenterParams;

struct CenterParams {
    n_samples: u32,
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
}

@compute @workgroup_size(256)
fn center_data(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = params.n_samples * params.n_features;
    if (idx >= total) {
        return;
    }

    let feature = idx % params.n_features;
    data[idx] = data[idx] - means[feature];
}
