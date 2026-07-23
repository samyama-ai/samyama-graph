// Batch inner product distance GPU kernel
//
// Computes inner product distance (1.0 - dot product) between query and K candidates.
// One thread per candidate.

@group(0) @binding(0) var<storage, read> query: array<f32>;
@group(0) @binding(1) var<storage, read> candidates: array<f32>;
@group(0) @binding(2) var<storage, read_write> distances: array<f32>;
@group(0) @binding(3) var<uniform> params: VectorParams;

struct VectorParams {
    dimensions: u32,
    candidate_count: u32,
    _pad1: u32,
    _pad2: u32,
}

@compute @workgroup_size(256)
fn inner_product_batch(@builtin(global_invocation_id) id: vec3<u32>) {
    let k = id.x;
    if (k >= params.candidate_count) {
        return;
    }

    let dim = params.dimensions;
    let offset = k * dim;
    var dot: f32 = 0.0;

    for (var i: u32 = 0u; i < dim; i = i + 1u) {
        dot = dot + query[i] * candidates[offset + i];
    }

    distances[k] = 1.0 - dot;
}
