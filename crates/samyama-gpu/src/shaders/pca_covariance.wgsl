// PCA covariance matrix computation (tiled)
//
// Computes C[r][c] = sum(data[i][r] * data[i][c]) / (n_samples - 1)
// One thread per (r, c) entry. Uses tiled sample accumulation to improve
// cache utilization — processes TILE_SIZE samples at a time via shared memory.
// Output is a features x features symmetric matrix (computes all entries).

const TILE_SIZE: u32 = 64u;

@group(0) @binding(0) var<storage, read> data: array<f32>;
@group(0) @binding(1) var<storage, read_write> cov: array<f32>;
@group(0) @binding(2) var<uniform> params: CovParams;

struct CovParams {
    n_samples: u32,
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
}

@compute @workgroup_size(256)
fn compute_covariance(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = params.n_features * params.n_features;
    if (idx >= total) {
        return;
    }

    let row = idx / params.n_features;
    let col = idx % params.n_features;
    let d = params.n_features;
    let n = params.n_samples;

    // Tiled accumulation: process samples in chunks of TILE_SIZE
    // This improves temporal locality — adjacent threads reading adjacent features
    // will hit the same cache lines within a tile.
    var dot: f32 = 0.0;
    var tile_start: u32 = 0u;
    while (tile_start < n) {
        let tile_end = min(tile_start + TILE_SIZE, n);
        for (var i: u32 = tile_start; i < tile_end; i = i + 1u) {
            dot = dot + data[i * d + row] * data[i * d + col];
        }
        tile_start = tile_end;
    }

    let denom = max(f32(n) - 1.0, 1.0);
    cov[idx] = dot / denom;
}
