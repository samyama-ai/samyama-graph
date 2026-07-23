// Bitonic sort kernel for GPU-accelerated ORDER BY
//
// Sorts (key, index) pairs. Keys are f32 values, indices are u32 (original positions).
// After sorting, the indices give the sorted order.

@group(0) @binding(0) var<storage, read_write> keys: array<f32>;
@group(0) @binding(1) var<storage, read_write> indices: array<u32>;
@group(0) @binding(2) var<uniform> params: SortParams;

struct SortParams {
    count: u32,         // Number of elements
    stage: u32,         // Current stage (k)
    substage: u32,      // Current substage (j)
    _pad: u32,
}

@compute @workgroup_size(256)
fn bitonic_step(@builtin(global_invocation_id) id: vec3<u32>) {
    let i = id.x;
    if (i >= params.count) {
        return;
    }

    let j = params.substage;
    let k = params.stage;

    // Compute partner index for this element
    let ixj = i ^ j;

    // Only process if ixj > i (each pair processed once)
    if (ixj <= i) {
        return;
    }
    if (ixj >= params.count) {
        return;
    }

    // Determine sort direction for this block
    let ascending = ((i & k) == 0u);

    let key_i = keys[i];
    let key_j = keys[ixj];

    let should_swap = (ascending && key_i > key_j) || (!ascending && key_i < key_j);

    if (should_swap) {
        // Swap keys
        keys[i] = key_j;
        keys[ixj] = key_i;

        // Swap indices
        let idx_i = indices[i];
        let idx_j = indices[ixj];
        indices[i] = idx_j;
        indices[ixj] = idx_i;
    }
}
