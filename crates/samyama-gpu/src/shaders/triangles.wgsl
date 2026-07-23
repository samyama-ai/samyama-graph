// Triangle counting GPU kernel (edge-centric)
//
// One thread per edge. For each edge (u, v) where u < v,
// count common neighbors via binary search in sorted adjacency lists.

@group(0) @binding(0) var<storage, read> edge_src: array<u32>;
@group(0) @binding(1) var<storage, read> edge_dst: array<u32>;
@group(0) @binding(2) var<storage, read> offsets: array<u32>;
@group(0) @binding(3) var<storage, read> targets: array<u32>;
@group(0) @binding(4) var<storage, read_write> triangle_count: atomic<u32>;
@group(0) @binding(5) var<uniform> params: TriangleParams;

struct TriangleParams {
    edge_count: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

// Binary search for value in sorted array slice [start..end)
fn binary_search(value: u32, start: u32, end: u32) -> bool {
    var lo = start;
    var hi = end;
    // Use `while` (not `loop`) so the naga validator can see the terminating
    // return below; a bare `loop` leaves an apparent fall-through path and
    // fails shader validation ("return value None does not match ... bool").
    while (lo < hi) {
        let mid = (lo + hi) / 2u;
        let mid_val = targets[mid];
        if (mid_val == value) {
            return true;
        } else if (mid_val < value) {
            lo = mid + 1u;
        } else {
            hi = mid;
        }
    }
    return false;
}

@compute @workgroup_size(256)
fn count_triangles(@builtin(global_invocation_id) id: vec3<u32>) {
    let edge_idx = id.x;
    if (edge_idx >= params.edge_count) {
        return;
    }

    let u = edge_src[edge_idx];
    let v = edge_dst[edge_idx];

    // Only process edges where u < v to avoid double-counting
    if (u >= v) {
        return;
    }

    let u_start = offsets[u];
    let u_end = offsets[u + 1u];
    let v_start = offsets[v];
    let v_end = offsets[v + 1u];

    // Find common neighbors w > v using merge-style intersection
    var ui = u_start;
    var vi = v_start;
    while (ui < u_end && vi < v_end) {
        let u_neighbor = targets[ui];
        let v_neighbor = targets[vi];

        if (u_neighbor == v_neighbor) {
            // Common neighbor found; only count if w > v
            if (u_neighbor > v) {
                atomicAdd(&triangle_count, 1u);
            }
            ui = ui + 1u;
            vi = vi + 1u;
        } else if (u_neighbor < v_neighbor) {
            ui = ui + 1u;
        } else {
            vi = vi + 1u;
        }
    }
}
