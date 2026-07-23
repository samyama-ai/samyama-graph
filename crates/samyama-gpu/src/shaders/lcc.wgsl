// Local Clustering Coefficient GPU kernel
//
// One thread per node. For each node, enumerate neighbor pairs and check
// adjacency via binary search in the sorted CSR tgts array.
// Supports both directed and undirected modes via params.directed flag.

@group(0) @binding(0) var<storage, read> offsets: array<u32>;
@group(0) @binding(1) var<storage, read> tgts: array<u32>;
@group(0) @binding(2) var<storage, read_write> coefficients: array<f32>;
@group(0) @binding(3) var<uniform> params: LccParams;
// Binding 4: directed outgoing adjacency offsets (only used when directed=1)
@group(0) @binding(4) var<storage, read> out_offsets: array<u32>;
// Binding 5: directed outgoing adjacency tgts (only used when directed=1)
@group(0) @binding(5) var<storage, read> out_tgts: array<u32>;

struct LccParams {
    node_count: u32,
    directed: u32,
    _pad2: u32,
    _pad3: u32,
}

// Binary search for value in sorted tgts[start..end)
fn has_edge(start: u32, end: u32, tgt: u32) -> bool {
    var lo = start;
    var hi = end;
    while (lo < hi) {
        let mid = (lo + hi) / 2u;
        let mid_val = tgts[mid];
        if (mid_val == tgt) {
            return true;
        } else if (mid_val < tgt) {
            lo = mid + 1u;
        } else {
            hi = mid;
        }
    }
    return false;
}

// Binary search in outgoing adjacency (directed mode)
fn has_out_edge(start: u32, end: u32, tgt: u32) -> bool {
    var lo = start;
    var hi = end;
    while (lo < hi) {
        let mid = (lo + hi) / 2u;
        let mid_val = out_tgts[mid];
        if (mid_val == tgt) {
            return true;
        } else if (mid_val < tgt) {
            lo = mid + 1u;
        } else {
            hi = mid;
        }
    }
    return false;
}

@compute @workgroup_size(256)
fn compute_lcc(@builtin(global_invocation_id) id: vec3<u32>) {
    let node = id.x;
    if (node >= params.node_count) {
        return;
    }

    let start = offsets[node];
    let end = offsets[node + 1u];
    let degree = end - start;

    if (degree < 2u) {
        coefficients[node] = 0.0;
        return;
    }

    if (params.directed == 0u) {
        // Undirected mode: count undirected edges among neighbors, divide by d*(d-1)/2
        var triangle_edges: u32 = 0u;
        for (var i = start; i < end; i = i + 1u) {
            let ni = tgts[i];
            let ni_start = offsets[ni];
            let ni_end = offsets[ni + 1u];

            for (var j = i + 1u; j < end; j = j + 1u) {
                let nj = tgts[j];
                if (has_edge(ni_start, ni_end, nj)) {
                    triangle_edges = triangle_edges + 1u;
                }
            }
        }

        let max_edges = degree * (degree - 1u) / 2u;
        coefficients[node] = f32(triangle_edges) / f32(max_edges);
    } else {
        // Directed mode: count directed edges among neighbors using outgoing adjacency
        // For each pair (u, w) of neighbors, check if u->w exists in the outgoing adjacency
        // Divide by d*(d-1) since each directed edge counts separately
        var directed_edges: u32 = 0u;
        for (var i = start; i < end; i = i + 1u) {
            let ni = tgts[i];
            let ni_out_start = out_offsets[ni];
            let ni_out_end = out_offsets[ni + 1u];

            for (var j = start; j < end; j = j + 1u) {
                if (i == j) {
                    continue;
                }
                let nj = tgts[j];
                // Check if ni -> nj exists in outgoing adjacency
                if (has_out_edge(ni_out_start, ni_out_end, nj)) {
                    directed_edges = directed_edges + 1u;
                }
            }
        }

        let max_edges = degree * (degree - 1u);
        coefficients[node] = f32(directed_edges) / f32(max_edges);
    }
}
