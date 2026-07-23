// PageRank GPU kernel
//
// One thread per node. Each thread computes the PageRank score for its node
// by summing contributions from incoming neighbors.

@group(0) @binding(0) var<storage, read> in_offsets: array<u32>;
@group(0) @binding(1) var<storage, read> in_sources: array<u32>;
@group(0) @binding(2) var<storage, read> out_degrees: array<u32>;
@group(0) @binding(3) var<storage, read> scores: array<f32>;
@group(0) @binding(4) var<storage, read_write> next_scores: array<f32>;
@group(0) @binding(5) var<uniform> params: PagerankParams;

struct PagerankParams {
    node_count: u32,
    damping: f32,
    base_score: f32,
    _padding: u32,
}

@compute @workgroup_size(256)
fn pagerank_iter(@builtin(global_invocation_id) id: vec3<u32>) {
    let node = id.x;
    if (node >= params.node_count) {
        return;
    }

    let start = in_offsets[node];
    let end = in_offsets[node + 1u];

    var sum: f32 = 0.0;
    for (var i = start; i < end; i = i + 1u) {
        let src = in_sources[i];
        let deg = out_degrees[src];
        if (deg > 0u) {
            sum = sum + scores[src] / f32(deg);
        }
    }

    next_scores[node] = params.base_score + params.damping * sum;
}
