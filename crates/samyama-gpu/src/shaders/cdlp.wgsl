// CDLP (Community Detection via Label Propagation) GPU kernel
//
// One thread per node. Each thread reads neighbor labels, finds the most
// frequent label (ties broken by smallest label), and writes it.

@group(0) @binding(0) var<storage, read> out_offsets: array<u32>;
@group(0) @binding(1) var<storage, read> out_targets: array<u32>;
@group(0) @binding(2) var<storage, read> in_offsets: array<u32>;
@group(0) @binding(3) var<storage, read> in_sources: array<u32>;
@group(0) @binding(4) var<storage, read> labels: array<u32>;
@group(0) @binding(5) var<storage, read_write> new_labels: array<u32>;
@group(0) @binding(6) var<uniform> params: CdlpParams;

struct CdlpParams {
    node_count: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

// Simple selection sort for small arrays (in registers)
// We collect unique labels and their counts, then pick the best.
// For nodes with degree <= MAX_DEGREE, this works on GPU.
// Higher-degree nodes should be handled by CPU fallback.
const MAX_DEGREE: u32 = 512u;

@compute @workgroup_size(256)
fn cdlp_iter(@builtin(global_invocation_id) id: vec3<u32>) {
    let node = id.x;
    if (node >= params.node_count) {
        return;
    }

    // Collect neighbor labels and count frequencies
    // We use a simple approach: track unique labels and counts
    let out_start = out_offsets[node];
    let out_end = out_offsets[node + 1u];
    let in_start = in_offsets[node];
    let in_end = in_offsets[node + 1u];

    let total_neighbors = (out_end - out_start) + (in_end - in_start);

    if (total_neighbors == 0u) {
        new_labels[node] = labels[node];
        return;
    }

    // Track best label found so far
    var best_label: u32 = 0xFFFFFFFFu;
    var best_count: u32 = 0u;

    // For each unique label among neighbors, count occurrences
    // This is O(degree^2) but works well for typical graph degrees
    // Process outgoing neighbors
    for (var i = out_start; i < out_end; i = i + 1u) {
        let candidate = labels[out_targets[i]];
        var count: u32 = 0u;

        // Count this label in outgoing
        for (var j = out_start; j < out_end; j = j + 1u) {
            if (labels[out_targets[j]] == candidate) {
                count = count + 1u;
            }
        }
        // Count this label in incoming
        for (var j = in_start; j < in_end; j = j + 1u) {
            if (labels[in_sources[j]] == candidate) {
                count = count + 1u;
            }
        }

        if (count > best_count || (count == best_count && candidate < best_label)) {
            best_count = count;
            best_label = candidate;
        }
    }

    // Process incoming neighbors (may have labels not seen in outgoing)
    for (var i = in_start; i < in_end; i = i + 1u) {
        let candidate = labels[in_sources[i]];
        var count: u32 = 0u;

        for (var j = out_start; j < out_end; j = j + 1u) {
            if (labels[out_targets[j]] == candidate) {
                count = count + 1u;
            }
        }
        for (var j = in_start; j < in_end; j = j + 1u) {
            if (labels[in_sources[j]] == candidate) {
                count = count + 1u;
            }
        }

        if (count > best_count || (count == best_count && candidate < best_label)) {
            best_count = count;
            best_label = candidate;
        }
    }

    new_labels[node] = best_label;
}
