//! CUDA kernel source strings (translated from WGSL shaders)
//!
//! Each kernel is a CUDA C string compiled at runtime via NVRTC.
//! The translation preserves the same algorithms and data layouts as the WGSL versions.

/// PageRank iteration kernel — one thread per node
pub const PAGERANK: &str = r#"
extern "C" __global__ void pagerank_iter(
    const unsigned int* __restrict__ in_offsets,
    const unsigned int* __restrict__ in_sources,
    const unsigned int* __restrict__ out_degrees,
    const float* __restrict__ scores,
    float* __restrict__ next_scores,
    unsigned int node_count,
    float damping,
    float base_score
) {
    unsigned int node = blockIdx.x * blockDim.x + threadIdx.x;
    if (node >= node_count) return;

    unsigned int start = in_offsets[node];
    unsigned int end = in_offsets[node + 1];

    float sum = 0.0f;
    for (unsigned int i = start; i < end; i++) {
        unsigned int src = in_sources[i];
        unsigned int deg = out_degrees[src];
        if (deg > 0) {
            sum += scores[src] / (float)deg;
        }
    }

    next_scores[node] = base_score + damping * sum;
}
"#;

/// Triangle counting kernel — one thread per edge, merge-intersection
pub const TRIANGLES: &str = r#"
extern "C" __global__ void count_triangles(
    const unsigned int* __restrict__ edge_src,
    const unsigned int* __restrict__ edge_dst,
    const unsigned int* __restrict__ offsets,
    const unsigned int* __restrict__ targets,
    unsigned int* __restrict__ triangle_count,
    unsigned int edge_count
) {
    unsigned int edge_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (edge_idx >= edge_count) return;

    unsigned int u = edge_src[edge_idx];
    unsigned int v = edge_dst[edge_idx];
    if (u >= v) return;

    unsigned int u_start = offsets[u];
    unsigned int u_end = offsets[u + 1];
    unsigned int v_start = offsets[v];
    unsigned int v_end = offsets[v + 1];

    unsigned int ui = u_start;
    unsigned int vi = v_start;
    while (ui < u_end && vi < v_end) {
        unsigned int u_neighbor = targets[ui];
        unsigned int v_neighbor = targets[vi];
        if (u_neighbor == v_neighbor) {
            if (u_neighbor > v) {
                atomicAdd(triangle_count, 1u);
            }
            ui++;
            vi++;
        } else if (u_neighbor < v_neighbor) {
            ui++;
        } else {
            vi++;
        }
    }
}
"#;

/// Batch cosine distance — one thread per candidate vector
pub const COSINE_DISTANCE: &str = r#"
extern "C" __global__ void cosine_batch(
    const float* __restrict__ query,
    const float* __restrict__ candidates,
    float* __restrict__ distances,
    unsigned int dimensions,
    unsigned int candidate_count
) {
    unsigned int k = blockIdx.x * blockDim.x + threadIdx.x;
    if (k >= candidate_count) return;

    unsigned int offset = k * dimensions;
    float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;

    for (unsigned int i = 0; i < dimensions; i++) {
        float a = query[i];
        float b = candidates[offset + i];
        dot += a * b;
        norm_a += a * a;
        norm_b += b * b;
    }

    if (norm_a <= 0.0f || norm_b <= 0.0f) {
        distances[k] = 1.0f;
    } else {
        distances[k] = 1.0f - dot / (sqrtf(norm_a) * sqrtf(norm_b));
    }
}
"#;

/// Batch inner product distance — one thread per candidate
pub const INNER_PRODUCT: &str = r#"
extern "C" __global__ void inner_product_batch(
    const float* __restrict__ query,
    const float* __restrict__ candidates,
    float* __restrict__ distances,
    unsigned int dimensions,
    unsigned int candidate_count
) {
    unsigned int k = blockIdx.x * blockDim.x + threadIdx.x;
    if (k >= candidate_count) return;

    unsigned int offset = k * dimensions;
    float dot = 0.0f;

    for (unsigned int i = 0; i < dimensions; i++) {
        dot += query[i] * candidates[offset + i];
    }

    distances[k] = 1.0f - dot;
}
"#;

/// Parallel reduction SUM kernel — shared memory tree reduction
pub const REDUCE_SUM: &str = r#"
extern "C" __global__ void reduce_sum(
    const float* __restrict__ input,
    float* __restrict__ output,
    unsigned int count
) {
    __shared__ float shared_data[256];

    unsigned int tid = threadIdx.x;
    unsigned int gid = blockIdx.x * blockDim.x + threadIdx.x;

    shared_data[tid] = (gid < count) ? input[gid] : 0.0f;
    __syncthreads();

    for (unsigned int stride = 128; stride > 0; stride >>= 1) {
        if (tid < stride) {
            shared_data[tid] += shared_data[tid + stride];
        }
        __syncthreads();
    }

    if (tid == 0) {
        output[blockIdx.x] = shared_data[0];
    }
}
"#;

/// Bitonic sort step kernel — one substage per dispatch
pub const BITONIC_SORT: &str = r#"
extern "C" __global__ void bitonic_step(
    float* __restrict__ keys,
    unsigned int* __restrict__ indices,
    unsigned int count,
    unsigned int stage,
    unsigned int substage
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;

    unsigned int ixj = i ^ substage;
    if (ixj <= i || ixj >= count) return;

    bool ascending = ((i & stage) == 0);
    float key_i = keys[i];
    float key_j = keys[ixj];

    bool should_swap = (ascending && key_i > key_j) || (!ascending && key_i < key_j);
    if (should_swap) {
        keys[i] = key_j;
        keys[ixj] = key_i;
        unsigned int idx_i = indices[i];
        unsigned int idx_j = indices[ixj];
        indices[i] = idx_j;
        indices[ixj] = idx_i;
    }
}
"#;

/// CDLP (Community Detection via Label Propagation) — one thread per node
pub const CDLP: &str = r#"
extern "C" __global__ void cdlp_iter(
    const unsigned int* __restrict__ offsets,
    const unsigned int* __restrict__ targets,
    const unsigned int* __restrict__ labels_in,
    unsigned int* __restrict__ labels_out,
    unsigned int node_count
) {
    unsigned int node = blockIdx.x * blockDim.x + threadIdx.x;
    if (node >= node_count) return;

    unsigned int start = offsets[node];
    unsigned int end = offsets[node + 1];
    unsigned int degree = end - start;

    if (degree == 0) {
        labels_out[node] = labels_in[node];
        return;
    }

    // Find most frequent label among neighbors (tie-break: smallest label)
    unsigned int best_label = labels_in[node];
    unsigned int best_count = 0;

    for (unsigned int i = start; i < end; i++) {
        unsigned int candidate = labels_in[targets[i]];
        unsigned int cnt = 0;
        for (unsigned int j = start; j < end; j++) {
            if (labels_in[targets[j]] == candidate) cnt++;
        }
        if (cnt > best_count || (cnt == best_count && candidate < best_label)) {
            best_count = cnt;
            best_label = candidate;
        }
    }

    labels_out[node] = best_label;
}
"#;

/// LCC (Local Clustering Coefficient) — one thread per node
pub const LCC: &str = r#"
__device__ bool binary_search_adj(
    const unsigned int* __restrict__ targets,
    unsigned int start, unsigned int end, unsigned int value
) {
    unsigned int lo = start, hi = end;
    while (lo < hi) {
        unsigned int mid = (lo + hi) / 2;
        unsigned int mid_val = targets[mid];
        if (mid_val == value) return true;
        else if (mid_val < value) lo = mid + 1;
        else hi = mid;
    }
    return false;
}

extern "C" __global__ void lcc_compute(
    const unsigned int* __restrict__ offsets,
    const unsigned int* __restrict__ targets,
    float* __restrict__ coefficients,
    unsigned int node_count,
    unsigned int directed  // 0 = undirected, 1 = directed
) {
    unsigned int node = blockIdx.x * blockDim.x + threadIdx.x;
    if (node >= node_count) return;

    unsigned int start = offsets[node];
    unsigned int end = offsets[node + 1];
    unsigned int degree = end - start;

    if (degree < 2) {
        coefficients[node] = 0.0f;
        return;
    }

    unsigned int triangle_edges = 0;
    for (unsigned int i = start; i < end; i++) {
        unsigned int neighbor = targets[i];
        unsigned int n_start = offsets[neighbor];
        unsigned int n_end = offsets[neighbor + 1];
        for (unsigned int j = i + 1; j < end; j++) {
            unsigned int other = targets[j];
            if (binary_search_adj(targets, n_start, n_end, other)) {
                triangle_edges++;
            }
        }
    }

    float denom = (directed == 0)
        ? (float)(degree * (degree - 1)) / 2.0f
        : (float)(degree * (degree - 1));

    coefficients[node] = (denom > 0.0f) ? (float)triangle_edges / denom : 0.0f;
}
"#;
