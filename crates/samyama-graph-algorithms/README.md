# Samyama Graph Algorithms

A high-performance, Rust-native library for graph analytics and topology analysis. Optimized for the Samyama Graph Database but usable as a standalone crate.

## Features

This library implements standard algorithms optimized for `GraphView` (a compact Adjacency List projection).

### 🔍 Pathfinding
- **BFS (Breadth-First Search)**: Find the shortest path in unweighted graphs.
- **Dijkstra**: Find the shortest path in weighted graphs.

### 🌐 Centrality & Community
- **PageRank**: Measure node importance/influence.
- **Weakly Connected Components (WCC)**: Find disjoint subgraphs.
- **Strongly Connected Components (SCC)**: Find cycles and strongly connected clusters using Tarjan's algorithm.

### ⚡ Flow & Topology
- **Max Flow (Edmonds-Karp)**: Calculate the maximum flow capacity between source and sink nodes.
- **Minimum Spanning Tree (Prim's)**: Find the MST of a weighted graph.
- **Triangle Counting**: Analyze graph density and clustering coefficients.

## Usage

### 1. Define a Graph View
The library operates on a `GraphView`, which is a read-only projection of your graph data.

```rust
use samyama_graph_algorithms::common::{GraphView, NodeId};
use std::collections::HashMap;

// Manually construct a view (or build from your storage engine)
let view = GraphView {
    node_count: 3,
    index_to_node: vec![10, 20, 30], // Map index 0->ID 10, 1->20...
    node_to_index: HashMap::from([(10, 0), (20, 1), (30, 2)]),
    outgoing: vec![
        vec![1],    // Node 0 connects to Node 1
        vec![2],    // Node 1 connects to Node 2
        vec![],     // Node 2 has no outgoing edges
    ],
    incoming: vec![/* ... */],
    weights: Some(vec![
        vec![1.0],  // Weight 0->1
        vec![2.5],  // Weight 1->2
        vec![],
    ]),
};
```

### 2. Run Algorithms

**PageRank**:
```rust
use samyama_graph_algorithms::pagerank::{page_rank, PageRankConfig};

let config = PageRankConfig::default(); // damping: 0.85, iter: 20
let scores = page_rank(&view, config);

for (node_id, score) in scores {
    println!("Node {}: {}", node_id, score);
}
```

**Shortest Path**:
```rust
use samyama_graph_algorithms::pathfinding::dijkstra;

// Find shortest path from ID 10 to ID 30
if let Some(result) = dijkstra(&view, 10, 30) {
    println!("Cost: {}", result.cost);
    println!("Path: {:?}", result.path);
}
```

**Max Flow**:
```rust
use samyama_graph_algorithms::flow::edmonds_karp;

// Calculate max flow from Source(10) to Sink(30)
if let Some(flow) = edmonds_karp(&view, 10, 30) {
    println!("Max Flow: {}", flow.max_flow);
}
```

## Integration with Samyama
This crate is the backend for `algo.*` Cypher procedures in Samyama.

| Algorithm | Cypher Procedure |
|-----------|------------------|
| PageRank | `CALL algo.pageRank('Label', 'REL')` |
| WCC | `CALL algo.wcc()` |
| SCC | `CALL algo.scc()` |
| Max Flow | `CALL algo.maxFlow(source, sink, 'capacity')` |
| MST | `CALL algo.mst('weight')` |
| Triangle Count | `CALL algo.triangleCount()` |

## GPU acceleration and CPU fallback

PageRank, CDLP, LCC and triangle count have a GPU path, compiled only with
`--features gpu` (ADR-025). The CPU path is the source of truth, and no call
fails because a GPU is missing: it runs on the CPU instead, and says why on
the `samyama::gpu` log target (HW-10):

| Reason | Level | Logged |
|---|---|---|
| Built without the `gpu` feature (the default build) | `info` | once per algorithm |
| GPU build, no CUDA or wgpu device available | `warn` | once per algorithm |
| Graph at or below the GPU threshold (`SAMYAMA_GPU_MIN_NODES`) | `debug` | every call |
| An option the GPU kernel does not implement (PageRank's dangling redistribution) | `debug` | every call |
| The GPU kernel returned an error: device busy, lost or failed | `warn` | every call |

"Once" means once recorded. A call made before a subscriber is installed, or
while `samyama::gpu` is filtered out, does not use up the report, so the
first call after logging is enabled still says it.

The threshold in a "graph at or below" line is the one dispatch applied. Before
the GPU runtime is first initialised the backend is unknown, so a graph at or
below the smallest threshold of any backend stays on the CPU without
initialising it, and the line names that smallest threshold (or
`SAMYAMA_GPU_MIN_NODES`, when set). Once the runtime
is initialised, the line names the active backend's own threshold: on Metal
that is 1,000,000 nodes, not 1,000.

A line reads, for example:

```text
PageRank ran on the CPU, not a GPU: this build has no GPU support (built without the `gpu` feature) (reported once per process)
```

To see the per-call reasons, raise the level for that target, e.g.
`RUST_LOG=samyama::gpu=debug`. The reasons themselves are
`cpu_fallback::CpuFallbackReason`; `gpu_dispatch::gpu_route` returns one.

## License
Apache-2.0