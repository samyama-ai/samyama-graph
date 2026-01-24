# Samyama Graph Algorithms

This crate implements standard graph algorithms for the [Samyama Graph Database](https://github.com/samyama-ai/samyama-graph).

## Algorithms

- **PageRank**: Node centrality scoring.
- **Weakly Connected Components (WCC)**: Find disjoint subgraphs.
- **BFS**: Unweighted shortest path.
- **Dijkstra**: Weighted shortest path.

## Usage

These algorithms operate on a `GraphView`, which is a lightweight, read-only topology view of the graph.

```rust
use samyama_graph_algorithms::{GraphView, page_rank, PageRankConfig};

// Construct view (usually done by samyama-graph adapter)
// let view = ...; 

// Run PageRank
let scores = page_rank(&view, PageRankConfig::default());
```
