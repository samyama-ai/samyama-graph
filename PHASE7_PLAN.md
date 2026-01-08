# Phase 7: Native Graph Algorithms - Implementation Plan

## Executive Summary

**Goal**: Transform Samyama from a storage engine into a **Graph Analytics Engine**.

**Value Proposition**: By running algorithms like **PageRank** and **Shortest Path** data-local (inside the DB), we avoid expensive data export/import cycles to external tools (like NetworkX or Spark). This enables real-time insights, such as re-ranking vector search results based on node centrality.

## Requirements Coverage

| Requirement | Description |
|-------------|-------------|
| **REQ-ALGO-001** | Implement **PageRank** for node centrality/importance |
| **REQ-ALGO-002** | Implement **Breadth-First Search (BFS)** for unweighted shortest path |
| **REQ-ALGO-003** | Implement **Dijkstra** for weighted shortest path |
| **REQ-ALGO-004** | Implement **Weakly Connected Components (WCC)** for community detection |
| **REQ-ALGO-005** | Expose algorithms via Cypher `CALL` procedures |
| **REQ-ALGO-006** | Write results back to graph (e.g., write PageRank score as a property) |

## Module Structure

```
src/
├── algo/                       # NEW Module
│   ├── mod.rs
│   ├── pagerank.rs             # PageRank implementation
│   ├── pathfinding.rs          # BFS / Dijkstra
│   ├── community.rs            # WCC / Louvain
│   └── common.rs               # Shared utilities (GraphView projection)
├── query/
│   ├── executor/
│       ├── operator.rs         # Update: Add AlgorithmOperator
```

## Implementation Roadmap (4 Weeks)

### Week 1: Foundation & PageRank
**Goal**: Calculate node importance.

1.  **Topology Projection**: The current `HashMap` storage is slow for global iteration. Implement a lightweight `GraphView` (CSR-like) that projects the graph into `Vec<Vec<usize>>` for fast algorithm execution.
2.  **PageRank Implementation**:
    *   Iterative power method.
    *   Configurable damping factor (0.85) and iterations (20).
    *   Output: `HashMap<NodeId, f64>`.
3.  **Write-Back**: Capability to write scores back as node properties (`n.pagerank`).

### Week 2: Pathfinding (BFS & Dijkstra)
**Goal**: Find optimal routes.

1.  **BFS**: Find shortest path (number of hops).
2.  **Dijkstra**: Find shortest path based on edge weight property (e.g., `distance`, `cost`).
3.  **Result Format**: Return path as a list of Nodes and Edges.

### Week 3: Community Detection
**Goal**: Find clusters.

1.  **Weakly Connected Components (WCC)**: Union-Find based implementation to identify disjoint subgraphs.
2.  **Use Case**: Identify isolated data islands.

### Week 4: Cypher Integration & Testing
**Goal**: Expose to users.

1.  **Procedures**:
    *   `CALL algo.pageRank(label, edgeType, {iterations: 20})`
    *   `CALL algo.shortestPath(source, target, {weight: 'cost'})`
2.  **Testing**: Verify correctness against known small graphs.
3.  **Performance**: Benchmark on 100k node graph.

## API Design

### Rust API
```rust
// PageRank
let scores = samyama::algo::page_rank(
    &store,
    Some("Person"), // Node Label filter
    Some("KNOWS"),  // Edge Type filter
    0.85,           // Damping
    20              // Iterations
)?;

// Shortest Path
let path = samyama::algo::shortest_path(
    &store,
    start_node_id,
    end_node_id,
    Some("cost")    // Weight property
)?;
```

### Cypher API
```cypher
// Calculate PageRank and stream results
CALL algo.pageRank('Person', 'KNOWS') 
YIELD node, score 
RETURN node.name, score 
ORDER BY score DESC LIMIT 10

// Calculate Shortest Path
MATCH (a:City {name: 'New York'}), (b:City {name: 'London'})
CALL algo.shortestPath(a, b, {weight: 'distance'}) 
YIELD path, cost
RETURN path, cost
```

## Risks & Mitigation
1.  **Memory Usage**: Graph algorithms often require loading the whole topology into RAM.
    *   *Mitigation*: We will use `NodeId` (u64) mappings to dense `usize` indices to minimize memory footprint during computation.
2.  **Blocking**: Running PageRank on a large graph can block the main thread.
    *   *Mitigation*: For MVP, we run synchronously. Future versions should spawn a background thread/task.

## Team & Resources
- **Engineers**: 1 Backend Engineer.
- **Duration**: 4 Weeks.

---
**Status**: Planned
**Version**: 1.0
