# Phase 7: Native Graph Algorithms - Implementation Summary

## Status: Planning 📝

**Start Date**: TBD
**Est. Duration**: 4 Weeks

## Overview

Phase 7 adds **Graph Analytics** capabilities to Samyama. Unlike transactional queries (OLTP) which touch a few nodes, these algorithms (OLAP) traverse the entire graph structure to derive global insights like centrality, community structure, and optimal paths.

## Core Features

1.  **PageRank**: Node importance ranking.
2.  **Shortest Path**: BFS (unweighted) and Dijkstra (weighted).
3.  **WCC**: Weakly Connected Components for community detection.
4.  **Cypher Procedures**: `CALL algo.*` integration.

## Implementation Checklist

### Week 1: Foundation & PageRank
- [ ] Create `src/algo` module
- [ ] Implement `GraphView` projection (HashMap -> Adjacency List)
- [ ] Implement PageRank algorithm
- [ ] Add unit tests for PageRank

### Week 2: Pathfinding
- [ ] Implement BFS
- [ ] Implement Dijkstra
- [ ] Add unit tests for pathfinding

### Week 3: Community Detection
- [ ] Implement Weakly Connected Components (WCC)
- [ ] Add unit tests for WCC

### Week 4: Cypher Integration
- [ ] Register `algo.pageRank` procedure
- [ ] Register `algo.shortestPath` procedure
- [ ] Register `algo.wcc` procedure
- [ ] End-to-end integration tests

## Dependencies

- No new external dependencies required (Pure Rust implementation).

## Architecture Notes

- **Graph Projection**: Algorithms run on a compact `Vec<Vec<usize>>` representation of the graph rather than iterating the `GraphStore` directly. This dramatically improves cache locality and iteration speed.
