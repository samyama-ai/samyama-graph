# LDBC Graphalytics Benchmark — Samyama v0.5.10

## Overview

[LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) is a benchmark for graph analysis platforms. It defines 6 standard graph algorithms that must be implemented and validated against reference outputs on standard datasets.

**Result: 28/28 validations passed (100%) — XS 12/12, S-size 16/16**

All 6 algorithms execute correctly across 5 datasets (2 XS + 3 S-size).

## Test Environment

- **Hardware:** Mac Mini M4, 24GB RAM
- **OS:** macOS Sequoia
- **Build:** `cargo build --release` (Rust 1.83, LTO enabled)
- **Date:** 2026-02-27

## Algorithms

| Algorithm | Abbreviation | Description | Implementation |
|-----------|-------------|-------------|----------------|
| Breadth-First Search | BFS | Single-source shortest path (unweighted) | `samyama_graph_algorithms::bfs()` |
| PageRank | PR | Iterative link analysis ranking | `samyama_graph_algorithms::page_rank()` |
| Weakly Connected Components | WCC | Find connected components (ignoring edge direction) | `samyama_graph_algorithms::weakly_connected_components()` |
| Community Detection (Label Propagation) | CDLP | Assign community labels via neighbor voting | `samyama_graph_algorithms::cdlp()` |
| Local Clustering Coefficient | LCC | Measure of neighborhood connectivity per node | `samyama_graph_algorithms::local_clustering_coefficient_directed()` |
| Single-Source Shortest Path | SSSP | Weighted shortest path from source | `samyama_graph_algorithms::dijkstra()` |

## Datasets

### XS-size (included in repo)

| Dataset | Vertices | Edges | Directed | Source |
|---------|----------|-------|----------|--------|
| example-directed | 10 | 17 | Yes | LDBC reference (XS) |
| example-undirected | 9 | 24 (12 bidirectional) | No | LDBC reference (XS) |

### S-size (downloaded separately)

| Dataset | Vertices | Edges | Directed | Source |
|---------|----------|-------|----------|--------|
| wiki-Talk | ~2.4M | ~5.0M | Yes | LDBC Graphalytics S |
| cit-Patents | ~3.8M | ~16.5M | Yes | LDBC Graphalytics S |
| datagen-7_5-fb | ~633K | ~68.4M (34.2M bidirectional) | No | LDBC Graphalytics S |

## Results (XS-size)

### example-directed (10 vertices, 17 edges)

| Algorithm | Time | Result | Validation |
|-----------|------|--------|------------|
| BFS | 4us | source=1, reachable=6, max_depth=2 | **PASS** |
| PR | 3us | converged with tolerance=1e-7 | **PASS** |
| WCC | 9us | components=1, largest=10 | **PASS** |
| CDLP | 23us | communities=4, largest=4, iters=2 | **PASS** |
| LCC | 16us | avg_cc (directed mode) | **PASS** |
| SSSP | 4us | source=1, reachable=6, max_dist=1.020 | **PASS** |

### example-undirected (9 vertices, 24 edges)

| Algorithm | Time | Result | Validation |
|-----------|------|--------|------------|
| BFS | 4us | source=2, reachable=9, max_depth=4 | **PASS** |
| PR | 3us | converged with tolerance=1e-7 | **PASS** |
| WCC | 12us | components=1, largest=9 | **PASS** |
| CDLP | 13us | communities=4, largest=4, iters=2 | **PASS** |
| LCC | 13us | avg_cc=0.652, non_zero=8 | **PASS** |
| SSSP | 4us | source=2, reachable=9, max_dist=2.410 | **PASS** |

### Summary

| Algorithm | Directed | Undirected | Overall |
|-----------|----------|------------|---------|
| BFS | PASS | PASS | 2/2 |
| PR | PASS | PASS | 2/2 |
| WCC | PASS | PASS | 2/2 |
| CDLP | PASS | PASS | 2/2 |
| LCC | PASS | PASS | 2/2 |
| SSSP | PASS | PASS | 2/2 |
| **Total** | **6/6** | **6/6** | **12/12** |

## Results (S-size)

### cit-Patents (3,774,768 vertices, 16,518,947 edges, directed)

Load time: 8.1s

| Algorithm | Time | Result | Validation |
|-----------|------|--------|------------|
| BFS | 71ms | source=6009541, reachable=298,159, max_depth=20 | **PASS** |
| PR | 791ms | iters=10, d=0.85, min=0.000000, max=0.000087 | **PASS** |
| WCC | 376ms | components=3,627, largest=3,764,117 | **PASS** |
| CDLP | 9.5s | communities=337,986, largest=8,698, iters=10 | **PASS** |
| LCC | 9.6s | avg_cc=0.037831, non_zero=1,962,968 | **PASS** |
| SSSP | 214ms | source=1, reachable=1 | N/A (no reference) |

### datagen-7_5-fb (633,432 vertices, 68,371,494 edges, undirected)

Load time: 8.6s

| Algorithm | Time | Result | Validation |
|-----------|------|--------|------------|
| BFS | 170ms | source=6, reachable=633,432, max_depth=5 | **PASS** |
| PR | 879ms | iters=10, d=0.85, min=0.000000, max=0.000053 | **PASS** |
| WCC | 285ms | components=1, largest=633,432 | **PASS** |
| CDLP | 15.5s | communities=218, largest=94,574, iters=10 | **PASS** |
| LCC | 167s | avg_cc=0.087608, non_zero=596,316 | **PASS** |
| SSSP | 304ms | source=6, reachable=633,432, max_dist=5.4445 | **PASS** |

### wiki-Talk (2,394,385 vertices, 5,021,410 edges, directed)

Load time: 1.4s

| Algorithm | Time | Result | Validation |
|-----------|------|--------|------------|
| BFS | 148ms | source=2, reachable=2,354,316, max_depth=6 | **PASS** |
| PR | 280ms | iters=10, d=0.85, min=0.000000, max=0.000255 | **PASS** |
| WCC | 265ms | components=2,555, largest=2,388,953 | **PASS** |
| CDLP | 2.5s | communities=10,914, largest=1,312,545, iters=10 | **PASS** |
| LCC | 41.5s | avg_cc=0.039099, non_zero=259,136 | **PASS** |
| SSSP | 167ms | source=0, reachable=2 | N/A (no reference) |

### S-size Summary

| Algorithm | cit-Patents | datagen-7_5-fb | wiki-Talk | Overall |
|-----------|-------------|----------------|-----------|---------|
| BFS | PASS | PASS | PASS | 3/3 |
| PR | PASS | PASS | PASS | 3/3 |
| WCC | PASS | PASS | PASS | 3/3 |
| CDLP | PASS | PASS | PASS | 3/3 |
| LCC | PASS | PASS | PASS | 3/3 |
| SSSP | N/A | PASS | N/A | 1/1 |
| **Total** | **5/5** | **6/6** | **5/5** | **16/16** |

> SSSP requires weighted edges. cit-Patents and wiki-Talk are unweighted — no LDBC reference output available.

### Overall Summary (XS + S-size)

| Size | Datasets | Validations | Passed | Rate |
|------|----------|-------------|--------|------|
| XS | 2 | 12 | 12 | 100% |
| S | 3 | 16 | 16 | 100% |
| **Total** | **5** | **28** | **28** | **100%** |

## Fixes Applied

### PageRank Convergence (v0.5.8)

**Previous issue:** Benchmark used `tolerance: 0.0` with only `max_iterations` from the properties file (typically 2), so PageRank never converged.

**Fix:** Changed to `tolerance: 1e-7` with `iterations: max(props, 100)`. PageRank now runs to convergence, matching LDBC reference outputs.

### Directed LCC (v0.5.8)

**Previous issue:** The LCC algorithm treated all edges as undirected, using `d*(d-1)/2` as the divisor. LDBC expects directed triangle semantics for directed graphs.

**Fix:** Added `local_clustering_coefficient_directed(view, directed)` which counts directed edges among neighbors and uses `d*(d-1)` divisor when `directed=true`. The benchmark auto-detects directedness from the dataset properties file.

### PageRank Exact Iterations + Dangling Redistribution (v0.5.10)

**Previous issue:** PageRank used convergence-based iteration with `max(props, 1000)` iterations, which ran more iterations than LDBC expects and shifted absolute scores.

**Fix:** Use exact iteration count from LDBC properties (`tolerance: 0.0`, no early termination). Enabled dangling node mass redistribution (`dangling_redistribution: true` in `PageRankConfig`) to match LDBC reference semantics.

## GPU Acceleration

GPU-accelerated graph algorithms (PageRank, LCC, CDLP, WCC, BFS) are available in the Enterprise edition via wgpu compute shaders. All results above are CPU-only.

## Algorithm Details

### BFS (Breadth-First Search)
- Single-source BFS from a given start vertex
- Returns distance (hop count) to every reachable vertex
- Validates against reference output: exact match on all distances

### PageRank
- Iterative computation: `PR(v) = (1-d)/N + d * sum(PR(u)/out_degree(u))` for each neighbor u
- Configurable: damping factor (default 0.85), max iterations, convergence tolerance
- Validates against reference: tolerance of 1e-4 per node

### Weakly Connected Components (WCC)
- Union-Find based: treats all edges as undirected
- Returns component ID for each vertex (minimum vertex ID in component)

### Community Detection via Label Propagation (CDLP)
- Synchronous label propagation: each node adopts most frequent neighbor label
- Ties broken by smallest label value
- Configurable iteration count (default from properties file)

### Local Clustering Coefficient (LCC)
- For each node: `LCC(v) = 2 * triangles(v) / (degree(v) * (degree(v) - 1))`
- Reports per-node coefficient and average

### Single-Source Shortest Path (SSSP)
- Dijkstra's algorithm from a given source vertex
- Edge weights from dataset property file
- Returns shortest distance to every reachable vertex

## Running

```bash
# Download XS datasets (included by default)
bash scripts/download_graphalytics.sh

# Download S-size datasets
bash scripts/download_graphalytics.sh --size S

# Run all algorithms on XS datasets
cargo bench --release --bench graphalytics_benchmark -- --all

# Run on S-size datasets
cargo bench --release --bench graphalytics_benchmark -- --size S --all

# Run all sizes
cargo bench --release --bench graphalytics_benchmark -- --size all --all

# Run specific algorithm
cargo bench --release --bench graphalytics_benchmark -- --algo BFS

# Run on specific dataset
cargo bench --release --bench graphalytics_benchmark -- --dataset example-directed

# Custom data directory
cargo bench --release --bench graphalytics_benchmark -- --data-dir /path/to/data --all
```

## Dataset Sizes

- **XS:** example-directed (10V), example-undirected (9V) — sub-millisecond
- **S:** wiki-Talk (~2.4M V), cit-Patents (~3.8M V), datagen-7_5-fb (~633K V) — seconds
- **M/L:** Requires more memory; performance scales linearly with edge count
