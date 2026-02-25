# LDBC Benchmark Results — Samyama v0.5.8

Samyama's query engine benchmarked against all four [LDBC Council](https://ldbcouncil.org/) benchmark suites.

**Test Environment:** Mac Mini M2 Pro (16GB RAM), macOS Sonoma, Rust 1.83 release build

**Date:** 2026-02-25

## Summary

| Benchmark | Queries | Passed | Pass Rate | Dataset | Total Time |
|-----------|---------|--------|-----------|---------|------------|
| [SNB Interactive](./SNB_INTERACTIVE.md) | 21 reads + 8 updates | 21/21 reads | **100%** | SF1 (3.18M nodes, 17.26M edges) | 108.1s |
| [SNB Business Intelligence](./SNB_BI.md) | 20 | 5/6 run (BI-7+ timeout) | **83%** (partial) | SF1 (same dataset) | ~42s (6 queries) |
| [Graphalytics](./GRAPHALYTICS.md) | 12 (6 algos x 2 datasets) | 9/12 | **75%** | example-directed (10V, 17E), example-undirected (9V, 24E) |  <1ms |
| [FinBench](./FINBENCH.md) | 21 (12 CR + 6 SR + 3 RW) | 21/21 | **100%** | Synthetic (7.7K nodes, 42.2K edges) | 371ms |

### Overall Coverage

- **4 LDBC benchmark suites** implemented
- **74 unique query/algorithm implementations** across all suites
- **56/60 passing** where fully executed (93%)

## Quick Start

```bash
# SNB Interactive (21 read queries, SF1 dataset required)
cargo run --release --example ldbc_benchmark -- --runs 3

# SNB Interactive with update operations
cargo run --release --example ldbc_benchmark -- --runs 3 --updates

# SNB Business Intelligence (20 analytical queries)
cargo run --release --example ldbc_bi_benchmark -- --runs 3

# Graphalytics (6 algorithms)
bash scripts/download_graphalytics.sh
cargo run --release --example graphalytics_benchmark -- --all

# FinBench (21 queries, synthetic data auto-generated)
cargo run --release --example finbench_benchmark -- --runs 3
```

## Detailed Reports

- [SNB Interactive — Full Results](./SNB_INTERACTIVE.md)
- [SNB Business Intelligence — Full Results](./SNB_BI.md)
- [Graphalytics — Full Results](./GRAPHALYTICS.md)
- [FinBench — Full Results](./FINBENCH.md)

## Benchmark Architecture

All benchmarks share a common pattern:

1. **Load** dataset into in-memory `GraphStore` via `EmbeddedClient`
2. **Warm up** each query once (populates AST cache)
3. **Benchmark** each query N times, recording min/median/max latency
4. **Report** results in a formatted table

Data loading uses the shared `ldbc_common` module for CSV parsing and graph population. Queries use `query_readonly()` (read lock) for reads and `query()` (write lock) for updates.
